import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  DEFAULT_SEND_GAP_MS,
  MAX_RATE_LIMIT_RETRIES,
  enqueueSend,
  extractRateLimitMs,
} from "./send-queue.js";

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("extractRateLimitMs", () => {
  it("extracts retry_after_ms from SDK-style error", () => {
    const err = { statusCode: 429, body: { errcode: "M_LIMIT_EXCEEDED", retry_after_ms: 1120 } };
    expect(extractRateLimitMs(err)).toBe(1120);
  });

  it("extracts retry_after_ms from flat error", () => {
    const err = { errcode: "M_LIMIT_EXCEEDED", retry_after_ms: 500 };
    expect(extractRateLimitMs(err)).toBe(500);
  });

  it("returns default when retry_after_ms missing", () => {
    const err = { statusCode: 429, body: { errcode: "M_LIMIT_EXCEEDED" } };
    expect(extractRateLimitMs(err)).toBe(2000);
  });

  it("clamps excessive retry_after_ms to max", () => {
    const err = { statusCode: 429, body: { errcode: "M_LIMIT_EXCEEDED", retry_after_ms: 120_000 } };
    expect(extractRateLimitMs(err)).toBe(30_000);
  });

  it("returns null for non-rate-limit errors", () => {
    expect(extractRateLimitMs(new Error("network"))).toBeNull();
    expect(extractRateLimitMs(null)).toBeNull();
    expect(extractRateLimitMs({ statusCode: 404, body: { errcode: "M_NOT_FOUND" } })).toBeNull();
  });
});

describe("enqueueSend", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("serializes sends per room", async () => {
    const gate = deferred<void>();
    const events: string[] = [];

    const first = enqueueSend("!room:example.org", async () => {
      events.push("start1");
      await gate.promise;
      events.push("end1");
      return "one";
    });
    const second = enqueueSend("!room:example.org", async () => {
      events.push("start2");
      events.push("end2");
      return "two";
    });

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    expect(events).toEqual(["start1"]);

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS * 2);
    expect(events).toEqual(["start1"]);

    gate.resolve();
    await first;
    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS - 1);
    expect(events).toEqual(["start1", "end1"]);
    await vi.advanceTimersByTimeAsync(1);
    await second;
    expect(events).toEqual(["start1", "end1", "start2", "end2"]);
  });

  it("does not serialize across different rooms", async () => {
    const events: string[] = [];

    const a = enqueueSend("!a:example.org", async () => {
      events.push("a");
      return "a";
    });
    const b = enqueueSend("!b:example.org", async () => {
      events.push("b");
      return "b";
    });

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    await Promise.all([a, b]);
    expect(events.sort()).toEqual(["a", "b"]);
  });

  it("continues queue after failures", async () => {
    const first = enqueueSend("!room:example.org", async () => {
      throw new Error("boom");
    }).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    );

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    const firstResult = await first;
    expect(firstResult.ok).toBe(false);
    if (firstResult.ok) {
      throw new Error("expected first queue item to fail");
    }
    expect(firstResult.error).toBeInstanceOf(Error);
    expect(firstResult.error.message).toBe("boom");

    const second = enqueueSend("!room:example.org", async () => "ok");
    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    await expect(second).resolves.toBe("ok");
  });

  it("continues queued work when the head task fails", async () => {
    const gate = deferred<void>();
    const events: string[] = [];

    const first = enqueueSend("!room:example.org", async () => {
      events.push("start1");
      await gate.promise;
      throw new Error("boom");
    }).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    );
    const second = enqueueSend("!room:example.org", async () => {
      events.push("start2");
      return "two";
    });

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    expect(events).toEqual(["start1"]);

    gate.resolve();
    const firstResult = await first;
    expect(firstResult.ok).toBe(false);
    if (firstResult.ok) {
      throw new Error("expected head queue item to fail");
    }
    expect(firstResult.error).toBeInstanceOf(Error);

    await vi.advanceTimersByTimeAsync(DEFAULT_SEND_GAP_MS);
    await expect(second).resolves.toBe("two");
    expect(events).toEqual(["start1", "start2"]);
  });

  it("retries on M_LIMIT_EXCEEDED and succeeds", async () => {
    let attempt = 0;
    const delayFn = vi.fn(async (_ms: number) => {});

    const result = enqueueSend(
      "!room:example.org",
      async () => {
        attempt++;
        if (attempt === 1) {
          throw { statusCode: 429, body: { errcode: "M_LIMIT_EXCEEDED", retry_after_ms: 1120 } };
        }
        return "ok";
      },
      { delayFn },
    );

    await expect(result).resolves.toBe("ok");
    expect(attempt).toBe(2);
    // gap delay + retry_after_ms delay
    expect(delayFn).toHaveBeenCalledTimes(2);
    expect(delayFn).toHaveBeenNthCalledWith(1, DEFAULT_SEND_GAP_MS);
    expect(delayFn).toHaveBeenNthCalledWith(2, 1120);
  });

  it("throws after exhausting rate-limit retries", async () => {
    const rateLimitErr = {
      statusCode: 429,
      body: { errcode: "M_LIMIT_EXCEEDED", retry_after_ms: 500 },
    };
    let attempts = 0;
    const delayFn = vi.fn(async (_ms: number) => {});

    const result = enqueueSend(
      "!room:example.org",
      async () => {
        attempts++;
        throw rateLimitErr;
      },
      { delayFn },
    ).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    );

    const outcome = await result;
    expect(outcome.ok).toBe(false);
    if (!outcome.ok) {
      expect(outcome.error).toBe(rateLimitErr);
    }
    expect(attempts).toBe(MAX_RATE_LIMIT_RETRIES + 1);
  });

  it("does not retry non-rate-limit errors", async () => {
    let attempts = 0;
    const delayFn = vi.fn(async (_ms: number) => {});

    const result = enqueueSend(
      "!room:example.org",
      async () => {
        attempts++;
        throw new Error("network failure");
      },
      { delayFn },
    ).then(
      () => ({ ok: true as const }),
      (error) => ({ ok: false as const, error }),
    );

    const outcome = await result;
    expect(outcome.ok).toBe(false);
    expect(attempts).toBe(1);
  });

  it("supports custom gap and delay injection", async () => {
    const events: string[] = [];
    const delayFn = vi.fn(async (_ms: number) => {});

    const first = enqueueSend(
      "!room:example.org",
      async () => {
        events.push("first");
        return "one";
      },
      { gapMs: 7, delayFn },
    );
    const second = enqueueSend(
      "!room:example.org",
      async () => {
        events.push("second");
        return "two";
      },
      { gapMs: 7, delayFn },
    );

    await expect(first).resolves.toBe("one");
    await expect(second).resolves.toBe("two");
    expect(events).toEqual(["first", "second"]);
    expect(delayFn).toHaveBeenCalledTimes(2);
    expect(delayFn).toHaveBeenNthCalledWith(1, 7);
    expect(delayFn).toHaveBeenNthCalledWith(2, 7);
  });
});
