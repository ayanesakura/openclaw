import { KeyedAsyncQueue } from "openclaw/plugin-sdk/keyed-async-queue";

export const DEFAULT_SEND_GAP_MS = 150;
export const MAX_RATE_LIMIT_RETRIES = 2;
const DEFAULT_RETRY_AFTER_MS = 2000;
const MAX_RETRY_AFTER_MS = 30_000;

type MatrixSendQueueOptions = {
  gapMs?: number;
  delayFn?: (ms: number) => Promise<void>;
};

/**
 * Extract wait time from a Matrix M_LIMIT_EXCEEDED error.
 * Returns the clamped retry_after_ms value, or null if the error
 * is not a rate-limit response.
 */
export function extractRateLimitMs(err: unknown): number | null {
  if (!err || typeof err !== "object") return null;
  const e = err as Record<string, unknown>;

  // matrix-bot-sdk wraps the HTTP response: { statusCode, body: { errcode, retry_after_ms } }
  const body = typeof e.body === "object" && e.body ? (e.body as Record<string, unknown>) : null;
  const errcode = body?.errcode ?? e.errcode;
  if (errcode !== "M_LIMIT_EXCEEDED" && e.statusCode !== 429) return null;

  const raw = body?.retry_after_ms ?? e.retry_after_ms ?? DEFAULT_RETRY_AFTER_MS;
  const ms = typeof raw === "number" && Number.isFinite(raw) ? raw : DEFAULT_RETRY_AFTER_MS;
  return Math.min(Math.max(ms, 0), MAX_RETRY_AFTER_MS);
}

// Serialize sends per room to preserve Matrix delivery order.
const roomQueues = new KeyedAsyncQueue();

export function enqueueSend<T>(
  roomId: string,
  fn: () => Promise<T>,
  options?: MatrixSendQueueOptions,
): Promise<T> {
  const gapMs = options?.gapMs ?? DEFAULT_SEND_GAP_MS;
  const delayFn = options?.delayFn ?? delay;
  return roomQueues.enqueue(roomId, async () => {
    await delayFn(gapMs);
    let lastErr: unknown;
    for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
      try {
        return await fn();
      } catch (err) {
        const retryMs = extractRateLimitMs(err);
        if (retryMs !== null && attempt < MAX_RATE_LIMIT_RETRIES) {
          await delayFn(retryMs);
          lastErr = err;
          continue;
        }
        throw err;
      }
    }
    // Unreachable in practice – the loop either returns or throws.
    throw lastErr;
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
