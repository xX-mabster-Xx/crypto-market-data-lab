/** Shared start-rate limiter for every Solana HTTP RPC connection in a worker. */

import type { FetchMiddleware } from "@solana/web3.js";

const DEFAULT_MINIMUM_INTERVAL_MS = 200;

let minimumIntervalMs = DEFAULT_MINIMUM_INTERVAL_MS;
let nextAllowedAtMs = 0;
let tail: Promise<void> = Promise.resolve();

export function configureRpcPacer(intervalMs: number | undefined): void {
  minimumIntervalMs = intervalMs ?? DEFAULT_MINIMUM_INTERVAL_MS;
  nextAllowedAtMs = 0;
  tail = Promise.resolve();
}

/**
 * web3.js middleware resolves its request only when `next` is called.  Chaining
 * those callbacks spaces request *starts* across all protocol Connections,
 * while allowing already-started HTTP responses to complete concurrently.
 */
export const sharedRpcFetchMiddleware: FetchMiddleware = (info, init, next) => {
  const scheduled = tail.then(async () => {
    const delayMs = Math.max(0, nextAllowedAtMs - Date.now());
    if (delayMs > 0) {
      await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
    }
    nextAllowedAtMs = Date.now() + minimumIntervalMs;
    next(info, init);
  });
  tail = scheduled.catch(() => undefined);
};
