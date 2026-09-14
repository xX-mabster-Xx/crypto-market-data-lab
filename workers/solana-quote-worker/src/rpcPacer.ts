/** Bounded, priority-aware start-rate scheduler for Solana HTTP RPC calls. */

import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";

import type { FetchFn, FetchMiddleware } from "@solana/web3.js";

export type RpcPriority = "interactive" | "bootstrap" | "refresh";

export interface RpcJobOptions {
  readonly priority: RpcPriority;
  readonly deadlineAtMs?: number;
  readonly coalesceKey?: string;
  readonly description: string;
}

export interface RpcSchedulerMetrics {
  readonly rpc_queue_total: number;
  readonly rpc_queue_interactive: number;
  readonly rpc_queue_bootstrap: number;
  readonly rpc_queue_refresh: number;
  readonly rpc_queue_capacity: number;
  readonly rpc_active: number;
  readonly rpc_active_capacity: number;
  readonly rpc_queue_high_watermark: number;
  readonly rpc_enqueued_total: number;
  readonly rpc_started_total: number;
  readonly rpc_completed_total: number;
  readonly rpc_failed_total: number;
  readonly rpc_coalesced_total: number;
  readonly rpc_expired_total: number;
  readonly rpc_rejected_overload_total: number;
  readonly rpc_queue_wait_average_ms: number;
  readonly rpc_queue_wait_max_ms: number;
  readonly rpc_last_request_start_monotonic_ms: number | null;
}

export class RpcSchedulerError extends Error {
  public constructor(
    message: string,
    public readonly code: string,
    options: ErrorOptions = {},
  ) {
    super(message, options);
    this.name = "RpcSchedulerError";
  }
}

export class RpcDeadlineExceededError extends RpcSchedulerError {
  public constructor(description: string) {
    super(`RPC job deadline expired before start: ${description}`, "RPC_DEADLINE_EXCEEDED");
    this.name = "RpcDeadlineExceededError";
  }
}

export class RpcSchedulerOverloadError extends RpcSchedulerError {
  public constructor(priority: RpcPriority, capacity: number, description: string) {
    super(
      `RPC scheduler queue capacity ${capacity} exceeded for ${priority} job: ${description}`,
      "RPC_SCHEDULER_OVERLOAD",
    );
    this.name = "RpcSchedulerOverloadError";
  }
}

export class RpcSchedulerClosedError extends RpcSchedulerError {
  public constructor(description: string) {
    super(`RPC scheduler is closed: ${description}`, "RPC_SCHEDULER_CLOSED");
    this.name = "RpcSchedulerClosedError";
  }
}

export class RpcSchedulerCloseTimeoutError extends RpcSchedulerError {
  public constructor(timeoutMs: number) {
    super(`RPC scheduler did not become idle within ${timeoutMs}ms`, "RPC_SCHEDULER_CLOSE_TIMEOUT");
    this.name = "RpcSchedulerCloseTimeoutError";
  }
}

interface RpcJob {
  options: RpcJobOptions;
  fn: () => Promise<unknown>;
  readonly enqueuedAtMs: number;
  readonly promise: Promise<unknown>;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
}

const PRIORITIES: readonly RpcPriority[] = ["interactive", "bootstrap", "refresh"];
const DEFAULT_MINIMUM_INTERVAL_MS = 200;
const DEFAULT_MAX_PENDING_JOBS = 256;
const DEFAULT_MAX_ACTIVE_JOBS = 32;
const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;
const scheduledRpcStart = new AsyncLocalStorage<boolean>();

function finiteNonNegative(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${name} must be finite and non-negative`);
  }
  return value;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function validateOptions(options: RpcJobOptions): void {
  if (!PRIORITIES.includes(options.priority)) {
    throw new TypeError(`unsupported RPC priority ${String(options.priority)}`);
  }
  if (options.description.trim().length === 0) {
    throw new TypeError("RPC job description must be non-empty");
  }
  if (options.deadlineAtMs !== undefined && !Number.isFinite(options.deadlineAtMs)) {
    throw new TypeError("RPC job deadline must be finite");
  }
  if (options.coalesceKey !== undefined && options.coalesceKey.trim().length === 0) {
    throw new TypeError("RPC coalesce key must be non-empty when supplied");
  }
}

/**
 * Starts RPC jobs by priority with a monotonic minimum interval.
 *
 * Pending jobs have a hard cap. Replaceable jobs share one Promise and one
 * queue node per coalesce key; replacing a job only swaps its latest closure.
 * Started requests are also capped, preventing slow responses from becoming
 * a second implicit unbounded queue.
 */
export class RpcScheduler {
  private queues: Record<RpcPriority, RpcJob[]> = {
    interactive: [],
    bootstrap: [],
    refresh: [],
  };
  private readonly pendingByCoalesceKey = new Map<string, RpcJob>();
  private readonly minimumIntervalMs: number;
  private readonly maxPendingJobs: number;
  private readonly maxActiveJobs: number;
  private readonly nowMs: () => number;

  private accepting = true;
  private closed = false;
  private pumpQueued = false;
  private startTimer: ReturnType<typeof setTimeout> | null = null;
  private nextAllowedStartMs = 0;
  private active = 0;
  private queueHighWatermark = 0;
  private enqueuedTotal = 0;
  private startedTotal = 0;
  private completedTotal = 0;
  private failedTotal = 0;
  private coalescedTotal = 0;
  private expiredTotal = 0;
  private rejectedOverloadTotal = 0;
  private totalQueueWaitMs = 0;
  private maxQueueWaitMs = 0;
  private lastRequestStartMs: number | null = null;
  private readonly idleWaiters = new Set<() => void>();

  public constructor(options: {
    readonly minimumIntervalMs?: number;
    readonly maxPendingJobs?: number;
    readonly maxActiveJobs?: number;
    readonly nowMs?: () => number;
  } = {}) {
    this.minimumIntervalMs = finiteNonNegative(
      options.minimumIntervalMs ?? DEFAULT_MINIMUM_INTERVAL_MS,
      "minimumIntervalMs",
    );
    this.maxPendingJobs = positiveInteger(
      options.maxPendingJobs ?? DEFAULT_MAX_PENDING_JOBS,
      "maxPendingJobs",
    );
    this.maxActiveJobs = positiveInteger(
      options.maxActiveJobs ?? DEFAULT_MAX_ACTIVE_JOBS,
      "maxActiveJobs",
    );
    this.nowMs = options.nowMs ?? (() => performance.now());
  }

  public scheduleRpc<T>(options: RpcJobOptions, fn: () => Promise<T>): Promise<T> {
    validateOptions(options);
    if (!this.accepting) {
      return Promise.reject(new RpcSchedulerClosedError(options.description));
    }
    const now = this.nowMs();
    this.purgeExpired(now);
    if (options.deadlineAtMs !== undefined && now >= options.deadlineAtMs) {
      this.expiredTotal += 1;
      return Promise.reject(new RpcDeadlineExceededError(options.description));
    }

    if (options.coalesceKey !== undefined) {
      const existing = this.pendingByCoalesceKey.get(options.coalesceKey);
      if (existing !== undefined) {
        if (existing.options.priority !== options.priority) {
          return Promise.reject(new RpcSchedulerError(
            `RPC coalesce key ${options.coalesceKey} changed priority`,
            "RPC_COALESCE_PRIORITY_MISMATCH",
          ));
        }
        existing.options = options;
        existing.fn = fn;
        this.coalescedTotal += 1;
        return existing.promise as Promise<T>;
      }
    }

    if (this.pendingCount() >= this.maxPendingJobs) {
      this.rejectedOverloadTotal += 1;
      return Promise.reject(new RpcSchedulerOverloadError(
        options.priority,
        this.maxPendingJobs,
        options.description,
      ));
    }

    let resolve!: (value: unknown) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<unknown>((accept, decline) => {
      resolve = accept;
      reject = decline;
    });
    const job: RpcJob = {
      options,
      fn,
      enqueuedAtMs: now,
      promise,
      resolve,
      reject,
    };
    this.queues[options.priority].push(job);
    if (options.coalesceKey !== undefined) {
      this.pendingByCoalesceKey.set(options.coalesceKey, job);
    }
    this.enqueuedTotal += 1;
    this.queueHighWatermark = Math.max(this.queueHighWatermark, this.pendingCount());
    this.requestPump();
    return promise as Promise<T>;
  }

  public schedule<T>(options: RpcJobOptions, fn: () => Promise<T>): Promise<T> {
    return this.scheduleRpc(options, fn);
  }

  public metrics(): RpcSchedulerMetrics {
    const interactive = this.queues.interactive.length;
    const bootstrap = this.queues.bootstrap.length;
    const refresh = this.queues.refresh.length;
    return {
      rpc_queue_total: interactive + bootstrap + refresh,
      rpc_queue_interactive: interactive,
      rpc_queue_bootstrap: bootstrap,
      rpc_queue_refresh: refresh,
      rpc_queue_capacity: this.maxPendingJobs,
      rpc_active: this.active,
      rpc_active_capacity: this.maxActiveJobs,
      rpc_queue_high_watermark: this.queueHighWatermark,
      rpc_enqueued_total: this.enqueuedTotal,
      rpc_started_total: this.startedTotal,
      rpc_completed_total: this.completedTotal,
      rpc_failed_total: this.failedTotal,
      rpc_coalesced_total: this.coalescedTotal,
      rpc_expired_total: this.expiredTotal,
      rpc_rejected_overload_total: this.rejectedOverloadTotal,
      rpc_queue_wait_average_ms: this.startedTotal === 0
        ? 0
        : this.totalQueueWaitMs / this.startedTotal,
      rpc_queue_wait_max_ms: this.maxQueueWaitMs,
      rpc_last_request_start_monotonic_ms: this.lastRequestStartMs,
    };
  }

  /** Stop admission and deterministically drain accepted jobs. */
  public async close(options: {
    readonly drain?: boolean;
    readonly timeoutMs?: number;
  } = {}): Promise<void> {
    if (this.closed) return;
    const drain = options.drain ?? true;
    const timeoutMs = finiteNonNegative(
      options.timeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS,
      "RPC scheduler close timeout",
    );
    if (timeoutMs === 0) throw new RangeError("RPC scheduler close timeout must be positive");
    this.accepting = false;
    if (!drain) this.rejectAllPending(new RpcSchedulerClosedError("scheduler shutdown"));
    this.requestPump();
    if (!this.idle()) {
      await new Promise<void>((resolve, reject) => {
        let settled = false;
        const onIdle = (): void => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          resolve();
        };
        const timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          this.idleWaiters.delete(onIdle);
          const error = new RpcSchedulerCloseTimeoutError(timeoutMs);
          this.rejectAllPending(error);
          if (this.startTimer !== null) {
            clearTimeout(this.startTimer);
            this.startTimer = null;
          }
          reject(error);
        }, timeoutMs);
        this.idleWaiters.add(onIdle);
        if (this.idle()) onIdle();
      });
    }
    this.closed = true;
  }

  private pendingCount(): number {
    return this.queues.interactive.length
      + this.queues.bootstrap.length
      + this.queues.refresh.length;
  }

  private idle(): boolean {
    return this.pendingCount() === 0
      && this.active === 0
      && this.startTimer === null
      && !this.pumpQueued;
  }

  private requestPump(): void {
    if (this.pumpQueued || this.startTimer !== null) return;
    this.pumpQueued = true;
    queueMicrotask(() => {
      this.pumpQueued = false;
      this.pump();
    });
  }

  private pump(): void {
    if (this.startTimer !== null) return;
    if (this.active >= this.maxActiveJobs) {
      this.resolveIdleWaiters();
      return;
    }
    const now = this.nowMs();
    this.purgeExpired(now);
    if (this.pendingCount() === 0) {
      this.resolveIdleWaiters();
      return;
    }
    const delayMs = Math.max(0, this.nextAllowedStartMs - now);
    if (delayMs > 0) {
      this.startTimer = setTimeout(() => {
        this.startTimer = null;
        this.pump();
      }, Math.max(1, Math.ceil(delayMs)));
      return;
    }
    const job = this.takeNext();
    if (job === null) {
      this.resolveIdleWaiters();
      return;
    }
    this.start(job, this.nowMs());
    this.requestPump();
  }

  private takeNext(): RpcJob | null {
    for (const priority of PRIORITIES) {
      const job = this.queues[priority].shift();
      if (job === undefined) continue;
      const key = job.options.coalesceKey;
      if (key !== undefined && this.pendingByCoalesceKey.get(key) === job) {
        this.pendingByCoalesceKey.delete(key);
      }
      return job;
    }
    return null;
  }

  private start(job: RpcJob, startedAtMs: number): void {
    this.startedTotal += 1;
    this.active += 1;
    this.lastRequestStartMs = startedAtMs;
    this.nextAllowedStartMs = startedAtMs + this.minimumIntervalMs;
    const waitMs = Math.max(0, startedAtMs - job.enqueuedAtMs);
    this.totalQueueWaitMs += waitMs;
    this.maxQueueWaitMs = Math.max(this.maxQueueWaitMs, waitMs);

    let result: Promise<unknown>;
    try {
      // An explicitly scheduled job owns this request-start slot. The web3.js
      // fetch adapter sees this context and does not enqueue the same request
      // a second time.
      result = Promise.resolve(scheduledRpcStart.run(true, job.fn));
    } catch (error) {
      result = Promise.reject(error);
    }
    void result.then(
      (value) => {
        this.completedTotal += 1;
        job.resolve(value);
      },
      (error) => {
        this.failedTotal += 1;
        job.reject(error);
      },
    ).finally(() => {
      this.active -= 1;
      this.requestPump();
      this.resolveIdleWaiters();
    });
  }

  private purgeExpired(now: number): void {
    for (const priority of PRIORITIES) {
      const retained: RpcJob[] = [];
      for (const job of this.queues[priority]) {
        if (job.options.deadlineAtMs !== undefined && now >= job.options.deadlineAtMs) {
          const key = job.options.coalesceKey;
          if (key !== undefined && this.pendingByCoalesceKey.get(key) === job) {
            this.pendingByCoalesceKey.delete(key);
          }
          this.expiredTotal += 1;
          job.reject(new RpcDeadlineExceededError(job.options.description));
        } else {
          retained.push(job);
        }
      }
      this.queues[priority] = retained;
    }
  }

  private rejectAllPending(error: RpcSchedulerError): void {
    if (this.startTimer !== null) {
      clearTimeout(this.startTimer);
      this.startTimer = null;
    }
    for (const priority of PRIORITIES) {
      for (const job of this.queues[priority]) job.reject(error);
      this.queues[priority] = [];
    }
    this.pendingByCoalesceKey.clear();
    this.resolveIdleWaiters();
  }

  private resolveIdleWaiters(): void {
    if (!this.idle()) return;
    for (const resolve of this.idleWaiters) resolve();
    this.idleWaiters.clear();
  }
}

function positiveEnvironmentInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  if (!/^[1-9][0-9]*$/u.test(raw)) throw new Error(`${name} must be a positive integer`);
  return positiveInteger(Number(raw), name);
}

const rpcContext = new AsyncLocalStorage<RpcJobOptions>();
let sharedScheduler = new RpcScheduler({
  maxPendingJobs: positiveEnvironmentInteger(
    "WORKER_MAX_RPC_PENDING_JOBS",
    DEFAULT_MAX_PENDING_JOBS,
  ),
  maxActiveJobs: positiveEnvironmentInteger(
    "WORKER_MAX_ACTIVE_RPC_JOBS",
    DEFAULT_MAX_ACTIVE_JOBS,
  ),
});

export function configureRpcPacer(
  intervalMs: number | undefined,
  maxPendingJobs?: number,
): void {
  const metrics = sharedScheduler.metrics();
  if (metrics.rpc_queue_total !== 0 || metrics.rpc_active !== 0) {
    throw new Error("cannot reconfigure RPC scheduler while work is pending or active");
  }
  sharedScheduler = new RpcScheduler({
    minimumIntervalMs: intervalMs ?? DEFAULT_MINIMUM_INTERVAL_MS,
    maxPendingJobs: maxPendingJobs ?? positiveEnvironmentInteger(
      "WORKER_MAX_RPC_PENDING_JOBS",
      DEFAULT_MAX_PENDING_JOBS,
    ),
    maxActiveJobs: positiveEnvironmentInteger(
      "WORKER_MAX_ACTIVE_RPC_JOBS",
      DEFAULT_MAX_ACTIVE_JOBS,
    ),
  });
}

export function scheduleRpc<T>(
  options: RpcJobOptions,
  fn: () => Promise<T>,
): Promise<T> {
  return sharedScheduler.scheduleRpc(options, fn);
}

/** Stable facade: engines may capture it before configureRpcPacer replaces the implementation. */
export const sharedRpcScheduler = {
  schedule<T>(options: RpcJobOptions, fn: () => Promise<T>): Promise<T> {
    return sharedScheduler.scheduleRpc(options, fn);
  },
};

export function withRpcJobOptions<T>(options: RpcJobOptions, fn: () => T): T {
  validateOptions(options);
  return rpcContext.run(options, fn);
}

export function rpcSchedulerMetrics(): RpcSchedulerMetrics {
  return sharedScheduler.metrics();
}

export async function closeRpcScheduler(timeoutMs = DEFAULT_CLOSE_TIMEOUT_MS): Promise<void> {
  await sharedScheduler.close({ timeoutMs });
}

function currentRpcOptions(): RpcJobOptions {
  return rpcContext.getStore() ?? {
    priority: "interactive",
    description: "solana web3.js RPC request",
  };
}

/** Production fetch path: overload/deadline errors reject the actual RPC Promise. */
export const sharedRpcFetch: FetchFn = (info, init) => scheduledRpcStart.getStore() === true
  ? globalThis.fetch(info, init)
  : sharedScheduler.scheduleRpc(
    currentRpcOptions(),
    () => globalThis.fetch(info, init),
  );

/**
 * Compatibility adapter for external engine code still accepting middleware.
 * Production engines use `sharedRpcFetch`, which can propagate typed errors.
 */
export const sharedRpcFetchMiddleware: FetchMiddleware = (info, init, next) => {
  if (scheduledRpcStart.getStore() === true) {
    next(info, init);
    return;
  }
  void sharedScheduler.scheduleRpc(currentRpcOptions(), async () => {
    next(info, init);
  }).catch((error: unknown) => {
    const controller = new AbortController();
    controller.abort(error);
    next(info, { ...init, signal: controller.signal });
  });
};
