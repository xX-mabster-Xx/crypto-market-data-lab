/** Bounded logical RPC admission plus per-physical-request HTTP start pacing. */

import { AsyncLocalStorage } from "node:async_hooks";
import { setMaxListeners } from "node:events";
import { performance } from "node:perf_hooks";

import type { FetchFn, FetchMiddleware } from "@solana/web3.js";

export type RpcPriority = "interactive" | "bootstrap" | "refresh";

export interface RpcJobOptions {
  readonly priority: RpcPriority;
  readonly deadlineAtMs?: number;
  readonly coalesceKey?: string;
  readonly description: string;
  readonly signal?: AbortSignal;
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
  readonly rpc_cancelled_total: number;
  readonly rpc_queue_wait_average_ms: number;
  readonly rpc_queue_wait_max_ms: number;
  readonly rpc_last_request_start_monotonic_ms: number | null;
}

export interface RpcRuntimeMetrics extends RpcSchedulerMetrics {
  readonly rpc_logical_queue_total: number;
  readonly rpc_logical_active: number;
  readonly rpc_logical_enqueued_total: number;
  readonly rpc_logical_started_total: number;
  readonly rpc_logical_completed_total: number;
  readonly rpc_logical_failed_total: number;
  readonly rpc_logical_cancelled_total: number;
  readonly rpc_logical_coalesced_total: number;
  readonly rpc_logical_expired_total: number;
  readonly rpc_logical_rejected_overload_total: number;
  readonly rpc_physical_queue_total: number;
  readonly rpc_physical_queue_interactive: number;
  readonly rpc_physical_queue_bootstrap: number;
  readonly rpc_physical_queue_refresh: number;
  readonly rpc_physical_queue_capacity: number;
  readonly rpc_physical_active: number;
  readonly rpc_physical_active_capacity: number;
  readonly rpc_physical_queue_high_watermark: number;
  readonly rpc_physical_enqueued_total: number;
  readonly rpc_physical_started_total: number;
  readonly rpc_physical_completed_total: number;
  readonly rpc_physical_failed_total: number;
  readonly rpc_physical_cancelled_total: number;
  readonly rpc_physical_expired_total: number;
  readonly rpc_physical_rejected_overload_total: number;
  readonly rpc_physical_queue_wait_average_ms: number;
  readonly rpc_physical_queue_wait_max_ms: number;
  readonly rpc_physical_last_request_start_monotonic_ms: number | null;
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

export class RpcRequestCancelledError extends RpcSchedulerError {
  public constructor(description: string, reason?: unknown) {
    super(
      `RPC request cancelled before completion: ${description}`,
      "RPC_REQUEST_CANCELLED",
      reason === undefined ? {} : { cause: reason },
    );
    this.name = "RpcRequestCancelledError";
  }
}

interface RpcJob {
  options: RpcJobOptions;
  fn: () => Promise<unknown>;
  readonly enqueuedAtMs: number;
  readonly promise: Promise<unknown>;
  readonly resolve: (value: unknown) => void;
  readonly reject: (reason: unknown) => void;
  abortListener?: () => void;
}

const PRIORITIES: readonly RpcPriority[] = ["interactive", "bootstrap", "refresh"];
const DEFAULT_MINIMUM_INTERVAL_MS = 200;
const DEFAULT_MAX_PENDING_JOBS = 256;
const DEFAULT_MAX_ACTIVE_JOBS = 32;
const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;

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
  if (options.coalesceKey !== undefined && options.signal !== undefined) {
    throw new TypeError("abort signals are not supported on coalesced RPC jobs");
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
  private cancelledTotal = 0;
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
    if (options.signal?.aborted === true) {
      this.cancelledTotal += 1;
      return Promise.reject(new RpcRequestCancelledError(
        options.description,
        options.signal.reason,
      ));
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
    if (options.signal !== undefined) {
      const onAbort = (): void => { this.cancelPending(job); };
      job.abortListener = onAbort;
      options.signal.addEventListener("abort", onAbort, { once: true });
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
      rpc_cancelled_total: this.cancelledTotal,
      rpc_queue_wait_average_ms: this.startedTotal === 0
        ? 0
        : this.totalQueueWaitMs / this.startedTotal,
      rpc_queue_wait_max_ms: this.maxQueueWaitMs,
      rpc_last_request_start_monotonic_ms: this.lastRequestStartMs,
    };
  }

  /** Stop admission, then either drain or explicitly reject pending jobs. */
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
      this.detachAbortListener(job);
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
      result = Promise.resolve(job.fn());
    } catch (error) {
      result = Promise.reject(error);
    }
    void result.then(
      (value) => {
        this.completedTotal += 1;
        job.resolve(value);
      },
      (error) => {
        if (job.options.signal?.aborted === true) {
          this.cancelledTotal += 1;
        } else {
          this.failedTotal += 1;
        }
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
          this.detachAbortListener(job);
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
    if (this.pendingCount() === 0 && this.startTimer !== null) {
      clearTimeout(this.startTimer);
      this.startTimer = null;
    }
  }

  private rejectAllPending(error: RpcSchedulerError): void {
    if (this.startTimer !== null) {
      clearTimeout(this.startTimer);
      this.startTimer = null;
    }
    for (const priority of PRIORITIES) {
      for (const job of this.queues[priority]) {
        this.detachAbortListener(job);
        job.reject(error);
      }
      this.queues[priority] = [];
    }
    this.pendingByCoalesceKey.clear();
    this.resolveIdleWaiters();
  }

  private cancelPending(job: RpcJob): void {
    const queue = this.queues[job.options.priority];
    const index = queue.indexOf(job);
    if (index < 0) return;
    queue.splice(index, 1);
    this.detachAbortListener(job);
    const key = job.options.coalesceKey;
    if (key !== undefined && this.pendingByCoalesceKey.get(key) === job) {
      this.pendingByCoalesceKey.delete(key);
    }
    this.cancelledTotal += 1;
    job.reject(new RpcRequestCancelledError(
      job.options.description,
      job.options.signal?.reason,
    ));
    if (this.pendingCount() === 0 && this.startTimer !== null) {
      clearTimeout(this.startTimer);
      this.startTimer = null;
    }
    this.requestPump();
    this.resolveIdleWaiters();
  }

  private detachAbortListener(job: RpcJob): void {
    if (job.abortListener === undefined || job.options.signal === undefined) return;
    job.options.signal.removeEventListener("abort", job.abortListener);
    job.abortListener = undefined;
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

function shutdownController(listenerCapacity: number): AbortController {
  const controller = new AbortController();
  // One listener per bounded queued/active physical request.  Raising the
  // EventTarget warning threshold to that exact bound avoids false leak
  // warnings without turning the shared shutdown signal into an unbounded bag.
  setMaxListeners(positiveInteger(listenerCapacity, "physical shutdown listener capacity"), controller.signal);
  return controller;
}

const rpcContext = new AsyncLocalStorage<RpcJobOptions>();
const initialPendingCapacity = positiveEnvironmentInteger(
  "WORKER_MAX_RPC_PENDING_JOBS",
  DEFAULT_MAX_PENDING_JOBS,
);
const initialLogicalActiveCapacity = positiveEnvironmentInteger(
  "WORKER_MAX_ACTIVE_RPC_JOBS",
  DEFAULT_MAX_ACTIVE_JOBS,
);
const initialPhysicalActiveCapacity = positiveEnvironmentInteger(
  "WORKER_MAX_ACTIVE_RPC_REQUESTS",
  initialLogicalActiveCapacity,
);
let physicalShutdownController = shutdownController(
  initialPendingCapacity + initialPhysicalActiveCapacity + 1,
);
let sharedLogicalScheduler = new RpcScheduler({
  minimumIntervalMs: 0,
  maxPendingJobs: initialPendingCapacity,
  maxActiveJobs: initialLogicalActiveCapacity,
});
let sharedPhysicalScheduler = new RpcScheduler({
  maxPendingJobs: initialPendingCapacity,
  maxActiveJobs: initialPhysicalActiveCapacity,
});

export function configureRpcPacer(
  intervalMs: number | undefined,
  maxPendingJobs?: number,
  maxActiveLogicalJobs?: number,
  maxActivePhysicalRequests?: number,
): void {
  const logical = sharedLogicalScheduler.metrics();
  const physical = sharedPhysicalScheduler.metrics();
  if (
    logical.rpc_queue_total !== 0
    || logical.rpc_active !== 0
    || physical.rpc_queue_total !== 0
    || physical.rpc_active !== 0
  ) {
    throw new Error("cannot reconfigure RPC scheduler while work is pending or active");
  }
  const pendingCapacity = maxPendingJobs ?? positiveEnvironmentInteger(
    "WORKER_MAX_RPC_PENDING_JOBS",
    DEFAULT_MAX_PENDING_JOBS,
  );
  const logicalActiveCapacity = maxActiveLogicalJobs ?? positiveEnvironmentInteger(
    "WORKER_MAX_ACTIVE_RPC_JOBS",
    DEFAULT_MAX_ACTIVE_JOBS,
  );
  const physicalActiveCapacity = maxActivePhysicalRequests ?? positiveEnvironmentInteger(
    "WORKER_MAX_ACTIVE_RPC_REQUESTS",
    logicalActiveCapacity,
  );
  physicalShutdownController = shutdownController(
    pendingCapacity + physicalActiveCapacity + 1,
  );
  sharedLogicalScheduler = new RpcScheduler({
    minimumIntervalMs: 0,
    maxPendingJobs: pendingCapacity,
    maxActiveJobs: logicalActiveCapacity,
  });
  sharedPhysicalScheduler = new RpcScheduler({
    minimumIntervalMs: intervalMs ?? DEFAULT_MINIMUM_INTERVAL_MS,
    maxPendingJobs: pendingCapacity,
    maxActiveJobs: physicalActiveCapacity,
  });
}

export function scheduleRpc<T>(
  options: RpcJobOptions,
  fn: () => Promise<T>,
): Promise<T> {
  return sharedLogicalScheduler.scheduleRpc(
    options,
    () => rpcContext.run(options, fn),
  );
}

/** Stable facade: engines may capture it before configureRpcPacer replaces the implementation. */
export const sharedRpcScheduler = {
  schedule<T>(options: RpcJobOptions, fn: () => Promise<T>): Promise<T> {
    return scheduleRpc(options, fn);
  },
};

export function withRpcJobOptions<T>(options: RpcJobOptions, fn: () => T): T {
  validateOptions(options);
  return rpcContext.run(options, fn);
}

/**
 * Legacy unprefixed fields retain logical-scheduler semantics.  Consumers
 * auditing the wire budget must use the explicit `rpc_physical_*` fields.
 */
export function rpcSchedulerMetrics(): RpcRuntimeMetrics {
  const logical = sharedLogicalScheduler.metrics();
  const physical = sharedPhysicalScheduler.metrics();
  return {
    ...logical,
    rpc_logical_queue_total: logical.rpc_queue_total,
    rpc_logical_active: logical.rpc_active,
    rpc_logical_enqueued_total: logical.rpc_enqueued_total,
    rpc_logical_started_total: logical.rpc_started_total,
    rpc_logical_completed_total: logical.rpc_completed_total,
    rpc_logical_failed_total: logical.rpc_failed_total,
    rpc_logical_cancelled_total: logical.rpc_cancelled_total,
    rpc_logical_coalesced_total: logical.rpc_coalesced_total,
    rpc_logical_expired_total: logical.rpc_expired_total,
    rpc_logical_rejected_overload_total: logical.rpc_rejected_overload_total,
    rpc_physical_queue_total: physical.rpc_queue_total,
    rpc_physical_queue_interactive: physical.rpc_queue_interactive,
    rpc_physical_queue_bootstrap: physical.rpc_queue_bootstrap,
    rpc_physical_queue_refresh: physical.rpc_queue_refresh,
    rpc_physical_queue_capacity: physical.rpc_queue_capacity,
    rpc_physical_active: physical.rpc_active,
    rpc_physical_active_capacity: physical.rpc_active_capacity,
    rpc_physical_queue_high_watermark: physical.rpc_queue_high_watermark,
    rpc_physical_enqueued_total: physical.rpc_enqueued_total,
    rpc_physical_started_total: physical.rpc_started_total,
    rpc_physical_completed_total: physical.rpc_completed_total,
    rpc_physical_failed_total: physical.rpc_failed_total,
    rpc_physical_cancelled_total: physical.rpc_cancelled_total,
    rpc_physical_expired_total: physical.rpc_expired_total,
    rpc_physical_rejected_overload_total: physical.rpc_rejected_overload_total,
    rpc_physical_queue_wait_average_ms: physical.rpc_queue_wait_average_ms,
    rpc_physical_queue_wait_max_ms: physical.rpc_queue_wait_max_ms,
    rpc_physical_last_request_start_monotonic_ms:
      physical.rpc_last_request_start_monotonic_ms,
  };
}

export async function closeRpcScheduler(timeoutMs = DEFAULT_CLOSE_TIMEOUT_MS): Promise<void> {
  physicalShutdownController.abort(new RpcSchedulerClosedError("physical transport shutdown"));
  await Promise.all([
    sharedLogicalScheduler.close({ drain: false, timeoutMs }),
    sharedPhysicalScheduler.close({ drain: false, timeoutMs }),
  ]);
}

function currentRpcOptions(): RpcJobOptions {
  return rpcContext.getStore() ?? {
    priority: "interactive",
    description: "solana web3.js RPC request",
  };
}

function physicalSignal(callerSignal: AbortSignal | null | undefined): AbortSignal {
  return callerSignal === null || callerSignal === undefined
    ? physicalShutdownController.signal
    : AbortSignal.any([callerSignal, physicalShutdownController.signal]);
}

function physicalOptions(
  options: RpcJobOptions,
  signal: AbortSignal,
): RpcJobOptions {
  return {
    priority: options.priority,
    ...(options.deadlineAtMs === undefined ? {} : { deadlineAtMs: options.deadlineAtMs }),
    description: `physical HTTP request for ${options.description}`,
    signal,
  };
}

async function rawFetchWithCancellation(
  info: Parameters<FetchFn>[0],
  init: Parameters<FetchFn>[1],
  signal: AbortSignal,
  description: string,
): Promise<Response> {
  if (signal.aborted) {
    throw new RpcRequestCancelledError(description, signal.reason);
  }
  return new Promise<Response>((resolve, reject) => {
    let settled = false;
    const finish = (fn: () => void): void => {
      if (settled) return;
      settled = true;
      signal.removeEventListener("abort", onAbort);
      fn();
    };
    const onAbort = (): void => {
      finish(() => { reject(new RpcRequestCancelledError(description, signal.reason)); });
    };
    signal.addEventListener("abort", onAbort, { once: true });
    let request: Promise<Response>;
    try {
      request = globalThis.fetch(info, { ...init, signal });
    } catch (error) {
      finish(() => { reject(error); });
      return;
    }
    void request.then(
      (response) => { finish(() => { resolve(response); }); },
      (error: unknown) => { finish(() => { reject(error); }); },
    );
  });
}

/** Production fetch path: every physical attempt passes through this bounded gate. */
export const sharedRpcFetch: FetchFn = (info, init) => {
  const options = currentRpcOptions();
  const signal = physicalSignal(init?.signal);
  const requestOptions = physicalOptions(options, signal);
  return sharedPhysicalScheduler.scheduleRpc(
    requestOptions,
    () => rawFetchWithCancellation(info, init, signal, requestOptions.description),
  );
};

/**
 * Compatibility adapter for external engine code still accepting middleware.
 * Production engines use `sharedRpcFetch`, which can propagate typed errors.
 */
export const sharedRpcFetchMiddleware: FetchMiddleware = (info, init, next) => {
  const signal = physicalSignal(init?.signal);
  const requestOptions = physicalOptions(currentRpcOptions(), signal);
  let started = false;
  void sharedPhysicalScheduler.scheduleRpc(requestOptions, async () => {
    started = true;
    next(info, { ...init, signal });
  }).catch((error: unknown) => {
    if (started) return;
    const controller = new AbortController();
    controller.abort(error);
    next(info, { ...init, signal: controller.signal });
  });
};
