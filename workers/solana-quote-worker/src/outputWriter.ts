import type { Writable } from "node:stream";

export type ProtocolMessage = Readonly<Record<string, unknown>>;

export interface ProtocolOutputMetrics {
  readonly stdout_blocked: boolean;
  readonly stdout_blocked_total: number;
  readonly stdout_drain_total: number;
  readonly stdout_lossless_queue_size: number;
  readonly stdout_lossless_queue_high_watermark: number;
  readonly stdout_lossless_queue_capacity: number;
  readonly stdout_state_pending_keys: number;
  readonly stdout_state_pending_keys_capacity: number;
  readonly stdout_state_coalesced_total: number;
  readonly stdout_write_failures_total: number;
  readonly stdout_lossless_overflow_total: number;
  readonly stdout_messages_written_total: number;
  readonly stdout_lossless_messages_written_total: number;
  readonly stdout_state_messages_written_total: number;
}

export interface ProtocolWritable {
  write(chunk: string): boolean;
  once(event: "drain", listener: () => void): unknown;
  on?(event: "error", listener: (error: Error) => void): unknown;
  off?(event: "error", listener: (error: Error) => void): unknown;
}

export class ProtocolOutputFatalError extends Error {
  public readonly code = "WORKER_OUTPUT_FATAL";

  public constructor(message: string, options: ErrorOptions = {}) {
    super(message, options);
    this.name = "ProtocolOutputFatalError";
  }
}

export class ProtocolOutputOverflowError extends ProtocolOutputFatalError {
  public readonly outputQueue: "lossless" | "state";
  public readonly capacity: number;

  public constructor(outputQueue: "lossless" | "state", capacity: number) {
    super(`worker ${outputQueue} output queue exceeded hard capacity ${capacity}`);
    this.name = "ProtocolOutputOverflowError";
    this.outputQueue = outputQueue;
    this.capacity = capacity;
  }
}

export class ProtocolOutputDrainTimeoutError extends ProtocolOutputFatalError {
  public readonly timeoutMs: number;

  public constructor(timeoutMs: number) {
    super(`worker output did not drain within ${timeoutMs}ms`);
    this.name = "ProtocolOutputDrainTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

interface QueuedMessage {
  readonly kind: "lossless" | "state";
  readonly message: ProtocolMessage;
}

const DEFAULT_MAX_LOSSLESS_QUEUE = 1_024;
const DEFAULT_MAX_STATE_PENDING_KEYS = 65_536;
const DEFAULT_LOSSLESS_BURST = 32;
const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved <= 0) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
  return resolved;
}

/**
 * A single JSONL writer for the worker protocol.
 *
 * Lossless messages retain FIFO order behind a hard cap. Replaceable state is
 * held as one unserialized latest value per stable key. A false write return
 * stops all further writes until `drain`, so Node's own Writable buffer can
 * never be extended by this producer while downstream is applying pressure.
 */
export class ProtocolEmitter {
  private readonly losslessQueue: ProtocolMessage[] = [];
  private readonly statePending = new Map<string, ProtocolMessage>();
  private readonly maxLosslessQueue: number;
  private readonly maxStatePendingKeys: number;
  private readonly losslessBurst: number;
  private readonly serialize: (message: ProtocolMessage) => string;
  private readonly onFatal: (error: ProtocolOutputFatalError) => void;
  private readonly errorListener = (error: Error): void => {
    this.writeFailures += 1;
    this.fail(new ProtocolOutputFatalError("worker stdout emitted an error", { cause: error }));
  };

  private blocked = false;
  private flushing = false;
  private closing = false;
  private closed = false;
  private failed: ProtocolOutputFatalError | null = null;
  private consecutiveLosslessWrites = 0;
  private losslessQueueHighWatermark = 0;
  private blockedTotal = 0;
  private drainTotal = 0;
  private stateCoalescedTotal = 0;
  private writeFailures = 0;
  private losslessOverflowTotal = 0;
  private messagesWritten = 0;
  private losslessMessagesWritten = 0;
  private stateMessagesWritten = 0;
  private idleWaiters = new Set<() => void>();

  public constructor(
    private readonly writable: ProtocolWritable,
    options: {
      readonly maxLosslessQueue?: number;
      readonly maxStatePendingKeys?: number;
      readonly losslessBurst?: number;
      readonly serialize?: (message: ProtocolMessage) => string;
      readonly onFatal?: (error: ProtocolOutputFatalError) => void;
    } = {},
  ) {
    this.maxLosslessQueue = positiveInteger(
      options.maxLosslessQueue,
      DEFAULT_MAX_LOSSLESS_QUEUE,
      "maxLosslessQueue",
    );
    this.maxStatePendingKeys = positiveInteger(
      options.maxStatePendingKeys,
      DEFAULT_MAX_STATE_PENDING_KEYS,
      "maxStatePendingKeys",
    );
    this.losslessBurst = positiveInteger(
      options.losslessBurst,
      DEFAULT_LOSSLESS_BURST,
      "losslessBurst",
    );
    this.serialize = options.serialize ?? JSON.stringify;
    this.onFatal = options.onFatal ?? (() => undefined);
    this.writable.on?.("error", this.errorListener);
  }

  public metrics(): ProtocolOutputMetrics {
    return {
      stdout_blocked: this.blocked,
      stdout_blocked_total: this.blockedTotal,
      stdout_drain_total: this.drainTotal,
      stdout_lossless_queue_size: this.losslessQueue.length,
      stdout_lossless_queue_high_watermark: this.losslessQueueHighWatermark,
      stdout_lossless_queue_capacity: this.maxLosslessQueue,
      stdout_state_pending_keys: this.statePending.size,
      stdout_state_pending_keys_capacity: this.maxStatePendingKeys,
      stdout_state_coalesced_total: this.stateCoalescedTotal,
      stdout_write_failures_total: this.writeFailures,
      stdout_lossless_overflow_total: this.losslessOverflowTotal,
      stdout_messages_written_total: this.messagesWritten,
      stdout_lossless_messages_written_total: this.losslessMessagesWritten,
      stdout_state_messages_written_total: this.stateMessagesWritten,
    };
  }

  public fatalError(): ProtocolOutputFatalError | null {
    return this.failed;
  }

  public emitLossless(message: ProtocolMessage): boolean {
    if (!this.accepting()) return false;
    if (this.canWriteImmediately()) {
      this.writeMessage({ kind: "lossless", message });
      return this.failed === null;
    }
    if (this.losslessQueue.length >= this.maxLosslessQueue) {
      this.losslessOverflowTotal += 1;
      this.fail(new ProtocolOutputOverflowError("lossless", this.maxLosslessQueue));
      return false;
    }
    this.losslessQueue.push(message);
    this.losslessQueueHighWatermark = Math.max(
      this.losslessQueueHighWatermark,
      this.losslessQueue.length,
    );
    this.flush();
    return true;
  }

  public emitState(coalesceKey: string, message: ProtocolMessage): boolean {
    if (!this.accepting()) return false;
    if (coalesceKey.trim().length === 0) {
      throw new TypeError("state coalesce key must be non-empty");
    }
    if (this.canWriteImmediately()) {
      this.writeMessage({ kind: "state", message });
      return this.failed === null;
    }
    if (this.statePending.has(coalesceKey)) {
      this.stateCoalescedTotal += 1;
      this.statePending.set(coalesceKey, message);
      return true;
    }
    if (this.statePending.size >= this.maxStatePendingKeys) {
      this.fail(new ProtocolOutputOverflowError("state", this.maxStatePendingKeys));
      return false;
    }
    this.statePending.set(coalesceKey, message);
    this.flush();
    return true;
  }

  /** Drain accepted output or fail within a bounded timeout. */
  public async drainAndClose(timeoutMs = DEFAULT_CLOSE_TIMEOUT_MS): Promise<void> {
    if (this.closed) return;
    if (this.failed !== null) throw this.failed;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new RangeError("output close timeout must be finite and positive");
    }
    this.closing = true;
    this.flush();
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
          const error = new ProtocolOutputDrainTimeoutError(timeoutMs);
          this.fail(error);
          reject(error);
        }, timeoutMs);
        this.idleWaiters.add(onIdle);
        if (this.idle()) onIdle();
      });
    }
    if (this.failed !== null) throw this.failed;
    this.closed = true;
    this.writable.off?.("error", this.errorListener);
  }

  private accepting(): boolean {
    return !this.closing && !this.closed && this.failed === null;
  }

  private canWriteImmediately(): boolean {
    return !this.blocked
      && !this.flushing
      && this.losslessQueue.length === 0
      && this.statePending.size === 0;
  }

  private idle(): boolean {
    return !this.blocked
      && !this.flushing
      && this.losslessQueue.length === 0
      && this.statePending.size === 0;
  }

  private flush(): void {
    if (this.flushing || this.blocked || this.failed !== null) return;
    this.flushing = true;
    try {
      while (!this.blocked && this.failed === null) {
        const next = this.nextMessage();
        if (next === null) break;
        this.writeMessage(next);
      }
    } finally {
      this.flushing = false;
      this.resolveIdleWaiters();
    }
  }

  private nextMessage(): QueuedMessage | null {
    const shouldWriteState = this.statePending.size > 0
      && (this.losslessQueue.length === 0 || this.consecutiveLosslessWrites >= this.losslessBurst);
    if (shouldWriteState) {
      const first = this.statePending.entries().next().value as
        | [string, ProtocolMessage]
        | undefined;
      if (first === undefined) return null;
      this.statePending.delete(first[0]);
      this.consecutiveLosslessWrites = 0;
      return { kind: "state", message: first[1] };
    }
    const lossless = this.losslessQueue.shift();
    if (lossless !== undefined) {
      this.consecutiveLosslessWrites += 1;
      return { kind: "lossless", message: lossless };
    }
    return null;
  }

  private writeMessage(queued: QueuedMessage): void {
    let accepted: boolean;
    try {
      const line = `${this.serialize(queued.message)}\n`;
      accepted = this.writable.write(line);
    } catch (error) {
      this.writeFailures += 1;
      this.fail(new ProtocolOutputFatalError("worker stdout write failed", { cause: error }));
      return;
    }
    this.messagesWritten += 1;
    if (queued.kind === "lossless") this.losslessMessagesWritten += 1;
    else this.stateMessagesWritten += 1;
    if (!accepted) {
      this.blocked = true;
      this.blockedTotal += 1;
      this.writable.once("drain", () => {
        if (!this.blocked || this.failed !== null) return;
        this.blocked = false;
        this.drainTotal += 1;
        this.flush();
      });
    }
  }

  private resolveIdleWaiters(): void {
    if (!this.idle()) return;
    for (const resolve of this.idleWaiters) resolve();
    this.idleWaiters.clear();
  }

  private fail(error: ProtocolOutputFatalError): void {
    if (this.failed !== null) return;
    this.failed = error;
    this.blocked = false;
    this.losslessQueue.length = 0;
    this.statePending.clear();
    this.resolveIdleWaiters();
    this.onFatal(error);
  }
}

export function nodeWritable(writable: Writable): ProtocolWritable {
  return writable;
}
