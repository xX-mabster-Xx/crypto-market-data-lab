/** Bounded runtime primitives shared by the Solana pool engines. */

import { performance } from "node:perf_hooks";

import type { RpcJobOptions } from "./rpcPacer.js";

/** Agent E owns actual admission/start pacing; engines only supply job context. */
export type RunRpcJob = <T>(options: RpcJobOptions, job: () => Promise<T>) => Promise<T>;

export interface PoolSlotProvenanceFields {
  readonly core_state_slot: number;
  readonly dependency_slot_min: number | null;
  readonly dependency_slot_max: number | null;
  readonly dependency_generation: number;
}

export interface PoolFreshnessSnapshot extends PoolSlotProvenanceFields {
  readonly core_received_at_monotonic_ms: number;
  readonly dependency_received_at_monotonic_ms: number | null;
  readonly last_successful_rpc_refresh_at_monotonic_ms: number | null;
}

/**
 * Core and dependency provenance intentionally have different clocks/slots.
 * A tick/bin/vault update can never make the core account look newer.
 */
export class PoolSlotProvenance {
  public coreStateSlot: number;
  public coreReceivedAtMs: number;
  public dependencyGeneration = 0;
  public dependencyReceivedAtMs: number | null = null;
  public lastSuccessfulRpcRefreshAtMs: number | null = null;
  public refreshInFlight = false;
  public refreshGeneration = 0;
  private readonly dependencySlots = new Map<string, number>();

  public constructor(coreStateSlot: number, receivedAtMs = performance.now()) {
    if (!Number.isSafeInteger(coreStateSlot) || coreStateSlot < 0) {
      throw new Error("core state slot must be a non-negative safe integer");
    }
    this.coreStateSlot = coreStateSlot;
    this.coreReceivedAtMs = receivedAtMs;
  }

  /** Equal-slot duplicates are first-writer-wins; lower slots are stale. */
  public acceptCore(slot: number, receivedAtMs = performance.now()): boolean {
    if (!Number.isSafeInteger(slot) || slot <= this.coreStateSlot) return false;
    this.coreStateSlot = slot;
    this.coreReceivedAtMs = receivedAtMs;
    return true;
  }

  /** Record one bounded subscribed dependency. */
  public acceptDependency(
    key: string,
    slot: number,
    receivedAtMs = performance.now(),
  ): boolean {
    return this.acceptDependencyVersion(key, slot, true, receivedAtMs);
  }

  /**
   * Advance factual account provenance independently from semantic content.
   * A validation at a newer slot with identical bytes must not manufacture a
   * new dependency generation or downstream state event.
   */
  public acceptDependencyVersion(
    key: string,
    slot: number,
    semanticChange: boolean,
    receivedAtMs = performance.now(),
  ): boolean {
    if (!key || !Number.isSafeInteger(slot) || slot < 0) return false;
    const previous = this.dependencySlots.get(key);
    if (previous !== undefined && slot <= previous) return false;
    this.dependencySlots.set(key, slot);
    if (semanticChange) this.dependencyGeneration += 1;
    this.dependencyReceivedAtMs = receivedAtMs;
    return true;
  }

  public forgetDependency(key: string): void {
    this.dependencySlots.delete(key);
  }

  public removeDependency(
    key: string,
    semanticChange = true,
    receivedAtMs = performance.now(),
  ): boolean {
    if (!this.dependencySlots.delete(key)) return false;
    if (semanticChange) this.dependencyGeneration += 1;
    this.dependencyReceivedAtMs = receivedAtMs;
    return true;
  }

  public dependencySlot(key: string): number | undefined {
    return this.dependencySlots.get(key);
  }

  public dependencyVersions(): readonly Readonly<{ key: string; slot: number }>[] {
    return [...this.dependencySlots]
      .map(([key, slot]) => Object.freeze({ key, slot }))
      .sort((left, right) => left.key.localeCompare(right.key));
  }

  public noteDependencyRefresh(receivedAtMs = performance.now()): void {
    this.dependencyReceivedAtMs = receivedAtMs;
  }

  public advanceDependencyGeneration(receivedAtMs = performance.now()): void {
    this.dependencyGeneration += 1;
    this.dependencyReceivedAtMs = receivedAtMs;
  }

  public noteRpcRefresh(receivedAtMs = performance.now()): void {
    this.lastSuccessfulRpcRefreshAtMs = receivedAtMs;
  }

  public fields(): PoolSlotProvenanceFields {
    let minimum: number | null = null;
    let maximum: number | null = null;
    for (const slot of this.dependencySlots.values()) {
      minimum = minimum === null ? slot : Math.min(minimum, slot);
      maximum = maximum === null ? slot : Math.max(maximum, slot);
    }
    return {
      core_state_slot: this.coreStateSlot,
      dependency_slot_min: minimum,
      dependency_slot_max: maximum,
      dependency_generation: this.dependencyGeneration,
    };
  }

  public freshnessSnapshot(): PoolFreshnessSnapshot {
    return Object.freeze({
      ...this.fields(),
      core_received_at_monotonic_ms: this.coreReceivedAtMs,
      dependency_received_at_monotonic_ms: this.dependencyReceivedAtMs,
      last_successful_rpc_refresh_at_monotonic_ms: this.lastSuccessfulRpcRefreshAtMs,
    });
  }
}

export interface FairMaintenanceStats {
  readonly selected_total: number;
}

/**
 * Deterministic round-robin selection over a changing keyed collection.
 * Eligibility remains engine-owned; advancing before I/O ensures one failing
 * or slow entry cannot be selected again ahead of every later entry.
 */
export class FairMaintenanceCursor {
  private nextStartKey: string | null = null;
  private fallbackIndex = 0;
  private selectedTotal = 0;

  public select<T>(
    values: Iterable<T>,
    keyOf: (value: T) => string,
    eligible: (value: T) => boolean,
  ): T | undefined {
    const items = [...values];
    if (items.length === 0) {
      this.nextStartKey = null;
      this.fallbackIndex = 0;
      return undefined;
    }
    const requestedIndex = this.nextStartKey === null
      ? -1
      : items.findIndex((item) => keyOf(item) === this.nextStartKey);
    const start = requestedIndex < 0 ? this.fallbackIndex % items.length : requestedIndex;
    for (let offset = 0; offset < items.length; offset += 1) {
      const index = (start + offset) % items.length;
      const item = items[index]!;
      if (!eligible(item)) continue;
      const nextIndex = (index + 1) % items.length;
      this.nextStartKey = keyOf(items[nextIndex]!);
      this.fallbackIndex = nextIndex;
      this.selectedTotal += 1;
      return item;
    }
    return undefined;
  }

  public stats(): FairMaintenanceStats {
    return { selected_total: this.selectedTotal };
  }
}

export interface LatestMailboxStats {
  readonly processing: boolean;
  readonly pending: number;
  readonly coalesced_total: number;
  readonly stale_or_duplicate_total: number;
  readonly errors_total: number;
  readonly processed_total: number;
  readonly maximum_pending: number;
}

/**
 * One running item plus one latest pending item. This is a mailbox, not a
 * Promise-tail: superseded AccountInfo buffers become unreachable immediately.
 */
export class LatestOnlyMailbox<T> {
  private processing = false;
  private pending: { readonly sequence: number; readonly value: T } | null = null;
  private processingSequence: number | null = null;
  private running: Promise<void> | null = null;
  private active = true;
  private acceptedSequence: number;
  private coalescedTotal = 0;
  private staleOrDuplicateTotal = 0;
  private errorsTotal = 0;
  private processedTotal = 0;
  private maximumPending = 0;

  public constructor(
    initialAcceptedSequence: number,
    private readonly process: (value: T, sequence: number) => Promise<boolean | void>,
    private readonly onError: (error: unknown) => void = () => undefined,
  ) {
    this.acceptedSequence = initialAcceptedSequence;
  }

  public submit(value: T, sequence: number): boolean {
    const floor = Math.max(
      this.acceptedSequence,
      this.processingSequence ?? Number.NEGATIVE_INFINITY,
    );
    if (!this.active || !Number.isSafeInteger(sequence) || sequence <= floor) {
      this.staleOrDuplicateTotal += 1;
      return false;
    }
    if (this.pending !== null) {
      if (sequence <= this.pending.sequence) {
        this.staleOrDuplicateTotal += 1;
        return false;
      }
      this.coalescedTotal += 1;
    }
    this.pending = { sequence, value };
    this.maximumPending = 1;
    if (!this.processing) {
      this.processing = true;
      const running = this.drain();
      this.running = running;
      void running.finally(() => {
        if (this.running === running) this.running = null;
      });
    }
    return true;
  }

  public async idle(): Promise<void> {
    while (this.running !== null) await this.running;
  }

  public async dispose(): Promise<void> {
    this.active = false;
    this.pending = null;
    await this.idle();
  }

  public stats(): LatestMailboxStats {
    return {
      processing: this.processing,
      pending: this.pending === null ? 0 : 1,
      coalesced_total: this.coalescedTotal,
      stale_or_duplicate_total: this.staleOrDuplicateTotal,
      errors_total: this.errorsTotal,
      processed_total: this.processedTotal,
      maximum_pending: this.maximumPending,
    };
  }

  private async drain(): Promise<void> {
    try {
      while (this.active && this.pending !== null) {
        const current = this.pending;
        this.pending = null;
        this.processingSequence = current.sequence;
        try {
          const accepted = await this.process(current.value, current.sequence);
          if (accepted !== false) {
            this.acceptedSequence = Math.max(this.acceptedSequence, current.sequence);
            this.processedTotal += 1;
          }
        } catch (error) {
          this.errorsTotal += 1;
          this.onError(error);
        } finally {
          this.processingSequence = null;
        }
      }
    } finally {
      this.processing = false;
    }
  }
}

export interface DebouncedEmitterStats {
  readonly pending: number;
  readonly coalesced_total: number;
  readonly external_emits_total: number;
}

/** One replaceable trailing timer per pool; output itself belongs to Agent E. */
export class DebouncedStateEmitter {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private pendingFingerprint: string | null = null;
  private lastEmittedFingerprint: string | null = null;
  private lastEmitAtMs = Number.NEGATIVE_INFINITY;
  private active = true;
  private coalescedTotal = 0;
  private externalEmitsTotal = 0;

  public constructor(
    private readonly emitLatest: () => void,
    private readonly minimumIntervalMs: number,
    private readonly now: () => number = () => performance.now(),
  ) {
    if (!Number.isFinite(minimumIntervalMs) || minimumIntervalMs < 0) {
      throw new Error("pool state emit interval must be non-negative");
    }
  }

  public request(fingerprint: string, mode: "immediate" | "debounced"): boolean {
    if (!this.active || fingerprint === this.lastEmittedFingerprint) return false;
    if (mode === "immediate") {
      this.cancelTimer();
      this.pendingFingerprint = fingerprint;
      this.flush();
      return true;
    }
    if (this.pendingFingerprint !== null) this.coalescedTotal += 1;
    this.pendingFingerprint = fingerprint;
    if (this.timer === null) {
      const delay = Math.max(0, this.minimumIntervalMs - (this.now() - this.lastEmitAtMs));
      this.timer = setTimeout(() => {
        this.timer = null;
        this.flush();
      }, delay);
    }
    return true;
  }

  public flushForTest(): void {
    this.cancelTimer();
    this.flush();
  }

  public dispose(): void {
    this.active = false;
    this.cancelTimer();
    this.pendingFingerprint = null;
  }

  public stats(): DebouncedEmitterStats {
    return {
      pending: this.pendingFingerprint === null ? 0 : 1,
      coalesced_total: this.coalescedTotal,
      external_emits_total: this.externalEmitsTotal,
    };
  }

  private flush(): void {
    const fingerprint = this.pendingFingerprint;
    this.pendingFingerprint = null;
    if (!this.active || fingerprint === null || fingerprint === this.lastEmittedFingerprint) return;
    this.emitLatest();
    this.lastEmittedFingerprint = fingerprint;
    this.lastEmitAtMs = this.now();
    this.externalEmitsTotal += 1;
  }

  private cancelTimer(): void {
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
  }
}

/** FNV-1a gives deterministic staggering without test-host randomness. */
export function deterministicStaggerMs(identity: string, windowMs: number): number {
  if (!Number.isFinite(windowMs) || windowMs <= 0) return 0;
  let hash = 0x811c9dc5;
  for (let index = 0; index < identity.length; index += 1) {
    hash ^= identity.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash % Math.floor(windowMs);
}

export function coreRefreshDue(
  provenance: PoolSlotProvenance,
  identity: string,
  nowMs: number,
  refreshAfterMs: number,
  staggerWindowMs: number,
): boolean {
  return coreRefreshOverdueMs(
    provenance,
    identity,
    nowMs,
    refreshAfterMs,
    staggerWindowMs,
  ) >= 0;
}

/** Negative means fresh; zero and above is the factual overdue duration. */
export function coreRefreshOverdueMs(
  provenance: PoolSlotProvenance,
  identity: string,
  nowMs: number,
  refreshAfterMs: number,
  staggerWindowMs: number,
): number {
  const freshness = Math.max(
    provenance.coreReceivedAtMs,
    provenance.lastSuccessfulRpcRefreshAtMs ?? Number.NEGATIVE_INFINITY,
  );
  return nowMs - (freshness + refreshAfterMs + deterministicStaggerMs(identity, staggerWindowMs));
}
