/** JSON-lines entrypoint for the read-only local Solana quote worker. */

import readline from "node:readline";

import {
  closeProtocolOutput,
  emit,
  emitState,
  parseWorkerInput,
  protocolOutputMetrics,
  safeError,
  setProtocolOutputFatalHandler,
  type ConfigureMessage,
} from "./protocol.js";
import { RaydiumClmmQuoteEngine } from "./raydiumClmm.js";
import { MeteoraDlmmQuoteEngine } from "./meteoraDlmm.js";
import { OrcaWhirlpoolQuoteEngine } from "./orcaWhirlpool.js";
import { RaydiumStandardQuoteEngine } from "./raydiumStandard.js";
import type { RaydiumCpmmSimulationCapture } from "./raydiumStandard.js";
import {
  closeRpcScheduler,
  configureRpcPacer,
  rpcSchedulerMetrics,
  withRpcJobOptions,
} from "./rpcPacer.js";
import {
  freezeSnapshot,
  raydiumCpmmSnapshotBundle,
  type SnapshotBundle,
} from "./simulation/snapshots.js";
import {
  simulatePathLegs,
  type SimulatePathLegInput,
  type SimulatePathResult,
} from "./simulation/path.js";

let raydiumEngine: RaydiumClmmQuoteEngine | null = null;
let meteoraEngine: MeteoraDlmmQuoteEngine | null = null;
let orcaEngine: OrcaWhirlpoolQuoteEngine | null = null;
let raydiumStandardEngine: RaydiumStandardQuoteEngine | null = null;
const workerGeneration = Math.floor(Math.random() * 0xffffff) + 1;
const bootId = `${process.pid.toString(36)}-${workerGeneration.toString(36)}-${Date.now().toString(36)}`;
interface SnapshotRegistryEntry {
  readonly kind: "raydium_cpmm";
  readonly slot: number;
  readonly bundle: SnapshotBundle;
  readonly stored_at_monotonic_ns: bigint;
  bytes: number;
  simulation?: {
    readonly request_id: string;
    readonly snapshot_token: string;
    readonly legs: readonly SimulatePathLegInput[];
    readonly initial_balances: readonly { asset_id: string; amount_raw: string }[];
    readonly deadline_monotonic_ns?: string;
    readonly result: SimulatePathResult;
  };
}

const simulationSnapshots = new Map<string, SnapshotRegistryEntry>();
const canceledSimulationRequests = new Set<string>();
const MAX_SNAPSHOT_BUNDLES = 32;
const MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024;
const DEFAULT_SNAPSHOT_TTL_NS = 30_000_000_000n;
const MAX_CANCELED_REQUESTS = 256;
let snapshotRegistryBytes = 0;
// A source epoch identifies this worker/source incarnation.  Individual
// snapshots are distinguished by snapshot_id/context_slot; taking another
// snapshot must not silently create a new source epoch.
const sourceEpoch = workerGeneration;
let snapshotTtlNs = DEFAULT_SNAPSHOT_TTL_NS;
const simulationRegistryCounters = { hits: 0, misses: 0, evictions: 0, byte_evictions: 0 };

function monotonicNs(): bigint { return process.hrtime.bigint(); }

function evictExpiredSnapshots(now = monotonicNs()): void {
  for (const [token, entry] of simulationSnapshots) {
    const validUntil = "state_valid_until_monotonic_ns" in entry.bundle
      ? entry.bundle.state_valid_until_monotonic_ns
      : undefined;
    if (now - entry.stored_at_monotonic_ns >= snapshotTtlNs
      || (validUntil !== undefined && now >= BigInt(validUntil))) {
      simulationSnapshots.delete(token);
      snapshotRegistryBytes -= entry.bytes;
      simulationRegistryCounters.evictions += 1;
    }
  }
}

function getSnapshotEntry(token: string): SnapshotRegistryEntry | undefined {
  evictExpiredSnapshots();
  const entry = simulationSnapshots.get(token);
  if (entry === undefined) {
    simulationRegistryCounters.misses += 1;
    return undefined;
  }
  simulationRegistryCounters.hits += 1;
  return entry;
}

function removeSnapshot(token: string, byteEviction = false): void {
  const entry = simulationSnapshots.get(token);
  if (entry === undefined) return;
  simulationSnapshots.delete(token);
  snapshotRegistryBytes -= entry.bytes;
  simulationRegistryCounters.evictions += 1;
  if (byteEviction) simulationRegistryCounters.byte_evictions += 1;
}

function rememberSnapshot(token: string, bundle: SnapshotBundle, slot: number): void {
  evictExpiredSnapshots();
  const storedAt = monotonicNs();
  const immutableBundle = freezeSnapshot(bundle);
  const bytes = Buffer.byteLength(JSON.stringify(immutableBundle), "utf8");
  const previous = simulationSnapshots.get(token);
  if (previous !== undefined) snapshotRegistryBytes -= previous.bytes;
  simulationSnapshots.set(token, {
    kind: "raydium_cpmm",
    slot,
    bundle: immutableBundle,
    stored_at_monotonic_ns: storedAt,
    bytes,
  });
  snapshotRegistryBytes += bytes;
  while (simulationSnapshots.size > MAX_SNAPSHOT_BUNDLES || snapshotRegistryBytes > MAX_SNAPSHOT_BYTES) {
    const oldest = simulationSnapshots.keys().next().value;
    if (typeof oldest !== "string") break;
    removeSnapshot(oldest, snapshotRegistryBytes > MAX_SNAPSHOT_BYTES);
  }
}

/** Account the bounded in-memory request/result journal as well as the snapshot. */
function rememberSimulationResult(
  token: string,
  entry: SnapshotRegistryEntry,
  simulation: NonNullable<SnapshotRegistryEntry["simulation"]>,
): boolean {
  const nextBytes = Buffer.byteLength(
    JSON.stringify({ snapshot: entry.bundle, simulation }),
    "utf8",
  );
  if (nextBytes > MAX_SNAPSHOT_BYTES) return false;
  const previousBytes = entry.bytes;
  entry.simulation = simulation;
  entry.bytes = nextBytes;
  snapshotRegistryBytes += nextBytes - previousBytes;
  while (simulationSnapshots.size > MAX_SNAPSHOT_BUNDLES || snapshotRegistryBytes > MAX_SNAPSHOT_BYTES) {
    const oldest = simulationSnapshots.keys().next().value;
    if (typeof oldest !== "string") break;
    removeSnapshot(oldest, snapshotRegistryBytes > MAX_SNAPSHOT_BYTES);
  }
  return simulationSnapshots.get(token) === entry;
}

function markCanceled(requestId: string): void {
  canceledSimulationRequests.add(requestId);
  while (canceledSimulationRequests.size > MAX_CANCELED_REQUESTS) {
    const oldest = canceledSimulationRequests.values().next().value;
    if (typeof oldest !== "string") break;
    canceledSimulationRequests.delete(oldest);
  }
}

/**
 * Offline handler seam used by tests and deterministic replay probes.  It
 * mirrors the worker's request lifecycle without opening readline, RPC, or a
 * quote engine: token lookup, TTL/cap eviction, cancellation, deadline,
 * ordered CPMM path execution, and evidence identity all remain testable.
 */
export class OfflineSimulationHandler {
  private readonly entries = new Map<string, {
    bundle: SnapshotBundle;
    stored_at_monotonic_ns: bigint;
    bytes: number;
    simulation?: {
      request_id: string;
      snapshot_token: string;
      legs: readonly SimulatePathLegInput[];
      initial_balances: readonly { asset_id: string; amount_raw: string }[];
      deadline_monotonic_ns?: string;
      result: SimulatePathResult;
    };
  }>();
  private readonly canceled = new Set<string>();
  private bytes = 0;
  private readonly counters = { hits: 0, misses: 0, evictions: 0 };

  public constructor(
    private readonly options: {
      readonly now_monotonic_ns?: () => bigint;
      readonly ttl_ns?: bigint;
      readonly max_items?: number;
      readonly max_bytes?: number;
    } = {},
  ) {}

  public get registrySize(): number { return this.entries.size; }
  public get registryBytes(): number { return this.bytes; }
  public get registryCounters(): Readonly<typeof this.counters> { return { ...this.counters }; }

  private now(): bigint { return this.options.now_monotonic_ns?.() ?? process.hrtime.bigint(); }

  private evict(): void {
    const now = this.now();
    const ttl = this.options.ttl_ns ?? DEFAULT_SNAPSHOT_TTL_NS;
    for (const [token, entry] of this.entries) {
      const validUntil = "state_valid_until_monotonic_ns" in entry.bundle
        ? entry.bundle.state_valid_until_monotonic_ns
        : undefined;
      if (now - entry.stored_at_monotonic_ns >= ttl
        || (validUntil !== undefined && now >= BigInt(validUntil))) {
        this.entries.delete(token);
        this.bytes -= entry.bytes;
        this.counters.evictions += 1;
      }
    }
  }

  public put(token: string, bundle: SnapshotBundle): void {
    this.evict();
    const immutable = freezeSnapshot(bundle);
    const bytes = Buffer.byteLength(JSON.stringify(immutable), "utf8");
    const previous = this.entries.get(token);
    if (previous !== undefined) this.bytes -= previous.bytes;
    this.entries.set(token, { bundle: immutable, stored_at_monotonic_ns: this.now(), bytes });
    this.bytes += bytes;
    this.trim();
  }

  private trim(): void {
    const maxItems = this.options.max_items ?? MAX_SNAPSHOT_BUNDLES;
    const maxBytes = this.options.max_bytes ?? MAX_SNAPSHOT_BYTES;
    while (this.entries.size > maxItems || this.bytes > maxBytes) {
      const token = this.entries.keys().next().value;
      if (typeof token !== "string") break;
      const entry = this.entries.get(token);
      if (entry === undefined) break;
      this.entries.delete(token);
      this.bytes -= entry.bytes;
      this.counters.evictions += 1;
    }
  }

  public cancel(requestId: string): void {
    this.canceled.add(requestId);
    while (this.canceled.size > MAX_CANCELED_REQUESTS) {
      const oldest = this.canceled.values().next().value;
      if (typeof oldest !== "string") break;
      this.canceled.delete(oldest);
    }
  }

  public simulate(request: {
    request_id: string;
    snapshot_token: string;
    legs: readonly SimulatePathLegInput[];
    initial_balances: readonly { asset_id: string; amount_raw: string }[];
    deadline_monotonic_ns?: string;
  }): Record<string, unknown> | undefined {
    this.evict();
    if (this.canceled.has(request.request_id)) {
      this.canceled.delete(request.request_id);
      return undefined;
    }
    const entry = this.entries.get(request.snapshot_token);
    if (entry === undefined) {
      this.counters.misses += 1;
      return {
        type: "simulate_path_result",
        request_id: request.request_id,
        status: "state_unavailable",
        complete: false,
        reason: "snapshot token is unknown or expired",
      };
    }
    this.counters.hits += 1;
    const deadline = request.deadline_monotonic_ns === undefined
      ? undefined
      : BigInt(request.deadline_monotonic_ns);
    if (deadline !== undefined && this.now() >= deadline) {
      return {
        type: "simulate_path_result",
        request_id: request.request_id,
        status: "deadline_exceeded",
        complete: false,
        reason: "deadline expired before simulation",
      };
    }
    const result = simulatePathLegs(entry.bundle, request.legs, request.initial_balances, {
      ...(deadline === undefined ? {} : { deadline_monotonic_ns: deadline }),
      now_monotonic_ns: () => this.now(),
    });
    if (this.canceled.has(request.request_id)) {
      this.canceled.delete(request.request_id);
      return undefined;
    }
    const simulation = { ...request, result };
    const nextBytes = Buffer.byteLength(
      JSON.stringify({ snapshot: entry.bundle, simulation }),
      "utf8",
    );
    if (nextBytes > (this.options.max_bytes ?? MAX_SNAPSHOT_BYTES)) {
      return {
        type: "simulate_path_result",
        request_id: request.request_id,
        snapshot_token: request.snapshot_token,
        status: "state_unavailable",
        complete: false,
        reason: "bounded simulation registry cannot retain this result",
      };
    }
    this.bytes += nextBytes - entry.bytes;
    entry.bytes = nextBytes;
    entry.simulation = simulation;
    this.trim();
    if (this.entries.get(request.snapshot_token) !== entry) return undefined;
    return {
      type: "simulate_path_result",
      request_id: request.request_id,
      snapshot_token: request.snapshot_token,
      status: result.status,
      complete: result.complete,
      reason: result.reason,
      leg_results: result.leg_results,
      final_balances: result.final_balances,
      snapshot_id: entry.bundle.snapshot_id,
      worker_generation: entry.bundle.worker_generation,
      boot_id: entry.bundle.boot_id,
      source_epoch: entry.bundle.source_epoch,
      context_slot: entry.bundle.context_slot,
    };
  }

  public evidence(requestId: string, token: string): Record<string, unknown> | undefined {
    this.evict();
    const entry = this.entries.get(token);
    if (entry?.simulation === undefined) return undefined;
    return {
      schema_version: 1,
      evidence_kind: "raydium_cpmm_simulation",
      snapshot_token: token,
      request: entry.simulation,
      snapshot: entry.bundle,
      result: entry.simulation.result,
      metadata: {
        request_id: requestId,
        snapshot_id: entry.bundle.snapshot_id,
        worker_generation: entry.bundle.worker_generation,
        boot_id: entry.bundle.boot_id,
        source_epoch: entry.bundle.source_epoch,
        context_slot: entry.bundle.context_slot,
        wallet_or_private_key_used: false,
        transactions_submitted: false,
      },
    };
  }
}

let configured = false;
let closing = false;
const inFlightQuotes = new Set<Promise<void>>();
const MAX_IN_FLIGHT_QUOTES = 32;
const DEFAULT_STATE_SNAPSHOT_REFRESH_INTERVAL_MS = 15_000;
const DEFAULT_MAINTENANCE_SCAN_INTERVAL_MS = 1_000;
const MAX_CONSECUTIVE_STATE_SNAPSHOT_ERRORS = 3;
let stateSnapshotRefreshIntervalMs = DEFAULT_STATE_SNAPSHOT_REFRESH_INTERVAL_MS;
let maintenanceScanIntervalMs = DEFAULT_MAINTENANCE_SCAN_INTERVAL_MS;
let stateSnapshotRefreshTimer: ReturnType<typeof setTimeout> | null = null;
let stateSnapshotRefreshTask: Promise<void> | null = null;
let stateSnapshotRefreshFailed = false;
let consecutiveStateSnapshotErrors = 0;
let fatalErrorReported = false;

interface SnapshotRefreshEngine {
  refreshAllPoolStates(): Promise<void>;
  runtimeStats(): Record<string, number>;
}

function reportFatal(stage: string, error: unknown): void {
  if (fatalErrorReported || closing) return;
  fatalErrorReported = true;
  emit({ type: "worker_error", stage, error: safeError(error) });
  void shutdown(1).finally(() => process.exit(1));
}

function installProcessHandlers(): void {
  setProtocolOutputFatalHandler((error) => {
    if (fatalErrorReported) return;
    fatalErrorReported = true;
    process.stderr.write(`[worker-output-fatal] ${safeError(error)}\n`);
    void shutdown(1).finally(() => process.exit(1));
  });
  process.on("uncaughtException", (error) => reportFatal("uncaught_exception", error));
  process.on("unhandledRejection", (error) => reportFatal("unhandled_rejection", error));
}

async function refreshStateSnapshots(): Promise<void> {
  const failures: Record<string, string> = {};
  const engines: Array<[string, SnapshotRefreshEngine | null]> = [
    ["raydium_clmm", raydiumEngine],
    ["raydium_standard", raydiumStandardEngine],
    ["meteora_dlmm", meteoraEngine],
    ["orca_whirlpool", orcaEngine],
  ];
  // Keep the refresh burst gentle for free RPC plans. Each engine itself uses
  // a bounded batch, and quote processing remains independently concurrent.
  for (const [protocol, engine] of engines) {
    if (engine === null) continue;
    try {
      await engine.refreshAllPoolStates();
    } catch (error) {
      failures[protocol] = safeError(error);
    }
  }
  const failedProtocols = Object.keys(failures);
  const engineMetrics = Object.fromEntries(
    engines.flatMap(([protocol, engine]) => engine === null ? [] : [[protocol, engine.runtimeStats()]]),
  );
  if (failedProtocols.length === 0) {
    consecutiveStateSnapshotErrors = 0;
    emitState("refresh_health", {
      type: "refresh_health",
      status: "ok",
      interval_ms: maintenanceScanIntervalMs,
      core_refresh_after_ms: stateSnapshotRefreshIntervalMs,
      consecutive_errors: 0,
      engine_metrics: engineMetrics,
      output_metrics: protocolOutputMetrics(),
      rpc_metrics: rpcSchedulerMetrics(),
    });
    return;
  }
  consecutiveStateSnapshotErrors += 1;
  emitState("refresh_health", {
    type: "refresh_health",
    status: "error",
    interval_ms: maintenanceScanIntervalMs,
    core_refresh_after_ms: stateSnapshotRefreshIntervalMs,
    failed_protocols: failedProtocols,
    errors: failures,
    consecutive_errors: consecutiveStateSnapshotErrors,
    engine_metrics: engineMetrics,
    output_metrics: protocolOutputMetrics(),
    rpc_metrics: rpcSchedulerMetrics(),
  });
  if (consecutiveStateSnapshotErrors >= MAX_CONSECUTIVE_STATE_SNAPSHOT_ERRORS) {
    stateSnapshotRefreshFailed = true;
    emit({
      type: "worker_error",
      stage: "state_snapshot_refresh",
      error: `state snapshot refresh failed ${consecutiveStateSnapshotErrors} consecutive times for ${failedProtocols.join(",")}`,
    });
  }
}

function scheduleStateSnapshotRefresh(): void {
  if (closing || stateSnapshotRefreshFailed) return;
  stateSnapshotRefreshTimer = setTimeout(() => {
    stateSnapshotRefreshTimer = null;
    stateSnapshotRefreshTask = refreshStateSnapshots().finally(() => {
      stateSnapshotRefreshTask = null;
      scheduleStateSnapshotRefresh();
    });
  }, maintenanceScanIntervalMs);
}

async function shutdown(exitCode = 0): Promise<void> {
  if (closing) return;
  closing = true;
  if (stateSnapshotRefreshTimer !== null) {
    clearTimeout(stateSnapshotRefreshTimer);
    stateSnapshotRefreshTimer = null;
  }
  if (stateSnapshotRefreshTask !== null) {
    await stateSnapshotRefreshTask.catch(() => undefined);
  }
  try {
    await Promise.all([
      raydiumEngine?.close(),
      raydiumStandardEngine?.close(),
      meteoraEngine?.close(),
      orcaEngine?.close(),
    ]);
  } catch (error) {
    emit({ type: "worker_error", stage: "shutdown", error: safeError(error) });
    exitCode = 1;
  }
  try {
    await closeRpcScheduler();
  } catch (error) {
    emit({ type: "worker_error", stage: "rpc_scheduler_shutdown", error: safeError(error) });
    exitCode = 1;
  }
  try {
    await closeProtocolOutput();
  } catch (error) {
    process.stderr.write(`[worker-output-shutdown-error] ${safeError(error)}\n`);
    exitCode = 1;
  }
  process.exitCode = exitCode;
}

async function configure(message: ConfigureMessage): Promise<void> {
  if (configured) throw new Error("worker already configured");
  stateSnapshotRefreshIntervalMs = message.state_snapshot_refresh_interval_ms
    ?? DEFAULT_STATE_SNAPSHOT_REFRESH_INTERVAL_MS;
  maintenanceScanIntervalMs = message.maintenance_scan_interval_ms
    ?? DEFAULT_MAINTENANCE_SCAN_INTERVAL_MS;
  snapshotTtlNs = BigInt(message.simulation_snapshot_ttl_ms ?? 30_000) * 1_000_000n;
  configureRpcPacer(
    message.rpc_http_min_request_interval_ms,
    message.rpc_max_pending_jobs,
  );
  if (message.raydium_clmm_pools.length > 0) {
    raydiumEngine = new RaydiumClmmQuoteEngine({
      onPoolState: (notice) => emitState(
        `pool_state:raydium_clmm:${notice.pool_id}`,
        { type: "pool_state", protocol: "raydium_clmm", ...notice },
      ),
    });
    await withRpcJobOptions(
      { priority: "bootstrap", description: "bootstrap Raydium CLMM engine" },
      () => raydiumEngine!.open(message),
    );
  }
  if (message.raydium_standard_pools.length > 0) {
    raydiumStandardEngine = new RaydiumStandardQuoteEngine({
      onPoolState: (protocol, notice) => emitState(
        `pool_state:${protocol}:${notice.pool_id}`,
        { type: "pool_state", protocol, ...notice },
      ),
    });
    await withRpcJobOptions(
      { priority: "bootstrap", description: "bootstrap Raydium standard engine" },
      () => raydiumStandardEngine!.open(message),
    );
  }
  if (message.meteora_dlmm_pools.length > 0) {
    meteoraEngine = new MeteoraDlmmQuoteEngine({
      onPoolState: (notice) => emitState(
        `pool_state:meteora_dlmm:${notice.pool_id}`,
        { type: "pool_state", protocol: "meteora_dlmm", ...notice },
      ),
    });
    await withRpcJobOptions(
      { priority: "bootstrap", description: "bootstrap Meteora DLMM engine" },
      () => meteoraEngine!.open(message),
    );
  }
  if (message.orca_whirlpool_pools.length > 0) {
    orcaEngine = new OrcaWhirlpoolQuoteEngine({
      onPoolState: (notice) => emitState(
        `pool_state:orca_whirlpool:${notice.pool_id}`,
        { type: "pool_state", protocol: "orca_whirlpool", ...notice },
      ),
    });
    await withRpcJobOptions(
      { priority: "bootstrap", description: "bootstrap Orca Whirlpool engine" },
      () => orcaEngine!.open(message),
    );
  }
  configured = true;
  emit({
    type: "ready",
    protocols: [
      ...(raydiumEngine === null ? [] : ["raydium_clmm"]),
      ...(raydiumStandardEngine === null
        ? []
        : [...new Set(message.raydium_standard_pools.map((pool) => pool.protocol))]),
      ...(meteoraEngine === null ? [] : ["meteora_dlmm"]),
      ...(orcaEngine === null ? [] : ["orca_whirlpool"]),
    ],
    configured_pool_count: message.raydium_clmm_pools.length
      + message.raydium_standard_pools.length
      + message.meteora_dlmm_pools.length
      + message.orca_whirlpool_pools.length,
    raydium_clmm_pool_count: message.raydium_clmm_pools.length,
    raydium_standard_pool_count: message.raydium_standard_pools.length,
    meteora_dlmm_pool_count: message.meteora_dlmm_pools.length,
    orca_whirlpool_pool_count: message.orca_whirlpool_pools.length,
    state_snapshot_refresh_interval_ms: stateSnapshotRefreshIntervalMs,
    core_refresh_after_ms: message.core_refresh_after_ms ?? stateSnapshotRefreshIntervalMs,
    maintenance_scan_interval_ms: maintenanceScanIntervalMs,
    refresh_stagger_window_ms: message.refresh_stagger_window_ms ?? 5_000,
    pool_state_emit_min_interval_ms: message.pool_state_emit_min_interval_ms ?? 100,
    simulation_snapshot_ttl_ms: Number(snapshotTtlNs / 1_000_000n),
    rpc_http_min_request_interval_ms: message.rpc_http_min_request_interval_ms ?? 200,
    rpc_max_pending_jobs: rpcSchedulerMetrics().rpc_queue_capacity,
    output_metrics: protocolOutputMetrics(),
    rpc_metrics: rpcSchedulerMetrics(),
    wallet_or_private_key_used: false,
    transactions_submitted: false,
    simulation_capabilities: {
      synthetic_cpmm_v1: true,
      raydium_cpmm: "supported",
      orca_whirlpool: "supported_offline_core",
      raydium_clmm: "unsupported_pending_adapter",
      meteora_dlmm: "unsupported_pending_adapter",
      raydium_amm_v4: "unsupported_pending_adapter",
    },
    canonical_codec: "sha256_sorted_keys_decimal_strings_v1",
  });
  scheduleStateSnapshotRefresh();
}

async function handleQuote(message: Extract<ReturnType<typeof parseWorkerInput>, { type: "quote_request" }>): Promise<void> {
  try {
    if (!configured) throw new Error("worker must be configured before quote requests");
    const activeRaydium = raydiumEngine as RaydiumClmmQuoteEngine | null;
    const activeRaydiumStandard = raydiumStandardEngine as RaydiumStandardQuoteEngine | null;
    const activeMeteora = meteoraEngine as MeteoraDlmmQuoteEngine | null;
    const activeOrca = orcaEngine as OrcaWhirlpoolQuoteEngine | null;
    const quote = await withRpcJobOptions(
      {
        priority: "interactive",
        description: `quote ${message.protocol}:${message.pool_id}`,
      },
      async () => message.protocol === "raydium_clmm"
        ? activeRaydium?.quote(message)
        : message.protocol === "raydium_cpmm" || message.protocol === "raydium_amm_v4"
          ? activeRaydiumStandard?.quote(message)
          : message.protocol === "meteora_dlmm"
            ? activeMeteora?.quote(message)
            : activeOrca?.quote(message),
    );
    if (quote === undefined) throw new Error(`protocol ${message.protocol} is not configured`);
    emit({ type: "quote_result", protocol: message.protocol, ...quote });
  } catch (error) {
    emit({
      type: "worker_error",
      stage: "quote",
      request_id: message.request_id,
      error: safeError(error),
    });
  }
}

async function handleSimulationMessage(message: Extract<
  ReturnType<typeof parseWorkerInput>,
  { type: "simulate_path_request" | "snapshot_request" | "export_simulation_evidence" | "cancel_simulation" }
>): Promise<void> {
  if (!configured) throw new Error("worker must be configured before simulation requests");
  if (message.type === "snapshot_request") {
    if (message.required_consistency !== "validated_multi_account_snapshot") {
      emit({
        type: "snapshot_result",
        request_id: message.request_id,
        status: "unsupported",
        reason: "atomic validated snapshot capture requires validated_multi_account_snapshot",
        pool_ids: message.pool_ids,
        required_consistency: message.required_consistency,
      });
      return;
    }
    const captured = processSnapshotRequest(message);
    emit({
      type: "snapshot_result",
      request_id: message.request_id,
      pool_ids: message.pool_ids,
      required_consistency: message.required_consistency,
      status: captured.status,
      reason: captured.reason,
      snapshot_token: captured.snapshot_token,
      snapshot: captured.snapshot,
      slot: captured.slot,
    });
    return;
  }
  if (message.type === "simulate_path_request") {
    if (message.deadline_monotonic_ns !== undefined
      && monotonicNs() >= BigInt(message.deadline_monotonic_ns)) {
      emit({ type: "simulate_path_result", request_id: message.request_id, status: "deadline_exceeded", reason: "deadline expired before simulation", snapshot_token: message.snapshot_token, complete: false, leg_results: [] });
      return;
    }
    const capturedEntry = getSnapshotEntry(message.snapshot_token);
    if (capturedEntry === undefined) {
      emit({
        type: "simulate_path_result",
        request_id: message.request_id,
        status: "state_unavailable",
        reason: "snapshot token is unknown or expired",
        snapshot_token: message.snapshot_token,
        complete: false,
        leg_results: [],
      });
      return;
    }
    // Give a same-turn cancel message a chance to be consumed before the
    // synchronous CPU simulation publishes anything.
    await new Promise<void>((resolve) => setImmediate(resolve));
    if (canceledSimulationRequests.delete(message.request_id)) return;
    const balances = (message.initial_balances ?? []).map(
      (entry) => ({ asset_id: entry.asset_id, amount_raw: entry.amount_raw }),
    );
    const simulated = simulatePathLegs(
      capturedEntry.bundle,
      message.legs as readonly SimulatePathLegInput[],
      balances,
      message.deadline_monotonic_ns === undefined
        ? {}
        : {
          deadline_monotonic_ns: BigInt(message.deadline_monotonic_ns),
          now_monotonic_ns: monotonicNs,
        },
    );
    if (canceledSimulationRequests.delete(message.request_id)) return;
    if (message.deadline_monotonic_ns !== undefined
      && monotonicNs() >= BigInt(message.deadline_monotonic_ns)) {
      emit({ type: "simulate_path_result", request_id: message.request_id, snapshot_token: message.snapshot_token, status: "deadline_exceeded", reason: "deadline expired during simulation", complete: false, leg_results: simulated.leg_results });
      return;
    }
    const simulation = {
      request_id: message.request_id,
      snapshot_token: message.snapshot_token,
      legs: message.legs as readonly SimulatePathLegInput[],
      initial_balances: balances,
      ...(message.deadline_monotonic_ns === undefined
        ? {}
        : { deadline_monotonic_ns: message.deadline_monotonic_ns }),
      result: simulated,
    };
    if (!rememberSimulationResult(message.snapshot_token, capturedEntry, simulation)) {
      emit({
        type: "simulate_path_result",
        request_id: message.request_id,
        snapshot_token: message.snapshot_token,
        status: "state_unavailable",
        reason: "bounded simulation registry cannot retain this result",
        complete: false,
        leg_results: simulated.leg_results,
      });
      return;
    }
    emit({
      type: "simulate_path_result",
      request_id: message.request_id,
      snapshot_token: message.snapshot_token,
      status: simulated.status,
      reason: simulated.reason,
      complete: simulated.complete,
      failed_leg_id: simulated.failed_leg_id,
      leg_results: simulated.leg_results,
      final_balances: simulated.final_balances,
      snapshot_id: capturedEntry.bundle.snapshot_id,
      worker_generation: capturedEntry.bundle.worker_generation,
      boot_id: capturedEntry.bundle.boot_id,
      source_epoch: capturedEntry.bundle.source_epoch,
      context_slot: capturedEntry.bundle.context_slot,
      snapshot_registry_hits: simulationRegistryCounters.hits,
      snapshot_registry_misses: simulationRegistryCounters.misses,
      snapshot_registry_evictions: simulationRegistryCounters.evictions,
      snapshot_registry_bytes: snapshotRegistryBytes,
    });
    return;
  }
  if (message.type === "export_simulation_evidence") {
    const capturedEntry = getSnapshotEntry(message.snapshot_token);
    if (capturedEntry === undefined || capturedEntry.simulation === undefined) {
      emit({
        type: "simulation_evidence_result",
        request_id: message.request_id,
        status: "unsupported",
        reason: "simulation_evidence_export_requires_a_completed_simulation_or_live_snapshot",
      });
      return;
    }
    const simulation = capturedEntry.simulation;
    const evidence = {
      schema_version: 1,
      evidence_kind: "raydium_cpmm_simulation",
      snapshot_token: message.snapshot_token,
      request: {
        request_id: simulation.request_id,
        snapshot_token: simulation.snapshot_token,
        legs: simulation.legs,
        initial_balances: simulation.initial_balances,
        ...(simulation.deadline_monotonic_ns === undefined
          ? {}
          : { deadline_monotonic_ns: simulation.deadline_monotonic_ns }),
      },
      snapshot: capturedEntry.bundle,
      result: simulation.result,
      metadata: {
        snapshot_id: capturedEntry.bundle.snapshot_id,
        worker_generation: capturedEntry.bundle.worker_generation,
        boot_id: capturedEntry.bundle.boot_id,
        source_epoch: capturedEntry.bundle.source_epoch,
        context_slot: capturedEntry.bundle.context_slot,
        model_version: capturedEntry.bundle.model_version,
        wallet_or_private_key_used: false,
        transactions_submitted: false,
      },
    };
    if (Buffer.byteLength(JSON.stringify(evidence), "utf8") > MAX_SNAPSHOT_BYTES) {
      emit({
        type: "simulation_evidence_result",
        request_id: message.request_id,
        status: "unsupported",
        reason: "simulation evidence exceeds bounded export size",
      });
      return;
    }
    emit({
      type: "simulation_evidence_result",
      request_id: message.request_id,
      status: "ok",
      snapshot_token: message.snapshot_token,
      evidence,
      reason: "snapshot, request, result and worker identity evidence exported",
    });
    return;
  }
  if (message.type === "cancel_simulation") {
    markCanceled(message.request_id);
    emit({
      type: "cancel_simulation_result",
      request_id: message.request_id,
      status: "ok",
      reason: "simulation cancellation acknowledged",
    });
    return;
  }
}

function processSnapshotRequest(
  message: Extract<ReturnType<typeof parseWorkerInput>, { type: "snapshot_request" }>,
): {
  status: "ok" | "unsupported" | "state_unavailable";
  reason: string;
  snapshot_token: string;
  snapshot: SnapshotBundle;
  slot: number;
  missing_protocols: string[];
} {
  const schedules = {
    status: "state_unavailable" as "ok" | "unsupported" | "state_unavailable",
    reason: "no protocol adapter produced state",
    snapshot_token: "",
    snapshot: undefined as unknown as SnapshotBundle,
    slot: 0,
    missing_protocols: [] as string[],
  };
  const engine = raydiumStandardEngine;
  if (engine === null) {
    schedules.status = "unsupported";
    schedules.reason = "raydium_standard engine is not configured";
    schedules.missing_protocols = ["raydium_cpmm"];
    return schedules;
  }
  const states = message.pool_ids.flatMap((poolId) => {
    try {
      return [engine.cpmmSimulationState(poolId)];
    } catch {
      schedules.missing_protocols.push(poolId);
      return [];
    }
  });
  if (states.length !== message.pool_ids.length) {
    schedules.status = "state_unavailable";
    schedules.reason = "some requested pool ids are not configured for Raydium CPMM";
    return schedules;
  }
  const createdAt = monotonicNs();
  // Compute the snapshot TTL as the minimum of the configured TTL and the
  // remaining lifetime of the oldest underlying state. Freshness comes from
  // the real monotonic receipt/validation data, not from capture time.
  const captures = message.pool_ids.map((poolId) => engine.cpmmSimulationCapture(poolId));
  const coreReceivedAtNs = Math.min(
    ...captures.map((c) => Math.round(c.freshness.core_received_at_monotonic_ms * 1_000_000)),
  );
  const depTimes = captures
    .map((c) => c.freshness.dependency_received_at_monotonic_ms)
    .filter((v): v is number => v !== null);
  const oldestReceiptNs = Math.min(
    coreReceivedAtNs,
    depTimes.length === 0 ? Number.POSITIVE_INFINITY : Math.min(...depTimes.map((v) => Math.round(v * 1_000_000))),
  );
  // remainingNs = oldestReceipt + snapshotTtlNs - now
  // If the oldest state is already stale, fall back to a zero-remaining TTL
  // so the snapshot is immediately expired rather than dishonestly fresh.
  const remainingNs = BigInt(oldestReceiptNs) + snapshotTtlNs - createdAt;
  const effectiveTtlNs = remainingNs > 0n ? remainingNs : 0n;
  const stateValidUntil = createdAt + effectiveTtlNs;
  const bundle = freezeSnapshot({
    ...raydiumCpmmSnapshotBundle(
      bootId,
      workerGeneration,
      message.request_id,
      states,
      sourceEpoch,
    ),
    snapshot_created_at_monotonic_ns: createdAt.toString(10),
    state_valid_until_monotonic_ns: stateValidUntil.toString(10),
  });
  const poolSet = states.map((state) => `solana:mainnet:${state.pool_id}`).sort().join(",");
  const snapshotToken = [
    bootId,
    workerGeneration.toString(10),
    message.request_id,
    bundle.context_slot.toString(10),
    poolSet,
    createdAt.toString(10),
  ].join(":");
  rememberSnapshot(snapshotToken, bundle, bundle.context_slot);
  schedules.status = "ok";
  schedules.reason = "captured";
  schedules.snapshot_token = snapshotToken;
  schedules.snapshot = bundle;
  schedules.slot = bundle.context_slot;
  return schedules;
}

export async function runWorker(): Promise<void> {
  installProcessHandlers();
  const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  for await (const line of input) {
    if (closing || line.trim().length === 0) continue;
    try {
      const message = parseWorkerInput(JSON.parse(line));
      if (message.type === "configure") {
        await configure(message);
      } else if (message.type === "quote_request") {
        const task = handleQuote(message);
        inFlightQuotes.add(task);
        void task.finally(() => inFlightQuotes.delete(task));
        if (inFlightQuotes.size >= MAX_IN_FLIGHT_QUOTES) {
          await Promise.race(inFlightQuotes);
        }
      } else if (
        message.type === "simulate_path_request"
        || message.type === "snapshot_request"
        || message.type === "export_simulation_evidence"
        || message.type === "cancel_simulation"
      ) {
        const task = handleSimulationMessage(message);
        inFlightQuotes.add(task);
        void task.finally(() => inFlightQuotes.delete(task));
        if (inFlightQuotes.size >= MAX_IN_FLIGHT_QUOTES) {
          await Promise.race(inFlightQuotes);
        }
      } else {
        await Promise.allSettled([...inFlightQuotes]);
        await shutdown();
        break;
      }
    } catch (error) {
      emit({ type: "worker_error", error: safeError(error) });
    }
  }

  await Promise.allSettled([...inFlightQuotes]);
  await shutdown();
}

if (process.argv.slice(1).some((argument) => /(^|\/)worker\.(?:ts|js)$/u.test(argument))) {
  await runWorker();
}
