/**
 * Narrow JSON-lines protocol between the Python supervisor and this local
 * TypeScript quote worker.  It deliberately has no instruction, wallet,
 * signer, transaction, or order message type.
 */

import {
  ProtocolEmitter,
  type ProtocolOutputFatalError,
  type ProtocolOutputMetrics,
} from "./outputWriter.js";

export interface PoolDescriptor {
  pool_id: string;
  label: string;
}

export interface RaydiumStandardPoolDescriptor extends PoolDescriptor {
  protocol: "raydium_cpmm" | "raydium_amm_v4";
}

export interface ConfigureMessage {
  type: "configure";
  rpc_http_url: string;
  rpc_ws_url?: string;
  raydium_clmm_pools: PoolDescriptor[];
  raydium_standard_pools: RaydiumStandardPoolDescriptor[];
  meteora_dlmm_pools: PoolDescriptor[];
  orca_whirlpool_pools: PoolDescriptor[];
  tick_cache_max_age_ms?: number;
  state_snapshot_refresh_interval_ms?: number;
  core_refresh_after_ms?: number;
  maintenance_scan_interval_ms?: number;
  refresh_stagger_window_ms?: number;
  pool_state_emit_min_interval_ms?: number;
  rpc_http_min_request_interval_ms?: number;
  rpc_max_pending_jobs?: number;
  simulation_snapshot_ttl_ms?: number;
  worker_stats_refresh_interval_ms?: number;
}

export interface QuoteRequestMessage {
  type: "quote_request";
  request_id: string;
  protocol:
    | "raydium_clmm"
    | "raydium_cpmm"
    | "raydium_amm_v4"
    | "meteora_dlmm"
    | "orca_whirlpool";
  pool_id: string;
  input_mint: string;
  output_mint: string;
  input_amount_raw: string;
  minimum_state_slot?: number;
}

export interface ShutdownMessage {
  type: "shutdown";
}

export interface BalanceEntry {
  readonly asset_id: string;
  readonly amount_raw: string;
}

export interface SimulatePathRequestMessage {
  type: "simulate_path_request";
  request_id: string;
  snapshot_token: string;
  legs: readonly SimulatePathLeg[];
  initial_balances: readonly BalanceEntry[];
  /** Decimal string: monotonic nanoseconds are not safely representable as JS numbers. */
  deadline_monotonic_ns?: string;
}

export interface SimulatePathLeg {
  readonly leg_id: string;
  readonly pool_id: string;
  readonly input_asset_id: string;
  readonly output_asset_id: string;
  readonly mode: "exact_in" | "exact_out";
  readonly amount_source: "literal" | "previous_output";
  readonly amount_raw?: string;
  readonly previous_leg_id?: string;
}

export interface SnapshotRequestMessage {
  type: "snapshot_request";
  request_id: string;
  pool_ids: readonly string[];
  required_consistency: "validated_multi_account_snapshot" | "slot_window_estimate" | "unknown";
}

export interface ExportSimulationEvidenceMessage {
  type: "export_simulation_evidence";
  request_id: string;
  snapshot_token: string;
}

export interface CancelSimulationMessage {
  type: "cancel_simulation";
  request_id: string;
}

/** Optional on-demand request for a fresh worker_stats snapshot. */
export interface WorkerStatsRequestMessage {
  type: "worker_stats_request";
  request_id: string;
}

export interface WorkerStatsResultMessage {
  type: "worker_stats_result";
  request_id: string;
  status: "ok" | "unsupported";
  reason?: string;
  stats?: WorkerStatsMessage;
}

/** Per-pool runtime metrics aggregated across configured engines. */
export interface PoolStatsByProtocol {
  /** Total configured pools for this protocol. */
  readonly pool_count: number;
  /** Number of pools with an in-flight refresh right now. */
  readonly refresh_inflight: number;
  /** Coalesced dependency/state updates that never reached the wire. */
  readonly coalesced_core_updates_total: number;
  /** External pool_state emissions published to the supervisor. */
  readonly external_pool_state_emits_total: number;
}

/** Memory gauge sampled from `process.memoryUsage()`, units: bytes. */
export interface WorkerMemoryStats {
  readonly rss_bytes: number;
  readonly heap_total_bytes: number;
  readonly heap_used_bytes: number;
  readonly external_bytes: number;
  readonly array_buffers_bytes: number;
  /** Worker uptime in seconds (monotonic since process start). */
  readonly uptime_seconds: number;
}

/** Snapshot of worker-level observability counters. */
export interface WorkerStatsMessage {
  readonly type: "worker_stats";
  readonly boot_id: string;
  readonly source_epoch: number;
  readonly memory: WorkerMemoryStats;
  readonly stdout: {
    readonly blocked: boolean;
    readonly lossless_queue_size: number;
    readonly state_pending_keys: number;
    readonly state_coalesced_total: number;
  };
  readonly rpc: {
    readonly queue_total: number;
    readonly queue_interactive: number;
    readonly queue_bootstrap: number;
    readonly queue_refresh: number;
    readonly active: number;
    readonly queue_high_watermark: number;
  };
  readonly pools: Record<string, PoolStatsByProtocol>;
  /** Wall clock ISO-8601 timestamp at which the stats were sampled. */
  readonly sampled_at_iso: string;
}

export type WorkerInput =
  | ConfigureMessage
  | QuoteRequestMessage
  | SimulatePathRequestMessage
  | SnapshotRequestMessage
  | ExportSimulationEvidenceMessage
  | CancelSimulationMessage
  | WorkerStatsRequestMessage
  | ShutdownMessage;

type JsonObject = Record<string, unknown>;
const MAX_SIMULATION_LEGS = 8;
const MAX_INITIAL_BALANCES = 64;

function object(value: unknown): JsonObject {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("worker message must be a JSON object");
  }
  return value as JsonObject;
}

function stringField(payload: JsonObject, name: string): string {
  const value = payload[name];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`worker message field ${name} must be a non-empty string`);
  }
  return value.trim();
}

function positiveIntegerField(payload: JsonObject, name: string): number | undefined {
  const value = payload[name];
  if (value === undefined) return undefined;
  if (!Number.isInteger(value) || typeof value !== "number" || value < 0) {
    throw new Error(`worker message field ${name} must be a non-negative integer`);
  }
  return value;
}

function rawIntegerField(payload: JsonObject, name: string, field: string): string | undefined {
  const value = payload[name];
  if (value === undefined) return undefined;
  // Accept a safe integer for compatibility with older supervisors, but
  // normalize it to a decimal string before it crosses the JSON boundary.
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`${field} must be a canonical base-10 integer string`);
    }
    return String(value);
  }
  if (typeof value !== "string" || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`${field} must be a canonical base-10 integer string`);
  }
  return value;
}

function simulatePathLegs(value: unknown): SimulatePathLeg[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error("simulate_path_request legs must be a non-empty array");
  }
  if (value.length > MAX_SIMULATION_LEGS) {
    throw new Error(`simulate_path_request supports at most ${MAX_SIMULATION_LEGS} legs`);
  }
  return value.map((item) => {
    const payload = object(item);
    const mode = stringField(payload, "mode");
    const amountSource = stringField(payload, "amount_source");
    if (mode !== "exact_in" && mode !== "exact_out") {
      throw new Error("simulation leg mode must be exact_in or exact_out");
    }
    if (amountSource !== "literal" && amountSource !== "previous_output") {
      throw new Error("simulation leg amount_source must be literal or previous_output");
    }
    const amountRaw = payload.amount_raw === undefined
      ? undefined
      : stringField(payload, "amount_raw");
    if (amountRaw !== undefined && !/^[1-9][0-9]*$/.test(amountRaw)) {
      throw new Error("simulation amount_raw must be a positive canonical integer string");
    }
    const leg: SimulatePathLeg = {
      leg_id: stringField(payload, "leg_id"),
      pool_id: stringField(payload, "pool_id"),
      input_asset_id: stringField(payload, "input_asset_id"),
      output_asset_id: stringField(payload, "output_asset_id"),
      mode,
      amount_source: amountSource,
      ...(amountRaw === undefined ? {} : { amount_raw: amountRaw }),
      ...(payload.previous_leg_id === undefined
        ? {}
        : { previous_leg_id: stringField(payload, "previous_leg_id") }),
    };
    return leg;
  });
}

function endpoint(value: string, scheme: "https:" | "wss:", field: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error(`worker message field ${field} must be a URL`);
  }
  if (parsed.protocol !== scheme || !parsed.hostname) {
    throw new Error(`worker message field ${field} must use ${scheme}//`);
  }
  return value;
}

function poolDescriptors(value: unknown, field: string): PoolDescriptor[] {
  if (!Array.isArray(value)) {
    throw new Error(`${field} must be an array`);
  }
  const result = value.map((item) => {
    const payload = object(item);
    return {
      pool_id: stringField(payload, "pool_id"),
      label: stringField(payload, "label"),
    };
  });
  if (new Set(result.map((item) => item.pool_id)).size !== result.length) {
    throw new Error(`${field} pool_id values must be unique`);
  }
  return result;
}

function raydiumStandardPoolDescriptors(value: unknown): RaydiumStandardPoolDescriptor[] {
  const descriptors = poolDescriptors(value, "raydium_standard_pools");
  return descriptors.map((descriptor, index) => {
    const raw = (value as unknown[])[index];
    const payload = object(raw);
    const protocol = stringField(payload, "protocol");
    if (protocol !== "raydium_cpmm" && protocol !== "raydium_amm_v4") {
      throw new Error("Raydium standard pool protocol must be raydium_cpmm or raydium_amm_v4");
    }
    return { ...descriptor, protocol };
  });
}

/** Parse one line without preserving or echoing any endpoint credentials. */
export function parseWorkerInput(value: unknown): WorkerInput {
  const payload = object(value);
  const type = stringField(payload, "type");
  if (type === "configure") {
    const httpUrl = endpoint(stringField(payload, "rpc_http_url"), "https:", "rpc_http_url");
    const wsUrl = payload.rpc_ws_url === undefined
      ? undefined
      : endpoint(stringField(payload, "rpc_ws_url"), "wss:", "rpc_ws_url");
    const tickCacheMaxAgeMs = positiveIntegerField(payload, "tick_cache_max_age_ms");
    if (tickCacheMaxAgeMs !== undefined && tickCacheMaxAgeMs === 0) {
      throw new Error("tick_cache_max_age_ms must be positive when supplied");
    }
    const stateSnapshotRefreshIntervalMs = positiveIntegerField(
      payload,
      "state_snapshot_refresh_interval_ms",
    );
    if (stateSnapshotRefreshIntervalMs !== undefined && stateSnapshotRefreshIntervalMs < 1_000) {
      throw new Error("state_snapshot_refresh_interval_ms must be at least 1000 when supplied");
    }
    const coreRefreshAfterMs = positiveIntegerField(payload, "core_refresh_after_ms");
    if (coreRefreshAfterMs !== undefined && coreRefreshAfterMs < 1_000) {
      throw new Error("core_refresh_after_ms must be at least 1000 when supplied");
    }
    const maintenanceScanIntervalMs = positiveIntegerField(payload, "maintenance_scan_interval_ms");
    if (maintenanceScanIntervalMs !== undefined && maintenanceScanIntervalMs < 100) {
      throw new Error("maintenance_scan_interval_ms must be at least 100 when supplied");
    }
    const refreshStaggerWindowMs = positiveIntegerField(payload, "refresh_stagger_window_ms");
    const effectiveCoreRefreshAfterMs = coreRefreshAfterMs
      ?? stateSnapshotRefreshIntervalMs
      ?? 15_000;
    if (
      refreshStaggerWindowMs !== undefined
      && (refreshStaggerWindowMs === 0 || refreshStaggerWindowMs > effectiveCoreRefreshAfterMs)
    ) {
      throw new Error("refresh_stagger_window_ms must be positive and no larger than core refresh");
    }
    const poolStateEmitMinIntervalMs = positiveIntegerField(
      payload,
      "pool_state_emit_min_interval_ms",
    );
    if (poolStateEmitMinIntervalMs !== undefined && poolStateEmitMinIntervalMs === 0) {
      throw new Error("pool_state_emit_min_interval_ms must be positive when supplied");
    }
    const rpcHttpMinRequestIntervalMs = positiveIntegerField(
      payload,
      "rpc_http_min_request_interval_ms",
    );
    if (rpcHttpMinRequestIntervalMs !== undefined && rpcHttpMinRequestIntervalMs < 25) {
      throw new Error("rpc_http_min_request_interval_ms must be at least 25 when supplied");
    }
    const rpcMaxPendingJobs = positiveIntegerField(payload, "rpc_max_pending_jobs");
    if (rpcMaxPendingJobs !== undefined && rpcMaxPendingJobs === 0) {
      throw new Error("rpc_max_pending_jobs must be positive when supplied");
    }
    const simulationSnapshotTtlMs = positiveIntegerField(payload, "simulation_snapshot_ttl_ms");
    if (simulationSnapshotTtlMs !== undefined && simulationSnapshotTtlMs === 0) {
      throw new Error("simulation_snapshot_ttl_ms must be positive when supplied");
    }
    const workerStatsRefreshIntervalMs = positiveIntegerField(payload, "worker_stats_refresh_interval_ms");
    if (workerStatsRefreshIntervalMs !== undefined && workerStatsRefreshIntervalMs === 0) {
      throw new Error("worker_stats_refresh_interval_ms must be positive when supplied");
    }
    const raydiumPools = poolDescriptors(payload.raydium_clmm_pools ?? [], "raydium_clmm_pools");
    const raydiumStandardPools = raydiumStandardPoolDescriptors(payload.raydium_standard_pools ?? []);
    const meteoraPools = poolDescriptors(payload.meteora_dlmm_pools ?? [], "meteora_dlmm_pools");
    const orcaPools = poolDescriptors(payload.orca_whirlpool_pools ?? [], "orca_whirlpool_pools");
    const allPoolIds = [...raydiumPools, ...raydiumStandardPools, ...meteoraPools, ...orcaPools]
      .map((item) => item.pool_id);
    if (allPoolIds.length === 0) {
      throw new Error("at least one local quote pool must be configured");
    }
    if (new Set(allPoolIds).size !== allPoolIds.length) {
      throw new Error("pool_id values must be unique across protocols");
    }
    return {
      type,
      rpc_http_url: httpUrl,
      ...(wsUrl === undefined ? {} : { rpc_ws_url: wsUrl }),
      raydium_clmm_pools: raydiumPools,
      raydium_standard_pools: raydiumStandardPools,
      meteora_dlmm_pools: meteoraPools,
      orca_whirlpool_pools: orcaPools,
      ...(tickCacheMaxAgeMs === undefined ? {} : { tick_cache_max_age_ms: tickCacheMaxAgeMs }),
      ...(stateSnapshotRefreshIntervalMs === undefined
        ? {}
        : { state_snapshot_refresh_interval_ms: stateSnapshotRefreshIntervalMs }),
      ...(coreRefreshAfterMs === undefined ? {} : { core_refresh_after_ms: coreRefreshAfterMs }),
      ...(maintenanceScanIntervalMs === undefined
        ? {}
        : { maintenance_scan_interval_ms: maintenanceScanIntervalMs }),
      ...(refreshStaggerWindowMs === undefined
        ? {}
        : { refresh_stagger_window_ms: refreshStaggerWindowMs }),
      ...(poolStateEmitMinIntervalMs === undefined
        ? {}
        : { pool_state_emit_min_interval_ms: poolStateEmitMinIntervalMs }),
      ...(rpcHttpMinRequestIntervalMs === undefined
        ? {}
        : { rpc_http_min_request_interval_ms: rpcHttpMinRequestIntervalMs }),
      ...(rpcMaxPendingJobs === undefined ? {} : { rpc_max_pending_jobs: rpcMaxPendingJobs }),
      ...(simulationSnapshotTtlMs === undefined
        ? {}
        : { simulation_snapshot_ttl_ms: simulationSnapshotTtlMs }),
      ...(workerStatsRefreshIntervalMs === undefined
        ? {}
        : { worker_stats_refresh_interval_ms: workerStatsRefreshIntervalMs }),
    };
  }
  if (type === "quote_request") {
    const protocol = stringField(payload, "protocol");
    if (
      protocol !== "raydium_clmm"
      && protocol !== "raydium_cpmm"
      && protocol !== "raydium_amm_v4"
      && protocol !== "meteora_dlmm"
      && protocol !== "orca_whirlpool"
    ) {
      throw new Error("unsupported local quote protocol");
    }
    const rawAmount = stringField(payload, "input_amount_raw");
    if (!/^[1-9][0-9]*$/.test(rawAmount)) {
      throw new Error("input_amount_raw must be a positive base-10 integer string");
    }
    const slot = positiveIntegerField(payload, "minimum_state_slot");
    return {
      type,
      request_id: stringField(payload, "request_id"),
      protocol,
      pool_id: stringField(payload, "pool_id"),
      input_mint: stringField(payload, "input_mint"),
      output_mint: stringField(payload, "output_mint"),
      input_amount_raw: rawAmount,
      ...(slot === undefined ? {} : { minimum_state_slot: slot }),
    };
  }
  if (type === "shutdown") return { type };
  if (type === "simulate_path_request") {
    const deadline = rawIntegerField(payload, "deadline_monotonic_ns", "deadline_monotonic_ns");
    const rawBalances = payload.initial_balances;
    if (!Array.isArray(rawBalances) || rawBalances.length === 0) {
      throw new Error("simulate_path_request requires non-empty initial_balances");
    }
    if (rawBalances.length > MAX_INITIAL_BALANCES) {
      throw new Error(`simulate_path_request supports at most ${MAX_INITIAL_BALANCES} initial balances`);
    }
    const balances = rawBalances.map((entry: unknown) => {
      const obj = object(entry);
      const amount = stringField(obj, "amount_raw");
      if (!/^(0|[1-9][0-9]*)$/.test(amount)) {
        throw new Error("balance amount_raw must be a canonical non-negative integer string");
      }
      return { asset_id: stringField(obj, "asset_id"), amount_raw: amount };
    });
    if (new Set(balances.map((entry) => entry.asset_id)).size !== balances.length) {
      throw new Error("initial_balances asset_id values must be unique");
    }
    return {
      type,
      request_id: stringField(payload, "request_id"),
      snapshot_token: stringField(payload, "snapshot_token"),
      legs: simulatePathLegs(payload.legs),
      initial_balances: balances,
      ...(deadline === undefined ? {} : { deadline_monotonic_ns: deadline }),
    };
  }
  if (type === "snapshot_request") {
    const consistency = stringField(payload, "required_consistency");
    if (
      consistency !== "validated_multi_account_snapshot"
      && consistency !== "slot_window_estimate"
      && consistency !== "unknown"
    ) {
      throw new Error("required_consistency is unsupported");
    }
    const poolIds = poolDescriptors(
      (Array.isArray(payload.pool_ids) ? payload.pool_ids : []).map((pool_id) => ({
        pool_id,
        label: "snapshot request",
      })),
      "pool_ids",
    ).map((pool) => pool.pool_id);
    if (poolIds.length === 0) throw new Error("snapshot_request requires at least one pool_id");
    return {
      type,
      request_id: stringField(payload, "request_id"),
      pool_ids: poolIds,
      required_consistency: consistency,
    };
  }
  if (type === "export_simulation_evidence") {
    return {
      type,
      request_id: stringField(payload, "request_id"),
      snapshot_token: stringField(payload, "snapshot_token"),
    };
  }
  if (type === "cancel_simulation") {
    return { type, request_id: stringField(payload, "request_id") };
  }
  if (type === "worker_stats_request") {
    return { type, request_id: stringField(payload, "request_id") };
  }
  throw new Error(`unsupported worker message type ${JSON.stringify(type)}`);
}

/** Replace query/path fragments so a provider key can never reach stderr. */
export function redactUrls(value: string): string {
  return value.replace(/(?:https?|wss?):\/\/[^\s"']+/gu, (candidate) => {
    try {
      const parsed = new URL(candidate);
      return `${parsed.protocol}//${parsed.host}`;
    } catch {
      return "[redacted-url]";
    }
  });
}

export function safeError(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return redactUrls(message).slice(0, 512);
}

function positiveEnvironmentInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  if (!/^[1-9][0-9]*$/u.test(raw)) {
    throw new Error(`${name} must be a positive integer`);
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be a positive safe integer`);
  }
  return parsed;
}

let outputFatalHandler = (error: ProtocolOutputFatalError): void => {
  process.stderr.write(`[worker-output-fatal] ${safeError(error)}\n`);
  process.exitCode = 1;
};

const protocolEmitter = new ProtocolEmitter(process.stdout, {
  maxLosslessQueue: positiveEnvironmentInteger("WORKER_MAX_LOSSLESS_OUTPUT_QUEUE", 1_024),
  maxStatePendingKeys: positiveEnvironmentInteger("WORKER_MAX_STATE_OUTPUT_KEYS", 65_536),
  onFatal: (error) => outputFatalHandler(error),
});

export function setProtocolOutputFatalHandler(
  handler: (error: ProtocolOutputFatalError) => void,
): void {
  outputFatalHandler = handler;
}

export function emitLossless(message: Record<string, unknown>): boolean {
  return protocolEmitter.emitLossless(message);
}

export function emitState(
  coalesceKey: string,
  message: Record<string, unknown>,
): boolean {
  return protocolEmitter.emitState(coalesceKey, message);
}

/** Compatibility wrapper for low-frequency control/result call sites. */
export function emit(message: Record<string, unknown>): boolean {
  return emitLossless(message);
}

export function protocolOutputMetrics(): ProtocolOutputMetrics {
  return protocolEmitter.metrics();
}

export async function closeProtocolOutput(timeoutMs = 5_000): Promise<void> {
  await protocolEmitter.drainAndClose(timeoutMs);
}
