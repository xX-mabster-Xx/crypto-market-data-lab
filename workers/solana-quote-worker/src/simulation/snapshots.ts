/** Build canonical immutable AMM snapshot bundles the Python core can decode. */

import type { RaydiumCpmmSimulationState } from "../raydiumStandard.js";

const CPMM_PROGRAM_ID = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const CHAIN_NAMESPACE = "solana";
const CHAIN_ID = "mainnet";
const MODEL_VERSION = "raydium_cpmm_v1";
const SCHEMA_VERSION = 1;

export interface RaydiumCpmmPoolRef {
  chain_namespace: string;
  chain_id: string;
  program_id: string;
  pool_address: string;
  protocol: "raydium_cpmm";
  protocol_revision: string;
  asset_0_id: string;
  asset_1_id: string;
  pool_spec_version: number;
}

export interface RaydiumCpmmPoolBundle {
  protocol: "raydium_cpmm";
  pool_id: string;
  vault_a_raw: string;
  vault_b_raw: string;
  protocol_fees_a_raw: string;
  protocol_fees_b_raw: string;
  fund_fees_a_raw: string;
  fund_fees_b_raw: string;
  creator_fees_a_raw: string;
  creator_fees_b_raw: string;
  trade_fee_rate: string;
  creator_fee_rate: string;
  protocol_fee_rate: string;
  fund_fee_rate: string;
  fee_on: string;
}

export interface RaydiumCpmmSnapshotBundle {

  schema_version: number;
  snapshot_id: string;
  worker_generation: number;
  source_epoch: number;
  boot_id: string;
  model_version: string;
  pool_refs: readonly RaydiumCpmmPoolRef[];
  dependency_vector: readonly unknown[];
  pools: readonly RaydiumCpmmPoolBundle[];
  context_slot: number;
  chain_consistency: string;
  /** Monotonic receipt time used to bind the opaque worker token. */
  snapshot_created_at_monotonic_ns?: string;
  state_valid_until_monotonic_ns?: string;
  sdk_versions: readonly (readonly [string, string])[];
}

/** Freeze nested arrays/objects so an evidence snapshot cannot be mutated by a caller. */
export function freezeSnapshot<T>(value: T): T {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value as Record<string, unknown>)) {
      freezeSnapshot(child);
    }
    Object.freeze(value);
  }
  return value;
}


export interface RaydiumClmmPoolRef {
  chain_namespace: string;
  chain_id: string;
  program_id: string;
  pool_address: string;
  protocol: "raydium_clmm";
  protocol_revision: string;
  asset_0_id: string;
  asset_1_id: string;
  pool_spec_version: number;
}

export interface ClmmTick {
  initialized: boolean;
  liquidity_net: bigint;
  liquidity_gross: bigint;
}

export interface ClmmTickArrayRef {
  start_tick_index: number;
  ticks: readonly ClmmTick[];
}

export interface RaydiumClmmPoolBundle {
  protocol: "raydium_clmm";
  pool_id: string;
  sqrt_price_x64: string;
  liquidity_raw: string;
  tick_current_index: number;
  tick_spacing: number;
  fee_rate: number;
  protocol_fee_rate: number;
  tick_arrays: readonly ClmmTickArrayRef[];
}

export interface RaydiumClmmSnapshotBundle {
  schema_version: number;
  snapshot_id: string;
  worker_generation: number;
  source_epoch: number;
  boot_id: string;
  model_version: string;
  pool_refs: readonly RaydiumClmmPoolRef[];
  dependency_vector: readonly unknown[];
  pools: readonly RaydiumClmmPoolBundle[];
  context_slot: number;
  chain_consistency: string;
  sdk_versions: readonly (readonly [string, string])[];
}

export interface MeteoraDlmmPoolRef {
  chain_namespace: string;
  chain_id: string;
  program_id: string;
  pool_address: string;
  protocol: "meteora_dlmm";
  protocol_revision: string;
  asset_0_id: string;
  asset_1_id: string;
  pool_spec_version: number;
}

export interface DlmmBinRef {
  bin_id: number;
  reserve_x_raw: string;
  reserve_y_raw: string;
  liquidity_raw: string;
  fee_x_raw: string;
  fee_y_raw: string;
}

export interface DlmmBinArrayRef {
  start_bin_id: number;
  bins: readonly DlmmBinRef[];
}

export interface MeteoraDlmmPoolBundle {
  protocol: "meteora_dlmm";
  pool_id: string;
  active_id: number;
  bin_step: number;
  reserve_x_raw: string;
  reserve_y_raw: string;
  fee_bps: number;
  protocol_fee_bps: number;
  bin_arrays: readonly DlmmBinArrayRef[];
}

export interface MeteoraDlmmSnapshotBundle {
  schema_version: number;
  snapshot_id: string;
  worker_generation: number;
  source_epoch: number;
  boot_id: string;
  model_version: string;
  pool_refs: readonly MeteoraDlmmPoolRef[];
  dependency_vector: readonly unknown[];
  pools: readonly MeteoraDlmmPoolBundle[];
  context_slot: number;
  chain_consistency: string;
  sdk_versions: readonly (readonly [string, string])[];
}

export interface RaydiumAmmV4PoolRef {
  chain_namespace: string;
  chain_id: string;
  program_id: string;
  pool_address: string;
  protocol: "raydium_amm_v4";
  protocol_revision: string;
  asset_0_id: string;
  asset_1_id: string;
  pool_spec_version: number;
}

export interface RaydiumAmmV4PoolBundle {
  protocol: "raydium_amm_v4";
  pool_id: string;
  vault_a_raw: string;
  vault_b_raw: string;
  fee_raw_a: string;
  fee_raw_b: string;
  fee_rate: number;
  need_take_pnl: boolean;
  open_orders: string | null;
  status: number;
}

export interface RaydiumAmmV4SnapshotBundle {
  schema_version: number;
  snapshot_id: string;
  worker_generation: number;
  source_epoch: number;
  boot_id: string;
  model_version: string;
  pool_refs: readonly RaydiumAmmV4PoolRef[];
  dependency_vector: readonly unknown[];
  pools: readonly RaydiumAmmV4PoolBundle[];
  context_slot: number;
  chain_consistency: string;
  sdk_versions: readonly (readonly [string, string])[];
}

export type SnapshotBundle =
  | RaydiumCpmmSnapshotBundle
  | OrcaWhirlpoolSnapshotBundle
  | RaydiumClmmSnapshotBundle
  | MeteoraDlmmSnapshotBundle
  | RaydiumAmmV4SnapshotBundle;

export type SimulationKind =
  | { kind: "raydium_cpmm"; bundle: RaydiumCpmmSnapshotBundle }
  | { kind: "orca_whirlpool"; bundle: OrcaWhirlpoolSnapshotBundle }
  | { kind: "raydium_clmm"; bundle: RaydiumClmmSnapshotBundle }
  | { kind: "meteora_dlmm"; bundle: MeteoraDlmmSnapshotBundle }
  | { kind: "raydium_amm_v4"; bundle: RaydiumAmmV4SnapshotBundle };

function canonicalAssetId(state: RaydiumCpmmSimulationState, mint: string, decimals: number): string {
  return `${CHAIN_NAMESPACE}:${CHAIN_ID}:${mint}:${decimals}`;
}

function canonicalPoolId(state: RaydiumCpmmSimulationState): string {
  return `${CHAIN_NAMESPACE}:${CHAIN_ID}:${state.pool_id}`;
}

export function raydiumCpmmSnapshotBundle(
  bootId: string,
  generation: number,
  requestId: string,
  states: readonly RaydiumCpmmSimulationState[],
  sourceEpoch = 0,
): RaydiumCpmmSnapshotBundle {
  const poolRefs = states.map<RaydiumCpmmPoolRef>((state) => ({
    chain_namespace: CHAIN_NAMESPACE,
    chain_id: CHAIN_ID,
    program_id: CPMM_PROGRAM_ID,
    pool_address: state.pool_id,
    protocol: "raydium_cpmm",
    protocol_revision: "v1",
    asset_0_id: canonicalAssetId(state, state.token_a_mint, state.token_a_decimals),
    asset_1_id: canonicalAssetId(state, state.token_b_mint, state.token_b_decimals),
    pool_spec_version: 1,
  }));
  const pools = states.map<RaydiumCpmmPoolBundle>((state) => ({
    pool_id: canonicalPoolId(state),
    protocol: "raydium_cpmm",
    vault_a_raw: state.vault_a_raw,
    vault_b_raw: state.vault_b_raw,
    protocol_fees_a_raw: state.protocol_fees_a_raw,
    protocol_fees_b_raw: state.protocol_fees_b_raw,
    fund_fees_a_raw: state.fund_fees_a_raw,
    fund_fees_b_raw: state.fund_fees_b_raw,
    creator_fees_a_raw: state.creator_fees_a_raw,
    creator_fees_b_raw: state.creator_fees_b_raw,
    trade_fee_rate: state.trade_fee_rate,
    creator_fee_rate: state.creator_fee_rate,
    protocol_fee_rate: state.protocol_fee_rate,
    fund_fee_rate: state.fund_fee_rate,
    fee_on: state.fee_on.toString(10),
  }));
  const contextSlot = states.length === 0 ? 0 : Math.max(...states.map((state) => state.slot));
  return freezeSnapshot({
    schema_version: SCHEMA_VERSION,
    snapshot_id: `snapshot-${requestId}`,
    worker_generation: generation,
    source_epoch: sourceEpoch,
    boot_id: bootId,
    model_version: MODEL_VERSION,
    pool_refs: poolRefs,
    dependency_vector: states.flatMap((state) => state.core_state_slot === undefined ? [] : [{
      pool_id: canonicalPoolId(state),
      core_state_slot: state.core_state_slot,
      dependency_slot_min: state.dependency_slot_min ?? null,
      dependency_slot_max: state.dependency_slot_max ?? null,
      dependency_generation: state.dependency_generation ?? 0,
    }]),
    pools,
    context_slot: contextSlot,
    chain_consistency: "validated_multi_account_snapshot",
    sdk_versions: [["@raydium-io/raydium-sdk-v2", "latest"]],
  });
}

/** Shape required to build a classic Orca Whirlpool snapshot bundle. */
export interface OrcaWhirlpoolSimulationState {
  pool_id: string;
  slot: number;
  core_state_slot?: number;
  dependency_slot_min?: number | null;
  dependency_slot_max?: number | null;
  dependency_generation?: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  sqrt_price_x64: string;
  liquidity_raw: string;
  tick_current_index: number;
  tick_spacing: number;
  fee_rate: number;
  protocol_fee_rate: number;
  fee_growth_global_a: string;
  fee_growth_global_b: string;
  protocol_fee_owed_a: string;
  protocol_fee_owed_b: string;
  tick_arrays: readonly OrcaWhirlpoolTickArray[];
}

export interface OrcaWhirlpoolTickArray {
  startTickIndex: number;
  ticks: readonly OrcaWhirlpoolTick[];
}

export interface OrcaWhirlpoolTick {
  initialized: boolean;
  liquidityNet: bigint;
  liquidityGross: bigint;
}

export interface OrcaWhirlpoolPoolRef {
  chain_namespace: string;
  chain_id: string;
  program_id: string;
  pool_address: string;
  protocol: "orca_whirlpool";
  protocol_revision: string;
  asset_0_id: string;
  asset_1_id: string;
  pool_spec_version: number;
}

export interface OrcaWhirlpoolPoolBundle {
  protocol: "orca_whirlpool";
  pool_id: string;
  sqrt_price_x64: string;
  liquidity_raw: string;
  tick_current_index: number;
  tick_spacing: number;
  fee_rate: number;
  protocol_fee_rate: number;
  fee_growth_global_a: string;
  fee_growth_global_b: string;
  protocol_fee_owed_a: string;
  protocol_fee_owed_b: string;
  tick_arrays: readonly OrcaWhirlpoolTickArray[];
}

const ORCA_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const ORCA_CHAIN_NAMESPACE = "solana";
const ORCA_CHAIN_ID = "mainnet";
const ORCA_MODEL_VERSION = "orca_whirlpool_v1";
const ORCA_SCHEMA_VERSION = 1;

export interface OrcaWhirlpoolSnapshotBundle {
  schema_version: number;
  snapshot_id: string;
  worker_generation: number;
  source_epoch: number;
  boot_id: string;
  model_version: string;
  pool_refs: readonly OrcaWhirlpoolPoolRef[];
  dependency_vector: readonly unknown[];
  pools: readonly OrcaWhirlpoolPoolBundle[];
  context_slot: number;
  chain_consistency: string;
  sdk_versions: readonly (readonly [string, string])[];
  tick_array_refs: readonly OrcaWhirlpoolTickArrayRef[];
}

export interface OrcaWhirlpoolTickArrayRef {
  pool_address: string;
  start_tick_index: number;
  tick_array_address: string;
}

function orcaCanonicalAssetId(mint: string, decimals: number): string {
  return `${ORCA_CHAIN_NAMESPACE}:${ORCA_CHAIN_ID}:${mint}:${decimals}`;
}

function orcaCanonicalPoolId(poolId: string): string {
  return `${ORCA_CHAIN_NAMESPACE}:${ORCA_CHAIN_ID}:${poolId}`;
}

export function orcaWhirlpoolSnapshotBundle(
  bootId: string,
  generation: number,
  requestId: string,
  states: readonly OrcaWhirlpoolSimulationState[],
): OrcaWhirlpoolSnapshotBundle {
  const poolRefs = states.map<OrcaWhirlpoolPoolRef>((state) => ({
    chain_namespace: ORCA_CHAIN_NAMESPACE,
    chain_id: ORCA_CHAIN_ID,
    program_id: ORCA_PROGRAM_ID,
    pool_address: state.pool_id,
    protocol: "orca_whirlpool",
    protocol_revision: "v1",
    asset_0_id: orcaCanonicalAssetId(state.token_a_mint, state.token_a_decimals),
    asset_1_id: orcaCanonicalAssetId(state.token_b_mint, state.token_b_decimals),
    pool_spec_version: 1,
  }));
  const pools = states.map<OrcaWhirlpoolPoolBundle>((state) => ({
    pool_id: orcaCanonicalPoolId(state.pool_id),
    protocol: "orca_whirlpool",
    sqrt_price_x64: state.sqrt_price_x64,
    liquidity_raw: state.liquidity_raw,
    tick_current_index: state.tick_current_index,
    tick_spacing: state.tick_spacing,
    fee_rate: state.fee_rate,
    protocol_fee_rate: state.protocol_fee_rate,
    fee_growth_global_a: state.fee_growth_global_a,
    fee_growth_global_b: state.fee_growth_global_b,
    protocol_fee_owed_a: state.protocol_fee_owed_a,
    protocol_fee_owed_b: state.protocol_fee_owed_b,
    tick_arrays: state.tick_arrays,
  }));
  const tickArrayRefs = states.flatMap<OrcaWhirlpoolTickArrayRef>((state) =>
    state.tick_arrays.map((arr) => ({
      pool_address: state.pool_id,
      start_tick_index: arr.startTickIndex,
      tick_array_address: `solana:mainnet:tickarray:${state.pool_id}:${arr.startTickIndex}`,
    })),
  );
  const contextSlot = states.length === 0 ? 0 : Math.max(...states.map((state) => state.slot));
  return {
    schema_version: ORCA_SCHEMA_VERSION,
    snapshot_id: `snapshot-${requestId}`,
    worker_generation: generation,
    source_epoch: 0,
    boot_id: bootId,
    model_version: ORCA_MODEL_VERSION,
    pool_refs: poolRefs,
    dependency_vector: states.flatMap((state) => state.core_state_slot === undefined ? [] : [{
      pool_id: orcaCanonicalPoolId(state.pool_id),
      core_state_slot: state.core_state_slot,
      dependency_slot_min: state.dependency_slot_min ?? null,
      dependency_slot_max: state.dependency_slot_max ?? null,
      dependency_generation: state.dependency_generation ?? 0,
    }]),
    pools,
    context_slot: contextSlot,
    chain_consistency: "validated_multi_account_snapshot",
    sdk_versions: [["@orca-so/whirlpools-sdk", "0.22.0"]],
    tick_array_refs: tickArrayRefs,
  };
}


// ---------------------------------------------------------------------------
// Raydium CLMM snapshot bundle builder
// ---------------------------------------------------------------------------

export interface RaydiumClmmSimulationState {
  pool_id: string;
  slot: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  sqrt_price_x64: string;
  liquidity_raw: string;
  tick_current_index: number;
  tick_spacing: number;
  fee_rate: number;
  protocol_fee_rate: number;
  tick_arrays: readonly {
    start_tick_index: number;
    ticks: readonly {
      initialized: boolean;
      liquidity_net: bigint;
      liquidity_gross: bigint;
    }[];
  }[];
}

const CLMM_PROGRAM_ID = "CLMM_PROGRAM";
const CLMM_MODEL_VERSION = "raydium_clmm_v1";

export function raydiumClmmSnapshotBundle(
  bootId: string,
  generation: number,
  requestId: string,
  states: readonly RaydiumClmmSimulationState[],
): RaydiumClmmSnapshotBundle {
  const poolRefs = states.map<RaydiumClmmPoolRef>((state) => ({
    chain_namespace: CHAIN_NAMESPACE,
    chain_id: CHAIN_ID,
    program_id: CLMM_PROGRAM_ID,
    pool_address: state.pool_id,
    protocol: "raydium_clmm",
    protocol_revision: "v1",
    asset_0_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_a_mint, state.token_a_decimals),
    asset_1_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_b_mint, state.token_b_decimals),
    pool_spec_version: 1,
  }));
  const pools = states.map<RaydiumClmmPoolBundle>((state) => ({
    pool_id: `${CHAIN_NAMESPACE}:${CHAIN_ID}:${state.pool_id}`,
    protocol: "raydium_clmm",
    sqrt_price_x64: state.sqrt_price_x64,
    liquidity_raw: state.liquidity_raw,
    tick_current_index: state.tick_current_index,
    tick_spacing: state.tick_spacing,
    fee_rate: state.fee_rate,
    protocol_fee_rate: state.protocol_fee_rate,
    tick_arrays: state.tick_arrays,
  }));
  const contextSlot = states.length === 0 ? 0 : Math.max(...states.map((state) => state.slot));
  return {
    schema_version: SCHEMA_VERSION,
    snapshot_id: `snapshot-clmm-${requestId}`,
    worker_generation: generation,
    source_epoch: 0,
    boot_id: bootId,
    model_version: CLMM_MODEL_VERSION,
    pool_refs: poolRefs,
    dependency_vector: [],
    pools,
    context_slot: contextSlot,
    chain_consistency: "validated_multi_account_snapshot",
    sdk_versions: [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
  };
}


// ---------------------------------------------------------------------------
// Meteora DLMM snapshot bundle builder
// ---------------------------------------------------------------------------

export interface MeteoraDlmmSimulationState {
  pool_id: string;
  slot: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  active_id: number;
  bin_step: number;
  reserve_x_raw: string;
  reserve_y_raw: string;
  fee_bps: number;
  protocol_fee_bps: number;
  bin_arrays: readonly DlmmBinArrayRef[];
}

const DLMM_PROGRAM_ID = "DLMM_PROGRAM";
const DLMM_MODEL_VERSION = "meteora_dlmm_v1";

export function meteoraDlmmSnapshotBundle(
  bootId: string,
  generation: number,
  requestId: string,
  states: readonly MeteoraDlmmSimulationState[],
): MeteoraDlmmSnapshotBundle {
  const poolRefs = states.map<MeteoraDlmmPoolRef>((state) => ({
    chain_namespace: CHAIN_NAMESPACE,
    chain_id: CHAIN_ID,
    program_id: DLMM_PROGRAM_ID,
    pool_address: state.pool_id,
    protocol: "meteora_dlmm",
    protocol_revision: "v1",
    asset_0_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_a_mint, state.token_a_decimals),
    asset_1_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_b_mint, state.token_b_decimals),
    pool_spec_version: 1,
  }));
  const pools = states.map<MeteoraDlmmPoolBundle>((state) => ({
    pool_id: `${CHAIN_NAMESPACE}:${CHAIN_ID}:${state.pool_id}`,
    protocol: "meteora_dlmm",
    active_id: state.active_id,
    bin_step: state.bin_step,
    reserve_x_raw: state.reserve_x_raw,
    reserve_y_raw: state.reserve_y_raw,
    fee_bps: state.fee_bps,
    protocol_fee_bps: state.protocol_fee_bps,
    bin_arrays: state.bin_arrays,
  }));
  const contextSlot = states.length === 0 ? 0 : Math.max(...states.map((state) => state.slot));
  return {
    schema_version: SCHEMA_VERSION,
    snapshot_id: `snapshot-dlmm-${requestId}`,
    worker_generation: generation,
    source_epoch: 0,
    boot_id: bootId,
    model_version: DLMM_MODEL_VERSION,
    pool_refs: poolRefs,
    dependency_vector: [],
    pools,
    context_slot: contextSlot,
    chain_consistency: "validated_multi_account_snapshot",
    sdk_versions: [["@meteora-ag/dlmm", "1.9.14"]],
  };
}


// ---------------------------------------------------------------------------
// Raydium AMM v4 snapshot bundle builder
// ---------------------------------------------------------------------------

export interface RaydiumAmmV4SimulationState {
  pool_id: string;
  slot: number;
  token_a_mint: string;
  token_b_mint: string;
  token_a_decimals: number;
  token_b_decimals: number;
  vault_a_raw: string;
  vault_b_raw: string;
  fee_raw_a: string;
  fee_raw_b: string;
  fee_rate: number;
  need_take_pnl: boolean;
  open_orders: string | null;
  status: number;
}

const AMM_V4_PROGRAM_ID = "AMM_V4_PROGRAM";
const AMM_V4_MODEL_VERSION = "raydium_amm_v4_v1";

export function raydiumAmmV4SnapshotBundle(
  bootId: string,
  generation: number,
  requestId: string,
  states: readonly RaydiumAmmV4SimulationState[],
): RaydiumAmmV4SnapshotBundle {
  const poolRefs = states.map<RaydiumAmmV4PoolRef>((state) => ({
    chain_namespace: CHAIN_NAMESPACE,
    chain_id: CHAIN_ID,
    program_id: AMM_V4_PROGRAM_ID,
    pool_address: state.pool_id,
    protocol: "raydium_amm_v4",
    protocol_revision: "v1",
    asset_0_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_a_mint, state.token_a_decimals),
    asset_1_id: canonicalAssetId(state as unknown as RaydiumCpmmSimulationState, state.token_b_mint, state.token_b_decimals),
    pool_spec_version: 1,
  }));
  const pools = states.map<RaydiumAmmV4PoolBundle>((state) => ({
    pool_id: `${CHAIN_NAMESPACE}:${CHAIN_ID}:${state.pool_id}`,
    protocol: "raydium_amm_v4",
    vault_a_raw: state.vault_a_raw,
    vault_b_raw: state.vault_b_raw,
    fee_raw_a: state.fee_raw_a,
    fee_raw_b: state.fee_raw_b,
    fee_rate: state.fee_rate,
    need_take_pnl: state.need_take_pnl,
    open_orders: state.open_orders,
    status: state.status,
  }));
  const contextSlot = states.length === 0 ? 0 : Math.max(...states.map((state) => state.slot));
  return {
    schema_version: SCHEMA_VERSION,
    snapshot_id: `snapshot-ammv4-${requestId}`,
    worker_generation: generation,
    source_epoch: 0,
    boot_id: bootId,
    model_version: AMM_V4_MODEL_VERSION,
    pool_refs: poolRefs,
    dependency_vector: [],
    pools,
    context_slot: contextSlot,
    chain_consistency: "validated_multi_account_snapshot",
    sdk_versions: [["@raydium-io/raydium-sdk-v2", "0.2.63-alpha"]],
  };
}
