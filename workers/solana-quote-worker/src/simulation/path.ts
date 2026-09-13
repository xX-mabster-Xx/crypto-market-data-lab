/** Pure ordered path executor over an immutable AMM snapshot bundle.
 *
 * Each leg consumes the post-state of the previous leg on the same pool
 * identity.  The observed snapshot bundle is never mutated; every transition
 * builds a fresh pooled body.  Unsupported routes or protocols return a typed
 * failure instead of a silent fallback.
 */

import {
  simulateRaydiumCpmmExactIn,
  simulateRaydiumCpmmExactOut,
  UnsupportedVariant,
  type RaydiumCpmmBody,
  type SwapTransition,
} from "./raydiumCpmm.js";
import {
  simulateOrcaExactIn,
  simulateOrcaExactOut,
  type OrcaWhirlpoolBody,
} from "./orcaWhirlpool.js";
import {
  simulateRaydiumClmmExactIn,
  simulateRaydiumClmmExactOut,
  type RaydiumClmmBody,
} from "./raydiumClmm.js";
import {
  simulateMeteoraDlmmExactIn,
  simulateMeteoraDlmmExactOut,
  type MeteoraDlmmBody,
} from "./meteoraDlmm.js";
import {
  simulateRaydiumAmmV4ExactIn,
  simulateRaydiumAmmV4ExactOut,
  type RaydiumAmmV4Body,
} from "./raydiumAmmV4.js";
import type {
  RaydiumCpmmPoolBundle,
  OrcaWhirlpoolPoolBundle,
  RaydiumClmmPoolBundle,
  MeteoraDlmmPoolBundle,
  RaydiumAmmV4PoolBundle,
  SnapshotBundle,
} from "./snapshots.js";

type AnyBody =
  | RaydiumCpmmBody
  | OrcaWhirlpoolBody
  | RaydiumClmmBody
  | MeteoraDlmmBody
  | RaydiumAmmV4Body;

interface AnyTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: AnyBody;
}

export interface SimulatePathLegInput {
  readonly leg_id: string;
  readonly pool_id: string;
  readonly input_asset_id: string;
  readonly output_asset_id: string;
  readonly mode: "exact_in" | "exact_out";
  readonly amount_source: "literal" | "previous_output";
  readonly amount_raw?: string;
  readonly previous_leg_id?: string;
}

export interface SimulatePathResult {
  status: "complete" | "unsupported" | "invalid_request" | "insufficient_balance" | "insufficient_liquidity" | "arithmetic_error" | "limit_exceeded" | "state_unavailable" | "deadline_exceeded";
  complete: boolean;
  reason: string;
  failed_leg_id: string | null;
  leg_results: readonly {
    leg_id: string;
    mode: "exact_in" | "exact_out";
    actual_gross_input_raw: string;
    input_used_for_curve_raw: string;
    gross_pool_output_raw: string;
    actual_net_output_raw: string;
    fee_amount_raw: string;
    reserve_0_after_raw: string;
    reserve_1_after_raw: string;
    fees: readonly { kind: string; amount_raw: string }[];
  }[];
  final_balances: readonly { asset_id: string; amount_raw: string }[];
}

export interface SimulatePathOptions {
  /** Optional live deadline. Values stay bigint so nanoseconds are exact. */
  readonly deadline_monotonic_ns?: bigint;
  /** Injectable monotonic clock for deterministic worker/unit tests. */
  readonly now_monotonic_ns?: () => bigint;
}

type LegResult = {
  leg_id: string;
  mode: "exact_in" | "exact_out";
  actual_gross_input_raw: string;
  input_used_for_curve_raw: string;
  gross_pool_output_raw: string;
  actual_net_output_raw: string;
  fee_amount_raw: string;
  reserve_0_after_raw: string;
  reserve_1_after_raw: string;
  fees: readonly { kind: string; amount_raw: string }[];
};

function canonicalPoolId(rawPoolId: string): string {
  return `solana:mainnet:${rawPoolId}`;
}

function toRaydiumBodyFromSnapshot(pool: RaydiumCpmmPoolBundle): RaydiumCpmmBody {
  return {
    pool_id: pool.pool_id,
    protocol: "raydium_cpmm" as const,
    vault_a_raw: BigInt(pool.vault_a_raw),
    vault_b_raw: BigInt(pool.vault_b_raw),
    protocol_fees_a_raw: BigInt(pool.protocol_fees_a_raw),
    protocol_fees_b_raw: BigInt(pool.protocol_fees_b_raw),
    fund_fees_a_raw: BigInt(pool.fund_fees_a_raw),
    fund_fees_b_raw: BigInt(pool.fund_fees_b_raw),
    creator_fees_a_raw: BigInt(pool.creator_fees_a_raw),
    creator_fees_b_raw: BigInt(pool.creator_fees_b_raw),
    trade_fee_rate: BigInt(pool.trade_fee_rate),
    creator_fee_rate: BigInt(pool.creator_fee_rate),
    protocol_fee_rate: BigInt(pool.protocol_fee_rate),
    fund_fee_rate: BigInt(pool.fund_fee_rate),
    fee_on: Number(pool.fee_on),
  };
}

function toOrcaBodyFromSnapshot(pool: OrcaWhirlpoolPoolBundle): OrcaWhirlpoolBody {
  return {
    pool_id: pool.pool_id,
    protocol: "orca_whirlpool" as const,
    sqrt_price_x64: BigInt(pool.sqrt_price_x64),
    liquidity_raw: BigInt(pool.liquidity_raw),
    tick_current_index: pool.tick_current_index,
    tick_spacing: pool.tick_spacing,
    fee_rate: BigInt(pool.fee_rate),
    protocol_fee_rate: BigInt(pool.protocol_fee_rate),
    fee_growth_global_a: BigInt(pool.fee_growth_global_a),
    fee_growth_global_b: BigInt(pool.fee_growth_global_b),
    protocol_fee_owed_a: BigInt(pool.protocol_fee_owed_a),
    protocol_fee_owed_b: BigInt(pool.protocol_fee_owed_b),
    tick_arrays: pool.tick_arrays.map((arr) => ({ startTickIndex: arr.startTickIndex, ticks: arr.ticks })),
  };
}

function toClmmBodyFromSnapshot(pool: RaydiumClmmPoolBundle): RaydiumClmmBody {
  return {
    protocol: "raydium_clmm" as const,
    pool_id: pool.pool_id,
    sqrt_price_x64: BigInt(pool.sqrt_price_x64),
    liquidity_raw: BigInt(pool.liquidity_raw),
    tick_current_index: pool.tick_current_index,
    tick_spacing: pool.tick_spacing,
    fee_rate: BigInt(pool.fee_rate),
    protocol_fee_rate: BigInt(pool.protocol_fee_rate),
    tick_arrays: pool.tick_arrays.map((arr) => ({
      startTickIndex: arr.start_tick_index,
      ticks: arr.ticks.map((t) => ({
        initialized: t.initialized,
        liquidityNet: BigInt(t.liquidity_net ?? 0),
        liquidityGross: BigInt(t.liquidity_gross ?? 0),
      })),
    })),
  };
}

function toDlmmBodyFromSnapshot(pool: MeteoraDlmmPoolBundle): MeteoraDlmmBody {
  return {
    protocol: "meteora_dlmm" as const,
    pool_id: pool.pool_id,
    active_id: Number(pool.active_id),
    bin_step: Number(pool.bin_step),
    reserve_x_raw: BigInt(pool.reserve_x_raw),
    reserve_y_raw: BigInt(pool.reserve_y_raw),
    fee_bps: BigInt(pool.fee_bps),
    protocol_fee_bps: BigInt(pool.protocol_fee_bps),
    bin_arrays: pool.bin_arrays.map((arr) => ({
      start_bin_id: Number(arr.start_bin_id),
      bins: arr.bins.map((b) => ({
        bin_id: Number(b.bin_id),
        reserve_x_raw: BigInt(b.reserve_x_raw),
        reserve_y_raw: BigInt(b.reserve_y_raw),
        liquidity_raw: BigInt(b.liquidity_raw ?? 0),
        fee_x_raw: BigInt(b.fee_x_raw ?? 0),
        fee_y_raw: BigInt(b.fee_y_raw ?? 0),
      })),
    })),
  };
}

function toAmmV4BodyFromSnapshot(pool: RaydiumAmmV4PoolBundle): RaydiumAmmV4Body {
  return {
    protocol: "raydium_amm_v4" as const,
    pool_id: pool.pool_id,
    vault_a_raw: BigInt(pool.vault_a_raw),
    vault_b_raw: BigInt(pool.vault_b_raw),
    fee_raw_a: BigInt(pool.fee_raw_a),
    fee_raw_b: BigInt(pool.fee_raw_b),
    fee_rate: BigInt(pool.fee_rate),
    need_take_pnl: Boolean(pool.need_take_pnl),
    open_orders: pool.open_orders ?? null,
    status: Number(pool.status),
  };
}

export function simulatePathLegs(
  snapshot: SnapshotBundle,
  legs: readonly SimulatePathLegInput[],
  initialBalances: readonly { asset_id: string; amount_raw: string }[] = [],
  options: SimulatePathOptions = {},
): SimulatePathResult {
  const pools = new Map<string, AnyBody>();
  for (const pool of snapshot.pools) {
    if (pool.protocol === "raydium_cpmm") {
      pools.set(pool.pool_id, toRaydiumBodyFromSnapshot(pool));
    } else if (pool.protocol === "orca_whirlpool") {
      pools.set(pool.pool_id, toOrcaBodyFromSnapshot(pool));
    } else if (pool.protocol === "raydium_clmm") {
      pools.set(pool.pool_id, toClmmBodyFromSnapshot(pool));
    } else if (pool.protocol === "meteora_dlmm") {
      pools.set(pool.pool_id, toDlmmBodyFromSnapshot(pool));
    } else if (pool.protocol === "raydium_amm_v4") {
      pools.set(pool.pool_id, toAmmV4BodyFromSnapshot(pool));
    }
  }
  const assetPairByPool = new Map<string, { asset0: string; asset1: string }>();
  for (const poolRef of snapshot.pool_refs) {
    assetPairByPool.set(canonicalPoolId(poolRef.pool_address), {
      asset0: poolRef.asset_0_id,
      asset1: poolRef.asset_1_id,
    });
  }
  const balances = new Map<string, bigint>();
  for (const balance of initialBalances) {
    if (!/^(0|[1-9][0-9]*)$/.test(balance.amount_raw)) {
      return fail("invalid_request", "initial balance must be a canonical non-negative integer string", legs[0], [], balances);
    }
    if (balances.has(balance.asset_id)) {
      return fail("invalid_request", "initial balance asset ids must be unique", legs[0], [], balances);
    }
    balances.set(balance.asset_id, BigInt(balance.amount_raw));
  }
  const previousOutputByLeg = new Map<string, bigint>();
  const legResults: LegResult[] = [];
  const seenLegIds = new Set<string>();
  let previousLegId: string | undefined;
  let previousOutputAssetId: string | undefined;

  const now = options.now_monotonic_ns ?? (() => process.hrtime.bigint());
  const snapshotValidUntil = "state_valid_until_monotonic_ns" in snapshot
    ? snapshot.state_valid_until_monotonic_ns
    : undefined;
  const failForClock = (leg: SimulatePathLegInput): SimulatePathResult | null => {
    if (options.deadline_monotonic_ns !== undefined && now() >= options.deadline_monotonic_ns) {
      return fail("deadline_exceeded", "deadline expired during simulation", leg, legResults, balances);
    }
    if (snapshotValidUntil !== undefined && now() >= BigInt(snapshotValidUntil)) {
      return fail("state_unavailable", "snapshot state_valid_until_monotonic_ns expired", leg, legResults, balances);
    }
    return null;
  };

  for (const leg of legs) {
    if (seenLegIds.has(leg.leg_id)) {
      return fail("invalid_request", "path leg ids must be unique", leg, legResults, balances);
    }
    seenLegIds.add(leg.leg_id);
    if (
      previousLegId !== undefined
      && (
        leg.input_asset_id !== previousOutputAssetId
        || (leg.amount_source === "previous_output" && leg.previous_leg_id !== previousLegId)
      )
    ) {
      return fail("invalid_request", "route shape is not a connected linear path", leg, legResults, balances);
    }
    const clockFailure = failForClock(leg);
    if (clockFailure !== null) return clockFailure;
    const poolId = leg.pool_id.includes("solana:mainnet:") ? leg.pool_id : canonicalPoolId(leg.pool_id);
    const body = pools.get(poolId);
    if (body === undefined) {
      return fail("state_unavailable", `pool ${leg.pool_id} is not in the snapshot`, leg, legResults, balances);
    }
    let amount: bigint;
    if (leg.amount_source === "literal") {
      if (leg.amount_raw === undefined) {
        return fail("invalid_request", "literal leg requires amount_raw", leg, legResults, balances);
      }
      amount = BigInt(leg.amount_raw);
    } else {
      const previous = previousOutputByLeg.get(leg.previous_leg_id ?? "");
      if (previous === undefined) {
        return fail("invalid_request", `no prior output for ${leg.previous_leg_id}`, leg, legResults, balances);
      }
      amount = previous;
    }
    if (amount <= 0n) {
      return fail("invalid_request", "requested amount must be positive", leg, legResults, balances);
    }
    const assetPair = assetPairByPool.get(poolId);
    if (assetPair === undefined) {
      return fail("invalid_request", `pool ${leg.pool_id} has no matching asset direction`, leg, legResults, balances);
    }
    const zeroForOne = leg.input_asset_id === assetPair.asset0;
    if (!zeroForOne && leg.input_asset_id !== assetPair.asset1) {
      return fail("invalid_request", `pool ${leg.pool_id} input asset is not configured`, leg, legResults, balances);
    }
    const aToB = zeroForOne;
    const expectedOutput = zeroForOne ? assetPair.asset1 : assetPair.asset0;
    if (leg.output_asset_id !== expectedOutput) {
      return fail("invalid_request", `pool ${leg.pool_id} leg does not cross the configured assets`, leg, legResults, balances);
    }
    let balance = balances.get(leg.input_asset_id) ?? 0n;
    if (balance < amount && leg.mode === "exact_in") {
      return fail("insufficient_balance", "leg input balance is insufficient", leg, legResults, balances);
    }
    let transition: AnyTransition;
    try {
      if (body.protocol === "raydium_cpmm") {
        transition = leg.mode === "exact_in"
          ? simulateRaydiumCpmmExactIn(body, amount, zeroForOne)
          : simulateRaydiumCpmmExactOut(body, amount, zeroForOne);
      } else if (body.protocol === "orca_whirlpool") {
        transition = leg.mode === "exact_in"
          ? simulateOrcaExactIn(body, amount, zeroForOne)
          : simulateOrcaExactOut(body, amount, zeroForOne);
      } else if (body.protocol === "raydium_clmm") {
        transition = leg.mode === "exact_in"
          ? simulateRaydiumClmmExactIn(body, amount, aToB)
          : simulateRaydiumClmmExactOut(body, amount, aToB);
      } else if (body.protocol === "meteora_dlmm") {
        transition = leg.mode === "exact_in"
          ? simulateMeteoraDlmmExactIn(body, amount, aToB)
          : simulateMeteoraDlmmExactOut(body, amount, aToB);
      } else if (body.protocol === "raydium_amm_v4") {
        transition = leg.mode === "exact_in"
          ? simulateRaydiumAmmV4ExactIn(body, amount, zeroForOne)
          : simulateRaydiumAmmV4ExactOut(body, amount, zeroForOne);
      } else {
        const unknownProtocol: string = (body as { protocol: string }).protocol ?? "unknown";
        throw new UnsupportedVariant(`unsupported protocol: ${unknownProtocol}`);
      }
    } catch (error) {
      if (error instanceof UnsupportedVariant) {
        return fail("insufficient_liquidity", error.message, leg, legResults, balances);
      }
      if (error instanceof RangeError) {
        return fail("arithmetic_error", error.message, leg, legResults, balances);
      }
      throw error;
    }
    const postTransitionClockFailure = failForClock(leg);
    if (postTransitionClockFailure !== null) return postTransitionClockFailure;
    // For exact-out, `amount` is the desired output and the actual input is
    // only known after the curve calculation. Never allow a negative balance.
    if (balance < transition.gross_input) {
      return fail("insufficient_balance", "leg input balance is insufficient", leg, legResults, balances);
    }
    balances.set(leg.input_asset_id, balance - transition.gross_input);
    const outputAsset = leg.output_asset_id;
    const outputBalance = balances.get(outputAsset) ?? 0n;
    balances.set(outputAsset, outputBalance + transition.net_output);
    previousOutputByLeg.set(leg.leg_id, transition.net_output);
    previousLegId = leg.leg_id;
    previousOutputAssetId = leg.output_asset_id;
    pools.set(poolId, transition.body_after);
    const [reserve0, reserve1] = effectiveReservesOf(transition.body_after);
    legResults.push({
      leg_id: leg.leg_id,
      mode: leg.mode,
      actual_gross_input_raw: transition.gross_input.toString(10),
      input_used_for_curve_raw: transition.effective_input.toString(10),
      gross_pool_output_raw: transition.gross_pool_output.toString(10),
      actual_net_output_raw: transition.net_output.toString(10),
      fee_amount_raw: (transition.gross_input - transition.effective_input).toString(10),
      reserve_0_after_raw: reserve0.toString(10),
      reserve_1_after_raw: reserve1.toString(10),
      fees: transition.fees.map((fee) => ({ kind: fee.kind, amount_raw: fee.amount_raw.toString(10) })),
    });
  }
  return {
    status: "complete",
    complete: true,
    reason: "all legs completed",
    failed_leg_id: null,
    leg_results: legResults,
    final_balances: [...balances.entries()].map(([asset_id, amount_raw]) => ({ asset_id, amount_raw: amount_raw.toString(10) })),
  };
}

function effectiveReservesOf(body: AnyBody): [bigint, bigint] {
  if (body.protocol === "raydium_cpmm") {
    return [
      body.vault_a_raw - body.protocol_fees_a_raw - body.fund_fees_a_raw - body.creator_fees_a_raw,
      body.vault_b_raw - body.protocol_fees_b_raw - body.fund_fees_b_raw - body.creator_fees_b_raw,
    ];
  }
  if (body.protocol === "meteora_dlmm") {
    return [body.reserve_x_raw, body.reserve_y_raw];
  }
  if (body.protocol === "raydium_amm_v4") {
    return [body.vault_a_raw - body.fee_raw_a, body.vault_b_raw - body.fee_raw_b];
  }
  // orca_whirlpool and raydium_clmm use concentrated liquidity sqrt-price math
  const tokenA = (body.liquidity_raw * body.sqrt_price_x64) >> 64n;
  const tokenB = (body.liquidity_raw << 64n) / body.sqrt_price_x64;
  return [tokenA, tokenB];
}

function fail(
  status: SimulatePathResult["status"],
  reason: string,
  leg: SimulatePathLegInput,
  legResults: readonly LegResult[],
  balances: Map<string, bigint>,
): SimulatePathResult {
  return {
    status,
    complete: false,
    reason,
    failed_leg_id: leg.leg_id,
    leg_results: legResults,
    final_balances: [...balances.entries()].map(([asset_id, amount_raw]) => ({ asset_id, amount_raw: amount_raw.toString(10) })),
  };
}
