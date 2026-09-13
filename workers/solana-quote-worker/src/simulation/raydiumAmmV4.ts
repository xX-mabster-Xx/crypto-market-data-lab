/** Pure Raydium AMM v4 post-trade adapter (offline core, restricted swap-only subset).
 *
 * Mirrors the verified Python core.  Only pools with direct vault accounting
 * and no OpenBook orderbook dependencies are supported.  Pools that require
 * PnL take or have OpenOrders are explicitly unsupported.
 */

import { UnsupportedVariant } from "./errors.js";
export { UnsupportedVariant } from "./errors.js";

const FEE_RATE_DENOMINATOR = 1_000_000n;

export interface RaydiumAmmV4Body {
  readonly protocol: "raydium_amm_v4";
  readonly pool_id: string;
  readonly vault_a_raw: bigint;
  readonly vault_b_raw: bigint;
  readonly fee_raw_a: bigint;
  readonly fee_raw_b: bigint;
  readonly fee_rate: bigint;
  readonly need_take_pnl: boolean;
  readonly open_orders: string | null;
  readonly status: number;
}

export interface SwapTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: RaydiumAmmV4Body;
}

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  if (denominator <= 0n) throw new Error("denominator must be positive");
  if (numerator <= 0n) return 0n;
  return (numerator + denominator - 1n) / denominator;
}

function preFeeAmount(amount: bigint, rate: bigint): bigint {
  if (rate <= 0n) return amount;
  return ceilDiv(amount * FEE_RATE_DENOMINATOR, FEE_RATE_DENOMINATOR - rate);
}

function directedReserves(vaultA: bigint, vaultB: bigint, zeroForOne: boolean): [bigint, bigint] {
  return zeroForOne ? [vaultA, vaultB] : [vaultB, vaultA];
}

function effectiveReserves(pool: RaydiumAmmV4Body): [bigint, bigint] {
  return [pool.vault_a_raw - pool.fee_raw_a, pool.vault_b_raw - pool.fee_raw_b];
}

function ammV4After(
  pool: RaydiumAmmV4Body,
  zeroForOne: boolean,
  inputAmount: bigint,
  outputAmount: bigint,
  fee: bigint,
): RaydiumAmmV4Body {
  if (zeroForOne) {
    return {
      ...pool,
      vault_a_raw: pool.vault_a_raw + inputAmount,
      vault_b_raw: pool.vault_b_raw - outputAmount,
      fee_raw_a: pool.fee_raw_a + fee,
    };
  }
  return {
    ...pool,
    vault_a_raw: pool.vault_a_raw - outputAmount,
    vault_b_raw: pool.vault_b_raw + inputAmount,
    fee_raw_b: pool.fee_raw_b + fee,
  };
}

export function simulateRaydiumAmmV4ExactIn(
  body: RaydiumAmmV4Body,
  amountIn: bigint,
  zeroForOne: boolean,
): SwapTransition {
  if (body.need_take_pnl) {
    throw new UnsupportedVariant("AMM v4 pool requires PnL take; unsupported in this subset");
  }
  if (body.open_orders !== null) {
    throw new UnsupportedVariant("AMM v4 pool has OpenBook integration; unsupported in this subset");
  }
  if (amountIn <= 0n) {
    throw new UnsupportedVariant("exact-in amount must be positive");
  }

  const [reserveIn, reserveOut] = directedReserves(...effectiveReserves(body), zeroForOne);
  const fee = ceilDiv(amountIn * BigInt(body.fee_rate), FEE_RATE_DENOMINATOR);
  const effectiveInput = amountIn - fee;
  if (effectiveInput <= 0n) {
    throw new UnsupportedVariant("fee consumes the entire input");
  }
  const output = (reserveOut * effectiveInput) / (reserveIn + effectiveInput);
  if (output <= 0n || output >= reserveOut) {
    throw new UnsupportedVariant("insufficient AMM v4 liquidity");
  }

  const after = ammV4After(body, zeroForOne, amountIn, output, fee);
  return {
    gross_input: amountIn,
    effective_input: effectiveInput,
    gross_pool_output: output,
    net_output: output,
    fees: [{ kind: "amm_v4_fee", amount_raw: fee }],
    body_after: after,
  };
}

export function simulateRaydiumAmmV4ExactOut(
  body: RaydiumAmmV4Body,
  amountOut: bigint,
  zeroForOne: boolean,
): SwapTransition {
  if (body.need_take_pnl) {
    throw new UnsupportedVariant("AMM v4 pool requires PnL take; unsupported in this subset");
  }
  if (body.open_orders !== null) {
    throw new UnsupportedVariant("AMM v4 pool has OpenBook integration; unsupported in this subset");
  }
  if (amountOut <= 0n) {
    throw new UnsupportedVariant("exact-out amount must be positive");
  }

  const [reserveIn, reserveOut] = directedReserves(...effectiveReserves(body), zeroForOne);
  if (amountOut >= reserveOut) {
    throw new UnsupportedVariant("exact-output amount must fit inside output reserve");
  }

  const target = ceilDiv(reserveIn * amountOut, reserveOut - amountOut);
  const preFee = preFeeAmount(target, BigInt(body.fee_rate));
  let low = preFee;
  let high = preFee * FEE_RATE_DENOMINATOR + 1n;
  let steps = 0;
  while (low < high) {
    steps++;
    if (steps > 64) {
      throw new UnsupportedVariant("exact-out inversion exceeded max iterations");
    }
    const candidate = (low + high) / 2n;
    const candidateFee = ceilDiv(candidate * BigInt(body.fee_rate), FEE_RATE_DENOMINATOR);
    if (candidate - candidateFee >= target) {
      high = candidate;
    } else {
      low = candidate + 1n;
    }
  }
  const amountIn = low;
  const fee = ceilDiv(amountIn * BigInt(body.fee_rate), FEE_RATE_DENOMINATOR);
  const effectiveInput = amountIn - fee;
  if (effectiveInput !== target) {
    throw new UnsupportedVariant("minimal exact-output gross input inversion failed");
  }
  const output = (reserveOut * effectiveInput) / (reserveIn + effectiveInput);
  if (output !== amountOut) {
    throw new UnsupportedVariant("exact-output output mismatch");
  }

  const after = ammV4After(body, zeroForOne, amountIn, amountOut, fee);
  return {
    gross_input: amountIn,
    effective_input: amountIn - fee,
    gross_pool_output: amountOut,
    net_output: amountOut,
    fees: [{ kind: "amm_v4_fee", amount_raw: fee }],
    body_after: after,
  };
}

export function effectiveReservesAmmV4(body: RaydiumAmmV4Body): [bigint, bigint] {
  return effectiveReserves(body);
}
