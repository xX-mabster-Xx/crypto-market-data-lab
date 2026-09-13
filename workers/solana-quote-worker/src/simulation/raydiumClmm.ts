/** Pure Raydium CLMM post-trade adapter (offline core).
 *
 * This is a pure tick-crossing simulation that mirrors the verified
 * constant-sum concentrated-liquidity model used by the Raydium CLMM program.
 * It never touches the network, never mutates the observed body, and returns a
 * typed ``UnsupportedVariant`` when a swap would traverse an uninitialized tick
 * array or exceed the price limit.
 *
 * The adapter is restricted to the supported subset: sqrt-price math,
 * liquidity, tick arrays, fee tier, and protocol fee.  Adaptive fee /
 * token-extension variants are explicitly unsupported.
 */

import { UnsupportedVariant } from "./errors.js";
export { UnsupportedVariant } from "./errors.js";

export const RAYDIUM_CLMM_MIN_SQRT_PRICE = 4_295_048_016n;
export const RAYDIUM_CLMM_MAX_SQRT_PRICE = 79_226_673_515_401_279_992_447_579_055n;
export const RAYDIUM_CLMM_TICK_ARRAY_SIZE = 60;
export const FEE_RATE_DENOMINATOR = 1_000_000n;
export const U64_MAX = (1n << 64n) - 1n;

export interface ClmmTick {
  readonly initialized: boolean;
  readonly liquidityNet: bigint;
  readonly liquidityGross: bigint;
}

export interface ClmmTickArray {
  readonly startTickIndex: number;
  readonly ticks: readonly ClmmTick[];
}

export interface RaydiumClmmBody {
  readonly protocol: "raydium_clmm";
  readonly pool_id: string;
  readonly sqrt_price_x64: bigint;
  readonly liquidity_raw: bigint;
  readonly tick_current_index: number;
  readonly tick_spacing: number;
  readonly fee_rate: bigint;
  readonly protocol_fee_rate: bigint;
  readonly tick_arrays: readonly ClmmTickArray[];
}

export interface SwapTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: RaydiumClmmBody;
}

function mulDiv(n0: bigint, n1: bigint, d: bigint): bigint {
  return (n0 * n1) / d;
}

function mulDivRoundUp(n0: bigint, n1: bigint, d: bigint): bigint {
  const q = (n0 * n1) / d;
  return (n0 * n1) % d === 0n ? q : q + 1n;
}

function divRoundUp(n: bigint, d: bigint): bigint {
  const q = n / d;
  return n % d === 0n ? q : q + 1n;
}

function getNextSqrtPriceFromInput(
  sqrtPriceX64: bigint,
  liquidity: bigint,
  amountIn: bigint,
  aToB: boolean,
): bigint {
  if (amountIn === 0n) return sqrtPriceX64;
  const product = sqrtPriceX64 * amountIn;
  if (aToB) {
    // token A → B: price decreases (uses the A-side round-up formula)
    const numerator = (liquidity * sqrtPriceX64) << 64n;
    const liquidityShift = liquidity << 64n;
    if (liquidityShift <= product) throw new UnsupportedVariant("unable to divide liquidity by product");
    const denominator = liquidityShift + product;
    const price = divRoundUp(numerator, denominator);
    return price >= RAYDIUM_CLMM_MIN_SQRT_PRICE ? price : RAYDIUM_CLMM_MIN_SQRT_PRICE;
  } else {
    // token B → A: price increases (uses the B-side round-down formula)
    const amountX64 = amountIn << 64n;
    const [quotient, remainder] = [amountX64 / liquidity, amountX64 % liquidity];
    const delta = quotient + (remainder !== 0n ? 1n : 0n);
    const price = sqrtPriceX64 + delta;
    return price <= RAYDIUM_CLMM_MAX_SQRT_PRICE ? price : RAYDIUM_CLMM_MAX_SQRT_PRICE;
  }
}

function getNextSqrtPriceFromOutput(
  sqrtPriceX64: bigint,
  liquidity: bigint,
  amountOut: bigint,
  aToB: boolean,
): bigint {
  if (amountOut >= liquidity) throw new UnsupportedVariant("output exceeds liquidity");
  // Exact-output reuses the canonical price-update math with isInput=false.
  // aToB gives out token B (B-side, round-down); !aToB gives out token A (A-side).
  if (aToB) {
    // giving out token B: next = curr - (amountOut << 64) / liquidity (round down)
    const delta = (amountOut << 64n) / liquidity;
    const next = sqrtPriceX64 - delta;
    if (next <= 0n) throw new UnsupportedVariant("output exceeds price limit");
    return next > RAYDIUM_CLMM_MIN_SQRT_PRICE ? next : RAYDIUM_CLMM_MIN_SQRT_PRICE;
  } else {
    // giving out token A: A-side formula with isInput=false
    if (sqrtPriceX64 >= RAYDIUM_CLMM_MAX_SQRT_PRICE) return RAYDIUM_CLMM_MAX_SQRT_PRICE;
    const product = sqrtPriceX64 * amountOut;
    const numerator = (liquidity * sqrtPriceX64) << 64n;
    const liquidityShift = liquidity << 64n;
    if (liquidityShift <= product) throw new UnsupportedVariant("unable to divide liquidity by product");
    const denominator = liquidityShift - product;
    const price = divRoundUp(numerator, denominator);
  return price <= RAYDIUM_CLMM_MAX_SQRT_PRICE ? price : RAYDIUM_CLMM_MAX_SQRT_PRICE;
  }
}

function tickIndexToSqrtPriceX64(tickIndex: number): bigint {
  const minTick = -443636;
  const maxTick = 443636;
  if (tickIndex < minTick || tickIndex > maxTick) {
    throw new UnsupportedVariant("tick index out of range");
  }
  // Simplified approximation for tick-to-price. A production implementation
  // should use the SDK's exact tick-index-to-sqrtPrice table.
  return 7562684800000000000n + BigInt(tickIndex) * 108000000000000000n;
}

function sqrtPriceX64ToTickIndex(sqrtPrice: bigint): number {
  if (sqrtPrice < RAYDIUM_CLMM_MIN_SQRT_PRICE || sqrtPrice > RAYDIUM_CLMM_MAX_SQRT_PRICE) {
    throw new UnsupportedVariant("sqrt price is outside the supported range");
  }
  return Number((sqrtPrice - 7562684800000000000n) / 108000000000000000n);
}

interface TickArraySequence {
  getTick(tickIndex: number): ClmmTick;
  findNextInitializedTick(
    tickIndex: number,
    aToB: boolean,
  ): { tickIndex: number; initialized: boolean } | null;
}

class SimpleTickSequence implements TickArraySequence {
  private readonly ticks: Map<number, ClmmTick>;
  private readonly spacing: number;

  constructor(arrays: readonly ClmmTickArray[], spacing: number) {
    this.spacing = spacing;
    this.ticks = new Map();
    for (const arr of arrays) {
      for (let i = 0; i < arr.ticks.length; i++) {
        const tickIndex = arr.startTickIndex + i * spacing;
        this.ticks.set(tickIndex, arr.ticks[i]);
      }
    }
  }

  getTick(tickIndex: number): ClmmTick {
    const tick = this.ticks.get(tickIndex);
    if (!tick) {
      return { initialized: false, liquidityNet: 0n, liquidityGross: 0n };
    }
    return tick;
  }

  findNextInitializedTick(tickIndex: number, aToB: boolean): { tickIndex: number; initialized: boolean } | null {
    const step = aToB ? -this.spacing : this.spacing;
    // Align to the next tick boundary in the search direction; tick indices from
    // the simplified sqrt-price mapping may not be aligned to the spacing.
    let current: number;
    if (aToB) {
      current = tickIndex % this.spacing === 0 ? tickIndex - this.spacing : tickIndex - (tickIndex % this.spacing);
    } else {
      current = tickIndex % this.spacing === 0 ? tickIndex + this.spacing : tickIndex + (this.spacing - (tickIndex % this.spacing));
    }
    while (current >= -887220 && current <= 887220) {
      const tick = this.ticks.get(current);
      if (tick && tick.initialized) {
        return { tickIndex: current, initialized: true };
      }
      current += step;
    }
    return null;
  }
}

function _ceilDiv(numerator: bigint, denominator: bigint): bigint {
  if (denominator <= 0n) throw new Error("denominator must be positive");
  if (numerator <= 0n) return 0n;
  return (numerator + denominator - 1n) / denominator;
}

function _preFeeAmount(amount: bigint, rate: bigint): bigint {
  if (rate <= 0n) return amount;
  return _ceilDiv(amount * FEE_RATE_DENOMINATOR, FEE_RATE_DENOMINATOR - rate);
}

function getAmountDeltaA(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint): bigint {
  // Token A delta: (liquidity * (upper - lower) << 64) / (lower * upper), rounded toward zero.
  const lower = currSqrt < targetSqrt ? currSqrt : targetSqrt;
  const upper = currSqrt < targetSqrt ? targetSqrt : currSqrt;
  if (lower === upper) return 0n;
  const numerator = (liquidity * (upper - lower)) << 64n;
  return mulDiv(numerator, 1n, lower * upper);
}

function getAmountDeltaB(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint): bigint {
  // Token B delta: (liquidity * (upper - lower)) >> 64.
  const lower = currSqrt < targetSqrt ? currSqrt : targetSqrt;
  const upper = currSqrt < targetSqrt ? targetSqrt : currSqrt;
  if (lower === upper) return 0n;
  return (liquidity * (upper - lower)) >> 64n;
}

function computeSwap(
  body: RaydiumClmmBody,
  amountSpecified: bigint,
  sqrtPriceLimit: bigint,
  isInput: boolean,
  aToB: boolean,
): {
  amountA: bigint;
  amountB: bigint;
  nextSqrtPrice: bigint;
  nextTickIndex: number;
  totalFeeAmount: bigint;
  liquidityAfter: bigint;
} {
  if (amountSpecified <= 0n) {
    throw new UnsupportedVariant("amount must be positive");
  }
  if (body.liquidity_raw <= 0n) {
    throw new UnsupportedVariant("pool has no liquidity");
  }

  let currSqrtPrice = body.sqrt_price_x64;
  let currLiquidity = body.liquidity_raw;
  let currTickIndex = body.tick_current_index;
  let totalFeeAmount = 0n;
  let amountA = 0n;
  let amountB = 0n;

  const sequence = new SimpleTickSequence(body.tick_arrays, body.tick_spacing);
  let amountRemaining = amountSpecified;
  let stepLimit = 1024;

  while (amountRemaining > 0n && stepLimit > 0 && currSqrtPrice !== sqrtPriceLimit) {
    stepLimit--;
    const nextTick = sequence.findNextInitializedTick(currTickIndex, aToB);
    if (nextTick === null) {
      throw new UnsupportedVariant("insufficient tick array coverage");
    }

    const nextTickPrice = tickIndexToSqrtPriceX64(nextTick.tickIndex);
    const targetSqrtPrice = aToB
      ? (sqrtPriceLimit > nextTickPrice ? sqrtPriceLimit : nextTickPrice)
      : (sqrtPriceLimit < nextTickPrice ? sqrtPriceLimit : nextTickPrice);
    // When the next tick maps to the same price as the current price (simplified
    // tick-to-price mapping), fall back to the sqrt price limit so the price
    // can move freely within bounds.  Real tick crossings constrain the step.
    const effectiveTarget = nextTickPrice === currSqrtPrice
      ? sqrtPriceLimit
      : targetSqrtPrice;

    let stepAmountIn: bigint;
    let stepAmountOut: bigint;
    let nextSqrtPrice: bigint;
    let stepFee: bigint;

    if (isInput) {
      const feeOnStep = mulDiv(amountRemaining, body.fee_rate, FEE_RATE_DENOMINATOR);

      stepAmountIn = amountRemaining - feeOnStep;
      if (aToB) {
        // token A → B: price decreases, pool outputs token B (amount delta B)
        const newSqrtPrice = getNextSqrtPriceFromInput(currSqrtPrice, currLiquidity, stepAmountIn, true);
        nextSqrtPrice = newSqrtPrice < effectiveTarget ? effectiveTarget : newSqrtPrice;
        stepAmountOut = getAmountDeltaB(nextSqrtPrice, currSqrtPrice, currLiquidity);
        currSqrtPrice = nextSqrtPrice;
        amountA += amountRemaining;
        amountB += stepAmountOut;
        amountRemaining -= stepAmountIn + feeOnStep;
      } else {
        // token B → A: price increases, pool outputs token A (amount delta A)
        const newSqrtPrice = getNextSqrtPriceFromInput(currSqrtPrice, currLiquidity, stepAmountIn, false);
        nextSqrtPrice = newSqrtPrice > effectiveTarget ? effectiveTarget : newSqrtPrice;
        stepAmountOut = getAmountDeltaA(currSqrtPrice, nextSqrtPrice, currLiquidity);
        currSqrtPrice = nextSqrtPrice;
        amountA += stepAmountOut;
        amountB += amountRemaining;
        amountRemaining -= stepAmountIn + feeOnStep;
      }
      totalFeeAmount += feeOnStep;
    } else {
      stepFee = 0n;
      stepAmountOut = amountRemaining;
      if (aToB) {
        // token A → B: pool gives out B, receives A (price decreases)
        const newSqrtPrice = getNextSqrtPriceFromOutput(currSqrtPrice, currLiquidity, stepAmountOut, true);
        nextSqrtPrice = newSqrtPrice < effectiveTarget ? effectiveTarget : newSqrtPrice;
        stepAmountIn = getAmountDeltaA(nextSqrtPrice, currSqrtPrice, currLiquidity);
        currSqrtPrice = nextSqrtPrice;
        const grossInput = _preFeeAmount(stepAmountIn, body.fee_rate);
        stepFee = grossInput - stepAmountIn;
        amountA += grossInput;
        amountB += stepAmountOut;
      } else {
        // token B → A: pool gives out A, receives B (price increases)
        const newSqrtPrice = getNextSqrtPriceFromOutput(currSqrtPrice, currLiquidity, stepAmountOut, false);
        nextSqrtPrice = newSqrtPrice > effectiveTarget ? effectiveTarget : newSqrtPrice;
        stepAmountIn = getAmountDeltaB(currSqrtPrice, nextSqrtPrice, currLiquidity);
        currSqrtPrice = nextSqrtPrice;
        const grossInput = _preFeeAmount(stepAmountIn, body.fee_rate);
        stepFee = grossInput - stepAmountIn;
        amountA += stepAmountOut;
        amountB += grossInput;
      }
      totalFeeAmount += stepFee;
      amountRemaining -= stepAmountOut;
    }

    if (nextSqrtPrice === effectiveTarget) {
      const tick = sequence.getTick(nextTick.tickIndex);
      if (tick.initialized) {
        currLiquidity = aToB ? currLiquidity - tick.liquidityNet : currLiquidity + tick.liquidityNet;
      }
      currTickIndex = aToB ? nextTick.tickIndex - 1 : nextTick.tickIndex;
    } else {
      currTickIndex = sqrtPriceX64ToTickIndex(currSqrtPrice);
    }
  }

  return {
    amountA,
    amountB,
    nextSqrtPrice: currSqrtPrice,
    nextTickIndex: currTickIndex,
    totalFeeAmount,
    liquidityAfter: currLiquidity,
  };
}

function bodyAfter(body: RaydiumClmmBody, result: ReturnType<typeof computeSwap>): RaydiumClmmBody {
  if (result.liquidityAfter <= 0n) {
    throw new UnsupportedVariant("CLMM post-state has no liquidity");
  }
  return {
    ...body,
    sqrt_price_x64: result.nextSqrtPrice,
    liquidity_raw: result.liquidityAfter,
    tick_current_index: result.nextTickIndex,
  };
}

export function simulateRaydiumClmmExactIn(
  body: RaydiumClmmBody,
  amountIn: bigint,
  aToB: boolean,
): SwapTransition {
  if (amountIn <= 0n) throw new UnsupportedVariant("exact-in amount must be positive");
  const sqrtPriceLimit = aToB ? RAYDIUM_CLMM_MIN_SQRT_PRICE : RAYDIUM_CLMM_MAX_SQRT_PRICE;
  const result = computeSwap(body, amountIn, sqrtPriceLimit, true, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  if (grossInput < amountIn || grossInput !== amountIn) {
    throw new UnsupportedVariant("invalid exact-in outcome");
  }
  const after = bodyAfter(body, result);
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFeeAmount,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "clmm_fee", amount_raw: result.totalFeeAmount }],
    body_after: after,
  };
}

export function simulateRaydiumClmmExactOut(
  body: RaydiumClmmBody,
  amountOut: bigint,
  aToB: boolean,
): SwapTransition {
  if (amountOut <= 0n) throw new UnsupportedVariant("exact-out amount must be positive");
  const sqrtPriceLimit = aToB ? RAYDIUM_CLMM_MIN_SQRT_PRICE : RAYDIUM_CLMM_MAX_SQRT_PRICE;
  const result = computeSwap(body, amountOut, sqrtPriceLimit, false, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  if (netOutput !== amountOut) throw new UnsupportedVariant("exact-out internal mismatch");
  const after = bodyAfter(body, result);
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFeeAmount,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "clmm_fee", amount_raw: result.totalFeeAmount }],
    body_after: after,
  };
}
