/** Pure Raydium CPMM post-trade adapter and path executor.
 *
 * This mirrors the verified Python core exactly (vault balances stay separate
 * from accrued protocol/fund/creator counters; effective reserves for the next
 * swap are re-derived as ``vault - counters``).  It never touches the network,
 * mutates a shared SDK instance, or fabricates a partial fill.
 */

const FEE_RATE_DENOMINATOR = 1_000_000n;
export const RAYDIUM_FEE_ON_BOTH = 0;
export const RAYDIUM_FEE_ON_TOKEN_A = 1;
export const RAYDIUM_FEE_ON_TOKEN_B = 2;

import { UnsupportedVariant } from "./errors.js";
export { UnsupportedVariant } from "./errors.js";

export interface RaydiumCpmmBody {
  readonly protocol: "raydium_cpmm";
  readonly pool_id: string;
  readonly vault_a_raw: bigint;
  readonly vault_b_raw: bigint;
  readonly protocol_fees_a_raw: bigint;
  readonly protocol_fees_b_raw: bigint;
  readonly fund_fees_a_raw: bigint;
  readonly fund_fees_b_raw: bigint;
  readonly creator_fees_a_raw: bigint;
  readonly creator_fees_b_raw: bigint;
  readonly trade_fee_rate: bigint;
  readonly creator_fee_rate: bigint;
  readonly protocol_fee_rate: bigint;
  readonly fund_fee_rate: bigint;
  readonly fee_on: number;
}

export interface SwapTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: RaydiumCpmmBody;
}

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  if (denominator <= 0n) throw new Error("denominator must be positive");
  if (numerator <= 0n) return 0n;
  return (numerator + denominator - 1n) / denominator;
}

function ceilRate(amount: bigint, rate: bigint): bigint {
  return ceilDiv(amount * rate, FEE_RATE_DENOMINATOR);
}

function floorRate(amount: bigint, rate: bigint): bigint {
  return (amount * rate) / FEE_RATE_DENOMINATOR;
}

function swapWithoutFeesIn(amountIn: bigint, reserveIn: bigint, reserveOut: bigint): bigint {
  return (reserveOut * amountIn) / (reserveIn + amountIn);
}

function preFeeAmount(amount: bigint, rate: bigint): bigint {
  if (rate <= 0n) return amount;
  return ceilDiv(amount * FEE_RATE_DENOMINATOR, FEE_RATE_DENOMINATOR - rate);
}

function splitCreatorFee(totalFee: bigint, tradeRate: bigint, creatorRate: bigint): bigint {
  const combined = tradeRate + creatorRate;
  if (combined <= 0n) return 0n;
  return (totalFee * creatorRate) / combined;
}

function creatorFeeOnInput(feeOn: number, zeroForOne: boolean): boolean {
  if (feeOn === RAYDIUM_FEE_ON_BOTH) return true;
  if (feeOn === RAYDIUM_FEE_ON_TOKEN_A) return zeroForOne;
  if (feeOn === RAYDIUM_FEE_ON_TOKEN_B) return !zeroForOne;
  throw new UnsupportedVariant("unsupported Raydium CPMM fee_on value");
}

function directedReserves(
  reserveA: bigint,
  reserveB: bigint,
  zeroForOne: boolean,
): [bigint, bigint] {
  return zeroForOne ? [reserveA, reserveB] : [reserveB, reserveA];
}

export function effectiveReserves(pool: RaydiumCpmmBody): [bigint, bigint] {
  return [
    pool.vault_a_raw - pool.protocol_fees_a_raw - pool.fund_fees_a_raw - pool.creator_fees_a_raw,
    pool.vault_b_raw - pool.protocol_fees_b_raw - pool.fund_fees_b_raw - pool.creator_fees_b_raw,
  ];
}

function raydiumAfter(
  pool: RaydiumCpmmBody,
  {
    zeroForOne,
    totalInput,
    netUserOutput,
    protocolFee,
    fundFee,
    creatorFee,
    creatorOnInput,
  }: {
    zeroForOne: boolean;
    totalInput: bigint;
    netUserOutput: bigint;
    protocolFee: bigint;
    fundFee: bigint;
    creatorFee: bigint;
    creatorOnInput: boolean;
  },
): RaydiumCpmmBody {
  const [effectiveA0, effectiveB0] = effectiveReserves(pool);
  const grossOutput = netUserOutput + (creatorOnInput ? 0n : creatorFee);
  const inputSideFee = protocolFee + fundFee + (creatorOnInput ? creatorFee : 0n);
  let effectiveA: bigint;
  let effectiveB: bigint;
  if (zeroForOne) {
    effectiveA = effectiveA0 + totalInput - inputSideFee;
    effectiveB = effectiveB0 - grossOutput;
  } else {
    effectiveA = effectiveA0 - grossOutput;
    effectiveB = effectiveB0 + totalInput - inputSideFee;
  }
  const protocolA = pool.protocol_fees_a_raw + (zeroForOne ? protocolFee : 0n);
  const protocolB = pool.protocol_fees_b_raw + (zeroForOne ? 0n : protocolFee);
  const fundA = pool.fund_fees_a_raw + (zeroForOne ? fundFee : 0n);
  const fundB = pool.fund_fees_b_raw + (zeroForOne ? 0n : fundFee);
  const creatorOnA = creatorOnInput === zeroForOne;
  const creatorA = pool.creator_fees_a_raw + (creatorOnA ? creatorFee : 0n);
  const creatorB = pool.creator_fees_b_raw + (creatorOnA ? 0n : creatorFee);
  const vaultA = effectiveA + protocolA + fundA + creatorA;
  const vaultB = effectiveB + protocolB + fundB + creatorB;
  const after: RaydiumCpmmBody = {
    ...pool,
    vault_a_raw: vaultA,
    vault_b_raw: vaultB,
    protocol_fees_a_raw: protocolA,
    protocol_fees_b_raw: protocolB,
    fund_fees_a_raw: fundA,
    fund_fees_b_raw: fundB,
    creator_fees_a_raw: creatorA,
    creator_fees_b_raw: creatorB,
  };
  const [afterA, afterB] = effectiveReserves(after);
  if (afterA <= 0n || afterB <= 0n) {
    throw new UnsupportedVariant("Raydium CPMM post-state has no effective liquidity");
  }
  return after;
}

export function simulateRaydiumCpmmExactIn(
  body: RaydiumCpmmBody,
  amountIn: bigint,
  zeroForOne: boolean,
): SwapTransition {
  const [reserveIn, reserveOut] = directedReserves(...effectiveReserves(body), zeroForOne);
  const creatorOnInput = creatorFeeOnInput(body.fee_on, zeroForOne);
  const tradeFee = ceilRate(amountIn, body.trade_fee_rate);
  let creatorFee = creatorOnInput ? ceilRate(amountIn, body.creator_fee_rate) : 0n;
  const swapInput = amountIn - tradeFee - creatorFee;
  if (swapInput <= 0n) {
    throw new UnsupportedVariant("Raydium CPMM fee consumes the entire input");
  }
  const grossPoolOutput = swapWithoutFeesIn(swapInput, reserveIn, reserveOut);
  if (grossPoolOutput <= 0n || grossPoolOutput >= reserveOut) {
    throw new UnsupportedVariant("insufficient Raydium CPMM liquidity");
  }
  const protocolFee = floorRate(tradeFee, body.protocol_fee_rate);
  const fundFee = floorRate(tradeFee, body.fund_fee_rate);
  if (!creatorOnInput) {
    creatorFee = ceilRate(grossPoolOutput, body.creator_fee_rate);
  }
  const netOutput = grossPoolOutput - (creatorOnInput ? 0n : creatorFee);
  if (netOutput <= 0n) {
    throw new UnsupportedVariant("Raydium CPMM creator fee consumes the entire output");
  }
  const bodyAfter = raydiumAfter(body, {
    zeroForOne,
    totalInput: amountIn,
    netUserOutput: netOutput,
    protocolFee,
    fundFee,
    creatorFee,
    creatorOnInput,
  });
  return {
    gross_input: amountIn,
    effective_input: swapInput,
    gross_pool_output: grossPoolOutput,
    net_output: netOutput,
    fees: [
      { kind: "protocol_fee", amount_raw: protocolFee },
      { kind: "fund_fee", amount_raw: fundFee },
      { kind: "creator_fee", amount_raw: creatorFee },
    ],
    body_after: bodyAfter,
  };
}

export function simulateRaydiumCpmmExactOut(
  body: RaydiumCpmmBody,
  amountOut: bigint,
  zeroForOne: boolean,
): SwapTransition {
  const [reserveIn, reserveOut] = directedReserves(...effectiveReserves(body), zeroForOne);
  if (amountOut <= 0n || amountOut >= reserveOut) {
    throw new UnsupportedVariant("exact-output amount must fit inside output reserve");
  }
  const creatorOnInput = creatorFeeOnInput(body.fee_on, zeroForOne);
  let grossOutput: bigint;
  let creatorFee: bigint;
  if (creatorOnInput) {
    grossOutput = amountOut;
    creatorFee = 0n;
  } else {
    grossOutput = preFeeAmount(amountOut, body.creator_fee_rate);
    creatorFee = grossOutput - amountOut;
  }
  if (grossOutput <= 0n || grossOutput >= reserveOut) {
    throw new UnsupportedVariant("exact-output amount must fit inside output reserve");
  }
  const swapWithoutFeesOut = (out: bigint): bigint =>
    ceilDiv(reserveIn * out, reserveOut - out);
  const preFeeInput = swapWithoutFeesOut(grossOutput);
  if (preFeeInput <= 0n) {
    throw new UnsupportedVariant("insufficient Raydium CPMM liquidity");
  }
  let grossInput: bigint;
  let tradeFee: bigint;
  if (creatorOnInput) {
    grossInput = preFeeAmount(preFeeInput, body.trade_fee_rate + body.creator_fee_rate);
    const totalInputFees = grossInput - preFeeInput;
    creatorFee = splitCreatorFee(totalInputFees, body.trade_fee_rate, body.creator_fee_rate);
    tradeFee = totalInputFees - creatorFee;
  } else {
    grossInput = preFeeAmount(preFeeInput, body.trade_fee_rate);
    tradeFee = grossInput - preFeeInput;
  }
  const protocolFee = floorRate(tradeFee, body.protocol_fee_rate);
  const fundFee = floorRate(tradeFee, body.fund_fee_rate);
  const bodyAfter = raydiumAfter(body, {
    zeroForOne,
    totalInput: grossInput,
    netUserOutput: amountOut,
    protocolFee,
    fundFee,
    creatorFee,
    creatorOnInput,
  });
  return {
    gross_input: grossInput,
    effective_input: preFeeInput,
    gross_pool_output: grossOutput,
    net_output: amountOut,
    fees: [
      { kind: "protocol_fee", amount_raw: protocolFee },
      { kind: "fund_fee", amount_raw: fundFee },
      { kind: "creator_fee", amount_raw: creatorFee },
    ],
    body_after: bodyAfter,
  };
}
