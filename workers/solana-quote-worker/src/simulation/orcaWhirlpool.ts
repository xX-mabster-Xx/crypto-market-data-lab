/** Pure classic Orca Whirlpool post-trade adapter (offline core).
 *
 * Faithful port of the pinned ``@orca-so/whirlpools-sdk`` 0.22.0 swap path
 * restricted to classic non-adaptive fee tiers.  It never mutates the observed
 * body and returns a typed ``UnsupportedVariant`` when a swap would traverse
 * outside the supplied tick arrays or invert a zero denominator.
 */

import { UnsupportedVariant } from "./errors.js";
export { UnsupportedVariant } from "./errors.js";

export const MIN_SQRT_PRICE = 4_295_048_016n;
export const MAX_SQRT_PRICE = 79_226_673_515_401_279_992_447_579_055n;
export const MIN_TICK_INDEX = -443_636;
export const MAX_TICK_INDEX = 443_636;
export const TICK_ARRAY_SIZE = 88;
export const FEE_RATE_MUL_VALUE = 1_000_000n;
export const U64_MAX = (1n << 64n) - 1n;
export const BIT_PRECISION = 14;
export const LOG_B_2_X32 = 59_543_866_431_248n;
export const LOG_B_P_ERR_MARGIN_LOWER_X64 = 184_467_440_737_095_516n;
export const LOG_B_P_ERR_MARGIN_UPPER_X64 = 15_793_534_762_490_258_745n;

const POSITIVE_FACTORS = [
  "79236085330515764027303304731", "79244008939048815603706035061",
  "79259858533276714757314932305", "79291567232598584799939703904",
  "79355022692464371645785046466", "79482085999252804386437311141",
  "79736823300114093921829183326", "80248749790819932309965073892",
  "81282483887344747381513967011", "83390072131320151908154831281",
  "87770609709833776024991924138", "97234110755111693312479820773",
  "119332217159966728226237229890", "179736315981702064433883588727",
  "407748233172238350107850275304", "2098478828474011932436660412517",
  "55581415166113811149459800483533", "38992368544603139932233054999993551",
] as const;

const NEGATIVE_FACTORS = [
  "18444899583751176498", "18443055278223354162", "18439367220385604838",
  "18431993317065449817", "18417254355718160513", "18387811781193591352",
  "18329067761203520168", "18212142134806087854", "17980523815641551639",
  "17526086738831147013", "16651378430235024244", "15030750278693429944",
  "12247334978882834399", "8131365268884726200", "3584323654723342297",
  "696457651847595233", "26294789957452057", "37481735321082",
] as const;

export interface WhirlpoolTick {
  readonly initialized: boolean;
  readonly liquidityNet: bigint;
  readonly liquidityGross: bigint;
}

export interface WhirlpoolTickArray {
  readonly startTickIndex: number;
  readonly ticks: readonly WhirlpoolTick[];
}

export interface OrcaWhirlpoolBody {
  readonly protocol: "orca_whirlpool";
  readonly pool_id: string;
  readonly sqrt_price_x64: bigint;
  readonly liquidity_raw: bigint;
  readonly tick_current_index: number;
  readonly tick_spacing: number;
  readonly fee_rate: bigint;
  readonly protocol_fee_rate: bigint;
  readonly fee_growth_global_a: bigint;
  readonly fee_growth_global_b: bigint;
  readonly protocol_fee_owed_a: bigint;
  readonly protocol_fee_owed_b: bigint;
  readonly tick_arrays: readonly WhirlpoolTickArray[];
}

export interface SwapTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: OrcaWhirlpoolBody;
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

function tickIndexToSqrtPriceX64(tickIndex: number): bigint {
  if (tickIndex > 0) {
    let ratio = (tickIndex & 1) !== 0
      ? 79_232_123_823_359_799_118_286_999_567n
      : 79_228_162_514_264_337_593_543_950_336n;
    for (let i = 1; i <= POSITIVE_FACTORS.length; i += 1) {
      if (((tickIndex >> i) & 1) !== 0) {
        ratio = (ratio * BigInt(POSITIVE_FACTORS[i - 1])) >> 96n;
      }
    }
    return ratio >> 32n;
  }
  const tick = Math.abs(tickIndex);
  let ratio = (tick & 1) !== 0 ? 18_445_821_805_675_392_311n : 18_446_744_073_709_551_616n;
  for (let i = 1; i <= NEGATIVE_FACTORS.length; i += 1) {
    if (((tick >> i) & 1) !== 0) {
      ratio = (ratio * BigInt(NEGATIVE_FACTORS[i - 1])) >> 64n;
    }
  }
  return ratio;
}

function sqrtPriceX64ToTickIndex(input: bigint): number {
  if (input > MAX_SQRT_PRICE || input < MIN_SQRT_PRICE) {
    throw new UnsupportedVariant("sqrt price is outside the supported range");
  }
  const msb = input.toString(2).length - 1;
  const adjustedMsb = BigInt(msb - 64);
  let log2pIntegerX32 = adjustedMsb << 32n;
  let bit = 1n << 63n;
  let precision = 0;
  let log2pFractionX64 = 0n;
  let r = msb >= 64 ? input >> BigInt(msb - 63) : input << BigInt(63 - msb);
  while (bit > 0n && precision < BIT_PRECISION) {
    r = r * r;
    const rMoreThanTwo = r >> 127n;
    r = r >> (63n + rMoreThanTwo);
    log2pFractionX64 += bit * rMoreThanTwo;
    bit >>= 1n;
    precision += 1;
  }
  const log2pFractionX32 = log2pFractionX64 >> 32n;
  const log2pX32 = log2pIntegerX32 + log2pFractionX32;
  const logBpX64 = log2pX32 * LOG_B_2_X32;
  const tickLow = Number((logBpX64 - LOG_B_P_ERR_MARGIN_LOWER_X64) >> 64n);
  const tickHigh = Number((logBpX64 + LOG_B_P_ERR_MARGIN_UPPER_X64) >> 64n);
  if (tickLow === tickHigh) return tickLow;
  if (tickIndexToSqrtPriceX64(tickHigh) <= input) return tickHigh;
  return tickLow;
}

function getAmountDeltaA(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint, roundUp: boolean): bigint {
  const [lower, upper] = currSqrt < targetSqrt ? [currSqrt, targetSqrt] : [targetSqrt, currSqrt];
  const numerator = (liquidity * (upper - lower)) << 64n;
  const denominator = lower * upper;
  const [quotient, remainder] = [numerator / denominator, numerator % denominator];
  const result = roundUp && remainder !== 0n ? quotient + 1n : quotient;
  if (result > U64_MAX) throw new UnsupportedVariant("token A delta exceeds u64");
  return result;
}

function getAmountDeltaB(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint, roundUp: boolean): bigint {
  const [lower, upper] = currSqrt < targetSqrt ? [currSqrt, targetSqrt] : [targetSqrt, currSqrt];
  const n1 = upper - lower;
  if (liquidity === 0n || n1 === 0n) return 0n;
  const product = liquidity * n1;
  if (product > (1n << 256n) - 1n) throw new UnsupportedVariant("token B delta exceeds u256");
  let result = product >> 64n;
  if (roundUp && (product & U64_MAX) > 0n) {
    if (result === U64_MAX) throw new UnsupportedVariant("token B delta overflows u64");
    result += 1n;
  }
  return result;
}

function amountFixedDelta(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint, isInput: boolean, aToB: boolean): bigint {
  return aToB === isInput
    ? getAmountDeltaA(currSqrt, targetSqrt, liquidity, isInput)
    : getAmountDeltaB(currSqrt, targetSqrt, liquidity, isInput);
}

function tryAmountFixedDelta(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint, isInput: boolean, aToB: boolean): bigint | null {
  try {
    return amountFixedDelta(currSqrt, targetSqrt, liquidity, isInput, aToB);
  } catch {
    return null;
  }
}

function amountUnfixedDelta(currSqrt: bigint, targetSqrt: bigint, liquidity: bigint, isInput: boolean, aToB: boolean): bigint {
  return aToB === isInput
    ? getAmountDeltaB(currSqrt, targetSqrt, liquidity, !isInput)
    : getAmountDeltaA(currSqrt, targetSqrt, liquidity, !isInput);
}

function getNextSqrtPriceFromARoundUp(sqrtPrice: bigint, liquidity: bigint, amount: bigint, isInput: boolean): bigint {
  if (amount === 0n) return sqrtPrice;
  const product = sqrtPrice * amount;
  const numerator = (liquidity * sqrtPrice) << 64n;
  if (numerator > (1n << 256n) - 1n) throw new UnsupportedVariant("getNextSqrtPriceFromA numerator overflow");
  const liquidityShiftLeft = liquidity << 64n;
  if (!isInput && liquidityShiftLeft <= product) throw new UnsupportedVariant("unable to divide liquidity by product");
  const denominator = isInput ? liquidityShiftLeft + product : liquidityShiftLeft - product;
  const price = divRoundUp(numerator, denominator);
  if (price < MIN_SQRT_PRICE) throw new UnsupportedVariant("swap price is below min sqrt price");
  if (price > MAX_SQRT_PRICE) throw new UnsupportedVariant("swap price is above max sqrt price");
  return price;
}

function getNextSqrtPriceFromBRoundDown(sqrtPrice: bigint, liquidity: bigint, amount: bigint, isInput: boolean): bigint {
  const amountX64 = amount << 64n;
  const [q, remainder] = [amountX64 / liquidity, amountX64 % liquidity];
  const delta = q + (!isInput && remainder !== 0n ? 1n : 0n);
  return isInput ? sqrtPrice + delta : sqrtPrice - delta;
}

function getNextSqrtPrice(sqrtPrice: bigint, liquidity: bigint, amount: bigint, isInput: boolean, aToB: boolean): bigint {
  return isInput === aToB
    ? getNextSqrtPriceFromARoundUp(sqrtPrice, liquidity, amount, isInput)
    : getNextSqrtPriceFromBRoundDown(sqrtPrice, liquidity, amount, isInput);
}

function computeSwapStep(
  amountRemaining: bigint,
  feeRate: bigint,
  currLiquidity: bigint,
  currSqrtPrice: bigint,
  targetSqrtPrice: bigint,
  isInput: boolean,
  aToB: boolean,
): { amountIn: bigint; amountOut: bigint; feeAmount: bigint; nextSqrtPrice: bigint } {
  const initialFixedDelta = tryAmountFixedDelta(currSqrtPrice, targetSqrtPrice, currLiquidity, isInput, aToB);
  let amountCalc = amountRemaining;
  if (isInput) {
    amountCalc = mulDiv(amountRemaining, FEE_RATE_MUL_VALUE - feeRate, FEE_RATE_MUL_VALUE);
  }
  let nextSqrtPrice;
  if (initialFixedDelta !== null && initialFixedDelta <= amountCalc) {
    nextSqrtPrice = targetSqrtPrice;
  } else {
    nextSqrtPrice = getNextSqrtPrice(currSqrtPrice, currLiquidity, amountCalc, isInput, aToB);
  }
  const isMaxSwap = nextSqrtPrice === targetSqrtPrice;
  const amountUnfixed = amountUnfixedDelta(currSqrtPrice, nextSqrtPrice, currLiquidity, isInput, aToB);
  const amountFixed = isMaxSwap && initialFixedDelta !== null
    ? initialFixedDelta
    : amountFixedDelta(currSqrtPrice, nextSqrtPrice, currLiquidity, isInput, aToB);
  const amountIn = isInput ? amountFixed : amountUnfixed;
  let amountOut = isInput ? amountUnfixed : amountFixed;
  if (!isInput && amountOut > amountRemaining) {
    amountOut = amountRemaining;
  }
  const feeAmount = isInput && !isMaxSwap
    ? amountRemaining - amountIn
    : mulDivRoundUp(amountIn, feeRate, FEE_RATE_MUL_VALUE - feeRate);
  return { amountIn, amountOut, feeAmount, nextSqrtPrice };
}

class TickSequence {
  private readonly spacing: number;
  private readonly aToB: boolean;
  private readonly arrays: readonly WhirlpoolTickArray[];
  private readonly byStart: Map<number, WhirlpoolTickArray>;
  private readonly startArrayIndex: number;

  constructor(body: OrcaWhirlpoolBody, aToB: boolean) {
    this.spacing = body.tick_spacing;
    this.aToB = aToB;
    const shift = aToB ? 0 : body.tick_spacing;
    const firstStart = startTickIndex(body.tick_current_index + shift, body.tick_spacing);
    const filtered = aToB
      ? [...body.tick_arrays].filter((a) => a.startTickIndex <= firstStart).sort((x, y) => y.startTickIndex - x.startTickIndex)
      : [...body.tick_arrays].filter((a) => a.startTickIndex >= firstStart).sort((x, y) => x.startTickIndex - y.startTickIndex);
    if (filtered.length === 0) throw new UnsupportedVariant("no tick array is available for the swap direction");
    this.arrays = filtered;
    this.byStart = new Map(this.arrays.map((a) => [a.startTickIndex, a]));
    this.startArrayIndex = arrayIndex(filtered[0].startTickIndex, body.tick_spacing);
  }

  private localArrayIndex(tickIndex: number): number {
    const arrayIndex = arrayIndexIndex(startTickIndex(tickIndex, this.spacing), this.spacing);
    return this.aToB ? this.startArrayIndex - arrayIndex : arrayIndex - this.startArrayIndex;
  }

  private isInBounds(tickIndex: number): boolean {
    const local = this.localArrayIndex(tickIndex);
    return local >= 0 && local < this.arrays.length;
  }

  getTick(tickIndex: number): WhirlpoolTick {
    if (!this.isInBounds(tickIndex)) throw new UnsupportedVariant("tick index is outside the provided tick array sequence");
    const array = this.arrays[this.localArrayIndex(tickIndex)];
    const offset = (tickIndex - array.startTickIndex) / this.spacing;
    if (offset < 0 || offset >= array.ticks.length) {
      throw new UnsupportedVariant("tick index is outside the provided tick array");
    }
    return array.ticks[offset];
  }

  findNextInitializedTick(currentIndex: number): number {
    const search = this.aToB ? currentIndex : currentIndex + this.spacing;
    if (!this.isInBounds(search)) throw new UnsupportedVariant("swap input traverses outside the supplied tick arrays");
    let tick = search;
    let guard = 0;
    while (this.isInBounds(tick) && guard < 10_000) {
      if (this.getTick(tick).initialized) return tick;
      tick += this.aToB ? -this.spacing : this.spacing;
      guard += 1;
    }
    return Math.max(
      Math.min(tick + (this.aToB ? this.spacing : -1), MAX_TICK_INDEX),
      MIN_TICK_INDEX,
    );
  }
}

function arrayIndex(startTick: number, spacing: number): number {
  return startTick / (spacing * TICK_ARRAY_SIZE);
}

function arrayIndexIndex(startTick: number, spacing: number): number {
  return arrayIndex(startTick, spacing);
}

function startTickIndex(tick: number, spacing: number): number {
  const realIndex = Math.floor(tick / spacing / TICK_ARRAY_SIZE);
  return realIndex * spacing * TICK_ARRAY_SIZE;
}

function calculateEstTokens(amount: bigint, amountRemaining: bigint, amountCalculated: bigint, aToB: boolean, isInput: boolean): [bigint, bigint] {
  return aToB === isInput
    ? [amount - amountRemaining, amountCalculated]
    : [amountCalculated, amount - amountRemaining];
}

function calculateProtocolFee(globalFee: bigint, protocolFeeRate: bigint): bigint {
  return globalFee * (protocolFeeRate / 10_000n);
}

function calculateFees(
  feeAmount: bigint,
  protocolFeeRate: bigint,
  currLiquidity: bigint,
  currProtocolFee: bigint,
  currFeeGrowth: bigint,
): { protocolFee: bigint; feeGrowth: bigint } {
  let globalFee = feeAmount;
  if (protocolFeeRate > 0n) {
    const delta = calculateProtocolFee(globalFee, protocolFeeRate);
    globalFee -= delta;
    currProtocolFee = currProtocolFee + currProtocolFee;
  }
  if (currLiquidity > 0n) {
    currFeeGrowth += (globalFee << 64n) / currLiquidity;
  }
  return { protocolFee: currProtocolFee, feeGrowth: currFeeGrowth };
}

function computeSwap(
  body: OrcaWhirlpoolBody,
  tokenAmount: bigint,
  sqrtPriceLimit: bigint,
  isInput: boolean,
  aToB: boolean,
): {
  amountA: bigint;
  amountB: bigint;
  nextTickIndex: number;
  nextSqrtPrice: bigint;
  totalFeeAmount: bigint;
  feeGrowthInput: bigint;
  protocolFee: bigint;
  liquidityAfter: bigint;
} {
  let amountRemaining = tokenAmount;
  let amountCalculated = 0n;
  let currSqrtPrice = body.sqrt_price_x64;
  let currLiquidity = body.liquidity_raw;
  let currTickIndex = body.tick_current_index;
  let totalFeeAmount = 0n;
  const feeRate = body.fee_rate;
  const protocolFeeRate = body.protocol_fee_rate;
  let currProtocolFee = 0n;
  let currFeeGrowth = aToB ? body.fee_growth_global_a : body.fee_growth_global_b;
  const sequence = new TickSequence(body, aToB);
  while (amountRemaining > 0n && currSqrtPrice !== sqrtPriceLimit) {
    const nextTickIndex = sequence.findNextInitializedTick(currTickIndex);
    const nextTickPrice = tickIndexToSqrtPriceX64(nextTickIndex);
    const sqrtPriceTarget = aToB
      ? (sqrtPriceLimit > nextTickPrice ? sqrtPriceLimit : nextTickPrice)
      : (sqrtPriceLimit < nextTickPrice ? sqrtPriceLimit : nextTickPrice);
    for (;;) {
      const step = computeSwapStep(
        amountRemaining,
        feeRate,
        currLiquidity,
        currSqrtPrice,
        sqrtPriceTarget,
        isInput,
        aToB,
      );
      totalFeeAmount += step.feeAmount;
      if (isInput) {
        amountRemaining -= step.amountIn;
        amountRemaining -= step.feeAmount;
        amountCalculated += step.amountOut;
      } else {
        amountRemaining -= step.amountOut;
        amountCalculated += step.amountIn;
        amountCalculated += step.feeAmount;
      }
      if (amountRemaining < 0n) throw new UnsupportedVariant("amount remaining became negative");
      if (amountCalculated > U64_MAX) throw new UnsupportedVariant("amount calculated exceeds u64");
      const fees = calculateFees(step.feeAmount, protocolFeeRate, currLiquidity, currProtocolFee, currFeeGrowth);
      currProtocolFee = fees.protocolFee;
      currFeeGrowth = fees.feeGrowth;
      if (step.nextSqrtPrice === nextTickPrice) {
        const tick = sequence.getTick(nextTickIndex);
        if (tick.initialized) {
          currLiquidity = aToB ? currLiquidity - tick.liquidityNet : currLiquidity + tick.liquidityNet;
        }
        currTickIndex = aToB ? nextTickIndex - 1 : nextTickIndex;
      } else {
        currTickIndex = sqrtPriceX64ToTickIndex(step.nextSqrtPrice);
      }
      currSqrtPrice = step.nextSqrtPrice;
      if (!(amountRemaining > 0n && currSqrtPrice !== sqrtPriceTarget)) break;
    }
  }
  const [amountA, amountB] = calculateEstTokens(tokenAmount, amountRemaining, amountCalculated, aToB, isInput);
  return {
    amountA,
    amountB,
    nextTickIndex: currTickIndex,
    nextSqrtPrice: currSqrtPrice,
    totalFeeAmount,
    feeGrowthInput: currFeeGrowth,
    protocolFee: currProtocolFee,
    liquidityAfter: currLiquidity,
  };
}

function bodyAfter(body: OrcaWhirlpoolBody, result: ReturnType<typeof computeSwap>, aToB: boolean): OrcaWhirlpoolBody {
  const feeGrowthA = aToB ? result.feeGrowthInput : body.fee_growth_global_a;
  const feeGrowthB = aToB ? body.fee_growth_global_b : result.feeGrowthInput;
  const protocolA = aToB ? body.protocol_fee_owed_a + result.protocolFee : body.protocol_fee_owed_a;
  const protocolB = aToB ? body.protocol_fee_owed_b : body.protocol_fee_owed_b + result.protocolFee;
  return { ...body, sqrt_price_x64: result.nextSqrtPrice, liquidity_raw: result.liquidityAfter, tick_current_index: result.nextTickIndex, fee_growth_global_a: feeGrowthA, fee_growth_global_b: feeGrowthB, protocol_fee_owed_a: protocolA, protocol_fee_owed_b: protocolB };
}

export function simulateOrcaExactIn(body: OrcaWhirlpoolBody, amountIn: bigint, aToB: boolean): SwapTransition {

  if (amountIn <= 0n) throw new UnsupportedVariant("exact-in amount must be positive");
  const sqrtPriceLimit = aToB ? MIN_SQRT_PRICE : MAX_SQRT_PRICE;
  const result = computeSwap(body, amountIn, sqrtPriceLimit, true, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  if (grossInput < amountIn || grossInput !== amountIn) {
    throw new UnsupportedVariant("invalid exact-in outcome");
  }
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFeeAmount,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "whirlpool_fee", amount_raw: result.totalFeeAmount }],
    body_after: bodyAfter(body, result, aToB),
  };
}

export function simulateOrcaExactOut(body: OrcaWhirlpoolBody, amountOut: bigint, aToB: boolean): SwapTransition {

  if (amountOut <= 0n) throw new UnsupportedVariant("exact-out amount must be positive");
  const sqrtPriceLimit = aToB ? MIN_SQRT_PRICE : MAX_SQRT_PRICE;
  const result = computeSwap(body, amountOut, sqrtPriceLimit, false, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  if (netOutput !== amountOut) throw new UnsupportedVariant("exact-out internal mismatch");
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFeeAmount,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "whirlpool_fee", amount_raw: result.totalFeeAmount }],
    body_after: bodyAfter(body, result, aToB),
  };
}

export function effectiveReserves(body: OrcaWhirlpoolBody): [bigint, bigint] {
  const tokenA = (body.liquidity_raw * body.sqrt_price_x64) >> 64n;
  const tokenB = (body.liquidity_raw << 64n) / body.sqrt_price_x64;
  return [tokenA, tokenB];
}
