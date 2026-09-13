import BN from "bn.js";
import { PublicKey } from "@solana/web3.js";
import {
  swapQuoteWithParams, NO_TOKEN_EXTENSION_CONTEXT,
  PriceMath, TICK_ARRAY_SIZE, TickUtil,
  MIN_SQRT_PRICE, MAX_SQRT_PRICE,
} from "@orca-so/whirlpools-sdk";
import { Percentage } from "@orca-so/common-sdk";
import { U64_MAX } from "@orca-so/common-sdk";

const tickSpacing = 64;
const currentTick = -6144;
const feeRate = 500;
const protocolFeeRate = 300;
const ZERO_TICK = () => ({
  initialized: false,
  liquidityNet: new BN(0),
  liquidityGross: new BN(0),
  feeGrowthOutsideA: new BN(0),
  feeGrowthOutsideB: new BN(0),
  rewardGrowthsOutside: [new BN(0), new BN(0), new BN(0)],
});

const pool = {
  whirlpoolsConfig: PublicKey.default,
  whirlpoolBump: [],
  feeRate,
  protocolFeeRate,
  liquidity: new BN("100000000000000"),
  sqrtPrice: PriceMath.tickIndexToSqrtPriceX64(currentTick),
  tickCurrentIndex: currentTick,
  protocolFeeOwedA: new BN(0),
  protocolFeeOwedB: new BN(0),
  tokenMintA: PublicKey.default,
  tokenVaultA: PublicKey.default,
  feeGrowthGlobalA: new BN(0),
  tokenMintB: PublicKey.default,
  tokenVaultB: PublicKey.default,
  feeGrowthGlobalB: new BN(0),
  rewardLastUpdatedTimestamp: new BN(0),
  rewardInfos: [],
  tickSpacing,
  feeTierIndexSeed: [tickSpacing & 0xff, (tickSpacing >> 8) & 0xff],
};

function buildTickArrays(current, spacing, aToB, initialized) {
  const shift = aToB ? 0 : spacing;
  const requested = [];
  const add = (off) => {
    try { requested.push(TickUtil.getStartTickIndex(current + shift, spacing, off)); }
    catch { /* clamped */ }
  };
  add(0);
  if (aToB) { add(-1); add(-2); }
  else { add(1); add(2); }
  const seen = [...new Set(requested)];
  return seen.map((start) => {
    const ticks = Array.from({ length: TICK_ARRAY_SIZE }, ZERO_TICK);
    for (const { tick, net } of initialized) {
      if (TickUtil.getStartTickIndex(tick, spacing, 0) === start) {
        const offset = Math.floor((tick - start) / spacing);
        ticks[offset] = { ...ZERO_TICK(), initialized: true, liquidityNet: new BN(net), liquidityGross: new BN(net) };
      }
    }
    return {
      address: PublicKey.default,
      startTickIndex: start,
      data: { startTickIndex: start, ticks, whirlpool: PublicKey.default },
    };
  });
}

const initialized = [
  { tick: -6208, net: -1000000000000 },
  { tick: -6336, net: -2000000000000 },
  { tick: -6464, net: -3000000000000 },
];

const cases = [];
const inputAmounts = ["1000000", "987654321", "1"];
for (const aToB of [true, false]) {
  const tickArrays = buildTickArrays(currentTick, tickSpacing, aToB, initialized);
  for (const amount of inputAmounts) {
    const quote = swapQuoteWithParams({
      whirlpoolData: pool,
      tokenAmount: new BN(amount),
      otherAmountThreshold: new BN(0),
      sqrtPriceLimit: new BN(aToB ? MIN_SQRT_PRICE : MAX_SQRT_PRICE),
      aToB,
      amountSpecifiedIsInput: true,
      tickArrays,
      oracleData: null,
      tokenExtensionCtx: NO_TOKEN_EXTENSION_CONTEXT,
    }, Percentage.fromFraction(0, 100));
    cases.push({
      a_to_b: aToB,
      amount_specified_is_input: true,
      input_amount_raw: amount,
      output_amount_raw: quote.estimatedAmountOut.toString(10),
      fee_amount_raw: quote.estimatedFeeAmount.toString(10),
      end_tick_index: quote.estimatedEndTickIndex,
      end_sqrt_price_x64: quote.estimatedEndSqrtPrice.toString(10),
      applied_fee_rate_min: quote.estimatedFeeRateMin,
      applied_fee_rate_max: quote.estimatedFeeRateMax,
    });
  }
  // exact out
  const outAmount = "500000";
  const quoteOut = swapQuoteWithParams({
    whirlpoolData: pool,
    tokenAmount: new BN(outAmount),
    otherAmountThreshold: U64_MAX,
    sqrtPriceLimit: new BN(aToB ? MIN_SQRT_PRICE : MAX_SQRT_PRICE),
    aToB,
    amountSpecifiedIsInput: false,
    tickArrays,
    oracleData: null,
    tokenExtensionCtx: NO_TOKEN_EXTENSION_CONTEXT,
  }, Percentage.fromFraction(0, 100));
  cases.push({
    a_to_b: aToB,
    amount_specified_is_input: false,
    output_amount_raw: outAmount,
    input_amount_raw: quoteOut.estimatedAmountIn.toString(10),
    fee_amount_raw: quoteOut.estimatedFeeAmount.toString(10),
    end_tick_index: quoteOut.estimatedEndTickIndex,
    end_sqrt_price_x64: quoteOut.estimatedEndSqrtPrice.toString(10),
    applied_fee_rate_min: quoteOut.estimatedFeeRateMin,
    applied_fee_rate_max: quoteOut.estimatedFeeRateMax,
  });
}

console.log(JSON.stringify({
  sdk: "@orca-so/whirlpools-sdk",
  sdk_version: "0.22.0",
  tick_spacing: tickSpacing,
  current_tick_index: currentTick,
  fee_rate: feeRate,
  protocol_fee_rate: protocolFeeRate,
  liquidity_raw: pool.liquidity.toString(10),
  sqrt_price_x64: pool.sqrtPrice.toString(10),
  cases,
}, null, 2));
