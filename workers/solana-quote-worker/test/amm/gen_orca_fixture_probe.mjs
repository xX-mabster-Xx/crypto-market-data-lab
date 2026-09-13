import BN from "bn.js";
import { PublicKey } from "@solana/web3.js";
import { swapQuoteWithParams, NO_TOKEN_EXTENSION_CONTEXT, PriceMath, TICK_ARRAY_SIZE } from "@orca-so/whirlpools-sdk";

const tickSpacing = 64;
const currentTick = -6144;
const sqrtPrice = PriceMath.tickIndexToSqrtPriceX64(currentTick);

function zeroTicks(n) {
  return Array.from({length: n}, () => ({
    initialized: false,
    liquidityNet: new BN(0),
    liquidityGross: new BN(0),
    feeGrowthOutsideA: new BN(0),
    feeGrowthOutsideB: new BN(0),
    rewardGrowthsOutside: [new BN(0), new BN(0), new BN(0)],
  }));
}

// Build a tick array whose offset 0 == currentTick (start = currentTick).
// For aToB, initialized ticks below current reduce liquidity via negative liquidityNet.
const startTickIndex = currentTick - (currentTick % tickSpacing);
const tickArray = {
  startTickIndex,
  ticks: zeroTicks(TICK_ARRAY_SIZE),
  whirlpool: new PublicKey(0).toBase58(),
};
// Need whirlpool as PublicKey per TickArrayData? Build via builder helpers.
console.log("startTickIndex", startTickIndex, "sqrtPrice", sqrtPrice.toString(10), "tickSpacing", tickSpacing);
