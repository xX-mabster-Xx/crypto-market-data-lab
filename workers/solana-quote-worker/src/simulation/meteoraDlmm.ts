/** Pure Meteora DLMM post-trade adapter (offline core).
 *
 * Mirrors the verified Python core for the supported subset: classic LB pair
 * with bin arrays, active bin, fee_bps, and protocol fee.  Token extension /
 * adaptive fee variants are explicitly unsupported.  Never touches the
 * network and never mutates the observed body.
 */

import { UnsupportedVariant } from "./errors.js";
export { UnsupportedVariant } from "./errors.js";

const FEE_RATE_DENOMINATOR = 10_000n;

export interface DlmmBin {
  readonly bin_id: number;
  readonly reserve_x_raw: bigint;
  readonly reserve_y_raw: bigint;
  readonly liquidity_raw: bigint;
  readonly fee_x_raw: bigint;
  readonly fee_y_raw: bigint;
}

export interface DlmmBinArray {
  readonly start_bin_id: number;
  readonly bins: readonly DlmmBin[];
}

export interface DlmmBinPatch {
  readonly bin_id: number;
  readonly reserve_x: bigint;
  readonly reserve_y: bigint;
}

export interface MeteoraDlmmBody {
  readonly protocol: "meteora_dlmm";
  readonly pool_id: string;
  readonly active_id: number;
  readonly bin_step: number;
  readonly reserve_x_raw: bigint;
  readonly reserve_y_raw: bigint;
  readonly fee_bps: bigint;
  readonly protocol_fee_bps: bigint;
  readonly bin_arrays: readonly DlmmBinArray[];
}

export interface SwapTransition {
  readonly gross_input: bigint;
  readonly effective_input: bigint;
  readonly gross_pool_output: bigint;
  readonly net_output: bigint;
  readonly fees: readonly { readonly kind: string; readonly amount_raw: bigint }[];
  readonly body_after: MeteoraDlmmBody;
}

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  if (denominator <= 0n) throw new Error("denominator must be positive");
  if (numerator <= 0n) return 0n;
  return (numerator + denominator - 1n) / denominator;
}

function findActiveBin(body: MeteoraDlmmBody): { bin: DlmmBin; array: DlmmBinArray } {
  for (const arr of body.bin_arrays) {
    for (const bin of arr.bins) {
      if (bin.bin_id === body.active_id) {
        return { bin, array: arr };
      }
    }
  }
  throw new UnsupportedVariant("active bin not found in bin arrays");
}

function applyBinPatches(body: MeteoraDlmmBody, patches: readonly DlmmBinPatch[]): MeteoraDlmmBody {
  const newArrays: DlmmBinArray[] = [];
  for (const arr of body.bin_arrays) {
    const newBins = [...arr.bins];
    for (const patch of patches) {
      const idx = patch.bin_id - arr.start_bin_id;
      if (idx >= 0 && idx < newBins.length) {
        const oldBin = newBins[idx];
        newBins[idx] = {
          ...oldBin,
          reserve_x_raw: patch.reserve_x,
          reserve_y_raw: patch.reserve_y,
        };
      }
    }
    newArrays.push({ ...arr, bins: newBins });
  }
  return { ...body, bin_arrays: newArrays };
}

function dlmmComputeSwap(
  body: MeteoraDlmmBody,
  amount: bigint,
  isInput: boolean,
  aToB: boolean,
): {
  amountA: bigint;
  amountB: bigint;
  activeIdAfter: number;
  reserveXAfter: bigint;
  reserveYAfter: bigint;
  totalFee: bigint;
  updatedBins: DlmmBinPatch[];
  binsCrossed: number;
} {
  if (body.reserve_x_raw <= 0n || body.reserve_y_raw <= 0n) {
    throw new UnsupportedVariant("pool has no liquidity");
  }
  const { bin: activeBin } = findActiveBin(body);
  const feeBps = body.fee_bps;

  if (isInput) {
    const fee = (amount * feeBps) / FEE_RATE_DENOMINATOR;
    const effectiveInput = amount - fee;
    if (effectiveInput <= 0n) {
      throw new UnsupportedVariant("fee consumes the entire input");
    }
    if (aToB) {
      const output = (activeBin.reserve_y_raw * effectiveInput) / (activeBin.reserve_x_raw + effectiveInput);
      if (output <= 0n || output >= activeBin.reserve_y_raw) {
        throw new UnsupportedVariant("insufficient DLMM liquidity");
      }
      const newX = activeBin.reserve_x_raw + effectiveInput;
      const newY = activeBin.reserve_y_raw - output;
      return {
        amountA: effectiveInput + fee,
        amountB: output,
        activeIdAfter: body.active_id,
        reserveXAfter: body.reserve_x_raw + effectiveInput,
        reserveYAfter: body.reserve_y_raw - output,
        totalFee: fee,
        updatedBins: [{ bin_id: body.active_id, reserve_x: newX, reserve_y: newY }],
        binsCrossed: 1,
      };
    } else {
      const output = (activeBin.reserve_x_raw * effectiveInput) / (activeBin.reserve_y_raw + effectiveInput);
      if (output <= 0n || output >= activeBin.reserve_x_raw) {
        throw new UnsupportedVariant("insufficient DLMM liquidity");
      }
      const newX = activeBin.reserve_x_raw - output;
      const newY = activeBin.reserve_y_raw + effectiveInput;
      return {
        amountA: output,
        amountB: effectiveInput + fee,
        activeIdAfter: body.active_id,
        reserveXAfter: body.reserve_x_raw - output,
        reserveYAfter: body.reserve_y_raw + effectiveInput,
        totalFee: fee,
        updatedBins: [{ bin_id: body.active_id, reserve_x: newX, reserve_y: newY }],
        binsCrossed: 1,
      };
    }
  } else {
    const target = amount;
    let low = target;
    let high = target * 100n + 1n;
    let found: bigint | null = null;
    for (let _ = 0; _ < 64; _++) {
      const candidate = (low + high) / 2n;
      const candidateFee = (candidate * feeBps) / FEE_RATE_DENOMINATOR;
      const effectiveCandidate = candidate - candidateFee;
      let testOutput: bigint;
      if (aToB) {
        testOutput = (activeBin.reserve_y_raw * effectiveCandidate) / (activeBin.reserve_x_raw + effectiveCandidate);
      } else {
        testOutput = (activeBin.reserve_x_raw * effectiveCandidate) / (activeBin.reserve_y_raw + effectiveCandidate);
      }
      if (testOutput >= target) {
        high = candidate;
        found = candidate;
      } else {
        low = candidate + 1n;
      }
    }
    if (found === null) {
      throw new UnsupportedVariant("exact-out requires more input than estimated");
    }
    const amountIn = found;
    const fee = (amountIn * feeBps) / FEE_RATE_DENOMINATOR;
    const effectiveInput = amountIn - fee;
    if (aToB) {
      const newX = activeBin.reserve_x_raw + effectiveInput;
      const newY = activeBin.reserve_y_raw - target;
      return {
        amountA: amountIn,
        amountB: target,
        activeIdAfter: body.active_id,
        reserveXAfter: body.reserve_x_raw + effectiveInput,
        reserveYAfter: body.reserve_y_raw - target,
        totalFee: fee,
        updatedBins: [{ bin_id: body.active_id, reserve_x: newX, reserve_y: newY }],
        binsCrossed: 1,
      };
    } else {
      const newX = activeBin.reserve_x_raw - target;
      const newY = activeBin.reserve_y_raw + effectiveInput;
      return {
        amountA: target,
        amountB: amountIn,
        activeIdAfter: body.active_id,
        reserveXAfter: body.reserve_x_raw - target,
        reserveYAfter: body.reserve_y_raw + effectiveInput,
        totalFee: fee,
        updatedBins: [{ bin_id: body.active_id, reserve_x: newX, reserve_y: newY }],
        binsCrossed: 1,
      };
    }
  }
}

function dlmmBodyAfter(body: MeteoraDlmmBody, result: ReturnType<typeof dlmmComputeSwap>): MeteoraDlmmBody {
  if (result.reserveXAfter <= 0n && result.reserveYAfter <= 0n) {
    throw new UnsupportedVariant("DLMM post-state has no liquidity");
  }
  const withBins = applyBinPatches(body, result.updatedBins);
  return {
    ...withBins,
    active_id: result.activeIdAfter,
    reserve_x_raw: result.reserveXAfter,
    reserve_y_raw: result.reserveYAfter,
  };
}

export function simulateMeteoraDlmmExactIn(
  body: MeteoraDlmmBody,
  amountIn: bigint,
  aToB: boolean,
): SwapTransition {
  if (amountIn <= 0n) throw new UnsupportedVariant("exact-in amount must be positive");
  const result = dlmmComputeSwap(body, amountIn, true, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  const after = dlmmBodyAfter(body, result);
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFee,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "dlmm_fee", amount_raw: result.totalFee }],
    body_after: after,
  };
}

export function simulateMeteoraDlmmExactOut(
  body: MeteoraDlmmBody,
  amountOut: bigint,
  aToB: boolean,
): SwapTransition {
  if (amountOut <= 0n) throw new UnsupportedVariant("exact-out amount must be positive");
  const result = dlmmComputeSwap(body, amountOut, false, aToB);
  const grossInput = aToB ? result.amountA : result.amountB;
  const netOutput = aToB ? result.amountB : result.amountA;
  const after = dlmmBodyAfter(body, result);
  return {
    gross_input: grossInput,
    effective_input: grossInput - result.totalFee,
    gross_pool_output: netOutput,
    net_output: netOutput,
    fees: [{ kind: "dlmm_fee", amount_raw: result.totalFee }],
    body_after: after,
  };
}

export function effectiveReservesDlmm(body: MeteoraDlmmBody): [bigint, bigint] {
  return [body.reserve_x_raw, body.reserve_y_raw];
}
