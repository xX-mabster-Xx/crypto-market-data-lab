import assert from "node:assert/strict";
import test from "node:test";

import {
  simulateRaydiumClmmExactIn,
  simulateRaydiumClmmExactOut,
  type RaydiumClmmBody,
} from "../src/simulation/raydiumClmm.js";
import {
  simulateMeteoraDlmmExactIn,
  simulateMeteoraDlmmExactOut,
  type MeteoraDlmmBody,
} from "../src/simulation/meteoraDlmm.js";
import {
  simulateRaydiumAmmV4ExactIn,
  simulateRaydiumAmmV4ExactOut,
  type RaydiumAmmV4Body,
} from "../src/simulation/raydiumAmmV4.js";
import {
  raydiumClmmSnapshotBundle,
  meteoraDlmmSnapshotBundle,
  raydiumAmmV4SnapshotBundle,
} from "../src/simulation/snapshots.js";
import {
  simulatePathLegs,
  type SimulatePathLegInput,
} from "../src/simulation/path.js";
import { UnsupportedVariant } from "../src/simulation/raydiumClmm.js";

const assetA = "solana:mainnet:mint-a:6";
const assetB = "solana:mainnet:mint-b:9";

// ---------------------------------------------------------------------------
// CLMM fixtures
// ---------------------------------------------------------------------------

const clmmState = {
  pool_id: "clmm-pool",
  slot: 50,
  token_a_mint: "mint-a",
  token_b_mint: "mint-b",
  token_a_decimals: 6,
  token_b_decimals: 9,
  sqrt_price_x64: "18362684800000000000",
  liquidity_raw: "1000000000000",
  tick_current_index: 100,
  tick_spacing: 10,
  fee_rate: 2500,
  protocol_fee_rate: 120,
  tick_arrays: [
    {
      start_tick_index: -240,
      ticks: Array.from({ length: 60 }, (_, i) => ({
        initialized: i === 24 || i === 36,
        liquidity_net: i === 24 || i === 36 ? 1000000n : 0n,
        liquidity_gross: 1000000n,
      })),
    },
  ],
};

const clmmBody: RaydiumClmmBody = {
  protocol: "raydium_clmm",
  pool_id: "clmm-pool",
  sqrt_price_x64: BigInt(clmmState.sqrt_price_x64),
  liquidity_raw: BigInt(clmmState.liquidity_raw),
  tick_current_index: clmmState.tick_current_index,
  tick_spacing: clmmState.tick_spacing,
  fee_rate: BigInt(clmmState.fee_rate),
  protocol_fee_rate: BigInt(clmmState.protocol_fee_rate),
  tick_arrays: clmmState.tick_arrays.map((arr) => ({
    startTickIndex: arr.start_tick_index,
    ticks: arr.ticks.map((t) => ({
      initialized: t.initialized,
      liquidityNet: t.liquidity_net,
      liquidityGross: t.liquidity_gross,
    })),
  })),
};

// ---------------------------------------------------------------------------
// DLMM fixtures
// ---------------------------------------------------------------------------

const dlmmBody: MeteoraDlmmBody = {
  protocol: "meteora_dlmm",
  pool_id: "dlmm-pool",
  active_id: 1,
  bin_step: 1,
  reserve_x_raw: 10000000000n,
  reserve_y_raw: 10000000000n,
  fee_bps: 20n,
  protocol_fee_bps: 5n,
  bin_arrays: [
    {
      start_bin_id: 0,
      bins: Array.from({ length: 10 }, (_, i) => ({
        bin_id: i,
        reserve_x_raw: 1000000n,
        reserve_y_raw: 1000000n,
        liquidity_raw: 1000000n,
        fee_x_raw: 0n,
        fee_y_raw: 0n,
      })),
    },
  ],
};

// ---------------------------------------------------------------------------
// AMM v4 fixtures
// ---------------------------------------------------------------------------

const ammV4Body: RaydiumAmmV4Body = {
  protocol: "raydium_amm_v4",
  pool_id: "ammv4-pool",
  vault_a_raw: 1000000000n,
  vault_b_raw: 1000000000n,
  fee_raw_a: 0n,
  fee_raw_b: 0n,
  fee_rate: 2500n,
  need_take_pnl: false,
  open_orders: null,
  status: 0,
};

// ---------------------------------------------------------------------------
// CLMM tests
// ---------------------------------------------------------------------------

test("CLMM exact-in a→b produces positive output and fee", () => {
  const result = simulateRaydiumClmmExactIn(clmmBody, 1000000000n, true);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
  assert.ok(result.fees[0].amount_raw > 0n, "fee must be positive");
  assert.equal(result.gross_input - result.fees[0].amount_raw, result.effective_input);
});

test("CLMM exact-in b→a produces positive output and fee", () => {
  const result = simulateRaydiumClmmExactIn(clmmBody, 1000000000n, false);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
  assert.ok(result.fees[0].amount_raw > 0n, "fee must be positive");
});

test("CLMM exact-out produces requested output", () => {
  const result = simulateRaydiumClmmExactOut(clmmBody, 500000000n, true);
  assert.equal(result.net_output, 500000000n);
  assert.ok(result.gross_input > result.net_output, "gross input must exceed output");
});

test("CLMM protocol mismatch raises UnsupportedVariant", () => {
  const wrongBody = { ...ammV4Body, protocol: "raydium_clmm" } as unknown as RaydiumClmmBody;
  assert.throws(
    () => simulateRaydiumClmmExactIn(wrongBody, 1000n, true),
    (err) => err instanceof Error,
  );
});

// ---------------------------------------------------------------------------
// DLMM tests
// ---------------------------------------------------------------------------

test("DLMM exact-in a→b produces positive output and fee", () => {
  const result = simulateMeteoraDlmmExactIn(dlmmBody, 1000000000n, true);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
  assert.ok(result.fees[0].amount_raw > 0n, "fee must be positive");
});

test("DLMM exact-in b→a produces positive output and fee", () => {
  const result = simulateMeteoraDlmmExactIn(dlmmBody, 1000000000n, false);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
});

test("DLMM exact-out produces requested output", () => {
  const result = simulateMeteoraDlmmExactOut(dlmmBody, 100000n, true);
  assert.equal(result.net_output, 100000n);
  assert.ok(result.gross_input > result.net_output, "gross input must exceed output");
});

// ---------------------------------------------------------------------------
// AMM v4 tests
// ---------------------------------------------------------------------------

test("AMM v4 exact-in a→b produces positive output and fee", () => {
  const result = simulateRaydiumAmmV4ExactIn(ammV4Body, 100000000n, true);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
  assert.ok(result.fees[0].amount_raw > 0n, "fee must be positive");
});

test("AMM v4 exact-in b→a produces positive output and fee", () => {
  const result = simulateRaydiumAmmV4ExactIn(ammV4Body, 100000000n, false);
  assert.ok(result.gross_input > 0n, "gross input must be positive");
  assert.ok(result.net_output > 0n, "net output must be positive");
});

test("AMM v4 exact-out produces requested output", () => {
  const result = simulateRaydiumAmmV4ExactOut(ammV4Body, 50000000n, true);
  assert.equal(result.net_output, 50000000n);
  assert.ok(result.gross_input > result.net_output, "gross input must exceed output");
});

test("AMM v4 with OpenBook rejected", () => {
  const openBookBody: RaydiumAmmV4Body = {
    ...ammV4Body,
    open_orders: "some-open-orders",
  };
  assert.throws(
    () => simulateRaydiumAmmV4ExactIn(openBookBody, 100000n, true),
    UnsupportedVariant,
  );
});

test("AMM v4 with needTakePnl rejected", () => {
  const pnlBody: RaydiumAmmV4Body = {
    ...ammV4Body,
    need_take_pnl: true,
  };
  assert.throws(
    () => simulateRaydiumAmmV4ExactIn(pnlBody, 100000n, true),
    UnsupportedVariant,
  );
});

// ---------------------------------------------------------------------------
// Multi-pool path tests
// ---------------------------------------------------------------------------

test("CLMM path round-trip uses post-state", () => {
  const bundle = raydiumClmmSnapshotBundle("boot-1", 1, "req-1", [clmmState]);
  const entry: SimulatePathLegInput = {
    leg_id: "entry",
    pool_id: "clmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000000",
  };
  const exit: SimulatePathLegInput = {
    leg_id: "exit",
    pool_id: "clmm-pool",
    input_asset_id: assetB,
    output_asset_id: assetA,
    mode: "exact_in",
    amount_source: "previous_output",
    previous_leg_id: "entry",
  };
  const result = simulatePathLegs(bundle, [entry, exit], [
    { asset_id: assetA, amount_raw: "1000000000000" },
  ]);
  assert.equal(result.status, "complete", result.reason);
  assert.equal(result.leg_results.length, 2);
  assert.equal(
    result.leg_results[0].actual_net_output_raw,
    result.leg_results[1].actual_gross_input_raw,
  );
});

test("DLMM path round-trip uses post-state", () => {
  const dlmmState = {
    pool_id: "dlmm-pool",
    slot: 50,
    token_a_mint: "mint-a",
    token_b_mint: "mint-b",
    token_a_decimals: 6,
    token_b_decimals: 9,
    active_id: 1,
    bin_step: 1,
    reserve_x_raw: "10000000000",
    reserve_y_raw: "10000000000",
    fee_bps: 20,
    protocol_fee_bps: 5,
    bin_arrays: [
      {
        start_bin_id: 0,
        bins: Array.from({ length: 10 }, (_, i) => ({
          bin_id: i,
          reserve_x_raw: "1000000",
          reserve_y_raw: "1000000",
          liquidity_raw: "1000000",
          fee_x_raw: "0",
          fee_y_raw: "0",
        })),
      },
    ],
  };
  const bundle = meteoraDlmmSnapshotBundle("boot-1", 1, "req-1", [dlmmState]);
  const entry: SimulatePathLegInput = {
    leg_id: "entry",
    pool_id: "dlmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000000",
  };
  const exit: SimulatePathLegInput = {
    leg_id: "exit",
    pool_id: "dlmm-pool",
    input_asset_id: assetB,
    output_asset_id: assetA,
    mode: "exact_in",
    amount_source: "previous_output",
    previous_leg_id: "entry",
  };
  const result = simulatePathLegs(bundle, [entry, exit], [
    { asset_id: assetA, amount_raw: "1000000000000" },
  ]);
  assert.equal(result.status, "complete", result.reason);
  assert.equal(result.leg_results.length, 2);
  assert.equal(
    result.leg_results[0].actual_net_output_raw,
    result.leg_results[1].actual_gross_input_raw,
  );
});

test("AMM v4 path round-trip uses post-state", () => {
  const ammV4State = {
    pool_id: "ammv4-pool",
    slot: 50,
    token_a_mint: "mint-a",
    token_b_mint: "mint-b",
    token_a_decimals: 6,
    token_b_decimals: 9,
    vault_a_raw: "1000000000",
    vault_b_raw: "1000000000",
    fee_raw_a: "0",
    fee_raw_b: "0",
    fee_rate: 2500,
    need_take_pnl: false,
    open_orders: null,
    status: 0,
  };
  const bundle = raydiumAmmV4SnapshotBundle("boot-1", 1, "req-1", [ammV4State]);
  const entry: SimulatePathLegInput = {
    leg_id: "entry",
    pool_id: "ammv4-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000000",
  };
  const exit: SimulatePathLegInput = {
    leg_id: "exit",
    pool_id: "ammv4-pool",
    input_asset_id: assetB,
    output_asset_id: assetA,
    mode: "exact_in",
    amount_source: "previous_output",
    previous_leg_id: "entry",
  };
  const result = simulatePathLegs(bundle, [entry, exit], [
    { asset_id: assetA, amount_raw: "1000000000000" },
  ]);
  assert.equal(result.status, "complete", result.reason);
  assert.equal(result.leg_results.length, 2);
  assert.equal(
    result.leg_results[0].actual_net_output_raw,
    result.leg_results[1].actual_gross_input_raw,
  );
});
