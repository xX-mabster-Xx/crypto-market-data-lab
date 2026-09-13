import assert from "node:assert/strict";
import test from "node:test";

import type { RaydiumCpmmSimulationState } from "../src/raydiumStandard.js";
import { raydiumCpmmSnapshotBundle } from "../src/simulation/snapshots.js";
import { simulatePathLegs, type SimulatePathLegInput } from "../src/simulation/path.js";

const state: RaydiumCpmmSimulationState = {
  pool_id: "cpmm-pool",
  label: "A/B",
  slot: 50,
  token_a_mint: "mint-a",
  token_b_mint: "mint-b",
  token_a_decimals: 6,
  token_b_decimals: 9,
  reserve_a_raw: "1000000000",
  reserve_b_raw: "1000000000",
  vault_a_raw: "1000000000",
  vault_b_raw: "1000000000",
  protocol_fees_a_raw: "0",
  protocol_fees_b_raw: "0",
  fund_fees_a_raw: "0",
  fund_fees_b_raw: "0",
  creator_fees_a_raw: "0",
  creator_fees_b_raw: "0",
  fee_on: 0,
  trade_fee_rate: "2500",
  creator_fee_rate: "120",
  protocol_fee_rate: "120",
  fund_fee_rate: "40",
};

const assetA = "solana:mainnet:mint-a:6";
const assetB = "solana:mainnet:mint-b:9";

test("Raydium CPMM path round-trip consumes post-state and leaves snapshot unchanged", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 1, "req-1", [state]);
  const beforeVault = bundle.pools[0].vault_a_raw;
  const entry: SimulatePathLegInput = {
    leg_id: "entry",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000000",
  };
  const exit: SimulatePathLegInput = {
    leg_id: "exit",
    pool_id: "cpmm-pool",
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
  const entryResult = result.leg_results[0];
  const exitResult = result.leg_results[1];
  assert.equal(entryResult.actual_gross_input_raw, "1000000");
  assert.equal(entryResult.actual_net_output_raw, exitResult.actual_gross_input_raw);
  // The observed snapshot is never mutated by a path branch.
  assert.equal(bundle.pools[0].vault_a_raw, beforeVault);
  // Vaults diverge from the snapshot after a successful path.
  assert.notEqual(result.leg_results[1].reserve_0_after_raw, "1000000000");
});

test("Raydium CPMM path returns typed state_unavailable for unknown pool", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 1, "req-2", [state]);
  const result = simulatePathLegs(bundle, [
    {
      leg_id: "leg-1",
      pool_id: "missing-pool",
      input_asset_id: assetA,
      output_asset_id: assetB,
      mode: "exact_in",
      amount_source: "literal",
      amount_raw: "1000",
    },
  ]);
  assert.equal(result.status, "state_unavailable");
  assert.equal(result.complete, false);
});

test("repeated visit of the same pool in one path sees prior post-state", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 1, "req-3", [state]);
  const result = simulatePathLegs(bundle, [
    {
      leg_id: "first",
      pool_id: "cpmm-pool",
      input_asset_id: assetA,
      output_asset_id: assetB,
      mode: "exact_in",
      amount_source: "literal",
      amount_raw: "1000000",
    },
    {
      leg_id: "second",
      pool_id: "cpmm-pool",
      input_asset_id: assetB,
      output_asset_id: assetA,
      mode: "exact_in",
      amount_source: "previous_output",
      previous_leg_id: "first",
    },
  ], [{ asset_id: assetA, amount_raw: "1000000000000" }]);
  assert.equal(result.status, "complete", result.reason);
  assert.equal(result.leg_results.length, 2);
  // Second swap sees a shifted pool, so raw outputs differ.
  assert.notEqual(
    result.leg_results[0].reserve_1_after_raw,
    result.leg_results[1].reserve_1_after_raw,
  );
});

test("CPMM path rejects an input asset outside the configured pool", () => {
  const snapshot = raydiumCpmmSnapshotBundle("boot-1", 1, "req-asset", [state]);
  const result = simulatePathLegs(snapshot, [{
    leg_id: "foreign-input",
    pool_id: "cpmm-pool",
    input_asset_id: "solana:mainnet:foreign:6",
    output_asset_id: assetA,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000",
  }], [{ asset_id: "solana:mainnet:foreign:6", amount_raw: "1000" }]);
  assert.equal(result.status, "invalid_request");
  assert.equal(result.complete, false);
});

test("CPMM path rejects duplicate leg ids and disconnected route shapes", () => {
  const snapshot = raydiumCpmmSnapshotBundle("boot-1", 1, "req-shape", [state]);
  const first: SimulatePathLegInput = {
    leg_id: "same",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000",
  };
  const duplicate = simulatePathLegs(snapshot, [first, {
    ...first,
    input_asset_id: assetB,
    output_asset_id: assetA,
    amount_source: "previous_output",
    previous_leg_id: "same",
    amount_raw: undefined,
  }], [{ asset_id: assetA, amount_raw: "1000" }]);
  assert.equal(duplicate.status, "invalid_request");

  const disconnected = simulatePathLegs(snapshot, [first, {
    leg_id: "second",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1",
  }], [{ asset_id: assetA, amount_raw: "1001" }]);
  assert.equal(disconnected.status, "invalid_request");
});

test("CPMM path requires a sufficient explicit initial balance", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 1, "req-balance", [state]);
  const result = simulatePathLegs(bundle, [{
    leg_id: "leg-1",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000000",
  }], [{ asset_id: assetA, amount_raw: "999999" }]);
  assert.equal(result.status, "insufficient_balance");
  assert.equal(result.complete, false);
});

test("expired CPMM snapshot is rejected before a live path starts", () => {
  const source = raydiumCpmmSnapshotBundle("boot-1", 1, "req-expired", [state]);
  const expired = {
    ...source,
    state_valid_until_monotonic_ns: "10",
  };
  const result = simulatePathLegs(expired, [{
    leg_id: "leg-1",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000",
  }], [{ asset_id: assetA, amount_raw: "1000" }], {
    now_monotonic_ns: () => 10n,
  });
  assert.equal(result.status, "state_unavailable");
  assert.equal(result.complete, false);
});

test("deadline is checked by the ordered path executor", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 1, "req-deadline", [state]);
  const result = simulatePathLegs(bundle, [{
    leg_id: "leg-1",
    pool_id: "cpmm-pool",
    input_asset_id: assetA,
    output_asset_id: assetB,
    mode: "exact_in",
    amount_source: "literal",
    amount_raw: "1000",
  }], [{ asset_id: assetA, amount_raw: "1000" }], {
    deadline_monotonic_ns: 10n,
    now_monotonic_ns: () => 10n,
  });
  assert.equal(result.status, "deadline_exceeded");
  assert.equal(result.complete, false);
});
