import assert from "node:assert/strict";
import test from "node:test";

import type { RaydiumCpmmSimulationState } from "../src/raydiumStandard.js";
import { createRequire } from "node:module";
import { raydiumCpmmSnapshotBundle } from "../src/simulation/snapshots.js";

const state: RaydiumCpmmSimulationState = {
  pool_id: "cpmm-pool",
  label: "HNT/USDC",
  slot: 12345,
  token_a_mint: "mint-a",
  token_b_mint: "mint-b",
  token_a_decimals: 6,
  token_b_decimals: 9,
  reserve_a_raw: "1000000000",
  reserve_b_raw: "2000000000",
  vault_a_raw: "1000005000",
  vault_b_raw: "2000003000",
  protocol_fees_a_raw: "1000",
  protocol_fees_b_raw: "2000",
  fund_fees_a_raw: "3000",
  fund_fees_b_raw: "4000",
  creator_fees_a_raw: "5000",
  creator_fees_b_raw: "6000",
  fee_on: 0,
  trade_fee_rate: "2500",
  creator_fee_rate: "120",
  protocol_fee_rate: "120",
  fund_fee_rate: "40",
};

test("raydium CPMM snapshot bundle matches the Python canonical shape", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 3, "req-1", [state]);

  assert.equal(bundle.schema_version, 1);
  assert.equal(bundle.snapshot_id, "snapshot-req-1");
  assert.equal(bundle.worker_generation, 3);
  assert.equal(bundle.model_version, "raydium_cpmm_v1");
  assert.equal(bundle.context_slot, 12345);
  assert.equal(bundle.chain_consistency, "validated_multi_account_snapshot");
  assert.equal(bundle.pools.length, 1);
  assert.equal(bundle.pool_refs[0].pool_address, "cpmm-pool");
  assert.equal(bundle.pool_refs[0].protocol, "raydium_cpmm");
  assert.equal(bundle.pool_refs[0].asset_0_id, "solana:mainnet:mint-a:6");
  assert.equal(bundle.pools[0].pool_id, "solana:mainnet:cpmm-pool");
  assert.equal(bundle.pools[0].vault_a_raw, "1000005000");
  assert.equal(bundle.pools[0].protocol_fees_a_raw, "1000");
  assert.equal(bundle.pools[0].fee_on, "0");
  assert.equal(bundle.sdk_versions[0][0], "@raydium-io/raydium-sdk-v2");
  assert.notEqual(bundle.sdk_versions[0][1], "latest");
  // SDK version must match the actually installed package.json metadata, not a hardcoded string
  const pkg = (createRequire(import.meta.url))("../package.json");
  assert.equal(bundle.sdk_versions[0][1], pkg.dependencies["@raydium-io/raydium-sdk-v2"]);
});

test("snapshot bundle keeps deterministic invariants for multiple pools", () => {
  const second: RaydiumCpmmSimulationState = { ...state, pool_id: "cpmm-pool-2", slot: 12000 };
  const bundle = raydiumCpmmSnapshotBundle("boot-1", 3, "req-2", [state, second]);

  assert.equal(bundle.pools.length, 2);
  assert.equal(bundle.context_slot, 12345);
  assert.deepEqual(bundle.pool_refs.map((p) => p.pool_address), ["cpmm-pool", "cpmm-pool-2"]);
});

test("CPMM snapshot bundle is deeply immutable and carries source epoch", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-immutable", 7, "req-immutable", [state], 11);
  assert.equal(bundle.source_epoch, 11);
  assert.equal(Object.isFrozen(bundle), true);
  assert.equal(Object.isFrozen(bundle.pool_refs), true);
  assert.equal(Object.isFrozen(bundle.pools), true);
});

test("separate captures from one worker keep the same source epoch", () => {
  const first = raydiumCpmmSnapshotBundle("boot-stable", 7, "req-a", [state], 7);
  const second = raydiumCpmmSnapshotBundle("boot-stable", 7, "req-b", [{ ...state, slot: 12346 }], 7);
  assert.equal(first.source_epoch, second.source_epoch);
  assert.notEqual(first.snapshot_id, second.snapshot_id);
  assert.notEqual(first.context_slot, second.context_slot);
});

test("slot summaries are not mislabeled as AccountVersion dependency evidence", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-provenance", 8, "req-provenance", [{
    ...state,
    core_state_slot: 105,
    dependency_slot_min: 108,
    dependency_slot_max: 110,
    dependency_generation: 7,
  }]);
  assert.deepEqual(bundle.dependency_vector, []);
});
