import assert from "node:assert/strict";
import { createRequire } from "node:module";
import test from "node:test";

import type { RaydiumCpmmSimulationState } from "../src/raydiumStandard.js";
import { OfflineSimulationHandler } from "../src/worker.js";
import { raydiumCpmmSnapshotBundle } from "../src/simulation/snapshots.js";

const pkg = createRequire(import.meta.url)("../package.json");

const baseState: RaydiumCpmmSimulationState = {
  pool_id: "cpmm-pool-e",
  label: "E/USDC",
  slot: 100,
  core_state_slot: 100,
  dependency_slot_min: 110,
  dependency_slot_max: 120,
  dependency_generation: 7,
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

test("E01: CPMM snapshot bundle resolves SDK version from installed package metadata, not latest", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-e", 1, "req-sdk", [baseState]);
  assert.equal(bundle.sdk_versions[0][0], "@raydium-io/raydium-sdk-v2");
  assert.notEqual(bundle.sdk_versions[0][1], "latest");
  assert.equal(bundle.sdk_versions[0][1], pkg.dependencies["@raydium-io/raydium-sdk-v2"]);
});

test("E02: per-pool core and dependency slot provenance survives serialization", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-e", 1, "req-prov", [baseState]);
  const pool = bundle.pools[0];
  assert.equal(pool.core_state_slot, 100);
  assert.equal(pool.dependency_slot_min, 110);
  assert.equal(pool.dependency_slot_max, 120);
  assert.equal(pool.dependency_generation, 7);
  // context_slot is the max core_state_slot across pools, not dependency max
  assert.equal(bundle.context_slot, 100);
});

test("E03: dependency_vector remains empty - slot summaries are not AccountVersion evidence", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-e", 1, "req-evidence", [baseState]);
  assert.deepEqual(bundle.dependency_vector, []);
});

test("E04: chain_consistency reflects actual validated multi-account snapshot capture", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-e", 1, "req-consistency", [baseState]);
  assert.equal(bundle.chain_consistency, "validated_multi_account_snapshot");
});

test("E05: snapshot is deeply frozen and immutable", () => {
  const bundle = raydiumCpmmSnapshotBundle("boot-e", 1, "req-freeze", [baseState]);
  assert.equal(Object.isFrozen(bundle), true);
  assert.equal(Object.isFrozen(bundle.pools), true);
  assert.equal(Object.isFrozen(bundle.pools[0]), true);
});

test("E06: freshness TTL is bounded by oldest underlying state receipt", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n });
  handler.put("token-fresh", {
    ...raydiumCpmmSnapshotBundle("boot-e", 1, "req-ttl", [baseState]),
    snapshot_created_at_monotonic_ns: "0",
    state_valid_until_monotonic_ns: "30000000000",
  });
  const result = handler.simulate({
    request_id: "sim-ttl",
    snapshot_token: "token-fresh",
    legs: [],
    initial_balances: [],
  });
  assert.notEqual(result?.status, "state_unavailable");
});

test("E07: stale underlying cache cannot obtain fresh-valid token", () => {
  let now = 1n;
  const handler = new OfflineSimulationHandler({
    now_monotonic_ns: () => now,
    ttl_ns: 30_000_000_000n,
  });
  handler.put("token-stale", {
    ...raydiumCpmmSnapshotBundle("boot-e", 1, "req-stale", [baseState]),
    snapshot_created_at_monotonic_ns: "1000",
    state_valid_until_monotonic_ns: "2000",
  });
  now = 10_000_000_000n;
  const result = handler.simulate({
    request_id: "sim-stale",
    snapshot_token: "token-stale",
    legs: [],
    initial_balances: [],
  });
  assert.equal(result?.status, "state_unavailable");
});

test("E08: expired state_valid_until causes eviction without re-extend on put", () => {
  // A snapshot whose state_valid_until has already passed at put time must
  // be evicted immediately, not given a fresh TTL window.
  const handler = new OfflineSimulationHandler({
    now_monotonic_ns: () => 100_000_000_000n,
    ttl_ns: 30_000_000_000n,
  });
  handler.put("token-expired", {
    ...raydiumCpmmSnapshotBundle("boot-e", 1, "req-noextend", [baseState]),
    snapshot_created_at_monotonic_ns: "1000",
    state_valid_until_monotonic_ns: "50000",
  });
  // Entry is stored on put; eviction runs on the next access (simulate)
  const result = handler.simulate({
    request_id: "sim-noextend",
    snapshot_token: "token-expired",
    legs: [],
    initial_balances: [],
  });
  assert.equal(result?.status, "state_unavailable");
  // After eviction, registry is empty
  assert.equal(handler.registrySize, 0);
});
