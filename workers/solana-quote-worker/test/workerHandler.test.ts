import assert from "node:assert/strict";
import test from "node:test";

import type { RaydiumCpmmSimulationState } from "../src/raydiumStandard.js";
import { OfflineSimulationHandler } from "../src/worker.js";
import { raydiumCpmmSnapshotBundle } from "../src/simulation/snapshots.js";

const state: RaydiumCpmmSimulationState = {
  pool_id: "handler-cpmm",
  label: "A/B",
  slot: 100,
  token_a_mint: "handler-a",
  token_b_mint: "handler-b",
  token_a_decimals: 6,
  token_b_decimals: 6,
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

const assetA = "solana:mainnet:handler-a:6";
const assetB = "solana:mainnet:handler-b:6";
const bundle = raydiumCpmmSnapshotBundle("boot-handler", 9, "handler-snapshot", [state], 9);

function request(requestId = "request-1", token = "token-1") {
  return {
    request_id: requestId,
    snapshot_token: token,
    legs: [{
      leg_id: "leg-buy",
      pool_id: "handler-cpmm",
      input_asset_id: assetA,
      output_asset_id: assetB,
      mode: "exact_in" as const,
      amount_source: "literal" as const,
      amount_raw: "1000000",
    }],
    initial_balances: [{ asset_id: assetA, amount_raw: "1000000" }],
  };
}

test("offline handler forwards balances into a complete CPMM result", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n });
  handler.put("token-1", bundle);
  const snapshotBytes = handler.registryBytes;
  const result = handler.simulate(request());
  assert.equal(result?.status, "complete");
  assert.equal(result?.complete, true);
  assert.ok(handler.registryBytes > snapshotBytes);
  assert.deepEqual(result?.final_balances, [
    { asset_id: assetA, amount_raw: "0" },
    { asset_id: assetB, amount_raw: "996386" },
  ]);
});

test("offline handler rejects unknown and expired snapshot tokens", () => {
  let now = 1n;
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => now, ttl_ns: 5n });
  const unknown = handler.simulate(request());
  assert.equal(unknown?.status, "state_unavailable");
  handler.put("token-1", bundle);
  now = 6n;
  const expired = handler.simulate(request());
  assert.equal(expired?.status, "state_unavailable");
  assert.equal(handler.registryCounters.evictions, 1);
});

test("offline handler rejects an expired deadline and suppresses canceled late results", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 10n });
  handler.put("token-1", bundle);
  const expired = handler.simulate({ ...request(), deadline_monotonic_ns: "10" });
  assert.equal(expired?.status, "deadline_exceeded");

  handler.cancel("canceled-request");
  assert.equal(handler.simulate(request("canceled-request")), undefined);
  // Cancellation state is consumed; request-id reuse is not poisoned.
  const reused = handler.simulate(request("canceled-request"));
  assert.equal(reused?.status, "complete");
});

test("offline handler bounds registry by item cap and exports identity evidence", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n, max_items: 1 });
  handler.put("token-1", bundle);
  handler.put("token-2", { ...bundle, snapshot_id: "handler-snapshot-2" });
  assert.equal(handler.registrySize, 1);
  assert.equal(handler.simulate(request("old", "token-1"))?.status, "state_unavailable");

  const result = handler.simulate(request("evidence-request", "token-2"));
  assert.equal(result?.status, "complete");
  const evidence = handler.evidence("export-request", "token-2");
  assert.equal(evidence?.evidence_kind, "raydium_cpmm_simulation");
  const metadata = evidence?.metadata as Record<string, unknown>;
  assert.equal(metadata.worker_generation, 9);
  assert.equal(metadata.boot_id, "boot-handler");
  assert.equal(metadata.source_epoch, 9);
  assert.equal((evidence?.request as Record<string, unknown>).request_id, "evidence-request");
  assert.equal((evidence?.result as Record<string, unknown>).complete, true);
});

test("offline handler rejects a result that would exceed byte budget", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n, max_bytes: 1024 });
  handler.put("token-1", bundle);
  const before = handler.registryBytes;
  assert.ok(before > 0);
  const result = handler.simulate({ ...request("big", "token-1") });
  assert.equal(result?.status, "state_unavailable");
  assert.equal(result?.complete, false);
  assert.ok(handler.registryBytes <= 1024);
});

test("offline handler rejects evidence export when no simulation was recorded", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n });
  handler.put("token-1", bundle);
  const missing = handler.evidence("export-missing", "token-1");
  assert.equal(missing, undefined);
  const unknown = handler.evidence("export-unknown", "token-never-stored");
  assert.equal(unknown, undefined);
});

test("offline handler keeps snapshot identity stable across stored simulation", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n });
  handler.put("token-1", bundle);
  const result = handler.simulate(request("id-check"));
  assert.equal(result?.complete, true);
  const snapshotId = (result as Record<string, unknown>).snapshot_id;
  const bootId = (result as Record<string, unknown>).boot_id;
  assert.equal(snapshotId, "snapshot-handler-snapshot");
  assert.equal(bootId, "boot-handler");
});

test("offline handler evicts oldest entry when byte budget is exceeded", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n, max_items: 10, max_bytes: 1500 });
  handler.put("token-1", bundle);
  handler.put("token-2", { ...bundle, snapshot_id: "handler-snapshot-2" });
  assert.ok(handler.registryBytes <= 1024);
  assert.ok(handler.registrySize <= 2);
});

test("offline handler late cancel suppresses a completed simulation result", () => {
  const handler = new OfflineSimulationHandler({ now_monotonic_ns: () => 1n });
  handler.put("token-1", bundle);
  handler.cancel("canceled-late");
  const late = handler.simulate(request("canceled-late"));
  assert.equal(late, undefined);
  const reused = handler.simulate(request("canceled-late"));
  assert.equal(reused?.status, "complete");
});
