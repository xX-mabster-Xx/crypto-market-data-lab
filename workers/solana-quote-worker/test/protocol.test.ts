import assert from "node:assert/strict";
import test from "node:test";

import { parseWorkerInput, redactUrls } from "../src/protocol.js";

test("configuration accepts RPC endpoints but never needs a wallet", () => {
  const message = parseWorkerInput({
    type: "configure",
    rpc_http_url: "https://provider.example/?api-key=test-value",
    rpc_ws_url: "wss://provider.example/?api-key=test-value",
    raydium_clmm_pools: [{ pool_id: "pool", label: "SOL/USDC" }],
    meteora_dlmm_pools: [],
    orca_whirlpool_pools: [],
    state_snapshot_refresh_interval_ms: 15_000,
    core_refresh_after_ms: 20_000,
    maintenance_scan_interval_ms: 1_000,
    refresh_stagger_window_ms: 5_000,
    pool_state_emit_min_interval_ms: 100,
    rpc_http_min_request_interval_ms: 200,
    rpc_max_pending_jobs: 256,
  });
  assert.equal(message.type, "configure");
  if (message.type === "configure") {
    assert.equal(message.raydium_clmm_pools[0]?.label, "SOL/USDC");
    assert.equal(message.state_snapshot_refresh_interval_ms, 15_000);
    assert.equal(message.core_refresh_after_ms, 20_000);
    assert.equal(message.maintenance_scan_interval_ms, 1_000);
    assert.equal(message.refresh_stagger_window_ms, 5_000);
    assert.equal(message.pool_state_emit_min_interval_ms, 100);
    assert.equal(message.rpc_http_min_request_interval_ms, 200);
    assert.equal(message.rpc_max_pending_jobs, 256);
  }
});

test("quote request is exact-input only and requires positive raw amount", () => {
  const message = parseWorkerInput({
    type: "quote_request",
    request_id: "request-1",
    protocol: "raydium_clmm",
    pool_id: "pool",
    input_mint: "mint-a",
    output_mint: "mint-b",
    input_amount_raw: "123456",
    minimum_state_slot: 42,
  });
  assert.equal(message.type, "quote_request");
  assert.throws(() => parseWorkerInput({ ...message, input_amount_raw: "0" }));
});

test("configuration and quotes accept Meteora DLMM pools", () => {
  const configured = parseWorkerInput({
    type: "configure",
    rpc_http_url: "https://provider.example",
    raydium_clmm_pools: [],
    meteora_dlmm_pools: [{ pool_id: "meteora-pool", label: "SOL/USDC Meteora" }],
    orca_whirlpool_pools: [],
  });
  assert.equal(configured.type, "configure");
  const quote = parseWorkerInput({
    type: "quote_request",
    request_id: "request-2",
    protocol: "meteora_dlmm",
    pool_id: "meteora-pool",
    input_mint: "mint-a",
    output_mint: "mint-b",
    input_amount_raw: "100",
  });
  assert.equal(quote.type, "quote_request");
});

test("configuration and quotes accept Orca Whirlpool pools", () => {
  const configured = parseWorkerInput({
    type: "configure",
    rpc_http_url: "https://provider.example",
    raydium_clmm_pools: [],
    meteora_dlmm_pools: [],
    orca_whirlpool_pools: [{ pool_id: "orca-pool", label: "SOL/USDC Orca" }],
  });
  assert.equal(configured.type, "configure");
  const quote = parseWorkerInput({
    type: "quote_request",
    request_id: "request-3",
    protocol: "orca_whirlpool",
    pool_id: "orca-pool",
    input_mint: "mint-a",
    output_mint: "mint-b",
    input_amount_raw: "100",
  });
  assert.equal(quote.type, "quote_request");
});

test("configuration distinguishes Raydium CPMM from legacy AMM v4", () => {
  const configured = parseWorkerInput({
    type: "configure",
    rpc_http_url: "https://provider.example",
    raydium_clmm_pools: [],
    raydium_standard_pools: [
      { pool_id: "cpmm-pool", label: "HNT/USDC CPMM", protocol: "raydium_cpmm" },
      { pool_id: "amm-pool", label: "SOL/USDC AMM", protocol: "raydium_amm_v4" },
    ],
    meteora_dlmm_pools: [],
    orca_whirlpool_pools: [],
  });
  assert.equal(configured.type, "configure");
  if (configured.type === "configure") {
    assert.deepEqual(
      configured.raydium_standard_pools.map((pool) => pool.protocol),
      ["raydium_cpmm", "raydium_amm_v4"],
    );
  }
});

test("errors redact query strings from provider endpoints", () => {
  assert.equal(
    redactUrls("failed https://provider.example/path?api-key=very-secret"),
    "failed https://provider.example",
  );
});

test("simulation messages validate exact raw paths and snapshots", () => {
  const simulation = parseWorkerInput({
    type: "simulate_path_request",
    request_id: "simulation-1",
    snapshot_token: "snapshot-token",
    initial_balances: [{ asset_id: "asset-a", amount_raw: "123456789012345678901" }],
    legs: [{
      leg_id: "leg-1",
      pool_id: "pool",
      input_asset_id: "asset-a",
      output_asset_id: "asset-b",
      mode: "exact_in",
      amount_source: "literal",
      amount_raw: "123456789012345678901",
    }],
  });
  assert.equal(simulation.type, "simulate_path_request");
  assert.throws(() => parseWorkerInput({
    ...simulation,
    legs: [{ ...simulation.legs[0], amount_raw: "1e21" }],
  }));

  const snapshot = parseWorkerInput({
    type: "snapshot_request",
    request_id: "snapshot-1",
    pool_ids: ["pool"],
    required_consistency: "validated_multi_account_snapshot",
  });
  assert.equal(snapshot.type, "snapshot_request");
  assert.throws(() => parseWorkerInput({ ...snapshot, required_consistency: "pinned" }));
});

test("simulation protocol requires canonical, non-duplicated initial balances", () => {
  const base = {
    type: "simulate_path_request" as const,
    request_id: "simulation-balances",
    snapshot_token: "snapshot-token",
    legs: [{
      leg_id: "leg-1",
      pool_id: "pool",
      input_asset_id: "asset-a",
      output_asset_id: "asset-b",
      mode: "exact_in" as const,
      amount_source: "literal" as const,
      amount_raw: "10",
    }],
  };
  assert.throws(() => parseWorkerInput({ ...base, initial_balances: [] }));
  assert.throws(() => parseWorkerInput({
    ...base,
    initial_balances: [
      { asset_id: "asset-a", amount_raw: "1" },
      { asset_id: "asset-a", amount_raw: "2" },
    ],
  }));
  assert.throws(() => parseWorkerInput({
    ...base,
    initial_balances: [{ asset_id: "asset-a", amount_raw: "01" }],
  }));
  const parsed = parseWorkerInput({
    ...base,
    initial_balances: [{ asset_id: "asset-a", amount_raw: "100000000000000000000000000001" }],
    deadline_monotonic_ns: "9007199254740993123",
  });
  assert.equal(parsed.type, "simulate_path_request");
  if (parsed.type === "simulate_path_request") {
    assert.equal(parsed.initial_balances[0]?.amount_raw, "100000000000000000000000000001");
    assert.equal(parsed.deadline_monotonic_ns, "9007199254740993123");
  }
});
