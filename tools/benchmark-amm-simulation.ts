#!/usr/bin/env node
/**
 * Offline benchmark for the post-trade AMM simulation path executor.
 *
 * Spec §14 requirements:
 *   - Minimum 10 000 warm CPMM paths and 1 000 of each tick/bin fixture
 *     class, with 1/2/4 legs, reporting median + p95 + p99.
 *   - Cold capture measured separately.
 *   - Targets: p95 pure CPMM <= 5 ms; p95 bounded CLMM/DLMM <= 50 ms;
 *     p99 event-loop stall <= 20 ms; memory within caps.
 *
 * Usage (run from project root):
 *   npx tsx tools/benchmark-amm-simulation.ts
 */

import { performance } from "node:perf_hooks";

import {
  raydiumCpmmSnapshotBundle,
  type RaydiumCpmmSnapshotBundle,
} from "../workers/solana-quote-worker/src/simulation/snapshots.js";
import {
  simulatePathLegs,
  type SimulatePathLegInput,
} from "../workers/solana-quote-worker/src/simulation/path.js";

// --- Synthetic CPMM state generator ---

const ASSET_A = "solana:mainnet:mint-a:6";
const ASSET_B = "solana:mainnet:mint-b:9";

function makeCpmmState(poolId: string, seed: number): RaydiumCpmmSimulationState {
  let s = seed;
  const rand = () => {
    s = (s * 1103515245 + 12345) & 0x7fffffff;
    return s;
  };
  const reserveA = 1_000_000_000n + BigInt(rand() % 1_000_000_000);
  const reserveB = 1_000_000_000n + BigInt(rand() % 1_000_000_000);
  return {
    pool_id: poolId,
    label: "bench-" + poolId,
    slot: 100,
    token_a_mint: "mint-a",
    token_b_mint: "mint-b",
    token_a_decimals: 6,
    token_b_decimals: 9,
    reserve_a_raw: reserveA.toString(),
    reserve_b_raw: reserveB.toString(),
    vault_a_raw: reserveA.toString(),
    vault_b_raw: reserveB.toString(),
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
}

function makeCpmmBundle(numPools: number, seed: number): RaydiumCpmmSnapshotBundle {
  const states = [];
  for (let i = 0; i < numPools; i++) {
    states.push(makeCpmmState("pool-" + i, seed + i));
  }
  return raydiumCpmmSnapshotBundle("boot-bench", 1, "req-bench", states);
}

function makeLegs(numLegs: number, poolIds: string[]): SimulatePathLegInput[] {
  const legs: SimulatePathLegInput[] = [];
  for (let i = 0; i < numLegs; i++) {
    const inputAsset = i % 2 === 0 ? ASSET_A : ASSET_B;
    const outputAsset = i % 2 === 0 ? ASSET_B : ASSET_A;
    legs.push({
      leg_id: "leg-" + i,
      pool_id: poolIds[i],
      input_asset_id: inputAsset,
      output_asset_id: outputAsset,
      mode: "exact_in",
      amount_source: i === 0 ? "literal" : "previous_output",
      amount_raw: i === 0 ? "100000000" : undefined,
      previous_leg_id: i === 0 ? undefined : "leg-" + (i - 1),
    });
  }
  return legs;
}

// --- Statistics helpers ---

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.ceil((p / 100) * sorted.length) - 1;
  return sorted[Math.max(0, Math.min(sorted.length - 1, idx))];
}

function mean(values: number[]): number {
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
}

interface BenchResult {
  label: string;
  count: number;
  mean_ms: number;
  median_ms: number;
  p95_ms: number;
  p99_ms: number;
  min_ms: number;
  max_ms: number;
  complete: number;
  status: string;
}

function runBenchmark(
  label: string,
  fn: () => void,
  iterations: number,
  checkResult: () => boolean = () => true,
): BenchResult {
  const warmup = Math.max(1, Math.floor(iterations / 10));
  for (let i = 0; i < warmup; i++) fn();

  const samples: number[] = [];
  let complete = 0;
  let lastStatus = "";
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    const result = fn();
    const elapsed = performance.now() - start;
    samples.push(elapsed);
    if (checkResult()) complete++;
  }
  const sorted = [...samples].sort((a, b) => a - b);
  return {
    label,
    count: iterations,
    mean_ms: mean(samples),
    median_ms: percentile(sorted, 50),
    p95_ms: percentile(sorted, 95),
    p99_ms: percentile(sorted, 99),
    min_ms: sorted[0],
    max_ms: sorted[sorted.length - 1],
    complete,
    status: lastStatus,
  };
}

function formatResult(r: BenchResult): string {
  return (
    "  " + r.label.padEnd(40) + " | " +
    "n=" + r.count.toString().padStart(6) + " | " +
    "mean=" + r.mean_ms.toFixed(3) + "ms | " +
    "median=" + r.median_ms.toFixed(3) + "ms | " +
    "p95=" + r.p95_ms.toFixed(3) + "ms | " +
    "p99=" + r.p99_ms.toFixed(3) + "ms | " +
    "min=" + r.min_ms.toFixed(3) + "ms | " +
    "max=" + r.max_ms.toFixed(3) + "ms"
  );
}

// --- Main ---

function main(): void {
  console.log("=== Post-trade AMM Simulation Benchmark ===");
  console.log();

  const WARM_CPMM_ITERS = 10_000;
  const LEG_COUNTS = [1, 2, 4];

  console.log("--- Warm CPMM paths (10,000 per config) ---");
  console.log();
  for (const numLegs of LEG_COUNTS) {
    const poolIds = Array.from({ length: numLegs }, (_, i) => "pool-" + i);
    const bundle = makeCpmmBundle(numLegs, 42);
    const legs = makeLegs(numLegs, poolIds);
    const balances = [{ asset_id: ASSET_A, amount_raw: "1000000000000" }];

    let lastResult = "";
    const result = runBenchmark(
      "CPMM " + numLegs + " leg(s) exact-in",
      () => {
        const res = simulatePathLegs(bundle, legs, balances);
        lastResult = res.status;
        return res;
      },
      WARM_CPMM_ITERS,
      () => lastResult === "complete",
    );
    console.log(formatResult(result));
    console.log("  status=" + lastResult);
    console.log();
  }

  console.log("--- Cold capture (first call after bundle creation) ---");
  console.log();
  for (const numLegs of LEG_COUNTS) {
    const poolIds = Array.from({ length: numLegs }, (_, i) => "pool-" + i);
    const bundle = makeCpmmBundle(numLegs, 42);
    const legs = makeLegs(numLegs, poolIds);
    const balances = [{ asset_id: ASSET_A, amount_raw: "1000000000000" }];

    const start = performance.now();
    const result = simulatePathLegs(bundle, legs, balances);
    const elapsed = performance.now() - start;
    console.log(
      "  CPMM " + numLegs + " leg(s) cold capture | " +
      elapsed.toFixed(3) + "ms | status=" + result.status,
    );
  }
  console.log();

  console.log("--- Event-loop stall check (p99 must be <= 20ms) ---");
  console.log();
  {
    const bundle = makeCpmmBundle(2, 99);
    const legs = makeLegs(2, ["pool-0", "pool-1"]);
    const balances = [{ asset_id: ASSET_A, amount_raw: "1000000000000" }];

    const stallSamples: number[] = [];
    for (let i = 0; i < 1000; i++) {
      const before = performance.now();
      simulatePathLegs(bundle, legs, balances);
      stallSamples.push(performance.now() - before);
    }
    const sorted = [...stallSamples].sort((a, b) => a - b);
    const p95 = percentile(sorted, 95);
    const p99 = percentile(sorted, 99);
    console.log("  1000 single-path stalls: p95=" + p95.toFixed(3) + "ms, p99=" + p99.toFixed(3) + "ms");
    console.log("  Target: p99 <= 20ms");
    console.log("  Result: " + (p99 <= 20 ? "PASS" : "FAIL (documented in output)"));
    console.log();
  }

  console.log("--- Memory usage ---");
  console.log();
  const memMB = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log("  Heap used: " + memMB.toFixed(2) + " MiB");
  console.log("  Cap (max_snapshot_bytes): 64 MiB total");
  console.log();

  console.log("--- Acceptance targets (§14) ---");
  console.log();
  console.log("  p95 pure CPMM <= 5 ms");
  console.log("  p95 bounded CLMM/DLMM <= 50 ms");
  console.log("  p99 event-loop stall <= 20 ms");
  console.log("  Memory within 64 MiB snapshot byte cap");
}

main();
