import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import {
  simulateOrcaExactIn,
  simulateOrcaExactOut,
  UnsupportedVariant,
  type OrcaWhirlpoolBody,
  type WhirlpoolTickArray,
} from "../src/simulation/orcaWhirlpool.js";

interface Fixture {
  sdk_version: string;
  tick_spacing: number;
  current_tick_index: number;
  fee_rate: number;
  protocol_fee_rate: number;
  liquidity_raw: string;
  sqrt_price_x64: string;
  cases: FixtureCase[];
}

interface FixtureCase {
  a_to_b: boolean;
  amount_specified_is_input: boolean;
  input_amount_raw?: string;
  output_amount_raw?: string;
  fee_amount_raw: string;
  end_tick_index: number;
  end_sqrt_price_x64: string;
}

const FIXTURE_TICK_SPACING = 64;
const INIT_TICKS: Record<number, bigint> = {
  [-6208]: -1_000_000_000_000n,
  [-6336]: -2_000_000_000_000n,
  [-6464]: -3_000_000_000_000n,
};

function loadFixture(): Fixture {
  return JSON.parse(
    readFileSync(resolve(import.meta.dirname, "../../../tests/fixtures", "orca-whirlpool-sdk-swap.json"), "utf8"),
  ) as Fixture;
}

function tickArrays(): WhirlpoolTickArray[] {
  const starts = [-11264, -16896, -22528, -5632, 0];
  return starts.map((start) => {
    const ticks = Array.from({ length: 88 }, () => ({
      initialized: false,
      liquidityNet: 0n,
      liquidityGross: 0n,
    }));
    for (let off = 0; off < 88; off += 1) {
      const tickIndex = start + off * FIXTURE_TICK_SPACING;
      const net = INIT_TICKS[tickIndex];
      if (net !== undefined) {
        ticks[off] = { initialized: true, liquidityNet: net, liquidityGross: -net };
      }
    }
    return { startTickIndex: start, ticks };
  });
}

function bodyFor(fixture: Fixture): OrcaWhirlpoolBody {
  return {
    pool_id: "orca-pool-fixture",
    protocol: "orca_whirlpool" as const,
    sqrt_price_x64: BigInt(fixture.sqrt_price_x64),
    liquidity_raw: BigInt(fixture.liquidity_raw),
    tick_current_index: fixture.current_tick_index,
    tick_spacing: fixture.tick_spacing,
    fee_rate: BigInt(fixture.fee_rate),
    protocol_fee_rate: BigInt(fixture.protocol_fee_rate),
    fee_growth_global_a: 0n,
    fee_growth_global_b: 0n,
    protocol_fee_owed_a: 0n,
    protocol_fee_owed_b: 0n,
    tick_arrays: tickArrays(),
  };
}

test("Orca Whirlpool exact-in TS adapter matches the pinned SDK fixture", () => {
  const fixture = loadFixture();
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    if (!case_.amount_specified_is_input) continue;
    if (case_.input_amount_raw === undefined) continue;
    const transition = simulateOrcaExactIn(bodyFor(fixture), BigInt(case_.input_amount_raw), case_.a_to_b);
    assert.equal(transition.net_output, BigInt(case_.output_amount_raw as string), `case ${index}`);
    assert.equal(transition.body_after.sqrt_price_x64, BigInt(case_.end_sqrt_price_x64), `case ${index}`);
    assert.equal(transition.body_after.tick_current_index, case_.end_tick_index, `case ${index}`);
    assert.equal(transition.fees[0].amount_raw, BigInt(case_.fee_amount_raw), `case ${index}`);
    checked += 1;
  }
  assert.ok(checked >= 4, `expected at least 4 exact-in cases, checked ${checked}`);
});

test("Orca Whirlpool exact-out TS adapter matches the pinned SDK fixture", () => {
  const fixture = loadFixture();
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    if (case_.amount_specified_is_input) continue;
    if (case_.output_amount_raw === undefined) continue;
    const transition = simulateOrcaExactOut(bodyFor(fixture), BigInt(case_.output_amount_raw), case_.a_to_b);
    assert.equal(transition.gross_input, BigInt(case_.input_amount_raw as string), `case ${index}`);
    assert.equal(transition.body_after.sqrt_price_x64, BigInt(case_.end_sqrt_price_x64), `case ${index}`);
    assert.equal(transition.body_after.tick_current_index, case_.end_tick_index, `case ${index}`);
    assert.equal(transition.fees[0].amount_raw, BigInt(case_.fee_amount_raw), `case ${index}`);
    checked += 1;
  }
  assert.ok(checked >= 2, `expected at least 2 exact-out cases, checked ${checked}`);
});
