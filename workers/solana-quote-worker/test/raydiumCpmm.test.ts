import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import {
  simulateRaydiumCpmmExactIn,
  simulateRaydiumCpmmExactOut,
  UnsupportedVariant,
  type RaydiumCpmmBody,
} from "../src/simulation/raydiumCpmm.js";

interface Fixture {
  trade_fee_rate: string;
  creator_fee_rate: string;
  protocol_fee_rate: string;
  fund_fee_rate: string;
  cases: FixtureCase[];
}

interface FixtureCase {
  reserve_a: string;
  reserve_b: string;
  fee_on: number;
  a_to_b: boolean;
  input_amount_raw?: string;
  output_amount_raw: string;
  trade_fee_raw: string;
  protocol_fee_raw: string;
  fund_fee_raw: string;
  creator_fee_raw: string;
  creator_fee_on_input: boolean;
}

function loadFixture(name: string): Fixture {
  return JSON.parse(
    readFileSync(resolve(import.meta.dirname, "../../../tests/fixtures", name), "utf8"),
  ) as Fixture;
}

function bodyFor(fixture: Fixture, case_: FixtureCase, poolId: string): RaydiumCpmmBody {
  return {
    pool_id: poolId,
    protocol: "raydium_cpmm" as const,
    vault_a_raw: BigInt(case_.reserve_a),
    vault_b_raw: BigInt(case_.reserve_b),
    protocol_fees_a_raw: 0n,
    protocol_fees_b_raw: 0n,
    fund_fees_a_raw: 0n,
    fund_fees_b_raw: 0n,
    creator_fees_a_raw: 0n,
    creator_fees_b_raw: 0n,
    trade_fee_rate: BigInt(fixture.trade_fee_rate),
    creator_fee_rate: BigInt(fixture.creator_fee_rate),
    protocol_fee_rate: BigInt(fixture.protocol_fee_rate),
    fund_fee_rate: BigInt(fixture.fund_fee_rate),
    fee_on: case_.fee_on,
  };
}

test("Raydium CPMM exact-in TS adapter matches the pinned SDK fixture", () => {
  const fixture = loadFixture("raydium-cpmm-sdk-swap-base-input.json");
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    const body = bodyFor(fixture, case_, `pool-${index}`);
    const sdkOutput = BigInt(case_.output_amount_raw);
    if (sdkOutput <= 0n) {
      assert.throws(
        () => simulateRaydiumCpmmExactIn(body, BigInt(case_.input_amount_raw as string), case_.a_to_b),
        UnsupportedVariant,
      );
      continue;
    }
    const transition = simulateRaydiumCpmmExactIn(body, BigInt(case_.input_amount_raw as string), case_.a_to_b);
    assert.equal(transition.net_output, sdkOutput, `case ${index}`);
    assert.equal(transition.fees[0].amount_raw, BigInt(case_.protocol_fee_raw), `case ${index}`);
    assert.equal(transition.fees[1].amount_raw, BigInt(case_.fund_fee_raw), `case ${index}`);
    assert.equal(transition.fees[2].amount_raw, BigInt(case_.creator_fee_raw), `case ${index}`);
    checked += 1;
  }
  assert.ok(checked >= 24, `expected at least 24 positive cases, checked ${checked}`);
});

test("Raydium CPMM exact-out TS adapter matches the pinned exact-output fixture", () => {
  const fixture = loadFixture("raydium-cpmm-sdk-swap-base-output.json");
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    const body = bodyFor(fixture, case_, `pool-out-${index}`);
    const outputAmount = BigInt(case_.output_amount_raw);
    const transition = simulateRaydiumCpmmExactOut(body, outputAmount, case_.a_to_b);
    assert.equal(transition.gross_input, BigInt(case_.input_amount_raw as string), `case ${index}`);
    assert.equal(transition.net_output, outputAmount, `case ${index}`);
    assert.equal(transition.fees[0].amount_raw, BigInt(case_.protocol_fee_raw), `case ${index}`);
    assert.equal(transition.fees[1].amount_raw, BigInt(case_.fund_fee_raw), `case ${index}`);
    assert.equal(transition.fees[2].amount_raw, BigInt(case_.creator_fee_raw), `case ${index}`);
    checked += 1;
  }
  assert.ok(checked >= 24);
});

function assertContractPostState(
  before: RaydiumCpmmBody,
  transition: ReturnType<typeof simulateRaydiumCpmmExactIn>,
  case_: FixtureCase,
): void {
  const after = transition.body_after as RaydiumCpmmBody;
  const creatorOnA = case_.creator_fee_on_input === case_.a_to_b;
  const protocolFee = BigInt(case_.protocol_fee_raw);
  const fundFee = BigInt(case_.fund_fee_raw);
  const creatorFee = BigInt(case_.creator_fee_raw);
  assert.equal(
    after.vault_a_raw,
    before.vault_a_raw + (case_.a_to_b ? transition.gross_input : -transition.net_output),
  );
  assert.equal(
    after.vault_b_raw,
    before.vault_b_raw + (case_.a_to_b ? -transition.net_output : transition.gross_input),
  );
  assert.equal(after.protocol_fees_a_raw, case_.a_to_b ? protocolFee : 0n);
  assert.equal(after.protocol_fees_b_raw, case_.a_to_b ? 0n : protocolFee);
  assert.equal(after.fund_fees_a_raw, case_.a_to_b ? fundFee : 0n);
  assert.equal(after.fund_fees_b_raw, case_.a_to_b ? 0n : fundFee);
  assert.equal(after.creator_fees_a_raw, creatorOnA ? creatorFee : 0n);
  assert.equal(after.creator_fees_b_raw, creatorOnA ? 0n : creatorFee);
}

test("Raydium CPMM exact-in post-state matches contract transfers in every fee mode", () => {
  const fixture = loadFixture("raydium-cpmm-sdk-swap-base-input.json");
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    if (BigInt(case_.output_amount_raw) <= 0n) continue;
    const body = bodyFor(fixture, case_, `post-in-${index}`);
    const transition = simulateRaydiumCpmmExactIn(
      body,
      BigInt(case_.input_amount_raw as string),
      case_.a_to_b,
    );
    assertContractPostState(body, transition, case_);
    checked += 1;
  }
  assert.ok(checked >= 24);
});

test("Raydium CPMM exact-out post-state matches contract transfers in every fee mode", () => {
  const fixture = loadFixture("raydium-cpmm-sdk-swap-base-output.json");
  let checked = 0;
  for (const [index, case_] of fixture.cases.entries()) {
    const body = bodyFor(fixture, case_, `post-out-${index}`);
    const transition = simulateRaydiumCpmmExactOut(
      body,
      BigInt(case_.output_amount_raw),
      case_.a_to_b,
    );
    assertContractPostState(body, transition, case_);
    checked += 1;
  }
  assert.ok(checked >= 24);
});
