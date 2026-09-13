import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { canonicalHash, canonicalJson } from "../src/ammCodec.js";

test("canonical codec matches Python for sorted keys and decimal strings", async () => {
  const value = {
    z: "text",
    amount: "123456789012345678901",
    flags: [true, false, null],
    nested: { b: 2, a: 1 },
  };
  assert.equal(canonicalJson(value), '{"amount":"123456789012345678901","flags":[true,false,null],"nested":{"a":1,"b":2},"z":"text"}');
  const hash = await canonicalHash(value, "test_domain");
  const expected = createHash("sha256")
    .update(canonicalJson({ domain: "test_domain", value }))
    .digest("hex");
  assert.equal(hash, expected);
});

test("golden fixture hash matches the Python codec", async () => {
  const payload = JSON.parse(
    readFileSync(resolve(import.meta.dirname, "../../../tests/fixtures/amm-canonical.json"), "utf8"),
  ) as { domain: string; value: unknown };
  const expected = JSON.parse(
    readFileSync(resolve(import.meta.dirname, "../../../tests/fixtures/amm-canonical-hash.json"), "utf8"),
  ) as { hash: string };
  assert.equal(await canonicalHash(payload.value, payload.domain), expected.hash);
});
