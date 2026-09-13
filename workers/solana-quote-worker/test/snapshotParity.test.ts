import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { canonicalHash } from "../src/ammCodec.js";

const FIXTURES_DIR = resolve(import.meta.dirname, "../../../tests/fixtures/amm_simulation");

const hashes = JSON.parse(
  readFileSync(resolve(FIXTURES_DIR, "snapshot-parity-hashes.json"), "utf8"),
) as { domain: string; fixtures: Record<string, { hash: string; file: string }> };

test("canonical hash parity: TS matches golden Python hashes (AS37)", async () => {
  assert.equal(hashes.domain, "amm_snapshot");

  for (const [protocol, entry] of Object.entries(hashes.fixtures)) {
    const projection = JSON.parse(
      readFileSync(resolve(FIXTURES_DIR, entry.file), "utf8"),
    );
    const computed = await canonicalHash(projection, "amm_snapshot");
    assert.equal(
      computed,
      entry.hash,
      `canonical hash mismatch for ${protocol}`,
    );
  }
});

test("canonical hash parity: TS deterministic across runs", async () => {
  const projection = JSON.parse(
    readFileSync(resolve(FIXTURES_DIR, "snapshot-parity-cpmm.json"), "utf8"),
  );
  const first = await canonicalHash(projection, "amm_snapshot");
  const second = await canonicalHash(projection, "amm_snapshot");
  assert.equal(first, second);
});
