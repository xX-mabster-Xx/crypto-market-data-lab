import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  coreRefreshDue,
  DebouncedStateEmitter,
  deterministicStaggerMs,
  LatestOnlyMailbox,
  PoolSlotProvenance,
} from "../src/engineRuntime.js";

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((accept) => { resolve = accept; });
  return { promise, resolve };
}

test("BUG-017 Raydium core mailbox keeps only first and latest of 100k updates", async () => {
  const gate = deferred();
  const started = deferred();
  const processed: number[] = [];
  const mailbox = new LatestOnlyMailbox<number>(0, async (value) => {
    processed.push(value);
    if (value === 1) {
      started.resolve();
      await gate.promise;
    }
  });

  assert.equal(mailbox.submit(1, 1), true);
  await started.promise;
  for (let slot = 2; slot <= 100_001; slot += 1) mailbox.submit(slot, slot);

  const blocked = mailbox.stats();
  assert.equal(blocked.processing, true);
  assert.equal(blocked.pending, 1);
  assert.equal(blocked.maximum_pending, 1);
  assert.equal(blocked.coalesced_total, 99_999);

  gate.resolve();
  await mailbox.idle();
  assert.deepEqual(processed, [1, 100_001]);
  assert.equal(mailbox.stats().pending, 0);
});

test("BUG-017 production CLMM path uses the mailbox instead of a Promise tail", () => {
  const source = readFileSync(new URL("../src/raydiumClmm.ts", import.meta.url), "utf8");
  assert.match(source, /coreMailbox\.submit/u);
  assert.doesNotMatch(source, /updateChain|\.then\(async \(\) =>/u);
});

test("BUG-017 mailbox continues with latest pending after processor failure", async () => {
  const gate = deferred();
  const started = deferred();
  const processed: number[] = [];
  const errors: string[] = [];
  const mailbox = new LatestOnlyMailbox<number>(100, async (value) => {
    processed.push(value);
    if (value === 101) {
      started.resolve();
      await gate.promise;
      throw new Error("synthetic decode failure");
    }
  }, (error) => errors.push(String(error)));

  mailbox.submit(101, 101);
  await started.promise;
  mailbox.submit(102, 102);
  mailbox.submit(103, 103);
  assert.equal(mailbox.submit(102, 102), false);
  gate.resolve();
  await mailbox.idle();

  assert.deepEqual(processed, [101, 103]);
  assert.equal(errors.length, 1);
  assert.equal(mailbox.stats().errors_total, 1);
  assert.equal(mailbox.stats().processing, false);
});

test("BUG-017 mailbox shutdown drops pending buffer and drains only active work", async () => {
  const gate = deferred();
  const started = deferred();
  const processed: number[] = [];
  const mailbox = new LatestOnlyMailbox<number>(0, async (value) => {
    processed.push(value);
    started.resolve();
    await gate.promise;
  });
  mailbox.submit(1, 1);
  await started.promise;
  mailbox.submit(2, 2);
  const closing = mailbox.dispose();
  assert.equal(mailbox.stats().pending, 0);
  gate.resolve();
  await closing;
  assert.deepEqual(processed, [1]);
  assert.equal(mailbox.stats().processing, false);
  assert.equal(mailbox.submit(3, 3), false);
});

test("BUG-019 dependency storm emits one trailing latest state and disposes timer", async () => {
  const provenance = new PoolSlotProvenance(100, 0);
  const emitted: Array<ReturnType<PoolSlotProvenance["fields"]>> = [];
  const emitter = new DebouncedStateEmitter(
    () => emitted.push(provenance.fields()),
    20,
  );
  emitter.request("initial", "immediate");

  for (let slot = 101; slot <= 1_100; slot += 1) {
    assert.equal(provenance.acceptDependency("tick-array", slot), true);
    emitter.request(`dependency:${provenance.dependencyGeneration}`, "debounced");
  }
  assert.equal(emitter.stats().pending, 1);
  assert.equal(emitter.stats().coalesced_total, 999);
  await new Promise<void>((resolve) => setTimeout(resolve, 30));

  assert.equal(emitted.length, 2);
  assert.deepEqual(emitted.at(-1), {
    core_state_slot: 100,
    dependency_slot_min: 1_100,
    dependency_slot_max: 1_100,
    dependency_generation: 1_000,
  });
  emitter.request("dependency:after-dispose", "debounced");
  emitter.dispose();
  await new Promise<void>((resolve) => setTimeout(resolve, 30));
  assert.equal(emitted.length, 2);
});

test("BUG-019 semantic duplicate does not emit and core update remains immediate", () => {
  let emits = 0;
  const emitter = new DebouncedStateEmitter(() => { emits += 1; }, 100);
  assert.equal(emitter.request("core:100", "immediate"), true);
  assert.equal(emitter.request("core:100", "immediate"), false);
  assert.equal(emitter.request("core:101", "immediate"), true);
  assert.equal(emits, 2);
  emitter.dispose();
});

test("BUG-019 immediate core update subsumes pending dependency notification", async () => {
  const state = { core: 100, dependency: 0 };
  const emitted: Array<{ core: number; dependency: number }> = [];
  const emitter = new DebouncedStateEmitter(
    () => emitted.push({ ...state }),
    20,
  );
  emitter.request("100:0", "immediate");
  state.dependency = 7;
  emitter.request("100:7", "debounced");
  state.core = 105;
  emitter.request("105:7", "immediate");

  await new Promise<void>((resolve) => setTimeout(resolve, 30));
  assert.deepEqual(emitted, [
    { core: 100, dependency: 0 },
    { core: 105, dependency: 7 },
  ]);
  assert.equal(emitter.stats().pending, 0);
  emitter.dispose();
});

test("BUG-019 production dependency callbacks debounce through Agent E state output", () => {
  const meteora = readFileSync(new URL("../src/meteoraDlmm.ts", import.meta.url), "utf8");
  const orca = readFileSync(new URL("../src/orcaWhirlpool.ts", import.meta.url), "utf8");
  const worker = readFileSync(new URL("../src/worker.ts", import.meta.url), "utf8");
  assert.match(meteora, /dependencyGeneration[\s\S]{0,100}"debounced"/u);
  assert.match(orca, /dependencyGeneration[\s\S]{0,100}"debounced"/u);
  assert.doesNotMatch(meteora, /pool\.slot\s*=\s*Math\.max/u);
  assert.doesNotMatch(orca, /pool\.slot\s*=\s*Math\.max/u);
  assert.match(worker, /emitState\([\s\S]{0,80}pool_state:meteora_dlmm/u);
  assert.match(worker, /emitState\([\s\S]{0,80}pool_state:orca_whirlpool/u);
});

test("BUG-020 dependency slot cannot impersonate or block a newer core slot", () => {
  const provenance = new PoolSlotProvenance(100, 1_000);
  assert.equal(provenance.acceptDependency("tick-a", 110, 1_100), true);
  assert.equal(provenance.acceptCore(105, 1_200), true);
  assert.deepEqual(provenance.fields(), {
    core_state_slot: 105,
    dependency_slot_min: 110,
    dependency_slot_max: 110,
    dependency_generation: 1,
  });
  assert.equal(provenance.acceptDependency("tick-a", 120, 1_300), true);
  assert.equal(provenance.acceptCore(106, 1_400), true);
  assert.equal(provenance.fields().core_state_slot, 106);
  assert.equal(provenance.fields().dependency_slot_max, 120);
});

test("BUG-023 fresh pools are skipped and deterministic staggering spreads stale pools", () => {
  const fresh = new PoolSlotProvenance(100, 10_000);
  assert.equal(coreRefreshDue(fresh, "pool-a", 12_000, 15_000, 5_000), false);

  const offsets = new Set<number>();
  let dueAtBoundary = 0;
  for (let index = 0; index < 100; index += 1) {
    const identity = `pool-${index}`;
    offsets.add(deterministicStaggerMs(identity, 5_000));
    const state = new PoolSlotProvenance(1, 0);
    if (coreRefreshDue(state, identity, 15_000, 15_000, 5_000)) dueAtBoundary += 1;
    assert.equal(coreRefreshDue(state, identity, 20_000, 15_000, 5_000), true);
  }
  assert.ok(offsets.size > 90);
  assert.ok(dueAtBoundary < 5);

  fresh.noteRpcRefresh(20_000);
  assert.equal(coreRefreshDue(fresh, "pool-a", 21_000, 15_000, 5_000), false);
});

test("BUG-023 production refreshes use Agent E low-priority coalescing keys", () => {
  for (const file of ["raydiumClmm.ts", "raydiumStandard.ts", "meteoraDlmm.ts", "orcaWhirlpool.ts"]) {
    const source = readFileSync(new URL(`../src/${file}`, import.meta.url), "utf8");
    assert.match(source, /priority: "refresh"/u, file);
    assert.match(source, /coalesceKey:/u, file);
    assert.match(source, /coreRefreshDue/u, file);
    assert.match(source, /refreshInFlight/u, file);
    assert.match(source, /refreshesUnchanged/u, file);
  }
});
