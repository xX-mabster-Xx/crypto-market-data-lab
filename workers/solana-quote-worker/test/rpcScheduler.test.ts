import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import test from "node:test";

import {
  closeRpcScheduler,
  configureRpcPacer,
  RpcDeadlineExceededError,
  RpcScheduler,
  RpcSchedulerCloseTimeoutError,
  RpcSchedulerOverloadError,
  rpcSchedulerMetrics,
  scheduleRpc,
  sharedRpcFetch,
} from "../src/rpcPacer.js";

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (reason: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((accept, decline) => {
    resolve = accept;
    reject = decline;
  });
  return { promise, resolve, reject };
}

async function nextTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

test("100k equivalent refresh jobs occupy one pending queue node", async () => {
  const scheduler = new RpcScheduler({ minimumIntervalMs: 0, maxPendingJobs: 4, maxActiveJobs: 1 });
  const activeGate = deferred<void>();
  const active = scheduler.scheduleRpc(
    { priority: "interactive", description: "active request" },
    async () => activeGate.promise,
  );
  await nextTurn();

  let first!: Promise<number>;
  let latest!: Promise<number>;
  for (let sequence = 0; sequence < 100_000; sequence += 1) {
    const scheduled = scheduler.scheduleRpc(
      {
        priority: "refresh",
        coalesceKey: "refresh:orca:pool-1",
        description: `refresh pool at generation ${sequence}`,
      },
      async () => sequence,
    );
    if (sequence === 0) first = scheduled;
    latest = scheduled;
  }
  assert.strictEqual(first, latest);
  const queued = scheduler.metrics();
  assert.equal(queued.rpc_active, 1);
  assert.equal(queued.rpc_queue_total, 1);
  assert.equal(queued.rpc_queue_refresh, 1);
  assert.equal(queued.rpc_coalesced_total, 99_999);

  activeGate.resolve();
  await active;
  assert.equal(await latest, 99_999);
  await scheduler.close();
  assert.equal(scheduler.metrics().rpc_queue_total, 0);
});

test("interactive RPC starts before queued maintenance after the current slot", async () => {
  const scheduler = new RpcScheduler({ minimumIntervalMs: 0, maxPendingJobs: 128, maxActiveJobs: 1 });
  const activeGate = deferred<void>();
  const active = scheduler.scheduleRpc(
    { priority: "interactive", description: "current active request" },
    async () => activeGate.promise,
  );
  await nextTurn();

  const starts: string[] = [];
  const refreshes = [...Array(100).keys()].map((index) => scheduler.scheduleRpc(
    {
      priority: "refresh",
      coalesceKey: `refresh:pool:${index}`,
      description: `refresh pool ${index}`,
    },
    async () => { starts.push(`refresh-${index}`); },
  ));
  const interactive = scheduler.scheduleRpc(
    { priority: "interactive", description: "live quote" },
    async () => { starts.push("interactive"); },
  );
  activeGate.resolve();
  await active;
  await interactive;
  assert.equal(starts[0], "interactive");
  await Promise.all(refreshes);
  await scheduler.close();
});

test("expired jobs never call their RPC function", async () => {
  let now = 0;
  const scheduler = new RpcScheduler({
    minimumIntervalMs: 0,
    maxPendingJobs: 4,
    maxActiveJobs: 1,
    nowMs: () => now,
  });
  const activeGate = deferred<void>();
  const active = scheduler.scheduleRpc(
    { priority: "interactive", description: "active request" },
    async () => activeGate.promise,
  );
  await nextTurn();
  let called = false;
  const expired = scheduler.scheduleRpc(
    { priority: "interactive", deadlineAtMs: 5, description: "deadline request" },
    async () => { called = true; },
  );
  now = 10;
  activeGate.resolve();
  await active;
  await assert.rejects(expired, RpcDeadlineExceededError);
  assert.equal(called, false);
  assert.equal(scheduler.metrics().rpc_expired_total, 1);
  await scheduler.close();
});

test("hard cap rejects overload with a typed error and never exceeds capacity", async () => {
  const scheduler = new RpcScheduler({ minimumIntervalMs: 0, maxPendingJobs: 2, maxActiveJobs: 1 });
  const activeGate = deferred<void>();
  const active = scheduler.scheduleRpc(
    { priority: "interactive", description: "active request" },
    async () => activeGate.promise,
  );
  await nextTurn();
  const first = scheduler.scheduleRpc(
    { priority: "bootstrap", description: "bootstrap one" },
    async () => 1,
  );
  const second = scheduler.scheduleRpc(
    { priority: "refresh", coalesceKey: "refresh:two", description: "refresh two" },
    async () => 2,
  );
  const rejected = scheduler.scheduleRpc(
    { priority: "interactive", description: "overflowing quote" },
    async () => 3,
  );
  await assert.rejects(rejected, RpcSchedulerOverloadError);
  assert.equal(scheduler.metrics().rpc_queue_total, 2);
  assert.equal(scheduler.metrics().rpc_queue_high_watermark, 2);
  assert.equal(scheduler.metrics().rpc_rejected_overload_total, 1);

  activeGate.resolve();
  await active;
  assert.deepEqual(await Promise.all([first, second]), [1, 2]);
  await scheduler.close();
});

test("actual RPC starts respect monotonic spacing", async () => {
  const minimumIntervalMs = 20;
  const scheduler = new RpcScheduler({ minimumIntervalMs, maxPendingJobs: 8, maxActiveJobs: 8 });
  const starts: number[] = [];
  const jobs = [...Array(4).keys()].map((index) => scheduler.scheduleRpc(
    { priority: "interactive", description: `spaced request ${index}` },
    async () => { starts.push(performance.now()); },
  ));
  await Promise.all(jobs);
  for (let index = 1; index < starts.length; index += 1) {
    assert.ok(
      starts[index]! - starts[index - 1]! >= minimumIntervalMs - 1,
      `starts ${index - 1}/${index} were too close`,
    );
  }
  await scheduler.close();
});

test("a rejected RPC does not stop later jobs", async () => {
  const scheduler = new RpcScheduler({ minimumIntervalMs: 0, maxPendingJobs: 4, maxActiveJobs: 1 });
  const failed = scheduler.scheduleRpc(
    { priority: "interactive", description: "provider failure" },
    async () => { throw new Error("RPC rejected"); },
  );
  const recovered = scheduler.scheduleRpc(
    { priority: "interactive", description: "next request" },
    async () => "ok",
  );
  await assert.rejects(failed, /RPC rejected/u);
  assert.equal(await recovered, "ok");
  const metrics = scheduler.metrics();
  assert.equal(metrics.rpc_failed_total, 1);
  assert.equal(metrics.rpc_completed_total, 1);
  await scheduler.close();
});

test("scheduler shutdown has a bounded timeout", async () => {
  const scheduler = new RpcScheduler({ minimumIntervalMs: 0, maxPendingJobs: 2, maxActiveJobs: 1 });
  const gate = deferred<void>();
  const active = scheduler.scheduleRpc(
    { priority: "interactive", description: "stuck request" },
    async () => gate.promise,
  );
  await nextTurn();
  await assert.rejects(
    scheduler.close({ timeoutMs: 10 }),
    RpcSchedulerCloseTimeoutError,
  );
  gate.resolve();
  await active;
  await nextTurn();
  assert.equal(scheduler.metrics().rpc_active, 0);
});

test("an explicitly scheduled production RPC is not scheduled twice by shared fetch", async () => {
  configureRpcPacer(0, 4);
  const originalFetch = globalThis.fetch;
  let rawFetches = 0;
  globalThis.fetch = async () => {
    rawFetches += 1;
    return new Response("{}", { status: 200 });
  };
  try {
    const response = await scheduleRpc(
      { priority: "refresh", coalesceKey: "refresh:test:pool", description: "production bridge" },
      async () => sharedRpcFetch("https://rpc.example", { method: "POST" }),
    );
    assert.equal(response.status, 200);
    assert.equal(rawFetches, 1);
    const metrics = rpcSchedulerMetrics();
    assert.equal(metrics.rpc_started_total, 1);
    assert.equal(metrics.rpc_logical_started_total, 1);
    assert.equal(metrics.rpc_physical_started_total, 1);
    await closeRpcScheduler();
  } finally {
    globalThis.fetch = originalFetch;
  }
});
