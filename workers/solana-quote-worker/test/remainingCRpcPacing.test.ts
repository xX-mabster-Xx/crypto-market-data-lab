import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import test from "node:test";

import {
  closeRpcScheduler,
  configureRpcPacer,
  RpcDeadlineExceededError,
  RpcRequestCancelledError,
  RpcSchedulerOverloadError,
  rpcSchedulerMetrics,
  scheduleRpc,
  sharedRpcFetch,
  sharedRpcFetchMiddleware,
  withRpcJobOptions,
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
  await new Promise<void>((resolve) => { setImmediate(resolve); });
}

function assertMinimumGaps(starts: readonly number[], minimumIntervalMs: number): void {
  const ordered = [...starts].sort((left, right) => left - right);
  for (let index = 1; index < ordered.length; index += 1) {
    assert.ok(
      ordered[index]! - ordered[index - 1]! >= minimumIntervalMs - 1,
      `physical starts ${index - 1}/${index} were only ${ordered[index]! - ordered[index - 1]!}ms apart`,
    );
  }
}

test("one logical job paces every sequential physical fetch", async () => {
  const minimumIntervalMs = 40;
  configureRpcPacer(minimumIntervalMs, 8);
  const originalFetch = globalThis.fetch;
  const starts: number[] = [];
  globalThis.fetch = async () => {
    starts.push(performance.now());
    return new Response("{}", { status: 200 });
  };
  try {
    await scheduleRpc(
      { priority: "refresh", description: "two physical requests" },
      async () => {
        await sharedRpcFetch("https://rpc.example/one", { method: "POST" });
        await sharedRpcFetch("https://rpc.example/two", { method: "POST" });
      },
    );
    assert.equal(starts.length, 2);
    assert.ok(
      starts[1]! - starts[0]! >= minimumIntervalMs - 1,
      `physical starts were only ${starts[1]! - starts[0]!}ms apart`,
    );
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("one logical job paces concurrent physical fan-out", async () => {
  const minimumIntervalMs = 20;
  configureRpcPacer(minimumIntervalMs, 8, 1, 8);
  const originalFetch = globalThis.fetch;
  const starts: number[] = [];
  globalThis.fetch = async () => {
    starts.push(performance.now());
    return new Response("{}", { status: 200 });
  };
  try {
    await scheduleRpc(
      { priority: "bootstrap", description: "concurrent SDK fan-out" },
      async () => Promise.all([
        sharedRpcFetch("https://rpc.example/one"),
        sharedRpcFetch("https://rpc.example/two"),
        sharedRpcFetch("https://rpc.example/three"),
      ]),
    );

    assert.equal(starts.length, 3);
    assertMinimumGaps(starts, minimumIntervalMs);
    const metrics = rpcSchedulerMetrics();
    assert.equal(metrics.rpc_logical_started_total, 1);
    assert.equal(metrics.rpc_physical_started_total, 3);
    assert.equal(metrics.rpc_physical_completed_total, 3);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("direct and nested requests share one physical budget without double starts", async () => {
  const minimumIntervalMs = 15;
  configureRpcPacer(minimumIntervalMs, 16, 4, 4);
  const originalFetch = globalThis.fetch;
  const starts: number[] = [];
  globalThis.fetch = async () => {
    starts.push(performance.now());
    return new Response("{}", { status: 200 });
  };
  try {
    await Promise.all([
      sharedRpcFetch("https://rpc.example/direct"),
      scheduleRpc(
        { priority: "bootstrap", description: "one nested request" },
        async () => sharedRpcFetch("https://rpc.example/nested-one"),
      ),
      scheduleRpc(
        { priority: "refresh", description: "two nested requests" },
        async () => Promise.all([
          sharedRpcFetch("https://rpc.example/nested-two"),
          sharedRpcFetch("https://rpc.example/nested-three"),
        ]),
      ),
    ]);

    assert.equal(starts.length, 4);
    assertMinimumGaps(starts, minimumIntervalMs);
    const metrics = rpcSchedulerMetrics();
    assert.equal(metrics.rpc_logical_started_total, 2);
    assert.equal(metrics.rpc_physical_started_total, 4);
    assert.equal(metrics.rpc_physical_enqueued_total, 4);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("logical concurrency one cannot deadlock a nested physical request", async () => {
  configureRpcPacer(0, 4, 1, 1);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => new Response("{}", { status: 200 });
  try {
    const result = scheduleRpc(
      { priority: "interactive", description: "logical single slot" },
      async () => (await sharedRpcFetch("https://rpc.example/nested")).status,
    );
    assert.equal(await Promise.race([
      result,
      new Promise<number>((_, reject) => {
        setTimeout(() => { reject(new Error("nested request deadlocked")); }, 250);
      }),
    ]), 200);
    assert.equal(rpcSchedulerMetrics().rpc_physical_started_total, 1);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("retry wrapper paces every transport attempt", async () => {
  const minimumIntervalMs = 20;
  configureRpcPacer(minimumIntervalMs, 8, 2, 2);
  const originalFetch = globalThis.fetch;
  const starts: number[] = [];
  globalThis.fetch = async () => {
    starts.push(performance.now());
    return new Response("{}", { status: starts.length < 3 ? 503 : 200 });
  };
  try {
    const response = await withRpcJobOptions(
      { priority: "interactive", description: "stub web3 retry wrapper" },
      async () => {
        let latest!: Response;
        for (let attempt = 0; attempt < 3; attempt += 1) {
          latest = await sharedRpcFetch("https://rpc.example/retry", { method: "POST" });
          if (latest.ok) break;
        }
        return latest;
      },
    );

    assert.equal(response.status, 200);
    assert.equal(starts.length, 3);
    assertMinimumGaps(starts, minimumIntervalMs);
    assert.equal(rpcSchedulerMetrics().rpc_physical_started_total, 3);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("physical gate prioritizes interactive requests over queued refreshes", async () => {
  configureRpcPacer(0, 16, 4, 1);
  const originalFetch = globalThis.fetch;
  const active = deferred<Response>();
  const activeStarted = deferred<void>();
  const starts: string[] = [];
  globalThis.fetch = async (info) => {
    const url = String(info);
    starts.push(url);
    if (url.endsWith("/hold")) {
      activeStarted.resolve();
      return active.promise;
    }
    return new Response("{}", { status: 200 });
  };
  try {
    const held = sharedRpcFetch("https://rpc.example/hold");
    await activeStarted.promise;
    const refreshes = [...Array(5).keys()].map((index) => withRpcJobOptions(
      { priority: "refresh", description: `refresh request ${index}` },
      () => sharedRpcFetch(`https://rpc.example/refresh-${index}`),
    ));
    const interactive = withRpcJobOptions(
      { priority: "interactive", description: "interactive request" },
      () => sharedRpcFetch("https://rpc.example/interactive"),
    );
    active.resolve(new Response("{}", { status: 200 }));

    await Promise.all([held, interactive, ...refreshes]);
    assert.match(starts[1]!, /\/interactive$/u);
    assert.equal(rpcSchedulerMetrics().rpc_physical_started_total, 7);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("physical overload deadline and queued cancellation reject before fetch", async () => {
  configureRpcPacer(0, 2, 2, 1);
  const originalFetch = globalThis.fetch;
  const active = deferred<Response>();
  const activeStarted = deferred<void>();
  let rawFetches = 0;
  globalThis.fetch = async () => {
    rawFetches += 1;
    activeStarted.resolve();
    return active.promise;
  };
  try {
    const held = sharedRpcFetch("https://rpc.example/hold");
    await activeStarted.promise;

    const controller = new AbortController();
    const cancelled = sharedRpcFetch(
      "https://rpc.example/cancelled",
      { signal: controller.signal },
    );
    const cancelledAssertion = assert.rejects(cancelled, RpcRequestCancelledError);
    const deadline = withRpcJobOptions(
      {
        priority: "refresh",
        deadlineAtMs: performance.now() + 10,
        description: "deadline before physical start",
      },
      () => sharedRpcFetch("https://rpc.example/expired"),
    );
    const deadlineAssertion = assert.rejects(deadline, RpcDeadlineExceededError);
    const overloaded = sharedRpcFetch("https://rpc.example/overloaded");
    await assert.rejects(overloaded, RpcSchedulerOverloadError);
    assert.equal(rpcSchedulerMetrics().rpc_physical_queue_total, 2);

    controller.abort("test cancellation");
    await cancelledAssertion;
    await new Promise<void>((resolve) => { setTimeout(resolve, 15); });
    active.resolve(new Response("{}", { status: 200 }));
    await held;
    await deadlineAssertion;

    const metrics = rpcSchedulerMetrics();
    assert.equal(rawFetches, 1);
    assert.equal(metrics.rpc_physical_started_total, 1);
    assert.equal(metrics.rpc_physical_cancelled_total, 1);
    assert.equal(metrics.rpc_physical_expired_total, 1);
    assert.equal(metrics.rpc_physical_rejected_overload_total, 1);
    assert.equal(metrics.rpc_physical_queue_total, 0);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("Promise.all physical fan-out remains bounded by the transport queue cap", async () => {
  configureRpcPacer(0, 4, 2, 1);
  const originalFetch = globalThis.fetch;
  const active = deferred<Response>();
  const started = deferred<void>();
  let rawFetches = 0;
  globalThis.fetch = async () => {
    rawFetches += 1;
    started.resolve();
    return active.promise;
  };
  try {
    const held = sharedRpcFetch("https://rpc.example/held");
    await started.promise;
    const fanout = [...Array(1_000).keys()].map((index) => sharedRpcFetch(
      `https://rpc.example/fanout-${index}`,
    ).then(
      () => "completed" as const,
      (error: unknown) => error,
    ));
    await nextTurn();

    const saturated = rpcSchedulerMetrics();
    assert.equal(saturated.rpc_physical_active, 1);
    assert.equal(saturated.rpc_physical_queue_total, 4);
    assert.equal(saturated.rpc_physical_queue_high_watermark, 4);
    assert.equal(saturated.rpc_physical_rejected_overload_total, 996);

    active.resolve(new Response("{}", { status: 200 }));
    await held;
    const results = await Promise.all(fanout);
    assert.equal(results.filter((value) => value === "completed").length, 4);
    assert.equal(
      results.filter((value) => value instanceof RpcSchedulerOverloadError).length,
      996,
    );
    assert.equal(rawFetches, 5);
    assert.equal(rpcSchedulerMetrics().rpc_physical_queue_total, 0);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("close cancels an active physical fetch even when raw fetch ignores abort", async () => {
  configureRpcPacer(0, 4, 1, 1);
  const originalFetch = globalThis.fetch;
  const started = deferred<void>();
  globalThis.fetch = () => {
    started.resolve();
    return new Promise<Response>(() => undefined);
  };
  try {
    const request = sharedRpcFetch("https://rpc.example/stuck");
    const rejection = assert.rejects(request, RpcRequestCancelledError);
    await started.promise;

    await closeRpcScheduler(250);
    await rejection;
    const metrics = rpcSchedulerMetrics();
    assert.equal(metrics.rpc_physical_active, 0);
    assert.equal(metrics.rpc_physical_queue_total, 0);
    assert.equal(metrics.rpc_physical_cancelled_total, 1);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("close cancels a request waiting on the interval timer", async () => {
  configureRpcPacer(100, 4, 1, 1);
  const originalFetch = globalThis.fetch;
  let rawFetches = 0;
  globalThis.fetch = async () => {
    rawFetches += 1;
    return new Response("{}", { status: 200 });
  };
  try {
    await sharedRpcFetch("https://rpc.example/first");
    const waiting = sharedRpcFetch("https://rpc.example/waiting");
    const rejection = assert.rejects(waiting, RpcRequestCancelledError);
    await nextTurn();
    assert.equal(rpcSchedulerMetrics().rpc_physical_queue_total, 1);

    await closeRpcScheduler(250);
    await rejection;
    assert.equal(rawFetches, 1);
    assert.equal(rpcSchedulerMetrics().rpc_physical_queue_total, 0);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("paused and failed fetches do not reset spacing or release a burst", async () => {
  const minimumIntervalMs = 20;
  configureRpcPacer(minimumIntervalMs, 8, 2, 4);
  const originalFetch = globalThis.fetch;
  const first = deferred<Response>();
  const thirdStarted = deferred<void>();
  const starts: number[] = [];
  globalThis.fetch = async () => {
    starts.push(performance.now());
    if (starts.length === 1) return first.promise;
    if (starts.length === 2) throw new Error("stub provider failure");
    thirdStarted.resolve();
    return new Response("{}", { status: 200 });
  };
  try {
    const paused = sharedRpcFetch("https://rpc.example/paused");
    const failed = sharedRpcFetch("https://rpc.example/failed");
    const failedAssertion = assert.rejects(failed, /stub provider failure/u);
    const recovered = sharedRpcFetch("https://rpc.example/recovered");
    await thirdStarted.promise;

    assert.equal((await recovered).status, 200);
    await failedAssertion;
    assertMinimumGaps(starts, minimumIntervalMs);
    first.resolve(new Response("{}", { status: 200 }));
    await paused;
    const metrics = rpcSchedulerMetrics();
    assert.equal(metrics.rpc_physical_started_total, 3);
    assert.equal(metrics.rpc_physical_completed_total, 2);
    assert.equal(metrics.rpc_physical_failed_total, 1);
  } finally {
    globalThis.fetch = originalFetch;
    await closeRpcScheduler();
  }
});

test("compatibility middleware uses the same physical start budget", async () => {
  const minimumIntervalMs = 20;
  configureRpcPacer(minimumIntervalMs, 4, 1, 2);
  const starts: number[] = [];
  const completed = deferred<void>();
  const next = (): void => {
    starts.push(performance.now());
    if (starts.length === 2) completed.resolve();
  };
  try {
    sharedRpcFetchMiddleware("https://rpc.example/middleware-one", {}, next);
    sharedRpcFetchMiddleware("https://rpc.example/middleware-two", {}, next);
    await completed.promise;

    assertMinimumGaps(starts, minimumIntervalMs);
    assert.equal(rpcSchedulerMetrics().rpc_physical_started_total, 2);
  } finally {
    await closeRpcScheduler();
  }
});
