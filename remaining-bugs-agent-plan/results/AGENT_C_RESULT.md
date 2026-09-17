# Agent C result — pacing every physical RPC request

Date: 2026-09-17  
Agent: C  
Requested model: OpenAI GPT-5.6 Sol, high reasoning (execution performed in the current Codex session).  
Baseline commit: `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`.

## Scope

Implemented the Agent C assignment for audit finding **O-01**, the residual
physical-request issue in **BUG-018**, and the Node-side request-level
invariant related to **BUG-009**. Changes are limited to `rpcPacer.ts`, its
scheduler regression file, a new Agent C transport test file and this result.
No engine, worker, protocol manifest, Python or other agent-owned file was
edited.

## Red → green evidence

The first regression used the production `scheduleRpc -> sharedRpcFetch ->
globalThis.fetch` path. One logical refresh job awaited two sequential physical
fetches with a configured 40 ms interval. Before the fix:

```text
AssertionError: physical starts were only 15.921784000000002ms apart
tests 1; pass 0; fail 1
```

After separating logical admission from the physical transport gate, the same
test passed with the file reporting about 43 ms for the two-request scenario.
The completed Agent C matrix contains **12 passing tests**.

## Root cause

`RpcScheduler.start()` previously wrapped the complete asynchronous logical job
in `scheduledRpcStart=true`. Every nested `sharedRpcFetch()` saw the inherited
flag and called raw `fetch()` directly. Therefore an SDK operation with two
reads/retries consumed one scheduler start while producing multiple unpaced
wire starts. Re-enqueuing those nested calls into the same scheduler would
deadlock when logical concurrency was one.

## Implementation

### Separate logical and physical schedulers

- The shared logical scheduler retains bounded queues, priorities, refresh
  coalescing, deadlines, hard overload rejection and active-job caps.
- Its configured start interval is zero: it schedules logical work, not wire
  starts.
- A distinct bounded physical scheduler owns the configured monotonic minimum
  interval and gates every `globalThis.fetch()` call made through
  `sharedRpcFetch`.
- `scheduleRpc(options, fn)` now runs `fn` inside its exact `RpcJobOptions`
  async context. Nested SDK fetches inherit priority and deadline without
  sharing the logical coalescing key.
- Direct `sharedRpcFetch` calls use the same physical scheduler with the
  documented default interactive context.

This design has no parent/child admission cycle: a logical job can occupy the
only logical slot while its nested fetch progresses through the independent
physical gate.

### Boundedness, cancellation and shutdown

- Physical pending requests use the same configured hard queue capacity; raw
  Promise fan-out beyond the cap receives typed `RpcSchedulerOverloadError`
  before `fetch()` starts.
- Added optional `RpcJobOptions.signal` and typed
  `RpcRequestCancelledError`. Queued cancellation removes the job and its
  listener immediately. Signals are intentionally rejected on coalesced jobs,
  avoiding ambiguous cancellation ownership for a shared Promise.
- A bounded shared shutdown signal reaches queued and active physical fetches.
  `closeRpcScheduler()` stops admission, rejects queued logical/physical work,
  and aborts active physical requests. The fetch wrapper races cancellation,
  so a non-cooperative raw test stub cannot leave the caller Promise hanging.
- Abort listeners are detached on start, expiration, rejection and
  cancellation. The shared signal's listener-warning limit is set to the exact
  configured pending + active bound, not disabled globally.
- Expiration/cancellation of the final timer-waiting job clears its timer, so
  close/idle detection does not wait for a now-useless wakeup.

### Compatibility middleware

`sharedRpcFetchMiddleware` now uses the same physical start gate. Every
middleware invocation/retry receives the same spacing, priority, deadline,
overload and pre-start cancellation behavior. Scheduling failure is surfaced
to web3.js by invoking its continuation with an already-aborted signal.

The web3.js `FetchMiddleware` callback exposes only modified request arguments,
not the response Promise. Consequently the gate can account and pace its actual
continuation/start, but response-completion/inflight/error accounting is exact
only for the production `sharedRpcFetch` adapter. Production engines already
use `sharedRpcFetch`; no broader middleware completion guarantee is claimed.

### Accounting

`rpcSchedulerMetrics()` is additive and explicit:

- legacy unprefixed `rpc_*` fields retain logical-scheduler semantics;
- `rpc_logical_*` aliases make logical jobs unambiguous;
- `rpc_physical_*` reports physical queue/priority counts, capacity, active
  requests, high watermark, actual enqueued/started/completed/failed/cancelled/
  expired/overload totals, queue waits, and last monotonic wire-start time;
- `rpc_cancelled_total` was added to the standalone scheduler metrics.

## Tests added

`test/remainingCRpcPacing.test.ts` exercises production adapters with only a
local `globalThis.fetch` recorder/stub:

1. one logical job with two sequential physical requests;
2. one logical job with concurrent `Promise.all` fan-out;
3. direct requests plus multiple nested jobs sharing one global wire budget;
4. logical concurrency one with no nested deadlock;
5. three retry attempts, each independently paced;
6. physical interactive priority ahead of queued refresh work;
7. overload, deadline-before-start and queued cancellation before raw fetch;
8. 1,000-request fan-out held to one active + four queued, with 996 typed
   overload rejections and only five actual raw starts;
9. close aborting an active raw fetch even when the stub ignores its signal;
10. close cancelling a request waiting on the spacing timer;
11. a paused request plus a failed request not resetting spacing or causing a
    burst;
12. compatibility middleware using the same physical start budget.

Existing `rpcScheduler.test.ts` still covers 100,000 coalescible refresh jobs
in one queue node, interactive priority over 100 refreshes, deadline, hard cap,
recovery after rejection and bounded scheduler-close timeout. Its production
bridge assertion now checks one logical start, one physical start and one raw
fetch for a single HTTP request.

## Commands and actual results

| Command | Result |
| --- | --- |
| Initial `node --import tsx test/remainingCRpcPacing.test.ts` | **1 failed**; physical gap 15.921784 ms vs required 40 ms |
| Final same Agent C test command | **12 passed** in about 0.39 s |
| `node --import tsx test/rpcScheduler.test.ts` | **8 passed**; includes 100,000 coalesced jobs |
| `npm run check` | Passed (`tsc --noEmit`) |
| `npm test` | **13 test files passed**, no failures |
| `git diff --check` | Passed |

Tests were offline: no DNS, RPC endpoint, credentials, wallet or transaction
submission was used.

## Status by audit/spec ID

| ID | Status | Evidence |
| --- | --- | --- |
| O-01 | **FIXED** | The exact audited production-path reproduction is green; sequential, concurrent, direct, nested and retry starts all pass one physical gate. |
| BUG-018 | **FIXED in Agent C scope** | Logical queue boundedness/priority/coalescing is preserved; physical queue and active work now have independent hard caps, deadlines, cancellation and shutdown. |
| BUG-009 request invariant | **FIXED for the Node HTTP adapters in scope** | Every `sharedRpcFetch` attempt and every compatibility-middleware continuation is paced at the physical boundary with no nested bypass. Python provider paths were not changed or re-certified here. |

## Stable handoff contract for D

No engine call-site migration is required. Preserve these APIs:

```text
scheduleRpc(options, fn)
sharedRpcScheduler.schedule(options, fn)
withRpcJobOptions(options, fn)
sharedRpcFetch(info, init)
```

- Use `scheduleRpc` / injected `RunRpcJob` for bounded logical SDK operations,
  priority and refresh coalescing.
- Construct every Solana `Connection` used by production engines with
  `fetch: sharedRpcFetch`; do not call raw `globalThis.fetch` in engines.
- Use `withRpcJobOptions` only to supply priority/deadline context around SDK
  work that is not itself admitted as a logical job.
- Do not wrap one raw request in another physical pacer. `sharedRpcFetch` is
  the sole wire-start gate.
- `coalesceKey` belongs to the logical job only and is deliberately stripped
  from physical attempts. Retries and fan-out are distinct wire requests.
- Within one logical job, each physical attempt inherits `priority` and
  `deadlineAtMs`. A deadline can therefore pass after logical admission but
  before the wire start, in which case raw fetch is not invoked.
- Do not combine `signal` with a coalesced logical job; cancellation ownership
  for a shared coalesced Promise is intentionally rejected.

## Metrics handoff for F

For wire quota and transport health, read:

```text
rpc_physical_queue_total
rpc_physical_queue_{interactive,bootstrap,refresh}
rpc_physical_queue_capacity
rpc_physical_active / rpc_physical_active_capacity
rpc_physical_queue_high_watermark
rpc_physical_{enqueued,started,completed,failed,cancelled,expired,rejected_overload}_total
rpc_physical_queue_wait_{average,max}_ms
rpc_physical_last_request_start_monotonic_ms
```

For logical engine work and maintenance pressure, use the corresponding
`rpc_logical_*` counters. Unprefixed fields remain for compatibility and refer
to the logical scheduler; they must not be presented as physical request
starts.

## Compatibility and risks

- `RunRpcJob`, `scheduleRpc`, `sharedRpcScheduler`, `withRpcJobOptions` and
  existing unprefixed metric fields remain source-compatible.
- `configureRpcPacer` only gains optional test/advanced active-cap arguments;
  existing worker calls are unchanged. `WORKER_MAX_ACTIVE_RPC_REQUESTS` is an
  optional transport-specific environment cap and falls back to
  `WORKER_MAX_ACTIVE_RPC_JOBS`.
- Closing the shared runtime is now cancellation-oriented rather than draining
  queued logical work. Worker shutdown already closes engines before this
  function, so the normal path should be idle; residual work fails explicitly
  instead of extending shutdown indefinitely.
- The compatibility middleware cannot observe response completion due to the
  upstream callback-only type. Start pacing and pre-start errors are covered;
  exact response lifecycle metrics come from production `sharedRpcFetch`.
- Short tests use the real monotonic `performance.now()` with a 1 ms tolerance;
  no wall clock is used for pacing.

## BLOCKED / not run

- The environment is Node **26.7.0**, while the manifest declares Node
  `>=24 <25`; Node 24 execution is **not verified**.
- A clean `npm ci`, real RPC/vendor behavior and the specification's long live
  soak were not run.
- The full cross-language integration gate belongs to Agent G. Python tests
  were not rerun because Agent C did not edit Python.

## Dirty baseline and change boundary

At start the worktree already contained the three user deletions, audit/archive
artifacts, Agent A's Python changes/tests/result and Agent B's analyzer
changes/tests/result. All were preserved. Intentional Agent C files are only:

- `workers/solana-quote-worker/src/rpcPacer.ts`;
- `workers/solana-quote-worker/test/rpcScheduler.test.ts`;
- `workers/solana-quote-worker/test/remainingCRpcPacing.test.ts`;
- `remaining-bugs-agent-plan/results/AGENT_C_RESULT.md`.

No commit, push, reset/clean, dependency installation, root action, network
request or live trade was performed.
