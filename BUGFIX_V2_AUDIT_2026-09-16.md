# Bugfix v2 verification and additional bug audit

Date: 2026-09-16  
Reference: `crypto-market-data-lab_bugfix_spec_v2.md`  
Audited commit: `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`  
Mode: read-only code audit and offline checks; **no fixes applied**.

## 1. Conclusion

**Not all previously reported bugs have been fixed.** Of the 24 specification items:

- **16:** implementation and checked regressions support the fix; no remaining instance of the original defect was found in the inspected scope.
- **7:** partially fixed — BUG-002, BUG-003, BUG-014, BUG-018, BUG-019, BUG-020, BUG-023.
- **1:** required feature substantially missing — BUG-024 (worker memory/backlog observability).

This is not a claim that the 16 items passed every acceptance requirement in the specification. In particular, the mandatory long-running live/soak gate and a clean installation under the declared Node runtime were not verified.

The findings below contain **12 distinct issues**, grouped as requested:

1. **6 correctness issues:** event-delivery corruption, lost dependency updates, stale dependency caches, fee-config rollback, incomplete/misleading snapshot provenance, and unrelated candidate lifecycle invalidation.
2. **6 operational issues:** request pacing bypass, refresh starvation, unchanged-state churn, state-store scaling, inconsistent runtime metadata, and missing observability.

Several are newly identified failure modes in or around the fixes, not simple persistence of the exact original implementation. The mapping in each finding distinguishes this.

## 2. Method, evidence, and limits

### Checks executed

| Check | Result |
| --- | --- |
| `.venv/bin/pytest -q -p no:cacheprovider` from repository root | **486 passed, 6 subtests passed**, 10.80 s |
| `npm run check` in `workers/solana-quote-worker` | TypeScript check passed |
| `npm test` in that directory | All **12 test files** passed |
| Additional offline probes calling production Python/TypeScript classes and methods | Reproduced R-01, R-02, R-03, R-05, O-01, O-02, O-03 as detailed below |

The Node checks ran with the already installed dependencies and **Node 26.7.0**, whereas the manifest declares `>=24 <25`. They therefore do not certify the supported runtime.

Evidence labels:

- **Reproduced:** an offline execution exercised the relevant production implementation. Network/SDK results were stubbed where specified; no live RPC was used.
- **Code-confirmed:** a concrete reachable state transition follows from inspected code, but was not executed end-to-end in this audit.
- **Not verified:** insufficient execution evidence; not automatically treated as an additional bug.

No dependencies were installed, live scanner was launched, network stress was generated, or source/test fixes were made. No commit or push was performed. The pre-existing deletions of `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, and `bug-002-keyed-coalescing-event-bus.patch` were left untouched. This report is the only intentional new file.

References use repository-relative paths and line numbers at the audited revision. A passing test means its assertions passed, not that the complete production pipeline is correct.

## 3. Verification matrix: all original specification items

“Supported” means the fix is present and supported by inspected code/passing checks, subject to Section 2. “Partial” means a concrete residual defect or regression prevents sign-off.

| Spec ID | Subject | Verdict | Evidence / remaining issue |
| --- | --- | --- | --- |
| BUG-001 | Dynamic-notional state cardinality | Supported | Stable `quote_slot_id`, requested-slot mapping, bounded unmapped diagnostics, analyzer replacement. `tests/test_bugfix_v2_selected.py` exercises 10,000 changing notionals and direction separation. This is not the full live/soak gate. |
| BUG-002 | Keyed coalescing event bus | **Partial** | FIFO eviction replaced, but concurrent backpressured same-key publishers corrupt the queue. **R-01**. |
| BUG-003 | Analyzer source-epoch invalidation | **Partial** | Old source caches are purged; late/future epoch tests pass in `tests/test_agent_c_bugfixes.py`. However, unrelated candidates are also closed. **R-06**. |
| BUG-004 | Hidden CEX reconnects | Supported | Supervisor owns replacement sessions; dead receiver/shard failure surfaces; transport/depth state resets. Production-source tests in `tests/test_agent_c_bugfixes.py`. |
| BUG-005 | State retention and capacity | Supported for boundedness | Physical idle retirement and hard key capacity are present; retention/capacity/index tests pass in `tests/test_realtime_scanner.py` and `tests/test_bugfix_v2_selected.py`. New scaling regression **O-04** is not a remaining unbounded-memory claim. |
| BUG-006 | Backoff reset after stable operation | Supported | Stable-run reset uses monotonic duration; `test_stable_run_resets_retry_backoff` passes in `tests/test_realtime_scanner.py`. |
| BUG-007 | Health before state acceptance | Supported | Received/accepted accounting separated; acceptance drives accepted-state health. `test_health_counters_only_advance_on_accepted_state` passes. |
| BUG-008 | QuoteBroker mismatch accounting | Supported | Remote/local mismatch results pass through failure accounting/gating. Both mismatch tests in `tests/test_quote_broker.py` pass. Inspected the actually imported `quote_broker` package implementation, not only the sibling legacy module. |
| BUG-009 | Provider pacing at request level | Supported in checked Python paths | Inspected Uniswap batching, Raydium/Jupiter and TON provider request paths and removal of redundant polling-level waits. Distinct Node request-pacing residual is **O-01 / BUG-018**. Live vendor timing/reconnect behavior not verified. |
| BUG-010 | Wall-clock lifecycle durations | Supported | Candidate lifecycle uses monotonic clocks; the three regressions in `tests/test_bug_010_monotonic_candidate_lifecycle.py` pass. This verdict does not certify every Node cache-age clock. |
| BUG-011 | Numeric validation | Supported | Finite values, bool rejection and exact integer validation covered by `tests/test_config_validation.py`, including NaN, infinities and fractional values. |
| BUG-012 | Decimal canonicalization | Supported | Shared finite fixed-point canonicalization; tests cover representation equivalence and rejection cases in `tests/test_bugfix_v2_selected.py`. |
| BUG-013 | Provider Protocol indentation | Supported | `config()` is a Protocol member at the correct indentation. No independent full-project Python static type-check gate was executed. |
| BUG-014 | Node engine/dependency reproducibility | **Partial** | `package.json` pins versions and Node 24; lockfile root metadata still has `latest`/ranges and Node >=22; snapshot SDK metadata also says `latest`. **O-05** and provenance aspect of **R-05**. |
| BUG-015 | Empty remote inflight-dedup test | Supported | `test_t25_ten_consumers_share_one_inflight_request` now launches ten consumers, checks one remote execution/shared result, then checks cache reuse. |
| BUG-016 | stdout backpressure | Supported at writer level | Unified bounded writer, control/state distinction, drain handling, fairness and overflow behavior present. `outputWriter.test.ts` includes 100,000 updates over 20 keys, blocked stdout and shutdown checks. Live pipe/RSS soak not verified. |
| BUG-017 | Unbounded CLMM Promise chain | Supported for mailbox boundedness | Production path uses a one-active/one-latest mailbox. `engineRuntime.test.ts` covers 100,000 updates, failure and shutdown on the helper. Production wiring assertions include source-text checks, not full live-engine coverage. |
| BUG-018 | Bounded RPC scheduler and pacing | **Partial** | Bounded queues/priorities/coalescing implemented and scheduler tests pass; nested physical requests bypass start pacing. **O-01**. |
| BUG-019 | Dependency emission churn | **Partial** | Debouncing and state coalescing implemented, but refresh increments generation and emits even for unchanged cached state. **O-03**. Dependency refresh correctness also fails in **R-03**. |
| BUG-020 | Core/dependency provenance | **Partial** | Separate provenance fields exist, but Python rejects dependency-only events, refresh can roll config backward, and snapshot export loses dependency evidence. **R-02, R-04, R-05**. |
| BUG-021 | Compact CEX events vs route evaluator | Supported | Explicit full-depth resolver, epoch invalidation and compact generic storage present. Production-builder integration with fake quote provider passes in `tests/test_agent_c_bugfixes.py`. |
| BUG-022 | Manifest vs evaluator wiring | Supported | Production builder connects evaluator, callbacks/status/shutdown; enabled-but-unavailable fail-fast and disabled-mode tests pass. Entry point enables intended routes. Live worker execution not verified. |
| BUG-023 | Refresh bursts and unchanged emits | **Partial** | Stale-driven selection and staggering exist, but fixed-order first-due selection can starve most pools; unchanged dependency refreshes still notify. **O-02, O-03**. |
| BUG-024 | Worker observability | **Missing required implementation** | Some metrics piggyback on refresh health, but required memory statistics, complete worker-stats/status integration and diagnostic coverage are absent. **O-06**. |

## 4. Category 1 — logic, correctness, or factual accuracy

### R-01 — Concurrent same-key publishers can corrupt the event bus

- **Priority:** High. **Evidence:** Reproduced.
- **Relationship:** BUG-002 incompletely fixed; new concurrency failure in the replacement bus.
- **Location:** `src/market_data_lab/realtime_scanner.py:499–525`.
- **Cause:** `publish()` checks whether a key is already pending before `Condition.wait()`, but does not repeat that check after waking. Two waiting publishers for the same key can both append it to `_pending_order`, while `_pending_latest` contains only one entry.
- **Offline reproduction:** Capacity 2; publish A and C; create concurrent tasks publishing B versions 3 and 4; let both block; consume A and C without yielding between consumptions; await both publishers. The observed order was `['B', 'B']`, while the payload dictionary held only `['B']`. Consume B (version 4), publish D, then call `next_event()` again: **`KeyError('B')`**.
- **Impact:** The consumer can crash despite valid input, interrupting analysis and losing reliable delivery of accepted market-state changes. The invariant “at most one queued position per pending key” is broken.
- **Missing regression:** Multiple waiting publishers for one key, not only a slow consumer or a single hot publisher.

### R-02 — Python drops dependency-only Solana state updates

- **Priority:** High. **Evidence:** Reproduced across the production source/store boundary.
- **Relationship:** BUG-020 incomplete; integration failure introduced/exposed by separating core and dependency slots.
- **Locations:** `src/market_data_lab/solana_realtime_scanner.py:1278–1401`; `src/market_data_lab/versioned_market_state.py:228–253`.
- **Cause:** Worker events correctly keep the same core `slot` when only ticks/bins/config change. Python assigns `chain_position=slot`; the envelope uses this as the source sequence. The versioned store rejects an equal sequence without considering `dependency_generation`.
- **Offline reproduction:** Drive `RaydiumLocalQuoteStateSource.run()` with the existing fake worker and publish into a real `RollingStateStore`. Send `(core=100, dependency_max=110, generation=1)`, followed by `(core=100, dependency_max=120, generation=2)`. Admission was **`[True, False]`**; retained generation remained **1**.
- **Impact:** The generic store, accepted-state health and downstream event-driven consumers miss a real dependency change. A worker’s internal quote cache may be newer than Python's view; this is not a claim that every direct worker quote is stale.
- **Missing regression:** Existing worker-contract tests collect a single event and inspect its fields. They do not send two dependency generations through real state admission and analyzer delivery.

### R-03 — Previously pushed dependencies cannot be reconciled by RPC refresh

- **Priority:** High. **Evidence:** Meteora reproduced; equivalent Orca behavior confirmed by code.
- **Relationship:** Additional stale-cache failure in the BUG-019/020 changes.
- **Locations:** `workers/solana-quote-worker/src/meteoraDlmm.ts:348–395`; `workers/solana-quote-worker/src/orcaWhirlpool.ts:472–489`.
- **Cause:** Any historical dependency slot causes refreshed SDK data to be replaced with the existing WS-cached value. The test is “has ever received WS provenance,” not “a newer WS update raced this refresh.” Cache timestamps are nevertheless reset and dependency generation advances.
- **Offline reproduction:** Use production `refreshBins()` with one already subscribed bin at dependency slot 110 and cached value `old`; stub the two SDK reads to return `new-rpc`. No new subscription or live network is needed. Two refreshes leave the bin **`old`**, advance generation from **1 to 3**, and request **two notifications**.
- **Impact:** If a notification is missed or a dependency subscription becomes stale, repeated refresh cannot repair that existing entry while it stays subscribed. Old contents can continue to be advertised with a recently reset cache age. Quotes that use those bins/ticks can be wrong.
- **Important distinction:** Blindly replacing slot-known data with contextless data would also be unsafe. The finding is that the current refresh does not validate/reconcile it yet marks it refreshed; it is not a recommendation to remove the guard without provenance handling.

### R-04 — CPMM refresh can overwrite a newer fee config with older data

- **Priority:** High. **Evidence:** Code-confirmed; no full binary-account regression executed.
- **Relationship:** Additional rollback failure under BUG-020.
- **Locations:** `workers/solana-quote-worker/src/raydiumStandard.ts:498–506`, `:724–757`; `src/engineRuntime.ts:49–60` within the same worker directory.
- **Trigger:** Core slot is 100; a config WS update is accepted at slot 120; a refresh response has context slot 105. This can occur when an earlier RPC result completes after the WS update or backend views differ.
- **Cause:** Refresh accepts the response because 105 > core 100, then assigns `pool.config = ...` before calling `acceptDependency("config", 105)`. That call rejects the lower slot, but its result is ignored and the config bytes have already been replaced.
- **Impact:** Quotes/simulation use the older fee configuration while dependency provenance still reports slot 120. Both calculated amounts and their provenance can be incorrect.
- **Needed regression:** Interleave a newer config update with completion of an older core/vault refresh; verify both config data and its recorded version remain consistent.

### R-05 — Snapshot export discards dependency provenance and overstates validation

- **Priority:** High for simulation/evidence consumers. **Evidence:** Builder reproduced; request freshness path code-confirmed.
- **Relationship:** BUG-020 propagation remains incomplete; additional simulation/evidence issue.
- **Locations:** `workers/solana-quote-worker/src/raydiumStandard.ts:212–241`; `workers/solana-quote-worker/src/simulation/snapshots.ts:239–294`; `workers/solana-quote-worker/src/worker.ts:848–887`.
- **Cause:** Live CPMM simulation state includes separate provenance fields, but the bundle builder does not carry them into the snapshot. `dependency_vector` is empty, `context_slot` is just the maximum core slot, and `chain_consistency` is unconditionally `validated_multi_account_snapshot`. Snapshot capture has no state-age check and grants a new token TTL based on capture time.
- **Offline reproduction:** Call the real builder with `slot=100`, `core_state_slot=100`, dependency min/max 110/120 and generation 7. Output: **`context_slot=100`**, **`dependency_vector=[]`**, **no corresponding summary fields**, **`chain_consistency="validated_multi_account_snapshot"`**. SDK metadata is also `@raydium-io/raydium-sdk-v2: latest`, despite an exact package pin.
- **Impact:** An evidence consumer cannot establish which dependency versions produced a simulated result from the bundle. The handler can capture old cached state as a newly valid token without establishing current market-state freshness. Fresh token age is not proof of fresh account data.
- **Scope qualification:** A missing full account-version vector must not be fabricated from summary slots. The issue is the loss of available provenance and unconditional validation/freshness implication, not a requirement that all independently updated accounts share one slot.

### R-06 — Any source epoch change closes unrelated candidates

- **Priority:** Medium. **Evidence:** Code-confirmed.
- **Relationship:** BUG-003's targeted invalidation works, but adds an overbroad lifecycle regression.
- **Locations:** `src/market_data_lab/unified_cycle_analyzer.py:238–267`; `src/market_data_lab/unified_perp_analyzer.py:741–815`; broadcast in `src/market_data_lab/unified_market_data.py:433–441`.
- **Cause:** Quote/leg removal filters by source and epoch, but both analyzers subsequently clear all dirty sets and close every active candidate, even when that source invalidated zero contributing legs. The production builder broadcasts source transitions to both analyzers.
- **Trigger:** A candidate depends only on healthy source A; unrelated source B starts/reconnects and advances its epoch.
- **Impact:** A valid candidate is recorded as closed with reason `source_epoch_advanced`; its persistence interval is interrupted, and later work may reopen it as a new candidate. Counts, durations and candidate history become misleading. This is more than unnecessary recomputation.
- **Missing regression:** An unrelated source transition must preserve an existing candidate and its scheduled evaluation while still purging genuinely affected legs.

## 5. Category 2 — efficiency, reliability, reproducibility, or convenience

These findings concern operation rather than an independently demonstrated wrong numerical result. If starvation or overload subsequently allows stale data into a result, that downstream correctness failure needs its own evidence; it is not assumed here.

### O-01 — A scheduled RPC job bypasses pacing for all nested fetches

- **Priority:** High. **Evidence:** Reproduced using the real shared scheduler/fetch adapter and a network stub.
- **Relationship:** BUG-018 partial; related to BUG-009's per-actual-request invariant.
- **Locations:** `workers/solana-quote-worker/src/rpcPacer.ts:385–399`, `:532–537`; engine-level scheduled SDK work in `workers/solana-quote-worker/src/raydiumClmm.ts:555–564`.
- **Cause:** `scheduledRpcStart.run(true, job.fn)` marks the entire asynchronous job as already paced. `sharedRpcFetch` bypasses the scheduler for every fetch in that async context, rather than accounting for each physical request. An SDK operation with multiple reads or a transport retry can therefore have more request starts than scheduler starts.
- **Offline reproduction:** Set minimum interval to 200 ms; run one scheduled job that awaits `sharedRpcFetch` twice. Stub `globalThis.fetch` to record starts and return locally. Observed **two request starts 15.60 ms apart**, but **one scheduler start**. The exact small gap is machine-dependent; being below 200 ms is the violation.
- **Impact:** Configured request-start budgets can be exceeded, increasing throttling/retry pressure; metrics undercount actual requests. Bounded logical job count is not a per-HTTP-request pacing guarantee.
- **Scope:** No claim about an exact live provider quota or the number of requests in every SDK method. The real adapter's bypass behavior was directly reproduced.

### O-02 — Fixed-order stale selection can starve later pools indefinitely

- **Priority:** High. **Evidence:** Reproduced with production selection logic and simulated successful refresh completion.
- **Relationship:** BUG-023 partial.
- **Locations:** `workers/solana-quote-worker/src/meteoraDlmm.ts:192–202`; equivalent first-due iteration in the other three engines; sequential worker maintenance at `workers/solana-quote-worker/src/worker.ts:398–412`.
- **Cause:** Every maintenance tick scans insertion order, refreshes the first due pool, and returns. It does not rotate or select the oldest overdue entry. With enough pools, early entries become due again before late entries are reached.
- **Offline reproduction:** Populate the real Meteora engine with 100 minimal pool records, each `PoolSlotProvenance(1, 0)`. Keep default 15,000 ms refresh age and 5,000 ms staggering. Replace only the actual refresh/IO completion with `noteRpcRefresh(now)`. Call `maintainStalePools(now)` every simulated second for 600 ticks from 20,000 ms. **20 pools refreshed; 80 never refreshed; the first pool refreshed 32 times.**
- **Impact:** Maintenance coverage is not guaranteed, even with instantaneous successful RPC. Real sequential RPC delays can reduce coverage further. Jitter alone is not fairness.
- **Missing regression:** Track service/maximum overdue age across 100 pools over many maintenance cycles, not just distinct hash offsets or one “fresh pool skipped” check.

### O-03 — Unchanged dependency refresh still advances generation and emits

- **Priority:** Medium. **Evidence:** Reproduced for Meteora; code-equivalent Orca path.
- **Relationship:** BUG-019 and BUG-023 partial.
- **Locations:** `workers/solana-quote-worker/src/meteoraDlmm.ts:385–395`; `workers/solana-quote-worker/src/orcaWhirlpool.ts:478–489`.
- **Cause:** Whenever a prior cache exists, refresh unconditionally increments `dependencyGeneration` and requests state emission. Since the emission key includes that generation, semantic deduplication treats the unchanged state as new.
- **Offline result:** The R-03 probe retained identical bin objects yet produced two generation increments and two notification requests across two refresh calls.
- **Impact:** Avoidable serialization, stdout traffic, Python ingestion and downstream evaluation continue. Debounce bounds a burst but does not suppress unchanged periodic work.
- **Classification:** The stale-content/freshness problem from the same path is separately recorded in R-03. This finding concerns the unnecessary work itself.

### O-04 — Normal state admission scans the entire store

- **Priority:** Medium. **Evidence:** Code-confirmed complexity, not a measured throughput regression.
- **Relationship:** Additional performance regression in BUG-005's retirement/index cleanup.
- **Location:** `src/market_data_lab/realtime_scanner.py:260–290`, `:318–341`.
- **Cause:** Every accepted event calls `_retire_state_key(..., keep_event_key=...)`. That helper copies/scans the complete `_state_key_by_event_key` map, copies/scans all `_latest` entries, and can scan index values again. This happens even for a normal update to an unchanged key.
- **Impact:** Admission is O(K) in live key count, with O(K) temporary allocations per tick, rather than a key-local operation. Invalidating K keys one at a time adds up to O(K²) scans. Hard memory bounds do not prevent CPU/event-loop stalls during busy streams or reconnects.
- **Missing regression:** Scaling across store sizes and epoch retirement cost. Existing bounded-cardinality/index-consistency tests do not measure this cost.

### O-05 — Runtime and dependency metadata remain inconsistent

- **Priority:** Medium. **Evidence:** Code/config-confirmed; clean install not executed.
- **Relationship:** BUG-014 partial.
- **Locations:** `workers/solana-quote-worker/package.json:7–34`; `workers/solana-quote-worker/package-lock.json:7–29`; `workers/solana-quote-worker/src/simulation/snapshots.ts:292`.
- **Facts:** The manifest declares Node `>=24 <25` and exact dependency versions. The root lockfile package still declares Node `>=22` and several `latest`/range specifications. Snapshot SDK metadata still reports `latest`. The audit environment used Node 26.7.0.
- **Impact:** Installation/runtime support and reproducibility claims are not internally consistent, and the successful local test run did not exercise the declared Node version. Exported SDK labels cannot identify the exact implementation used.
- **Important limit:** Stale root lockfile metadata alone does **not** prove that `npm ci` fails or that resolved dependencies are unpinned. No such failure is asserted. A clean Node 24 installation remains an unverified acceptance gate.

### O-06 — Required worker memory statistics and usable status integration are absent

- **Priority:** Medium. **Evidence:** Source inspection and negative symbol search.
- **Relationship:** BUG-024 not implemented to the specified contract.
- **Locations:** Specification §29; `workers/solana-quote-worker/src/worker.ts:414–442`; `src/market_data_lab/solana_realtime_scanner.py:1236–1269`.
- **Facts:** Worker refresh-health messages contain engine, output and RPC metrics, so observability is not completely absent. However, source searches find no `process.memoryUsage()` / `worker_stats` implementation in the production worker. Required RSS, heap, external/array-buffer memory and the full compact statistics contract are missing. Python persists only the compact refresh-health summary, which omits the nested queue/engine metrics; it does not expose the requested latest worker-stats object and complete status fields.
- **Impact:** Operators cannot use the specified status surface to distinguish heap/buffer growth, blocked stdout and queue pressure, or demonstrate a bounded-memory plateau. Required warning/contract tests are not present as a complete implementation.
- **Classification:** This is missing operational instrumentation, not evidence that a memory leak is already happening.

## 6. Why the green test suite did not catch these issues

1. The bus tests do not cover two same-key producers both sleeping on capacity and waking together.
2. The Python worker-contract test verifies fields on a collected event, not admission of successive dependency generations into the production store.
3. Several `engineRuntime.test.ts` tests exercise small runtime helpers; production integration checks also use source-text assertions. They do not test real refresh-cache reconciliation or the 100-pool maintenance selection loop.
4. Scheduler tests validate scheduler jobs. The missing boundary is several physical fetches inside one scheduled SDK/job context.
5. Generation-based notification deduplication works only if generation changes represent actual state changes. The helper cannot detect unconditional increments by callers.
6. Existing source-epoch tests verify affected-source purging but do not preserve/check an unrelated active candidate's lifecycle.

These are coverage gaps explaining the findings, not a claim that the existing passing tests are useless.

## 7. Acceptance evidence still outstanding

The following were **not performed or certified** during this audit:

- A clean `npm ci` and all worker checks under Node 24 in a fresh environment.
- The specification's mandatory 30–60 minute live/soak run with RSS/heap/external memory, queue high-water marks, refresh latency and CPU measurements.
- Live RPC/WS fault injection, dropped notifications, actual vendor throttling and reconnect recovery.
- Full production-engine dependency storms and quote interleavings for every Solana protocol; helper tests alone do not establish this.
- An independent complete Python static type-check gate.

The report should therefore be read as a concrete audit with verified counterexamples, **not** an exhaustive proof that no other bugs exist. The correctness findings block an “all bugs fixed” conclusion regardless of whether the unperformed gates later pass.

## 8. Change boundary

Only this report was added. Application code, tests, configuration, dependencies, git history, and the user's pre-existing deletions were not changed. The reproduction checks used process-local stubs and did not send live market requests.
