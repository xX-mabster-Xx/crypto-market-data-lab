# Agent D result — Solana dependency reconciliation and refresh fairness

Date: 2026-09-17  
Agent: D  
Baseline commit: `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`  
Requested runtime profile: OpenAI GPT-5.6 Sol, high reasoning (work was performed in the current Codex session).

## Scope and status

This result covers the Agent D assignment in
`remaining-bugs-agent-plan/04_AGENT_D_SOLANA_ENGINES.md`.

| Audit ID | Status in D scope | Evidence |
| --- | --- | --- |
| R-03 | **FIXED** | Meteora and Orca now reconcile context-bearing RPC snapshots against per-account WS versions; both production-engine regressions pass. |
| R-04 | **FIXED** | Delayed CPMM core snapshot advances core `100 -> 105` without rolling config/vault versions `120` back; real layout, quote, simulation and immutable-capture assertions pass. |
| O-02 | **FIXED** | All four engines use a rotating cursor. In the deterministic 100-pool/600-tick test every pool is serviced and service-count spread is at most one. |
| O-03 | **FIXED** | Newer identical dependency bytes advance factual validation slots without advancing semantic generation or emitting state. |

Umbrella status:

- **BUG-019, D residual:** fixed and stress-tested. Existing bounded debounce remains intact.
- **BUG-020, D residual:** R-03/R-04 fixed. The umbrella issue is still **PARTIAL** until Agent E closes snapshot/evidence item R-05 and the integration gate runs.
- **BUG-023, D residual:** O-02/O-03 fixed. Final project-wide status belongs to the later integration/soak gate.
- **BUG-017:** not changed; the bounded latest-only mailbox regression remains green.

No commit, push, live trading or dependency upgrade was performed.

## Dirty baseline and ownership

The worktree already contained unrelated work when D started:

- user deletions: `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`,
  `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`,
  `bug-002-keyed-coalescing-event-bus.patch`;
- Agent A/B Python changes and tests;
- Agent C `rpcPacer.ts`, scheduler tests and result;
- the audit, task-plan directory and archive.

Those changes were preserved. D intentionally changed only:

- `workers/solana-quote-worker/src/engineRuntime.ts`
- `workers/solana-quote-worker/src/meteoraDlmm.ts`
- `workers/solana-quote-worker/src/orcaWhirlpool.ts`
- `workers/solana-quote-worker/src/raydiumClmm.ts`
- `workers/solana-quote-worker/src/raydiumStandard.ts`
- `workers/solana-quote-worker/test/engineRuntime.test.ts`
- `workers/solana-quote-worker/test/remainingDSolanaEngines.test.ts` (new)
- this result file

`worker.ts`, `rpcPacer.ts`, snapshot codecs, protocol schema, Python code and
package metadata were not edited by D.

## Red -> green evidence

The initial four production-path regressions were run before the fixes:

```text
tests 4; pass 0; fail 4

Meteora: cached value remained 'old' instead of 'rpc-bin'
Orca: old refresh path failed before producing a repaired tick
CPMM: delayed snapshot changed trade_fee_rate '1000' -> '2000'
fairness: Meteora serviced 20/100 pools; 80 were starved
```

The exact fairness probe used the real `maintainStalePools()` selection path,
100 pool records, default 15 s age + 5 s deterministic stagger, and 600
one-second maintenance ticks. Only the IO completion was stubbed.

After the implementation, the expanded D regression file reports:

```text
tests 14; pass 14; fail 0
```

It now covers both dependency engines, CPMM, all four maintenance engines,
listener-removal failure/retry, and the 100,000-callback stress case.

## What changed

### R-03 and O-03 — version-aware dependency reconciliation

Meteora and Orca now:

1. use SDK discovery only to determine the nearby account addresses;
2. subscribe before the authoritative read, with a membership guard on every
   callback;
3. read all discovered accounts through
   `getMultipleAccountsInfoAndContext(..., "processed")`;
4. decode and validate the complete vector into temporary structures;
5. verify that the core revision did not change across discovery/read;
6. commit the whole dependency set synchronously;
7. preserve a WS value whose per-account slot is newer than or equal to the
   RPC context slot (same slot is deterministic first-writer-wins);
8. let a newer RPC value repair an existing subscribed cache entry after a
   missed WS notification.

Each account keeps raw `Buffer` bytes. Equality is per account, so one update
does not serialize or fingerprint the entire tick/bin set. A newer validation
with identical bytes advances the factual account slot and validation time but
does not advance `dependency_generation` or request an external state event.
Add/remove and changed bytes do advance the semantic generation.

`bin_cache_age_ms` / `tick_cache_age_ms` for these two engines now measure the
last successful full context-bearing dependency snapshot. A single WS account
update no longer falsely marks the whole set as fully revalidated; its own
arrival remains represented in dependency provenance.

Account-set removal deletes data and provenance atomically. Failed listener
removal leaves an inert, membership-guarded ID in a retry set. A later refresh
retries cleanup and refuses to create a duplicate listener while cleanup is
still failing. Runtime stats expose `retired_dependency_subscriptions`.

### R-04 — staged CPMM core/config/vault commit

Raydium standard/CPMM now stores immutable copies of raw bytes and account
owners for core, both vaults and config. RPC refresh:

- validates owners and decodes accepted versions before mutation;
- compares each dependency against its own slot;
- permits a newer core snapshot while retaining newer config/vault bytes;
- never mutates a dependency whose version was rejected, and completes all
  potentially throwing decode/copy work before the synchronous commit;
- makes equal-slot duplicates first-writer-wins;
- does not advance provenance on malformed data.

The same policy is used by both awaited RPC refresh and the debounced WS
core+vault commit; the latter has a dedicated regression for core 105 with
newer vault versions 120.

The selected consistency policy is an honest mixed-version capture: core may
be slot 105 while config/vaults remain slot 120. `core_state_slot` stays 105,
dependency summaries stay 120, and quote/simulation use exactly the bytes
described by those versions.

The new `cpmmSimulationCapture(poolId)` handoff returns a frozen state object,
frozen freshness snapshot and frozen account evidence records containing exact
address, role, owner, slot and copied base64 bytes. This is the engine-side API
for Agent E; D did not alter snapshot serialization or claim R-05 complete.

### O-02 — starvation-free maintenance

`FairMaintenanceCursor` performs deterministic round-robin selection over the
current keyed pool collection. The cursor advances before awaiting IO, so a
slow or failing pool cannot monopolize later cycles. Fresh and in-flight pools
are skipped. The existing one-pool-per-engine-per-tick behavior, deterministic
stagger and Agent C low-priority/coalesced RPC path remain unchanged; no
refresh-all burst was introduced.

All four engines now expose:

- `maintenance_selected_total`
- `maintenance_due_pool_count`
- `maintenance_maximum_overdue_ms`

These metrics make overload/backlog visible without promising an impossible
freshness SLA when demand exceeds RPC capacity.

## Interface and compatibility notes

New engine-runtime API:

- `PoolSlotProvenance.acceptDependencyVersion(key, slot, semanticChange, time)`
- `PoolSlotProvenance.removeDependency(...)`
- `PoolSlotProvenance.dependencyVersions()`
- `PoolSlotProvenance.freshnessSnapshot()`
- `FairMaintenanceCursor`
- `coreRefreshOverdueMs(...)`

Contract for Agent A:

- dependency generation is monotonic within a pool/worker epoch;
- it changes only for semantic content/add/remove, not identical-byte
  revalidation;
- constructing the next pool/worker epoch resets it naturally;
- core slot and dependency slots remain independent.

Contract for Agent E:

- `cpmmSimulationCapture()` is a coherent synchronous copy, not shared mutable
  SDK state;
- `freshnessSnapshot()` timestamps are monotonic values meaningful only inside
  this process/boot;
- cross-process protocols must transmit age/remaining lifetime, not compare
  raw monotonic timestamps;
- summary slots must not be invented into account evidence; the capture's
  account records contain the real per-account versions.

Backward compatibility retained:

- legacy `slot` / `state_slot` remains the core-state slot, never the max
  dependency slot;
- existing `refreshAllPoolStates()` remains as the stale-driven compatibility
  hook;
- `RunRpcJob`, scheduler/coalescing keys and public worker protocol were not
  changed;
- quote and simulation arithmetic were not changed.

## Tests executed

Environment actually used:

```text
Node v26.7.0
npm 12.0.2
```

Commands and results:

```text
npm run check
  PASS — tsc --noEmit

node --import tsx test/remainingDSolanaEngines.test.ts
  PASS — 14/14
  Includes 100,000 callbacks per engine family (200,000 total) across
  10 Meteora + 10 Orca pool fixtures;
  one pending emit per pool, exact final bytes/slot/generation,
  >=99,000 coalesced updates per engine family.

node --import tsx test/engineRuntime.test.ts
  PASS — 12/12
  Includes the existing 100,000-update latest-only core mailbox regression.

npm test
  PASS — 14/14 test files, 0 failures
  Includes existing CLMM/DLMM/AMM-v4, Orca simulation, CPMM,
  snapshot bundle/parity, worker-handler, output and scheduler suites.

git diff --check
  PASS
```

The D matrix specifically verifies:

- repeated unchanged refresh;
- newer RPC repair of a subscribed stale entry;
- older RPC after newer WS and equal-slot duplicate policy;
- missing account and decode failure without false freshness;
- dependency membership add/remove and listener cleanup/retry;
- core revision changing during awaited dependency read;
- real quote/simulation input uses repaired dependency state;
- pending dependency debounce subsumed by immediate core update in both
  Meteora and Orca;
- shutdown cancels pending timers and removes subscriptions;
- CPMM delayed/fresh/malformed/equal-slot snapshots with real Raydium layouts;
- immutable raw account capture and correct fee quote;
- 100 pools / 600 ticks across all four engines;
- failure, slow/in-flight pool, fresh pool, and pool add/remove around cursor;
- runtime backlog metrics and 100,000 dependency callbacks.

## NOT RUN / remaining risk

- **Node 24: NOT RUN.** `package.json` requires `>=24 <25`, but the available
  runtime is Node 26.7.0. TypeScript and all tests passed on Node 26; this is not
  a substitute for the required Node 24 gate.
- **Live Solana RPC/WS: NOT RUN.** Tests are intentionally offline and use real
  production algorithms with IO/clock/SDK boundaries stubbed. No endpoint,
  secret or trading account was used.
- **Long soak / heap profile: NOT RUN.** The deterministic 100,000-update
  stress passed, but no 30–60 minute worker soak or live reconnect test was
  performed; that belongs to the later integration/observability gate.
- **Full Python/project gate: NOT RUN by D.** D changed no Python files. Agent G
  owns the final combined suite after E/F complete.
- SDK helpers are still used to discover the relevant bin/tick addresses, but
  all bytes committed to the cache are independently fetched with an RPC
  context and decoded by the real production decoder. A core-revision race
  causes a controlled retry instead of a mixed commit.
