# Agent A result — Python event plane and state admission

Date: 2026-09-16  
Agent: A  
Requested model: OpenAI GPT-5.6 Sol, high reasoning (execution performed in the current Codex session).  
Baseline commit: `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`.

## Scope

Implemented the Agent A assignment for audit findings **R-01**, **R-02** and
**O-04**, covering the residual Python-side work in **BUG-002**, **BUG-005**
and **BUG-020**. The implementation is limited to the event bus, state
admission/version contract, pool-state normalization and Agent A regressions.
Analyzer changes already made by Agent B were preserved and not edited.

## Red → green evidence

Before production changes, the initial three regressions were run together:

```text
.venv/bin/pytest -q -p no:cacheprovider tests/test_remaining_a_event_state.py
FFF
3 failed in 0.15s
```

The failures were the audited behaviors:

- concurrent `B@3` / `B@4` publishers produced `_pending_order == ['B', 'B']`;
- `(core=100, dependency_generation=1)` followed by `(100, 2)` produced
  admissions `[True, False]`;
- one hot update with ten unrelated keys counted 20 full-map entries scanned.

After the fixes and completion of the acceptance matrix, the same regression
file passes **15 tests and 5 subtests**. The first test also consumes the
post-race key and then a new distinct key, so the historical `KeyError('B')`
path is exercised rather than inferred from source text.

## Changes

### `src/market_data_lab/realtime_scanner.py`

- `CoalescingEventBus.publish()` now re-evaluates key presence and the store's
  latest accepted payload after every condition wakeup. A stale waiter can no
  longer append a duplicate queue position or overwrite a newer accepted
  value.
- Added explicit current `waiting_publishers`, idempotent async `close()`, and
  `EventBusClosed`. Close wakes saturated publishers and empty-queue consumers;
  `RealtimeScanner.run()` closes the bus during shutdown before cancelling
  tasks.
- Cancellation of either the older or newer same-key waiter leaves the queue
  consistent. If the newer waiter is cancelled after store admission, the
  surviving older waiter queues the store's newer value, not its own stale
  payload.
- Added `MarketEvent.source_revision` and projects it into the versioned
  envelope while retaining `chain_position` unchanged as the real core slot.
- Added the reverse index `state_key -> event keys`. Retirement, replacement
  and epoch cleanup now touch only entries mapped to the affected state key;
  normal same-key admission no longer scans/copies all keys.

### `src/market_data_lab/versioned_market_state.py`

- Added the validated, ordered `SourceStateRevision(primary_sequence,
  dependency_sequence)` value type.
- `VersionedMarketState.put()` compares composite revisions lexicographically
  when both current and incoming records provide them, with distinct
  `duplicate_source_revision` and `out_of_order_source_revision` diagnostics.
- The composite primary sequence must equal `source_sequence`; this prevents a
  synthetic or dependency-max slot from replacing the core chain position.
- Events that do not opt into a composite revision preserve the prior
  source-sequence/receipt behavior.

### `src/market_data_lab/solana_realtime_scanner.py`

- Validated local worker pool events now carry
  `SourceStateRevision(core_state_slot, dependency_generation)`.
- Existing strict provenance validation remains: `slot == core_state_slot`,
  non-negative generation, ordered dependency min/max, and required fields.

### `tests/test_remaining_a_event_state.py`

Added 15 offline production-code regressions (plus parameterized subtests):

- the exact capacity-2 A/C + two waiting B publisher failure schedule;
- cancellation of the older and newer waiters, and close/shutdown wakeup;
- capacities 1 and 2, distinct-key FIFO backpressure, hot/cold fairness,
  1,000 hot-key updates, and rejected-store events not touching the queue;
- `RaydiumLocalQuoteStateSource.run -> RollingStateStore ->
  CoalescingEventBus -> callback` with dependency-only, exact duplicate,
  generation regression, newer core and old-core/newer-generation cases;
- health acceptance/rejection counts and exact revision rejection counters;
- epoch reset and stale prior-epoch rejection;
- operation-count checks at 10, 1,000 and 10,000 live keys;
- linear affected-key epoch retirement, state/event-key reuse, capacity and TTL
  cleanup across all indexes;
- 100,000 dynamic values using two stable logical slots.

## Commands and actual results

| Command | Result |
| --- | --- |
| Initial `.venv/bin/pytest -q -p no:cacheprovider tests/test_remaining_a_event_state.py` | **3 failed** in 0.15 s (expected RED) |
| Final `.venv/bin/pytest -q -p no:cacheprovider tests/test_remaining_a_event_state.py` | **15 passed, 5 subtests passed** in 2.41 s |
| Related scanner/version/worker/selected regressions | **42 passed, 5 subtests passed** in 2.89 s |
| `.venv/bin/pytest -q -p no:cacheprovider` | **511 passed, 11 subtests passed** in 11.76 s |
| `.venv/bin/python -m compileall -q src/market_data_lab tests/test_remaining_a_event_state.py` | Passed |
| `git diff --check` | Passed |

The compact non-gating allocation/time observation for 100,000 updates was:

```text
updates=100000 logical_keys=2 history_entries=8
elapsed_seconds=3.156110 current_bytes=6185 peak_bytes=7613
```

This is an observation from this host, not a hardware-dependent performance
assertion. The deterministic gate is the operation-count test: hot updates and
epoch retirement perform zero full scans of the forward mapping.

## Status by audit/spec ID

| ID | Status | Evidence |
| --- | --- | --- |
| R-01 | **FIXED** | Exact reproduced race is green; one queue position per key, latest value delivery, both cancellation orders and close wakeup are runtime-tested. |
| R-02 | **FIXED at the Python admission boundary** | Dependency-only change is accepted and reaches a real callback; duplicates/regressions/old core/old epoch are rejected; accepted-state health remains exact. |
| O-04 | **FIXED** | Reverse index removes per-tick full-map scans and per-key epoch rescan; 10/1,000/10,000 operation-count and 100,000-update tests pass. |
| BUG-002 | **FIXED in Agent A scope** | Keyed latest-state/backpressure invariants and shutdown behavior pass offline; broader live soak is not claimed. |
| BUG-005 | **FIXED in Agent A scope** | Existing boundedness/TTL/capacity behavior is preserved and cleanup is now key-local. |
| BUG-020 | **PARTIAL globally** | R-02 is fixed. Node cache reconciliation and snapshot provenance findings R-03/R-04/R-05 belong to other agents and remain outside this result. |
| BUG-001/007/012 | **Preserved** | Full Python suite passes; stable-key, accepted-health and canonicalization regressions were not changed. |

## Stable handoff contract for D/E/G

For local Solana pool state, ordering is scoped to one `(source_id,
source_epoch, state_key)` and uses:

```text
source_sequence = chain_position = core_state_slot
source_revision = (core_state_slot, dependency_generation)
```

Comparison is lexicographic:

1. a larger core slot is newer regardless of dependency slot maxima;
2. at the same core slot, a larger dependency generation is newer;
3. an equal pair is idempotent/duplicate;
4. a smaller pair is stale, so an older core cannot return by claiming a
   larger dependency generation;
5. a larger source epoch invalidates the prior epoch and may reset both core
   slot and dependency generation; a prior epoch can never return.

`dependency_slot_min/max` remain factual provenance only and are never packed
into `chain_position`. A core update with unchanged dependencies is accepted
when its real core slot advances. A dependency-only update is accepted when
generation advances. Same-core/same-generation updates are duplicates because
the worker exposes no finer core write-version contract.

Producer requirement for D/E: within one worker/source epoch,
`dependency_generation` must be monotonic and must change whenever the emitted
dependency content changes. It may reset only with a new source epoch. Missing
or malformed required provenance remains a source error, not a guessed
revision. Consumers not using `source_revision` retain the legacy optional
`source_sequence` behavior.

## Interface/schema and compatibility

- `MarketEvent` and `EventEnvelope` gain one optional field; existing callers
  are source-compatible and continue on the old ordering path.
- `PutStatus` gains two explicit values for composite revision diagnostics.
- Bus snapshots gain additive `waiting_publishers` and `closed` fields; all
  existing metric names, including `dropped_events`, are preserved.
- `close()` is additive and is now used by the scanner's own shutdown path.
- The reverse index mirrors existing bounded mappings: it has at most one
  membership per live event key and is removed on TTL, capacity, replacement
  and epoch retirement. It is not an unbounded event log.

## Risks and not-run checks

- No live RPC/WebSocket, real SDK worker, wallet, trading, or external network
  request was used.
- The specification's 30–60 minute live soak was not run; that is a later
  integration gate.
- Node 24 and Node worker suites were not run because Agent A did not edit the
  Node scope.
- No project-wide static type checker is configured; compileall and all Python
  tests passed, but a non-existent type-check gate is not claimed.
- If a producer changes core contents more than once at the same core slot
  without advancing dependency generation, the present upstream contract has
  no write-version field to distinguish those states. Such a producer must add
  a real revision component rather than synthesize a chain slot.

## Dirty baseline and change boundary

At start, the worktree already contained the three user deletions, the prior
audit/archive/agent-plan artifacts, and Agent B's analyzer/test/result changes.
All were preserved. Intentional Agent A files are only:

- `src/market_data_lab/realtime_scanner.py`;
- `src/market_data_lab/versioned_market_state.py`;
- `src/market_data_lab/solana_realtime_scanner.py` (pool-state revision only);
- `tests/test_remaining_a_event_state.py`;
- `remaining-bugs-agent-plan/results/AGENT_A_RESULT.md`.

No commit, push, reset/clean, dependency installation, root action or live
trade was performed.
