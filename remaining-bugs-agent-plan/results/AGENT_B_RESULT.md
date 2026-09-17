# Agent B result — precise candidate lifecycle invalidation

Date: 2026-09-16  
Agent: B  
Requested model: OpenAI GPT-5.6 Luna, high reasoning (execution performed in the current Codex session).  
Baseline commit: `aeba8dcb09cd53d0e696c9254bf384d27b0e3344`.

## Scope

Implemented the Agent B assignment for audit finding **R-06** and the residual
part of **BUG-003**. The change is limited to candidate provenance and source
epoch invalidation in the two analyzers; no event-bus, Node, snapshot, RPC or
package changes were made.

## Red → green evidence

On a clean extracted copy of the baseline commit, this production-path probe
created a candidate through `UnifiedCycleAnalyzer._observe_cycle`, then
advanced an unrelated source epoch. The result was:

```text
{'active_before': 1, 'active_after_unrelated_transition': 0, 'candidate_closed': 1}
```

That is the original R-06 failure: an unrelated transition closed the active
candidate. The same scenario is now covered by
`test_cycle_unrelated_epoch_preserves_candidate_and_dirty_work` and the real
event/evaluation test. The new regression file passes **10 tests**.

## Changes

### `src/market_data_lab/unified_cycle_analyzer.py`

- Added `dependencies: frozenset[(source, epoch)]` to `_ActiveCandidate`.
- Candidate provenance is collected from explicit cycle leg metadata, mapped
  CEX sources, and legacy DEX quote cache matching by provider/round/notional.
- `_purge_source_epoch` now closes only active candidates whose actual source
  dependency belongs to the invalidated epoch.
- Removed unconditional clearing of all dirty direct/triangle work; quote
  removal already removes only work tied to that quote.
- Persisted candidate events include compact dependency pairs.
- Existing default construction remains compatible for tests/adapters that do
  not provide dependency metadata.

### `src/market_data_lab/unified_perp_analyzer.py`

- Added the same dependency set to `_ActiveCandidate`.
- Added source and epoch to compact spot/perp leg records, allowing exact
  provenance collection for direct, spot/perp and multi-leg candidates.
- `_purge_source_epoch` now closes only affected candidates and preserves
  unrelated dirty bases. Removed spot/perp/DEX/linear state marks only their
  actual base(s) dirty for reevaluation.
- Candidate event and compact-cycle output retain dependency pairs.
- Existing source-epoch ordering, old-epoch rejection, idempotence and
  monotonic lifecycle timing remain in place.

### `tests/test_remaining_b_candidate_epochs.py`

Added 10 offline regressions covering:

- real cycle event/evaluation candidate surviving unrelated epoch change;
- real perp event/evaluation candidate surviving unrelated epoch change;
- exact affected-source closure and one-time close;
- same-base candidates with one affected and one unaffected;
- multi-source candidates;
- pending (not-yet-persisted) candidate counters;
- dirty-work preservation;
- old-epoch rejection, repeated-transition idempotence and fresh-epoch reopen;
- monotonic/start identity and shutdown cleanup.

The tests exercise `_observe_cycle`, which is the production candidate
construction path, and two tests additionally drive `handle_event` through the
actual analyzer evaluation workers. No network or SDK is used.

## Commands and actual results

| Command | Result |
| --- | --- |
| `.venv/bin/pytest -q tests/test_remaining_b_candidate_epochs.py` | **10 passed** in 0.26 s |
| `.venv/bin/pytest -q tests/test_remaining_b_candidate_epochs.py tests/test_unified_cycle_analyzer.py tests/test_unified_perp_analyzer.py tests/test_agent_c_bugfixes.py tests/test_bug_010_monotonic_candidate_lifecycle.py` | **47 passed** in 1.16 s |
| `.venv/bin/pytest -q -p no:cacheprovider` | **496 passed, 6 subtests passed** in 9.47 s |
| `python -m py_compile src/market_data_lab/unified_cycle_analyzer.py src/market_data_lab/unified_perp_analyzer.py` | Passed |
| `git diff --check` | Passed |

The baseline red probe was run from an extracted clean commit under `/tmp`
and did not alter this worktree.

## Status by audit ID

| ID | Status | Evidence |
| --- | --- | --- |
| R-06 | **FIXED in this scope** | 10 new tests, including both real analyzer event/evaluation paths; unrelated transitions preserve active candidate identity and dirty work, affected transitions close exactly once. |
| BUG-003 | **PARTIAL globally** | The over-broad candidate invalidation covered by R-06 is fixed. Other BUG-003 acceptance requirements (full composition/live reconnect/soak gates) belong to the broader integration review and were not re-certified here. |
| BUG-010 | **Preserved** | Existing monotonic lifecycle tests pass; no wall-clock duration logic was changed. |

## Interface/schema and compatibility

`_ActiveCandidate` receives one backward-compatible defaulted field. Candidate
event payloads gain a `dependencies` list of `{source, source_epoch}` objects;
perp compact leg records now expose `source` and `source_epoch`, additive
fields for readers. No existing fields were renamed or reinterpreted. Unknown
or legacy candidates with an empty dependency set are conservatively preserved
on unrelated transitions rather than guessed closed.

The dependency set is bounded by the finite legs of one candidate. No global
unbounded index was introduced. Dirty sets remain bounded by their existing
route/base work limits.

## Handoff contract

For downstream agents A/D/G, a candidate dependency is the pair
`(source, source_epoch)` belonging to an actual leg. On a transition to
`new_epoch`, invalidate a candidate only when its pair has the same source and
an epoch strictly less than `new_epoch` (equivalently, `<= old_epoch`). The
core/source slot must not be synthesized from another leg's timestamp. A
candidate with no trustworthy dependency metadata is preserved conservatively;
adapters that create such candidates should add explicit leg metadata before
claiming epoch invalidation. Repeated transitions at the current epoch are
no-ops, while a lower epoch remains an error.

## Risks and not-run checks

- Full live CEX/WS reconnect behavior, real RPC, and the specification's
  30–60 minute soak gate were not run.
- Node 24 installation/runtime, other agents' contracts, and full production
  composition were not checked in this task.
- The complete Python static type-check gate is not configured and was not
  claimed.
- A cycle calculator that supplies neither explicit leg metadata nor a
  uniquely matchable legacy DEX quote is treated as having no dependency; it
  is not guessed from a provider/base string. Such a legacy adapter should
  adopt the additive leg provenance fields before relying on epoch closure.

## Dirty baseline and change boundary

At start, the worktree already contained the three user deletions, the prior
audit, the agent-plan files and archive. I preserved them. Intentional changes
for Agent B are only the two analyzer files, this result file, and the new
regression test file. No commit, push, dependency installation, root action,
live trade, or external market request was performed.
