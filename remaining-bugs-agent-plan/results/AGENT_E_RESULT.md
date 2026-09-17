# Agent E — Snapshot Evidence and Freshness Result

## Summary

**Agent ID:** E
**Status:** FIXED (R-05, BUG-020)
**Model:** OpenAI GPT-5.6 Sol (Reasoning: high)

## Baseline

- **Commit:** `aeba8dcb09cd53d0e696c9254bf384d27b0e3344` (audit baseline)
- **Dirty state:** Yes — working tree has modifications to `worker.ts`, `snapshots.ts`, `contracts.py`, `replay.py`, test files, and new Agent E test files. The pre-existing user deletions of `TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md`, `TECH_SPEC_POST_TRADE_AMM_SIMULATION.md`, and `bug-002-keyed-coalescing-event-bus.patch` were preserved and left untouched.

## Audit IDs Closed

- **R-05** — Incomplete/misleading snapshot provenance (BUG-020)

## Defect Description

The `raydiumCpmmSnapshotBundle` in `snapshots.ts` and the `processSnapshotRequest` in `worker.ts` had the following issues:

1. **SDK version hardcoded as `"latest"`** — The CPMM snapshot builder hardcoded `sdk_versions` to `["@raydium-io/raydium-sdk-v2", "latest"]` instead of resolving the actual installed package version.
2. **Lost per-pool provenance** — The snapshot bundle's `context_slot` was computed as `Math.max(state.slot)` across all pools, losing the distinction between core pool account slots and dependency (vault/config/tick-array) slots. Per-pool `core_state_slot`, `dependency_slot_min`, `dependency_slot_max`, and `dependency_generation` were not included in the snapshot bundle.
3. **TTL derived from capture time** — `processSnapshotRequest` set `state_valid_until_monotonic_ns` to `createdAt + snapshotTtlNs`, ignoring the actual freshness of the underlying state. A stale underlying cache could obtain a "fresh" TTL window simply because a new capture was taken.
4. **`dependency_vector` was always `[]`** — While slot summaries were not mislabeled as AccountVersion evidence (the comment was correct), the actual per-pool provenance was not surfaced in the bundle at all.

## Changes Made

### `workers/solana-quote-worker/src/simulation/snapshots.ts`
- Added `createRequire` import and `_installedSdkVersions()` helper that resolves the actual installed `@raydium-io/raydium-sdk-v2` version from `package.json` metadata.
- Added per-pool provenance fields to `RaydiumCpmmPoolBundle` interface: `core_state_slot`, `dependency_slot_min`, `dependency_slot_max`, `dependency_generation`.
- Updated `raydiumCpmmSnapshotBundle` to populate these per-pool provenance fields from the simulation state (which already carries them via `pool.provenance.fields()`).
- Replaced `sdk_versions: [["@raydium-io/raydium-sdk-v2", "latest"]]` with `sdk_versions: _installedSdkVersions()` in the CPMM builder.

### `workers/solana-quote-worker/src/worker.ts`
- Added import for `RaydiumCpmmSimulationCapture` type from `raydiumStandard.js`.
- Updated `processSnapshotRequest` to:
  - Use `engine.cpmmSimulationCapture(poolId)` to obtain both simulation state and `PoolFreshnessSnapshot` data.
  - Compute the effective TTL as `min(configured TTL, remaining TTL from oldest state receipt)`. Freshness is derived from real monotonic receipt/validation data (`core_received_at_monotonic_ms` and `dependency_received_at_monotonic_ms`), not capture time.
  - If the oldest state is already stale (remaining TTL <= 0), the snapshot `state_valid_until_monotonic_ns` is set to a past timestamp so the snapshot is immediately expired, rather than dishonestly fresh.
  - `state_valid_until_monotonic_ns` is set to `createdAt + effectiveTtlNs`.

### `src/market_data_lab/amm_simulation/contracts.py`
- Added `core_state_slot`, `dependency_slot_min`, `dependency_slot_max`, `dependency_generation` fields to `RaydiumCpmmPoolBody` (with defaults: `0`, `None`, `None`, `0` for legacy compatibility).
- Added these fields to `economic_projection` property.
- Added validation in `__post_init__`: non-negative `core_state_slot`, valid slot ordering (`dependency_slot_min <= dependency_slot_max`).

### `src/market_data_lab/amm_simulation/replay.py`
- Updated `_decode_snapshot` to extract per-pool provenance fields (`core_state_slot`, `dependency_slot_min`, `dependency_slot_max`, `dependency_generation`) from the bundle's pool entries and pass them to `RaydiumCpmmPoolBody`. Missing fields default to `0`/`None` for legacy compatibility.

### Test Updates
- `workers/solana-quote-worker/test/snapshotBundle.test.ts` — Updated `sdk_versions` assertion to verify the actual installed version matches `package.json` metadata, not `"latest"`. Added `createRequire` import.
- `tests/test_amm_simulation_worker.py` — Replaced `"latest"` with `"0.2.63-alpha"` in test fixture bundles.
- `workers/solana-quote-worker/test/workerHandler.test.ts` — Increased byte budget from 1024/1500 to 2048 to accommodate new provenance fields in the bundle size.
- `workers/solana-quote-worker/test/remainingESnapshotEvidence.test.ts` — New TS test file (8 tests, E01-E08).
- `tests/test_remaining_e_snapshot_evidence.py` — New Python test file (9 tests, E01-E09).

## Commands and Results

```
# TypeScript type check
$ npx tsc --noEmit
# Result: passes (0 errors)

# TypeScript tests
$ npm test
# Result: 15 suites, 15 pass, 0 fail

# Python tests (Agent E tests only)
$ .venv/bin/python -m pytest tests/test_remaining_e_snapshot_evidence.py -v
# Result: 9 passed

# Full Python test suite
$ .venv/bin/python -m pytest -x -q
# Result: 520 passed, 11 subtests passed (11.80s)
```

## Red→Green Evidence

**Before fix (red):** The `raydiumCpmmSnapshotBundle` produced `sdk_versions: [["@raydium-io/raydium-sdk-v2", "latest"]]` and `pools[0]` had no `core_state_slot`/`dependency_slot_*` fields. `processSnapshotRequest` set `state_valid_until_monotonic_ns` to `createdAt + snapshotTtlNs` unconditionally.

**After fix (green):**
- `test E01` verifies `sdk_versions[0][1]` equals the actual installed version from `package.json`, not `"latest"`.
- `test E02` verifies per-pool `core_state_slot`/`dependency_slot_min/max`/`dependency_generation` survive serialization and Python decode.
- `test E07` verifies stale `state_valid_until_monotonic_ns` is properly set on the snapshot.
- `test E08` verifies expired `state_valid_until` causes eviction without re-extension.

## Interface/Schema Changes

- `RaydiumCpmmPoolBundle` (TS): Added `core_state_slot: number`, `dependency_slot_min: number | null`, `dependency_slot_max: number | null`, `dependency_generation: number`.
- `RaydiumCpmmPoolBody` (Python): Added `core_state_slot: int = 0`, `dependency_slot_min: int | None = None`, `dependency_slot_max: int | None = None`, `dependency_generation: int = 0` (all with defaults for backward compatibility).
- `economic_projection` of `RaydiumCpmmPoolBody`: Now includes the provenance fields.
- Schema version remains 1 (compatible addition — new fields are additive with defaults).

## Compatibility

- **Backward compatibility:** All new fields have defaults. Legacy snapshots without provenance fields decode with `core_state_slot=0`, `dependency_slot_min=None`, `dependency_slot_max=None`, `dependency_generation=0`.
- **Hash parity:** The `economic_projection` of `RaydiumCpmmPoolBody` now includes provenance fields. However, the parity test fixtures (`snapshot-parity-*.json`) use the `CpmmPoolBody` (synthetic) for `snapshot-parity-cpmm.json` and don't include provenance fields in the `RaydiumCpmmPoolBody` fixture (`snapshot-parity-rcpmm.json`). Both Python and TS produce identical hashes because the default values (`0`, `None`, `0`) are consistently included in both implementations.
- **TS type check:** Passes cleanly under Node 26.7.0 (audit note: manifest declares `>=24 <25`).

## Risks and Blocked/Not-Run

- **Node 24 runtime:** The audit used Node 26.7.0. The manifest declares `>=24 <25`. A clean Node 24 installation was not performed (belongs to Agent F). TS type check and tests pass on Node 26.7.0.
- **Integration test:** Agent E provides unit-level red→green evidence. A full production integration test over the real request path (capture→worker response→Python decode→simulation→evidence) is owned by Agent G.
- **No live RPC:** All tests are offline. The `processSnapshotRequest` changes assume `cpmmSimulationCapture` returns valid freshness data, which is verified by the existing `engineRuntime.test.ts` and `remainingDSolanaEngines.test.ts` tests.
