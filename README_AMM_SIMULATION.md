# Post-trade AMM simulation

This module is a read-only shadow simulation and never submits transactions.

## Current capability matrix

| Protocol | Variant | Exact-in | Exact-out | Post-state | Notes |
| --- | --- | --- | --- | --- | --- |
| Synthetic CPMM v1 | synthetic | Yes | Yes | Yes | Exact-in, exact-out, local post-state overlay, offline evidence replay |
| Raydium CPMM | raydium_cpmm | Yes | Yes | Yes | Worker emits full vault/counter snapshots; Python core decodes and replays offline |
| Orca Whirlpool classic | orca_whirlpool | Yes | Yes | Yes | Pinned to @orca-so/whirlpools-sdk 0.22.0 (tick crossing, price/liquidity post-state, exact-in/out) |
| Raydium CLMM | raydium_clmm | Yes | Yes | Yes | Tick-array transitions, post-state, concentrated liquidity sqrt-price math |
| Meteora DLMM | meteora_dlmm | Yes | Yes | Yes | Bin transitions, active bin, variable fee state; pinned to @meteora-ag/dlmm 1.9.14 |
| Raydium AMM v4 | raydium_amm_v4 | Yes | Yes | Yes | Restricted swap-only subset (vault accounting, no OpenBook); OpenBook-dependent pools rejected |

The live feature remains disabled by default through [amm_simulation] enabled = false.
Enable it only after the selected protocol adapter reports complete post-state in tests.

## Acceptance matrix (spec Section 18)

| Criterion | Status |
| --- | --- |
| All stages A-C implemented | Done |
| Capabilities match tests | Done |
| AS01/T09: synthetic 100 to 90 to 99, states 1100/910 and 1001/1000 | Passing |
| AS37: evidence canonical hash parity Python and TS | Passing |
| Post-state consumed by repeated swaps | Verified |
| Token conservation + integer exactness | Verified |
| Complete local snapshot -> 0 remote calls | Verified |
| Evidence bundle replay offline | Verified |
| Benchmark: p95 CPMM <= 5 ms, p99 stall <= 20 ms, memory <= 64 MiB | Passing |

## Migration notes

- Protocol adapters moved from Unsupported to Supported.
- Existing QuoteResult schema is unchanged; sequential results use a dedicated
  AmmPathResult schema with explicit state_after_scope and evidence_hash.
- Legacy estimate_local_path remains available; simulate_local_path is the
  new shadow entry point.
- amm_simulation enabled = false must stay false in production until a
  sequential result passes end-to-end integration tests (spec Section 12.2).
- Old dex_perp_paired_exact_quote_* rows are left independent; sequential
  results get a separate analysis_kind.

## Supported SDK versions

| Package | Version |
| --- | --- |
| @raydium-io/raydium-sdk-v2 | 0.2.63-alpha |
| @orca-so/whirlpools-sdk | 0.22.0 |
| @meteora-ag/dlmm | 1.9.14 |

## Python core

```bash
PYTHONPATH=src .venv/bin/python -m unittest tests.test_amm_simulation
```

The deterministic executor accepts immutable AmmSnapshot objects, executes linear
paths in a local overlay, supports exact-in and exact-out integer arithmetic, and
returns per-leg state hashes.  Repeated swaps see the virtual post-state while the
observed snapshot stays unchanged.

## Worker canonical codec parity

```bash
npm --prefix workers/solana-quote-worker run check
npm --prefix workers/solana-quote-worker test
```

The TypeScript test loads tests/fixtures/amm_simulation/snapshot-parity-hashes.json
and compares its SHA-256 with the projection JSON files.  The Python fixture
generator uses the same canonical codec from src/market_data_lab/amm_simulation/codec.py.
The fixtures contain only offline synthetic data.

The Raydium CPMM adapter (raydiumCpmm.ts) is locked against the pinned SDK
fixtures for both exact-in and exact-out.  The snapshot bundle builder
(simulation/snapshots.ts) emits the exact shape the Python decoder
(replay.py::_decode_snapshot) consumes, and the path executor
(simulation/path.ts) mirrors the verified Python adapter so a worker branch
never mutates the observed snapshot.

## Broker integration

QuoteBroker.simulate_local_path routes a local after-state backend through the
shared exact key, LRU cache, deadline handling, and in-flight deduplication.  Local
simulation requests do not consume the remote provider budget.  Legacy
estimate_local_path remains available.

The composition root unified_market_data.build_unified_market_data_scanner
exposes the managed local worker through the source seam and registers the
config-gated AMM simulation backend.  The public source API
RaydiumLocalQuoteStateSource.capture_snapshot / simulate_path /
export_simulation_evidence is a typed read-only bridge to the worker and is
refused explicitly unless amm_simulation enabled = true.  WorkerBackend
in amm_simulation.backend translates typed domain requests to the narrow
worker protocol.

## DEX/perp sequential model (shadow)

The analyzer (unified_perp_analyzer) supports a config-gated sequential unwind
model dex_perp_sequential_flat_model.  When a sequential simulator is injected,
an exact buy-plus-sell-via-post-state projection is combined with the current perp
BBO and fees into a dex_post_trade_pool_state_simulated = true row with
candidate_eligible = false and execution_ready = false.  Old
dex_perp_paired_exact_quote_* rows are left independent and are never promoted.
Routing the local shadow backend also requires amm_simulation enabled = true.

## Offline commands

```bash
# Python core tests
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_amm_simulation*.py"

# Python canonical hash parity (AS37)
PYTHONPATH=src .venv/bin/python -m unittest tests.test_amm_simulation_parity

# TypeScript tests
npm --prefix workers/solana-quote-worker test

# TypeScript type-check
npm --prefix workers/solana-quote-worker run check

# Benchmark
cd workers/solana-quote-worker && npm run benchmark

# Evidence replay
PYTHONPATH=src .venv/bin/python -c "from market_data_lab.amm_simulation import build_evidence_bundle, load_evidence_bundle, replay_evidence_bundle; bundle = load_evidence_bundle('data/amm-evidence.json'); replay_evidence_bundle(bundle)"
```

## Evidence replay

Build and save an evidence bundle with:

```python
from market_data_lab.amm_simulation import (
    build_evidence_bundle,
    load_evidence_bundle,
    replay_evidence_bundle,
    save_evidence_bundle,
)

bundle = build_evidence_bundle(request, result)
save_evidence_bundle(bundle, "data/amm-evidence.json")
loaded = load_evidence_bundle("data/amm-evidence.json")
replayed = replay_evidence_bundle(loaded)
```

The replay is completely offline and fails if the expected result does not match.
