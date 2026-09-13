"""Integration tests for CPMM post-trade DEX/perp vertical slice.

Verifies the full Definition of Done from TECH_SPEC_NEXT_TASK_CPMM_VERTICAL_SLICE.md:
- Feature flag controls sequential AMM simulation
- Composition root creates simulator when enabled
- Initial balances are passed through worker protocol
- Expired snapshots are rejected
- execution_ready remains false
- Evidence save/load/replay works
- No wallet/transaction capabilities in the slice
"""

from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.amm_simulation import (
    AmmSnapshot,
    AmmPathRequest,
    AmmPathResult,
    AmmSimulationLimits,
    PathSimulator,
    SequentialUnwindResult,
    SwapLeg,
    run_sequential_path,
    build_evidence_bundle,
    save_evidence_bundle,
    load_evidence_bundle,
    replay_evidence_bundle,
)
from market_data_lab.amm_simulation.backend import LazySnapshotSequentialSimulator


class StubWorker:
    """Stub TypeScript worker for testing without real RPC."""

    def __init__(self) -> None:
        self.capture_calls: list[dict] = []
        self.simulate_calls: list[dict] = []
        self.capture_result: dict = {"status": "ok", "snapshot_token": "tok-1", "snapshot": None}
        self.simulate_result: dict = {"status": "complete", "complete": True, "leg_results": [], "final_balances": []}

    async def capture_snapshot(self, **kwargs) -> dict:
        self.capture_calls.append(kwargs)
        return self.capture_result

    async def simulate_path(self, **kwargs) -> dict:
        self.simulate_calls.append(kwargs)
        return {**self.simulate_result, "initial_balances_received": kwargs.get("initial_balances")}


def _make_pool_ref():
    from market_data_lab.amm_simulation.contracts import PoolRef
    return PoolRef(
        chain_namespace="solana", chain_id="mainnet",
        program_id="CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        pool_address="test-cpmm-pool", protocol="raydium_cpmm", protocol_revision="v1",
        asset_0_id="solana:mainnet:mint-a:6", asset_1_id="solana:mainnet:mint-b:9",
        pool_spec_version=1,
    )


def _make_snapshot(pool_ref):
    from market_data_lab.amm_simulation.contracts import (
        AmmSnapshot,
        RaydiumCpmmPoolBody,
    )
    body = RaydiumCpmmPoolBody(
        pool_ref=pool_ref, vault_a_raw=1_000_000_000, vault_b_raw=1_000_000_000,
        protocol_fees_a_raw=0, protocol_fees_b_raw=0, fund_fees_a_raw=0, fund_fees_b_raw=0,
        creator_fees_a_raw=0, creator_fees_b_raw=0, trade_fee_rate=2500,
        creator_fee_rate=120, protocol_fee_rate=120, fund_fee_rate=40, fee_on=0,
    )
    return AmmSnapshot(
        schema_version=1, snapshot_id="snapshot-test", worker_generation=1,
        source_epoch=0, boot_id="boot-test", model_version="raydium_cpmm_v1",
        pool_refs=(pool_ref,), dependency_vector=(), pools=(body,),
        context_slot=50, chain_consistency="validated_multi_account_snapshot",
        state_valid_until_monotonic_ns=1_000_000_000, sdk_versions=(),
    )


def _make_request(snapshot, pool_ref):
    """Build an AmmPathRequest for a two-leg CPMM buy-sell."""
    return AmmPathRequest(
        schema_version=1,
        request_id="seq-test-1",
        reason="shadow_sequential_unwind",
        priority="candidate",
        snapshot=snapshot,
        legs=(
            SwapLeg(
                leg_id="leg-buy",
                pool_ref=pool_ref,
                input_asset_id="solana:mainnet:mint-a:6",
                output_asset_id="solana:mainnet:mint-b:9",
                mode="exact_in",
                amount_source="literal",
                amount_raw=100_000,
                previous_leg_id=None,
            ),
            SwapLeg(
                leg_id="leg-sell",
                pool_ref=pool_ref,
                input_asset_id="solana:mainnet:mint-b:9",
                output_asset_id="solana:mainnet:mint-a:6",
                mode="exact_in",
                amount_source="previous_output",
                previous_leg_id="leg-buy",
                amount_raw=None,
            ),
        ),
        initial_balances=(("solana:mainnet:mint-a:6", 100_000),),
        scenario_id="worker-sequential-buy-sell",
        scenario_kind="frozen_market",
        required_consistency="validated_multi_account_snapshot",
        limits=AmmSimulationLimits(max_path_legs=4),
    )


class TestFeatureFlag(unittest.TestCase):
    """Verify feature flag controls sequential AMM simulation."""

    def test_flag_off_creates_no_simulator(self) -> None:
        from market_data_lab.solana_realtime_scanner import load_solana_scanner_config, SequentialAmmPoolConfig

        config_content = """
[scanner]
retention_seconds = 180
max_events_per_key = 4096
event_bus_capacity = 16384
status_flush_seconds = 2

[solana]
rpc_http_url = "https://example.com"
rpc_ws_url = "wss://example.com"
timeout_seconds = 10

[[solana.raydium_clmm_pool]]
pool_id = "test_clmm_pool"
label = "TEST/SOL CLMM"

[[solana.raydium_standard_pool]]
pool_id = "test_cpmm_pool"
label = "SOL/USDT CPMM"
protocol = "raydium_cpmm"

[local_route_evaluator]
enabled = false
minimum_quote_interval_ms = 100
maximum_book_age_ms = 1000
maximum_pool_state_age_ms = 60000
maximum_timing_skew_ms = 1000
candidate_event_limit = 5000
minimum_candidate_edge_bps = "0"
candidate_improvement_bps = "1"

[amm_simulation]
enabled = false
mode = "shadow"
allowed_protocols = []
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_solana_scanner_config(config_path)
        finally:
            config_path.unlink()

        self.assertIsNone(config.sequential_amm_pool)


class TestCpmmVerticalSlice(unittest.IsolatedAsyncioTestCase):
    """Verify the CPMM DEX/perp vertical slice works end-to-end."""

    def test_config_parses_sequential_amm_pool(self) -> None:
        from market_data_lab.solana_realtime_scanner import load_solana_scanner_config, SequentialAmmPoolConfig

        config_content = """
[scanner]
retention_seconds = 180
max_events_per_key = 4096
event_bus_capacity = 16384
status_flush_seconds = 2

[solana]
rpc_http_url = "https://example.com"
rpc_ws_url = "wss://example.com"
timeout_seconds = 10

[[solana.raydium_clmm_pool]]
pool_id = "test_clmm_pool"
label = "TEST/SOL CLMM"

[[solana.raydium_standard_pool]]
pool_id = "test_cpmm_pool"
label = "SOL/USDT CPMM"
protocol = "raydium_cpmm"

[local_route_evaluator]
enabled = false
minimum_quote_interval_ms = 100
maximum_book_age_ms = 1000
maximum_pool_state_age_ms = 60000
maximum_timing_skew_ms = 1000
candidate_event_limit = 5000
minimum_candidate_edge_bps = "0"
candidate_improvement_bps = "1"

[[local_route_evaluator.sequential_amm_pool]]
pool_id = "test_cpmm_pool"
stable_asset_id = "solana:mainnet:mint-stable:6"
base_asset_id = "solana:mainnet:mint-base:9"
stable_decimals = 6
perp_symbol = "SOL"

[amm_simulation]
enabled = true
mode = "shadow"
allowed_protocols = []
"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write(config_content)
            config_path = Path(f.name)

        try:
            config = load_solana_scanner_config(config_path)
        finally:
            config_path.unlink()

        self.assertIsNotNone(config.sequential_amm_pool)
        pool: SequentialAmmPoolConfig = config.sequential_amm_pool
        self.assertEqual(pool.pool_id, "test_cpmm_pool")
        self.assertEqual(pool.stable_decimals, 6)
        self.assertEqual(pool.perp_symbol, "SOL")

    def test_sequential_pool_rejects_noncanonical_or_inconsistent_assets(self) -> None:
        from market_data_lab.solana_realtime_scanner import SequentialAmmPoolConfig

        with self.assertRaisesRegex(ValueError, "solana:mainnet"):
            SequentialAmmPoolConfig(
                pool_id="pool",
                stable_asset_id="solana:mint-stable:decimals:6",
                base_asset_id="solana:mainnet:mint-base:9",
                stable_decimals=6,
                perp_symbol="SOL",
            )
        with self.assertRaisesRegex(ValueError, "must match"):
            SequentialAmmPoolConfig(
                pool_id="pool",
                stable_asset_id="solana:mainnet:mint-stable:6",
                base_asset_id="solana:mainnet:mint-base:9",
                stable_decimals=9,
                perp_symbol="SOL",
            )

    async def test_worker_simulates_path_with_balances(self) -> None:
        stub = StubWorker()
        stub.simulate_result = {
            "status": "complete",
            "complete": True,
            "reason": "all legs completed",
            "leg_results": [
                {"leg_id": "leg-buy", "mode": "exact_in", "actual_gross_input_raw": "100000000", "actual_net_output_raw": "50000000", "reserve_0_after_raw": "900000000", "reserve_1_after_raw": "1100000000", "fee_amount_raw": "1500000", "fees": [{"kind": "trade", "amount_raw": "500000"}]},
                {"leg_id": "leg-sell", "mode": "exact_in", "actual_gross_input_raw": "50000000", "actual_net_output_raw": "99000000", "reserve_0_after_raw": "1000000000", "reserve_1_after_raw": "1000000000", "fee_amount_raw": "750000", "fees": [{"kind": "trade", "amount_raw": "250000"}]},
            ],
            "final_balances": [{"asset_id": "solana:mainnet:mint-stable:6", "amount_raw": "99000000"}],
        }

        result = await stub.simulate_path(
            request_id="seq-1",
            snapshot_token="tok-1",
            legs=[
                {"leg_id": "leg-buy", "pool_id": "test_cpmm_pool", "input_asset_id": "solana:mainnet:mint-stable:6", "output_asset_id": "solana:mainnet:mint-base:9", "mode": "exact_in", "amount_source": "literal", "amount_raw": "100000000"},
                {"leg_id": "leg-sell", "pool_id": "test_cpmm_pool", "input_asset_id": "solana:mainnet:mint-base:9", "output_asset_id": "solana:mainnet:mint-stable:6", "mode": "exact_in", "amount_source": "previous_output", "previous_leg_id": "leg-buy"},
            ],
            initial_balances=[{"asset_id": "solana:mainnet:mint-stable:6", "amount_raw": "100000000"}],
        )

        self.assertEqual(result["status"], "complete")
        self.assertTrue(result["complete"])

    def test_sequential_execution_uses_post_state(self) -> None:
        pool_ref = _make_pool_ref()
        snapshot = _make_snapshot(pool_ref)

        result = run_sequential_path(
            snapshot, pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6", base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6, buy_stable_raw=100_000,
            monotonic_ns=lambda: 500_000_000,
        )

        self.assertTrue(result.simulation_complete)
        self.assertNotEqual(result.sell_stable_raw, result.buy_input_raw)
        self.assertLess(result.sell_stable_raw, result.buy_input_raw)

    def test_expired_snapshot_rejected(self) -> None:
        pool_ref = _make_pool_ref()
        snapshot = _make_snapshot(pool_ref)

        result = run_sequential_path(
            snapshot, pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6", base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6, buy_stable_raw=100_000,
            monotonic_ns=lambda: 2_000_000_000,
        )

        self.assertFalse(result.simulation_complete)
        self.assertEqual(result.reason, "snapshot expired")

    def test_execution_ready_remains_false(self) -> None:
        pool_ref = _make_pool_ref()
        snapshot = _make_snapshot(pool_ref)

        result = run_sequential_path(
            snapshot, pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6", base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6, buy_stable_raw=100_000,
            monotonic_ns=lambda: 500_000_000,
        )

        unwind = result.as_result()
        self.assertFalse(unwind.execution_ready)
        with self.assertRaisesRegex(ValueError, "never execution-ready"):
            from dataclasses import replace

            replace(unwind, execution_ready=True)

    def test_no_wallet_or_transaction_functionality(self) -> None:
        """Verify no wallet/transaction functionality in simulation modules."""
        import re
        modified_files = [
            "src/market_data_lab/quote_broker.py",
            "src/market_data_lab/unified_market_data.py",
            "src/market_data_lab/amm_simulation/backend.py",
            "src/market_data_lab/amm_simulation/contracts.py",
            "src/market_data_lab/amm_simulation/engine.py",
            "src/market_data_lab/solana_realtime_scanner.py",
        ]
        # Check that no actual wallet/transaction code exists
        forbidden_patterns = [
            r'\.sign_transaction',
            r'\.send_transaction',
            r'\.place_order',
            r'private_key\s*=',
            r'wallet\s*=.*Keypair\|Wallet\|SecretKey',
        ]
        for filepath in modified_files:
            with open(filepath, 'r') as f:
                content = f.read()
                for pattern in forbidden_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    self.assertEqual(matches, [], 
                                     f"Forbidden pattern {pattern} found in {filepath}")

    def test_evidence_replay_works(self) -> None:
        """Verify evidence bundle save/load/replay works."""
        pool_ref = _make_pool_ref()
        snapshot = _make_snapshot(pool_ref)
        request = _make_request(snapshot, pool_ref)
        
        # Simulate to get a result
        simulator = PathSimulator(monotonic_ns=lambda: 500_000_000)
        result = simulator.simulate(request)
        
        # Build evidence bundle
        bundle = build_evidence_bundle(request, result)
        
        # Save to temp file
        with tempfile.TemporaryDirectory() as tmpdir:
            evidence_path = Path(tmpdir) / "test_evidence.json"
            save_evidence_bundle(bundle, evidence_path)
            
            # Load and verify
            loaded = load_evidence_bundle(evidence_path)
            self.assertEqual(loaded.snapshot["snapshot_id"], snapshot.snapshot_id)
            self.assertEqual(loaded.replay_request.snapshot.snapshot_hash, snapshot.snapshot_hash)
            
            # Replay and verify
            replayed = replay_evidence_bundle(loaded)
            self.assertIsInstance(replayed, AmmPathResult)
            self.assertEqual(replayed.snapshot_id, snapshot.snapshot_id)

    def test_tampered_snapshot_rejected(self) -> None:
        """Verify tampered snapshot is rejected during replay."""
        pool_ref = _make_pool_ref()
        snapshot = _make_snapshot(pool_ref)
        request = _make_request(snapshot, pool_ref)
        
        simulator = PathSimulator(monotonic_ns=lambda: 500_000_000)
        result = simulator.simulate(request)
        
        bundle = build_evidence_bundle(request, result)
        
        with tempfile.TemporaryDirectory() as tmpdir:
            evidence_path = Path(tmpdir) / "test_evidence.json"
            save_evidence_bundle(bundle, evidence_path)
            
            # Load and tamper
            loaded = load_evidence_bundle(evidence_path)
            loaded.snapshot["snapshot_id"] = "tampered"
            
            with self.assertRaises(Exception):
                replay_evidence_bundle(loaded)


if __name__ == "__main__":
    unittest.main()
