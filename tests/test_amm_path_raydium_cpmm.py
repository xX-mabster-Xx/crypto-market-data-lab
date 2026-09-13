from __future__ import annotations

import unittest
import tempfile
from pathlib import Path

from market_data_lab.amm_simulation import (
    AmmPathRequest,
    AmmSimulationLimits,
    AmmSnapshot,
    PathSimulator,
    PoolRef,
    RaydiumCpmmPoolBody,
    SwapLeg,
    build_evidence_bundle,
    load_evidence_bundle,
    replay_evidence_bundle,
    save_evidence_bundle,
)


def _pool_ref() -> PoolRef:
    return PoolRef(
        chain_namespace="solana",
        chain_id="mainnet",
        program_id="CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        pool_address="fixture-pool",
        protocol="raydium_cpmm",
        protocol_revision="fixture",
        asset_0_id="asset-a",
        asset_1_id="asset-b",
        pool_spec_version=1,
    )


def _snapshot() -> AmmSnapshot:
    pool_ref = _pool_ref()
    return AmmSnapshot(
        schema_version=1,
        snapshot_id="snapshot-raydium-cpmm",
        worker_generation=1,
        source_epoch=1,
        boot_id="boot",
        model_version="raydium_cpmm_v1",
        pool_refs=(pool_ref,),
        dependency_vector=(),
        pools=(
            RaydiumCpmmPoolBody(
                pool_ref=pool_ref,
                vault_a_raw=1_000_000_000,
                vault_b_raw=1_000_000_000,
                protocol_fees_a_raw=0,
                protocol_fees_b_raw=0,
                fund_fees_a_raw=0,
                fund_fees_b_raw=0,
                creator_fees_a_raw=0,
                creator_fees_b_raw=0,
                trade_fee_rate=2_500,
                creator_fee_rate=120,
                protocol_fee_rate=120,
                fund_fee_rate=40,
                fee_on=0,
            ),
        ),
        context_slot=1,
        chain_consistency="validated_multi_account_snapshot",
        state_valid_until_monotonic_ns=10_000_000_000,
    )


def _leg(
    pool_ref: PoolRef,
    *,
    leg_id: str = "leg-1",
    input_asset_id: str = "asset-a",
    output_asset_id: str = "asset-b",
    amount_source: str = "literal",
    amount_raw: int | None = 1_000_000,
    previous_leg_id: str | None = None,
) -> SwapLeg:
    return SwapLeg(
        leg_id=leg_id,
        pool_ref=pool_ref,
        input_asset_id=input_asset_id,
        output_asset_id=output_asset_id,
        mode="exact_in",
        amount_source=amount_source,  # type: ignore[arg-type]
        amount_raw=amount_raw,
        previous_leg_id=previous_leg_id,
    )


def _request(legs: tuple[SwapLeg, ...], balances: tuple[tuple[str, int], ...]) -> AmmPathRequest:
    return AmmPathRequest(
        schema_version=1,
        request_id="raydium-path",
        reason="shadow_verification",
        priority="candidate",
        snapshot=_snapshot(),
        legs=legs,
        initial_balances=balances,
        scenario_id="raydium-cpmm-round-trip",
        limits=AmmSimulationLimits(max_path_legs=4),
    )


class RaydiumCpmmPathTest(unittest.TestCase):
    def test_round_trip_uses_post_state_and_separates_fee_counters(self) -> None:
        pool_ref = _pool_ref()
        first = _leg(pool_ref)
        second = _leg(
            pool_ref,
            leg_id="leg-2",
            input_asset_id="asset-b",
            output_asset_id="asset-a",
            amount_source="previous_output",
            amount_raw=None,
            previous_leg_id="leg-1",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(
            _request((first, second), (("asset-a", 1_000_000),)),
        )

        self.assertTrue(result.complete, result.reason)
        self.assertEqual(len(result.leg_results), 2)
        for leg in result.leg_results:
            self.assertGreater(leg.fee_amount_raw, 0)
            self.assertNotEqual(leg.state_before_hash, leg.state_after_hash)
        balances = dict(result.final_balances)
        self.assertLess(balances["asset-a"], 1_000_000)
        self.assertGreaterEqual(balances["asset-b"], 0)

    def test_observed_snapshot_remains_unchanged_after_raydium_path(self) -> None:
        pool_ref = _pool_ref()
        snapshot = _snapshot()
        request = AmmPathRequest(
            schema_version=1,
            request_id="raydium-isolation",
            reason="shadow_verification",
            priority="candidate",
            snapshot=snapshot,
            legs=(_leg(pool_ref),),
            initial_balances=(("asset-a", 1_000_000),),
            scenario_id="raydium-cpmm-isolation",
        )
        PathSimulator(monotonic_ns=lambda: 0).simulate(request)

        self.assertEqual(snapshot.pools[0].vault_a_raw, 1_000_000_000)
        self.assertEqual(snapshot.pools[0].vault_b_raw, 1_000_000_000)
        self.assertEqual(snapshot.pools[0].protocol_fees_a_raw, 0)

    def test_unsupported_protocol_spec_version_is_typed_refusal(self) -> None:
        pool_ref = _pool_ref()
        snapshot = _snapshot()
        unsupported = RaydiumCpmmPoolBody(
            pool_ref=PoolRef(
                chain_namespace=pool_ref.chain_namespace,
                chain_id=pool_ref.chain_id,
                program_id=pool_ref.program_id,
                pool_address="other-pool",
                protocol="meteora_dlmm",
                protocol_revision="fixture",
                asset_0_id="asset-a",
                asset_1_id="asset-b",
                pool_spec_version=1,
            ),
            vault_a_raw=1_000_000_000,
            vault_b_raw=1_000_000_000,
            protocol_fees_a_raw=0,
            protocol_fees_b_raw=0,
            fund_fees_a_raw=0,
            fund_fees_b_raw=0,
            creator_fees_a_raw=0,
            creator_fees_b_raw=0,
            trade_fee_rate=2_500,
            creator_fee_rate=120,
            protocol_fee_rate=120,
            fund_fee_rate=40,
            fee_on=0,
        )
        request = AmmPathRequest(
            schema_version=1,
            request_id="raydium-unsupported",
            reason="shadow_verification",
            priority="candidate",
            snapshot=AmmSnapshot(
                schema_version=snapshot.schema_version,
                snapshot_id="unsupported",
                worker_generation=1,
                source_epoch=1,
                boot_id="boot",
                model_version="unsupported",
                pool_refs=(unsupported.pool_ref,),
                dependency_vector=(),
                pools=(unsupported,),
                context_slot=1,
                chain_consistency="validated_multi_account_snapshot",
                state_valid_until_monotonic_ns=10_000_000_000,
            ),
            legs=(
                SwapLeg(
                    leg_id="leg-1",
                    pool_ref=unsupported.pool_ref,
                    input_asset_id="asset-a",
                    output_asset_id="asset-b",
                    mode="exact_in",
                    amount_source="literal",
                    amount_raw=1_000,
                    previous_leg_id=None,
                ),
            ),
            initial_balances=(("asset-a", 1_000),),
            scenario_id="unsupported",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)

        self.assertFalse(result.complete)
        self.assertEqual(result.status, "unsupported")
        self.assertIn("unsupported_protocol", result.reason or "")

    def test_raydium_evidence_round_trips_through_offline_replay(self) -> None:
        pool_ref = _pool_ref()
        request = _request((_leg(pool_ref),), (("asset-a", 1_000_000),))
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        bundle = build_evidence_bundle(request, result)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raydium-evidence.json"
            save_evidence_bundle(bundle, path)
            replayed = replay_evidence_bundle(load_evidence_bundle(path))

        self.assertEqual(replayed, result)

    def test_cli_replay_smoke(self) -> None:
        from io import StringIO
        from contextlib import redirect_stdout
        from market_data_lab.amm_simulation.cli import main as cli_main

        pool_ref = _pool_ref()
        request = _request((_leg(pool_ref),), (("asset-a", 1_000_000),))
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        bundle = build_evidence_bundle(request, result)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "raydium-evidence.json"
            save_evidence_bundle(bundle, path)
            stdout = StringIO()
            with redirect_stdout(stdout):
                exit_code = cli_main(["--summary", str(path)])

        self.assertEqual(exit_code, 0)
        self.assertIn('"status": "complete"', stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
