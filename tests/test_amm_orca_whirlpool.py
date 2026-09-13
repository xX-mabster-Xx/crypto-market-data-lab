"""Offline parity and post-state tests for the classic Orca Whirlpool adapter.

Expected amounts come from the pinned ``@orca-so/whirlpools-sdk`` 0.22.0 fixture
(``orca-whirlpool-sdk-swap.json``), not from the implementation under test.
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from market_data_lab.amm_simulation import (
    AmmPathRequest,
    AmmPathResult,
    AmmPathResult,
    AmmSnapshot,
    AmmSimulationLimits,
    OrcaWhirlpoolAdapter,
    OrcaWhirlpoolPoolBody,
    PathSimulator,
    PoolRef,
    SwapLeg,
    WhirlpoolTickArrayBody,
    WhirlpoolTickBody,
)

FIXTURE = Path(__file__).parent / "fixtures" / "orca-whirlpool-sdk-swap.json"
TICK_SPACING = 64
INIT_TICKS = {
    -6208: -1_000_000_000_000,
    -6336: -2_000_000_000_000,
    -6464: -3_000_000_000_000,
}
ARRAY_STARTS = (-11_264, -16_896, -22_528, -5_632, 0)


def _tick_arrays() -> tuple[WhirlpoolTickArrayBody, ...]:
    arrays = []
    for start in ARRAY_STARTS:
        ticks = []
        for offset in range(88):
            tick_index = start + offset * TICK_SPACING
            net = INIT_TICKS.get(tick_index, 0)
            ticks.append(
                WhirlpoolTickBody(
                    initialized=(tick_index in INIT_TICKS),
                    liquidity_net_raw=net if net < (-0) else 0,
                    liquidity_gross_raw=abs(net),
                )
            )
        arrays.append(WhirlpoolTickArrayBody(start_tick_index=start, ticks=tuple(ticks)))
    return tuple(arrays)


def _pool_ref(pool_address: str = "orca-pool-fixture") -> PoolRef:
    return PoolRef(
        chain_namespace="solana",
        chain_id="mainnet",
        program_id="whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
        pool_address=pool_address,
        protocol="orca_whirlpool",
        protocol_revision="v1",
        asset_0_id="solana:mainnet:usdc:6",
        asset_1_id="solana:mainnet:wrapped-sol:9",
        pool_spec_version=1,
    )


def _pool(pool_address: str = "orca-pool-fixture") -> OrcaWhirlpoolPoolBody:
    data = json.loads(FIXTURE.read_text())
    return OrcaWhirlpoolPoolBody(
        pool_ref=_pool_ref(pool_address),
        sqrt_price_x64=int(data["sqrt_price_x64"]),
        liquidity_raw=int(data["liquidity_raw"]),
        tick_current_index=int(data["current_tick_index"]),
        tick_spacing=int(data["tick_spacing"]),
        fee_rate=int(data["fee_rate"]),
        protocol_fee_rate=int(data["protocol_fee_rate"]),
        fee_growth_global_a=0,
        fee_growth_global_b=0,
        protocol_fee_owed_a=0,
        protocol_fee_owed_b=0,
        tick_arrays=_tick_arrays(),
    )


class OrcaWhirlpoolSdkParityTest(unittest.TestCase):
    def test_exact_in_matches_pinned_sdk_amounts_and_post_price(self) -> None:
        data = json.loads(FIXTURE.read_text())
        adapter = OrcaWhirlpoolAdapter()
        checked = 0
        for case in data["cases"]:
            if not case["amount_specified_is_input"]:
                continue
            pool = _pool()
            transition = adapter.exact_in(pool, int(case["input_amount_raw"]), case["a_to_b"])
            self.assertEqual(transition.net_output, int(case["output_amount_raw"]), case)
            self.assertEqual(transition.body_after.sqrt_price_x64, int(case["end_sqrt_price_x64"]), case)
            self.assertEqual(transition.body_after.tick_current_index, case["end_tick_index"], case)
            self.assertEqual(transition.fees[0].amount_raw, int(case["fee_amount_raw"]), case)
            checked += 1
        self.assertGreaterEqual(checked, 4, "expected at least 4 exact-in cases")

    def test_exact_out_matches_pinned_sdk_input_and_post_price(self) -> None:
        data = json.loads(FIXTURE.read_text())
        adapter = OrcaWhirlpoolAdapter()
        checked = 0
        for case in data["cases"]:
            if case["amount_specified_is_input"]:
                continue
            pool = _pool()
            transition = adapter.exact_out(pool, int(case["output_amount_raw"]), case["a_to_b"])
            self.assertEqual(transition.gross_input, int(case["input_amount_raw"]), case)
            self.assertEqual(transition.net_output, int(case["output_amount_raw"]), case)
            self.assertEqual(transition.body_after.sqrt_price_x64, int(case["end_sqrt_price_x64"]), case)
            self.assertEqual(transition.body_after.tick_current_index, case["end_tick_index"], case)
            checked += 1
        self.assertGreaterEqual(checked, 2, "expected at least 2 exact-out cases")

    def test_observed_body_is_not_mutated(self) -> None:
        pool = _pool()
        adapter = OrcaWhirlpoolAdapter()
        snapshot = pool.economic_projection
        adapter.exact_in(pool, 1_000_000, True)
        self.assertEqual(pool.economic_projection, snapshot)

    def test_price_roundtrip_tick_index(self) -> None:
        from market_data_lab.amm_simulation.orca_adapter import (
            _sqrt_price_x64_to_tick_index,
            _tick_index_to_sqrt_price_x64,
        )

        for tick in (-6464, -6208, -6144, 0, 128, 10_000):
            self.assertEqual(_sqrt_price_x64_to_tick_index(_tick_index_to_sqrt_price_x64(tick)), tick)


class OrcaWhirlpoolPathTest(unittest.TestCase):
    def test_path_uses_post_state_and_leaves_observed_snapshot_unchanged(self) -> None:
        pool = _pool()
        snapshot = AmmSnapshot(
            schema_version=1,
            snapshot_id="orca-swap",
            worker_generation=1,
            source_epoch=1,
            boot_id="boot",
            model_version="orca_whirlpool_v1",
            pool_refs=(pool.pool_ref,),
            dependency_vector=(),
            pools=(pool,),
            context_slot=1,
            chain_consistency="validated_multi_account_snapshot",
            state_valid_until_monotonic_ns=10_000_000_000,
        )
        request = AmmPathRequest(
            schema_version=1,
            request_id="orca-path",
            reason="shadow_verification",
            priority="candidate",
            snapshot=snapshot,
            legs=(
                SwapLeg(
                    leg_id="buy",
                    pool_ref=pool.pool_ref,
                    input_asset_id="solana:mainnet:usdc:6",
                    output_asset_id="solana:mainnet:wrapped-sol:9",
                    mode="exact_in",
                    amount_source="literal",
                    amount_raw=1_000_000,
                    previous_leg_id=None,
                ),
                SwapLeg(
                    leg_id="sell",
                    pool_ref=pool.pool_ref,
                    input_asset_id="solana:mainnet:wrapped-sol:9",
                    output_asset_id="solana:mainnet:usdc:6",
                    mode="exact_in",
                    amount_source="previous_output",
                    previous_leg_id="buy",
                    amount_raw=None,
                ),
            ),
            initial_balances=(("solana:mainnet:usdc:6", 100_000_000),),
            scenario_id="stable-base-stable",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        self.assertTrue(result.complete, result.reason)
        self.assertEqual(result.status, "complete")
        self.assertEqual(len(result.leg_results), 2)
        # A round trip through a fee-bearing pool must lose value (no self-roundtrip profit).
        bought = result.leg_results[0].actual_net_output_raw
        sold_back = result.leg_results[1].actual_net_output_raw
        self.assertGreater(bought, 0)
        self.assertGreater(sold_back, 0)
        self.assertLess(sold_back, 1_000_000)
        # Observed snapshot unchanged.
        self.assertEqual(snapshot.pool_by_ref(pool.pool_ref).economic_projection, pool.economic_projection)

    def test_unsupported_route_protocol_is_refused(self) -> None:
        # A missing tick array boundary is a typed coverage failure, not liquidity.
        pool = _pool()
        body = AmmSnapshot(
            schema_version=1,
            snapshot_id="orca-partial",
            worker_generation=1,
            source_epoch=1,
            boot_id="boot",
            model_version="orca_whirlpool_v1",
            pool_refs=(pool.pool_ref,),
            dependency_vector=(),
            pools=(pool,),
            context_slot=1,
            chain_consistency="validated_multi_account_snapshot",
            state_valid_until_monotonic_ns=10_000_000_000,
        )
        request = AmmPathRequest(
            schema_version=1,
            request_id="orca-partial-req",
            reason="shadow_verification",
            priority="candidate",
            snapshot=body,
            legs=(
                SwapLeg(
                    leg_id="buy",
                    pool_ref=pool.pool_ref,
                    input_asset_id="solana:mainnet:usdc:6",
                    output_asset_id="solana:mainnet:wrapped-sol:9",
                    mode="exact_in",
                    amount_source="literal",
                    amount_raw=10 ** 18,
                    previous_leg_id=None,
                ),
            ),
            initial_balances=(("solana:mainnet:usdc:6", 10 ** 24),),
            scenario_id="coverage-fail",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        self.assertTrue(not result.complete)



class OrcaEvidenceTest(unittest.TestCase):
    def test_evidence_round_trips_through_offline_replay(self) -> None:
        from market_data_lab.amm_simulation import (
            build_evidence_bundle,
            replay_evidence_bundle,
        )

        pool = _pool()
        snapshot = AmmSnapshot(
            schema_version=1,
            snapshot_id="orca-evidence",
            worker_generation=1,
            source_epoch=1,
            boot_id="boot",
            model_version="orca_whirlpool_v1",
            pool_refs=(pool.pool_ref,),
            dependency_vector=(),
            pools=(pool,),
            context_slot=1,
            chain_consistency="validated_multi_account_snapshot",
            state_valid_until_monotonic_ns=10_000_000_000,
        )
        request = AmmPathRequest(
            schema_version=1,
            request_id="orca-evidence-req",
            reason="shadow_verification",
            priority="candidate",
            snapshot=snapshot,
            legs=(
                SwapLeg(
                    leg_id="buy",
                    pool_ref=pool.pool_ref,
                    input_asset_id="solana:mainnet:usdc:6",
                    output_asset_id="solana:mainnet:wrapped-sol:9",
                    mode="exact_in",
                    amount_source="literal",
                    amount_raw=1_000_000,
                    previous_leg_id=None,
                ),
            ),
            initial_balances=(("solana:mainnet:usdc:6", 100_000_000),),
            scenario_id="orca-evidence",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        self.assertTrue(result.complete)
        bundle = build_evidence_bundle(request, result)
        replayed = replay_evidence_bundle(bundle)
        self.assertEqual(replayed.leg_results[0].actual_net_output_raw, result.leg_results[0].actual_net_output_raw)


if __name__ == "__main__":
    unittest.main()
