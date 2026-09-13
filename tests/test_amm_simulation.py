from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from market_data_lab.amm_simulation import (
    AmmPathRequest,
    AmmSnapshot,
    AssetRef,
    CpmmPoolBody,
    PathSimulator,
    PoolRef,
    SwapLeg,
    build_evidence_bundle,
    load_evidence_bundle,
    replay_evidence_bundle,
    save_evidence_bundle,
)


def _assets() -> tuple[AssetRef, AssetRef]:
    common = {
        "chain_namespace": "synthetic",
        "chain_id": "fixture",
        "decimals": 0,
        "token_program": "synthetic-token",
        "token_extensions_fingerprint": "none",
        "spec_version": 1,
    }
    return (
        AssetRef(asset_id="synthetic:fixture:A", address_or_native_id="A", **common),
        AssetRef(asset_id="synthetic:fixture:B", address_or_native_id="B", **common),
    )


def _pool(asset_a: AssetRef, asset_b: AssetRef) -> PoolRef:
    return PoolRef(
        chain_namespace="synthetic",
        chain_id="fixture",
        program_id="synthetic-cpmm",
        pool_address="pool-1",
        protocol="synthetic_cpmm_v1",
        protocol_revision="v1",
        asset_0_id=asset_a.asset_id,
        asset_1_id=asset_b.asset_id,
        pool_spec_version=1,
    )


def _snapshot(pool_ref: PoolRef, *, numerator: int = 0) -> AmmSnapshot:
    return AmmSnapshot(
        schema_version=1,
        snapshot_id="snapshot-1",
        worker_generation=1,
        source_epoch=0,
        boot_id="boot-1",
        model_version="synthetic_cpmm_v1",
        pool_refs=(pool_ref,),
        dependency_vector=(),
        pools=(CpmmPoolBody(pool_ref=pool_ref, reserve_0_raw=1000, reserve_1_raw=1000, fee_numerator=numerator, fee_denominator=100),),
        context_slot=1,
        chain_consistency="validated_multi_account_snapshot",
        state_valid_until_monotonic_ns=1,
    )


def _leg(pool_ref: PoolRef, asset_a: AssetRef, asset_b: AssetRef, **overrides) -> SwapLeg:
    values = {
        "leg_id": "leg-1",
        "pool_ref": pool_ref,
        "input_asset_id": asset_a.asset_id,
        "output_asset_id": asset_b.asset_id,
        "mode": "exact_in",
        "amount_source": "literal",
        "amount_raw": 100,
        "previous_leg_id": None,
    }
    values.update(overrides)
    return SwapLeg(**values)


def _request(snapshot: AmmSnapshot, legs: tuple[SwapLeg, ...], balances: tuple[tuple[str, int], ...]) -> AmmPathRequest:
    return AmmPathRequest(
        schema_version=1,
        request_id="request-1",
        reason="acceptance",
        priority="test",
        snapshot=snapshot,
        legs=legs,
        initial_balances=balances,
        scenario_id="golden",
        deadline_monotonic_ns=None,
    )


class AmmSimulationTest(unittest.TestCase):
    def test_as01_t09_synthetic_sequential_roundtrip(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref)
        first = _leg(pool_ref, asset_a, asset_b)
        second = _leg(
            pool_ref,
            asset_b,
            asset_a,
            leg_id="leg-2",
            input_asset_id=asset_b.asset_id,
            output_asset_id=asset_a.asset_id,
            amount_source="previous_output",
            amount_raw=None,
            previous_leg_id="leg-1",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(
            _request(snapshot, (first, second), ((asset_a.asset_id, 100),)),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.reason, "all legs completed")
        self.assertEqual([item.actual_net_output_raw for item in result.leg_results], [90, 99])
        self.assertEqual(result.leg_results[0].reserve_0_after_raw, 1100)
        self.assertEqual(result.leg_results[0].reserve_1_after_raw, 910)
        self.assertEqual(result.leg_results[1].reserve_0_after_raw, 1001)
        self.assertEqual(result.leg_results[1].reserve_1_after_raw, 1000)
        self.assertEqual(dict(result.final_balances), {asset_a.asset_id: 99, asset_b.asset_id: 0})

    def test_as02_t09_fee_case_and_invariant(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref, numerator=1)
        first = _leg(pool_ref, asset_a, asset_b)
        second = _leg(
            pool_ref,
            asset_b,
            asset_a,
            leg_id="leg-2",
            input_asset_id=asset_b.asset_id,
            output_asset_id=asset_a.asset_id,
            amount_source="previous_output",
            amount_raw=None,
            previous_leg_id="leg-1",
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(
            _request(snapshot, (first, second), ((asset_a.asset_id, 100),)),
        )

        self.assertTrue(result.complete)
        self.assertEqual([item.actual_net_output_raw for item in result.leg_results], [90, 97])
        self.assertEqual([item.fee_amount_raw for item in result.leg_results], [1, 1])
        self.assertEqual(result.leg_results[1].reserve_0_after_raw, 1003)
        self.assertEqual(result.leg_results[1].reserve_1_after_raw, 1000)

    def test_as03_exact_out_requires_99_not_98(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref)
        leg = _leg(
            pool_ref,
            asset_a,
            asset_b,
            mode="exact_out",
            amount_raw=90,
            maximum_gross_input_raw=100,
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(
            _request(snapshot, (leg,), ((asset_a.asset_id, 100),)),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.leg_results[0].actual_gross_input_raw, 99)
        self.assertEqual(result.leg_results[0].actual_net_output_raw, 90)

    def test_as04_observed_snapshot_is_not_mutated_and_paths_are_isolated(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref)
        leg = _leg(pool_ref, asset_a, asset_b)
        simulator = PathSimulator(monotonic_ns=lambda: 0)
        first = simulator.simulate(_request(snapshot, (leg,), ((asset_a.asset_id, 100),)))
        second = simulator.simulate(_request(snapshot, (leg,), ((asset_a.asset_id, 100),)))

        self.assertEqual(snapshot.pools[0].reserve_0_raw, 1000)
        self.assertEqual(snapshot.pools[0].reserve_1_raw, 1000)
        self.assertEqual(first.leg_results[0].state_after_hash, second.leg_results[0].state_after_hash)

    def test_as05_insufficient_balance_fails_whole_path(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref)
        leg = _leg(pool_ref, asset_a, asset_b)
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(
            _request(snapshot, (leg,), ((asset_a.asset_id, 99),)),
        )

        self.assertFalse(result.complete)
        self.assertEqual(result.status, "insufficient_balance")
        self.assertEqual(result.reason, "initial scenario balance does not cover the leg")
        self.assertEqual(result.failed_leg_id, "leg-1")

    def test_as06_evidence_replays_offline(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        snapshot = _snapshot(pool_ref)
        request = _request(snapshot, (_leg(pool_ref, asset_a, asset_b),), ((asset_a.asset_id, 100),))
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        bundle = build_evidence_bundle(request, result)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            save_evidence_bundle(bundle, path)
            loaded = load_evidence_bundle(path)
            replayed = replay_evidence_bundle(loaded)
            self.assertEqual(replayed, result)

    def test_as_property_seeded_token_conservation_and_determinism(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        simulator = PathSimulator(monotonic_ns=lambda: 0)
        # Fixed seed so the property loop is reproducible without new deps.
        state = {"seed": 0x5EED}
        for iteration in range(8):
            state["x"] = (state["seed"] + iteration * 0x9E3779B9) & 0xFFFFFFFF
            state["y"] = (state["x"] ^ 0x5DEECE66D) & 0xFFFFFFFF
            input_amount = 100 + (state["x"] % 400)
            reserves = [1_000 + (state["y"] % 9_000), 1_000 + (state["x"] % 7_000)]
            snapshot = AmmSnapshot(
                schema_version=1,
                snapshot_id="snapshot-prop",
                worker_generation=1,
                source_epoch=0,
                boot_id="boot-prop",
                model_version="synthetic_cpmm_v1",
                pool_refs=(pool_ref,),
                dependency_vector=(),
                pools=(CpmmPoolBody(
                    pool_ref=pool_ref,
                    reserve_0_raw=reserves[0],
                    reserve_1_raw=reserves[1],
                    fee_numerator=0,
                    fee_denominator=100,
                ),),
                context_slot=1,
                chain_consistency="validated_multi_account_snapshot",
                state_valid_until_monotonic_ns=1,
            )
            leg = _leg(pool_ref, asset_a, asset_b, amount_raw=input_amount)
            first = simulator.simulate(_request(snapshot, (leg,), ((asset_a.asset_id, input_amount),)))
            # Token conservation: input debit equals gross input, output is net.
            self.assertEqual(
                dict(first.final_balances)[asset_b.asset_id],
                first.leg_results[0].actual_net_output_raw,
            )
            # Pool conservation for the synthetic model (a→b): A gains the full
            # gross input, B loses exactly the gross pool output.
            self.assertEqual(
                first.leg_results[0].reserve_0_after_raw,
                reserves[0] + first.leg_results[0].actual_gross_input_raw,
            )
            self.assertEqual(
                first.leg_results[0].reserve_1_after_raw,
                reserves[1] - first.leg_results[0].gross_pool_output_raw,
            )
            # Determinism across an independent identical request.
            second = simulator.simulate(_request(snapshot, (leg,), ((asset_a.asset_id, input_amount),)))
            self.assertEqual(first.leg_results[0].state_after_hash, second.leg_results[0].state_after_hash)
            # No negative reserves in the projected pool.
            self.assertGreater(first.leg_results[0].reserve_0_after_raw, 0)
            self.assertGreater(first.leg_results[0].reserve_1_after_raw, 0)

    def test_as_property_zero_fee_no_positive_self_roundtrip(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        simulator = PathSimulator(monotonic_ns=lambda: 0)
        for input_amount in (10, 50, 100, 333):
            snapshot = _snapshot(pool_ref, numerator=0)
            first = _leg(pool_ref, asset_a, asset_b, amount_raw=input_amount)
            second = _leg(
                pool_ref,
                asset_b,
                asset_a,
                leg_id="leg-2",
                input_asset_id=asset_b.asset_id,
                output_asset_id=asset_a.asset_id,
                amount_source="previous_output",
                amount_raw=None,
                previous_leg_id="leg-1",
            )
            result = simulator.simulate(
                _request(snapshot, (first, second), ((asset_a.asset_id, input_amount),)),
            )
            # Zero-fee roundtrip back to the same reserve ratio never returns a
            # surplus of A: price impact and rounding make it strictly smaller.
            self.assertLess(dict(result.final_balances)[asset_a.asset_id], input_amount)

    def test_as_property_increasing_fee_does_not_increase_output(self) -> None:
        asset_a, asset_b = _assets()
        pool_ref = _pool(asset_a, asset_b)
        simulator = PathSimulator(monotonic_ns=lambda: 0)
        input_amount = 100
        outputs = []
        for numerator in (0, 1, 5, 25):
            snapshot = _snapshot(pool_ref, numerator=numerator)
            leg = _leg(pool_ref, asset_a, asset_b, amount_raw=input_amount)
            result = simulator.simulate(
                _request(snapshot, (leg,), ((asset_a.asset_id, input_amount),)),
            )
            outputs.append(result.leg_results[0].actual_net_output_raw)
        self.assertEqual(outputs, sorted(outputs, reverse=True))


if __name__ == "__main__":
    unittest.main()
