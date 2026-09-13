"""Focused broker, TTL and evidence acceptance tests for the CPMM slice."""

from __future__ import annotations

import inspect
import tempfile
import unittest
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

from market_data_lab.amm_simulation import (
    AmmPathResult,
    AmmSimulationLimits,
    PathSimulator,
    build_evidence_bundle,
    load_evidence_bundle,
    replay_evidence_bundle,
    save_evidence_bundle,
)
from market_data_lab.quote_broker import (
    QuoteBroker,
    QuoteBudgetPolicy,
    QuoteFailurePolicy,
    SharedQuoteBudgetManager,
)

from tests.test_amm_simulation import _request, _snapshot


class CpmmBrokerEvidenceTest(unittest.IsolatedAsyncioTestCase):
    def test_typed_api_is_not_shadowed_and_legacy_api_remains(self) -> None:
        typed = inspect.signature(QuoteBroker.simulate_amm_path)
        legacy = inspect.signature(QuoteBroker.simulate_local_path)
        self.assertIn("request", typed.parameters)
        self.assertIn("deadline_monotonic_ns", typed.parameters)
        self.assertEqual(legacy.parameters["request"].name, "request")

    def test_expired_snapshot_has_typed_live_status(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        result = PathSimulator(monotonic_ns=lambda: 2_000).simulate(request)
        self.assertFalse(result.complete)
        self.assertEqual(result.status, "state_unavailable")

    def test_expired_historical_snapshot_requires_explicit_replay(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        live = PathSimulator(monotonic_ns=lambda: 2_000).simulate(request)
        self.assertEqual(live.status, "state_unavailable")
        replayed = PathSimulator(monotonic_ns=lambda: 2_000).simulate(request, replay=True)
        self.assertTrue(replayed.complete)

    def test_evidence_roundtrip_and_tamper_rejection(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        bundle = build_evidence_bundle(request, result)
        self.assertEqual(bundle.snapshot["boot_id"], snapshot.boot_id)
        self.assertEqual(bundle.snapshot["worker_generation"], snapshot.worker_generation)
        self.assertEqual(bundle.snapshot["source_epoch"], snapshot.source_epoch)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            save_evidence_bundle(bundle, path)
            loaded = load_evidence_bundle(path)
        self.assertIsInstance(replay_evidence_bundle(loaded), AmmPathResult)
        for component, key, value in (
            ("request", "request_id", "tampered-request"),
            ("snapshot", "snapshot_id", "tampered-snapshot"),
            ("expected_result", "status", "tampered-result"),
        ):
            tampered = replace(
                loaded,
                request=deepcopy(loaded.request),
                snapshot=deepcopy(loaded.snapshot),
                expected_result=deepcopy(loaded.expected_result),
            )
            getattr(tampered, component)[key] = value
            with self.assertRaises(ValueError):
                replay_evidence_bundle(tampered)

    def test_evidence_size_cap_is_enforced_on_canonical_wire_payload(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        request = replace(
            request,
            limits=AmmSimulationLimits(max_evidence_bundle_bytes=1),
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        with self.assertRaisesRegex(ValueError, "max_evidence_bundle_bytes"):
            build_evidence_bundle(request, result)

    def test_result_evidence_hash_is_not_recursive(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        bundle = build_evidence_bundle(
            request,
            replace(result, evidence_hash="previous-or-external-hash"),
        )
        self.assertIsNone(bundle.expected_result["evidence_hash"])
        self.assertEqual(bundle.evidence_hash, build_evidence_bundle(request, result).evidence_hash)

    def test_evidence_rejects_result_from_another_worker_incarnation(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        result = PathSimulator(monotonic_ns=lambda: 0).simulate(request)
        with self.assertRaisesRegex(ValueError, "provenance"):
            build_evidence_bundle(
                request,
                replace(result, worker_generation=result.worker_generation + 1),
            )

    async def test_amm_path_does_not_consume_remote_budget(self) -> None:
        from tests.test_amm_simulation import _leg

        snapshot, pool_ref, asset_a, asset_b = _snapshot_fixture()
        request = _request(
            snapshot,
            (_leg(pool_ref, asset_a, asset_b),),
            ((asset_a.asset_id, 100),),
        )
        budgets = SharedQuoteBudgetManager(
            {"vendor": QuoteBudgetPolicy(
                max_concurrency=1,
                max_requests=1,
            )},
        )
        broker = QuoteBroker(
            backends={},
            budgets=budgets,
            failure_policy=QuoteFailurePolicy(),
            monotonic_ns=lambda: 0,
        )
        await broker.simulate_amm_path(request)
        self.assertEqual(budgets.snapshot(now_ns=0)["domains"]["vendor"]["total_started"], 0)


def _snapshot_fixture():
    from tests.test_amm_simulation import _assets, _pool

    asset_a, asset_b = _assets()
    pool_ref = _pool(asset_a, asset_b)
    snapshot = _snapshot(pool_ref)
    return snapshot, pool_ref, asset_a, asset_b
