from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.amm_simulation.backend import WorkerBackend


class StubWorker:
    def __init__(self) -> None:
        self.capture_calls: list[dict[str, object]] = []
        self.simulate_calls: list[dict[str, object]] = []
        self.capture_result: dict[str, object] = {"status": "ok", "snapshot_token": "tok-1"}
        self.simulate_result: dict[str, object] = {"status": "complete", "complete": True}

    async def capture_snapshot(self, **kwargs: object) -> dict[str, object]:
        self.capture_calls.append(dict(kwargs))
        return self.capture_result

    async def simulate_path(self, **kwargs: object) -> dict[str, object]:
        self.simulate_calls.append(dict(kwargs))
        return self.simulate_result


class WorkerBackendTest(unittest.IsolatedAsyncioTestCase):
    async def test_capture_snapshot_passes_pool_ids_as_tuple(self) -> None:
        stub = StubWorker()
        backend = WorkerBackend(worker=stub)  # type: ignore[arg-type]
        result = await backend.capture_snapshot(
            request_id="sim-1",
            pool_ids=frozenset({"abc", "def"}),
            required_consistency="validated_multi_account_snapshot",
        )
        self.assertEqual(result, {"status": "ok", "snapshot_token": "tok-1"})
        self.assertEqual(len(stub.capture_calls), 1)
        kwargs = stub.capture_calls[0]
        self.assertEqual(kwargs["request_id"], "sim-1")
        self.assertEqual(set(kwargs["pool_ids"]), {"abc", "def"})
        self.assertEqual(kwargs["required_consistency"], "validated_multi_account_snapshot")

    async def test_simulate_path_passes_legs_through(self) -> None:
        stub = StubWorker()
        backend = WorkerBackend(worker=stub)  # type: ignore[arg-type]
        legs = ({"pool_id": "abc", "amount_raw": 100},)
        result = await backend.simulate_path(
            request_id="sim-2",
            snapshot_token="tok-1",
            legs=legs,
        )
        self.assertTrue(result["complete"])
        self.assertEqual(len(stub.simulate_calls), 1)
        kwargs = stub.simulate_calls[0]
        self.assertEqual(kwargs["request_id"], "sim-2")
        self.assertEqual(kwargs["snapshot_token"], "tok-1")
        self.assertEqual(kwargs["legs"], legs)


def _worker_bundle() -> dict[str, object]:
    return {
        "schema_version": 1,
        "snapshot_id": "snapshot-req-seq",
        "worker_generation": 3,
        "source_epoch": 0,
        "boot_id": "boot-seq",
        "model_version": "raydium_cpmm_v1",
        "pool_refs": [
            {
                "chain_namespace": "solana",
                "chain_id": "mainnet",
                "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
                "pool_address": "cpmm-pool",
                "protocol": "raydium_cpmm",
                "protocol_revision": "v1",
                "asset_0_id": "solana:mainnet:mint-a:6",
                "asset_1_id": "solana:mainnet:mint-b:9",
                "pool_spec_version": 1,
            }
        ],
        "dependency_vector": [],
        "pools": [
            {
                "pool_id": "solana:mainnet:cpmm-pool",
                "vault_a_raw": "1000000000",
                "vault_b_raw": "1000000000",
                "protocol_fees_a_raw": "0",
                "protocol_fees_b_raw": "0",
                "fund_fees_a_raw": "0",
                "fund_fees_b_raw": "0",
                "creator_fees_a_raw": "0",
                "creator_fees_b_raw": "0",
                "trade_fee_rate": "2500",
                "creator_fee_rate": "120",
                "protocol_fee_rate": "120",
                "fund_fee_rate": "40",
                "fee_on": "0",
            }
        ],
        "context_slot": 50,
        "chain_consistency": "validated_multi_account_snapshot",
        "sdk_versions": [["@raydium-io/raydium-sdk-v2", "latest"]],
    }


class WorkerBackendSequentialTest(unittest.IsolatedAsyncioTestCase):
    async def test_decode_and_run_sequential_buy_sell_on_post_state(self) -> None:
        from market_data_lab.amm_simulation.backend import decode_worker_snapshot, run_sequential_path

        snapshot = decode_worker_snapshot(_worker_bundle())
        pool_ref = snapshot.pools[0].pool_ref
        execution = run_sequential_path(
            snapshot,
            pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
            buy_stable_raw=1_000_000,
        )
        self.assertTrue(execution.simulation_complete, execution.reason)
        self.assertGreater(execution.sell_net_base_raw, 0)
        self.assertLess(execution.final_stable, execution.initial_stable)
        self.assertGreater(execution.final_stable, Decimal("0"))

    async def test_mismatched_base_asset_is_rejected(self) -> None:
        from market_data_lab.amm_simulation.backend import decode_worker_snapshot, run_sequential_path

        snapshot = decode_worker_snapshot(_worker_bundle())
        pool_ref = snapshot.pools[0].pool_ref
        with self.assertRaisesRegex(ValueError, "does not match"):
            run_sequential_path(
                snapshot,
                pool_ref=pool_ref,
                stable_asset_id="solana:mainnet:mint-a:6",
                base_asset_id="solana:mainnet:wrong:9",
                stable_decimals=6,
                buy_stable_raw=1_000_000,
            )


if __name__ == "__main__":
    unittest.main()


class WorkerSequentialSimulatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_simulator_runs_buy_sell_from_cached_snapshot(self) -> None:
        from market_data_lab.amm_simulation.backend import (
            WorkerSequentialSimulator,
            decode_worker_snapshot,
        )

        snapshot = decode_worker_snapshot(_worker_bundle())
        pool_ref = snapshot.pools[0].pool_ref

        class Quote:
            input_amount_raw = 1_000_000

        simulator = WorkerSequentialSimulator(
            snapshot=snapshot,
            pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
        )
        result = simulator.simulate_buy_sell(Quote())
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.provider, "raydium_cpmm")
        self.assertLess(result.final_stable, result.initial_stable)
        self.assertEqual(simulator.calls, 1)

    async def test_simulator_returns_none_when_amount_missing(self) -> None:
        from market_data_lab.amm_simulation.backend import (
            WorkerSequentialSimulator,
            decode_worker_snapshot,
        )

        snapshot = decode_worker_snapshot(_worker_bundle())
        pool_ref = snapshot.pools[0].pool_ref

        class EmptyQuote:
            input_amount_raw = None

        simulator = WorkerSequentialSimulator(
            snapshot=snapshot,
            pool_ref=pool_ref,
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
        )
        self.assertIsNone(simulator.simulate_buy_sell(EmptyQuote()))


if __name__ == "__main__":
    unittest.main()


class LazySnapshotSimulatorTest(unittest.IsolatedAsyncioTestCase):
    async def test_initialize_captures_and_runs_path(self) -> None:
        from market_data_lab.amm_simulation.backend import LazySnapshotSequentialSimulator

        class StubSource:
            async def capture_snapshot(self, **kwargs: object) -> dict[str, object]:
                return {"status": "ok", "snapshot_token": "t", "snapshot": _worker_bundle()}

        simulator = LazySnapshotSequentialSimulator(
            source=StubSource(),
            pool_id="solana:mainnet:cpmm-pool",
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
        )
        self.assertIsNone(simulator.simulate_buy_sell(object()))

        class Quote:
            input_amount_raw = 1_000_000

        await simulator.initialize()
        result = simulator.simulate_buy_sell(Quote())
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.pool_id, "solana:mainnet:cpmm-pool")

    async def test_initialize_reports_capture_failure(self) -> None:
        from market_data_lab.amm_simulation.backend import LazySnapshotSequentialSimulator

        class FailingSource:
            async def capture_snapshot(self, **kwargs: object) -> dict[str, object]:
                return {"status": "unsupported", "reason": "dep_lock"}

        simulator = LazySnapshotSequentialSimulator(
            source=FailingSource(),
            pool_id="solana:mainnet:cpmm-pool",
            stable_asset_id="solana:mainnet:mint-a:6",
            base_asset_id="solana:mainnet:mint-b:9",
            stable_decimals=6,
        )
        await simulator.initialize()
        descriptor = simulator.describe()
        self.assertEqual(simulator.simulate_buy_sell(object()), None)
        self.assertIn("snapshot capture failed", (descriptor.get("error") or ""))


if __name__ == "__main__":
    unittest.main()
