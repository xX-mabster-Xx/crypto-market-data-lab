from __future__ import annotations

import asyncio
from types import SimpleNamespace
import unittest

from market_data_lab.solana_quote_worker import QuoteWorkerPool
from market_data_lab.solana_quote_worker import RaydiumLocalQuoteWorker


class RaydiumLocalQuoteWorkerSimulationTest(unittest.IsolatedAsyncioTestCase):
    async def _worker(self) -> RaydiumLocalQuoteWorker:
        return RaydiumLocalQuoteWorker(
            rpc_http_url="https://rpc.example",
            rpc_ws_url="wss://rpc.example",
            pools=(QuoteWorkerPool("pool-one", "POOL"),),
            event_capacity=64,
        )

    async def test_snapshot_result_dispatches_to_pending_simulation(self) -> None:
        worker = await self._worker()
        future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        worker._pending_simulations["sim-1"] = future  # type: ignore[attr-defined]
        worker._dispatch({  # type: ignore[attr-defined]
            "type": "snapshot_result",
            "request_id": "sim-1",
            "status": "ok",
            "snapshot_token": "snapshot-7",
        })

        result = await asyncio.wait_for(future, timeout=1)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["snapshot_token"], "snapshot-7")

    async def test_simulate_path_result_dispatches_to_pending_simulation(self) -> None:
        worker = await self._worker()
        future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        worker._pending_simulations["sim-2"] = future  # type: ignore[attr-defined]
        worker._dispatch({  # type: ignore[attr-defined]
            "type": "simulate_path_result",
            "request_id": "sim-2",
            "status": "complete",
            "complete": True,
            "leg_results": [],
        })

        result = await asyncio.wait_for(future, timeout=1)
        self.assertTrue(result["complete"])

    async def test_late_simulation_result_is_counted_and_dropped(self) -> None:
        worker = await self._worker()
        before = worker._late_simulation_results_dropped  # type: ignore[attr-defined]
        worker._dispatch({  # type: ignore[attr-defined]
            "type": "simulate_path_result",
            "request_id": "obsolete",
            "status": "complete",
        })
        self.assertEqual(worker._late_simulation_results_dropped, before + 1)  # type: ignore[attr-defined]

    async def test_worker_error_fails_pending_simulation_future(self) -> None:
        worker = await self._worker()
        future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        worker._pending_simulations["sim-3"] = future  # type: ignore[attr-defined]
        worker._dispatch({"type": "worker_error", "request_id": "sim-3", "error": "boom"})  # type: ignore[attr-defined]

        with self.assertRaises(RuntimeError):
            await asyncio.wait_for(future, timeout=1)

    async def test_cancel_settles_original_waiter_and_drops_late_complete(self) -> None:
        worker = await self._worker()
        worker._process = SimpleNamespace(returncode=None)  # type: ignore[attr-defined]
        simulation_future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        worker._pending_simulations["sim-cancel"] = simulation_future  # type: ignore[attr-defined]

        async def fake_send(payload: dict[str, object]) -> None:
            self.assertEqual(payload["type"], "cancel_simulation")
            worker._dispatch({  # type: ignore[attr-defined]
                "type": "cancel_simulation_result",
                "request_id": "sim-cancel",
                "status": "ok",
            })

        worker._send = fake_send  # type: ignore[method-assign]
        cancellation = await worker.cancel_simulation(request_id="sim-cancel")
        self.assertEqual(cancellation["status"], "ok")
        self.assertEqual((await simulation_future)["status"], "canceled")
        before = worker._late_simulation_results_dropped  # type: ignore[attr-defined]
        worker._dispatch({  # type: ignore[attr-defined]
            "type": "simulate_path_result",
            "request_id": "sim-cancel",
            "status": "complete",
            "complete": True,
        })
        self.assertEqual(worker._late_simulation_results_dropped, before + 1)  # type: ignore[attr-defined]
        self.assertNotIn("sim-cancel", worker._pending_cancellations)  # type: ignore[attr-defined]
        reused_cancel = await worker.cancel_simulation(request_id="sim-cancel")
        self.assertEqual(reused_cancel["status"], "ok")


class RaydiumLocalQuoteWorkerEvidenceTest(unittest.IsolatedAsyncioTestCase):
    async def _worker(self) -> RaydiumLocalQuoteWorker:
        return RaydiumLocalQuoteWorker(
            rpc_http_url="https://rpc.example",
            rpc_ws_url="wss://rpc.example",
            pools=(QuoteWorkerPool("pool-one", "POOL"),),
            event_capacity=64,
        )

    async def test_export_evidence_dispatches_and_resolves_pending_future(self) -> None:
        worker = await self._worker()
        future: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()
        worker._pending_simulations["ev-1"] = future  # type: ignore[attr-defined]
        worker._dispatch({  # type: ignore[attr-defined]
            "type": "simulation_evidence_result",
            "request_id": "ev-1",
            "status": "ok",
            "snapshot_token": "snap-9",
            "evidence": {"schema_version": 1},
        })

        result = await asyncio.wait_for(future, timeout=1)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["evidence"]["schema_version"], 1)


class RaydiumLocalQuoteWorkerPathRequestTest(unittest.IsolatedAsyncioTestCase):
    async def test_simulate_path_sends_balances_and_exact_decimal_deadline(self) -> None:
        worker = RaydiumLocalQuoteWorker(
            rpc_http_url="https://rpc.example",
            rpc_ws_url="wss://rpc.example",
            pools=(QuoteWorkerPool("pool-one", "POOL"),),
            event_capacity=64,
        )
        worker._process = SimpleNamespace(returncode=None)  # type: ignore[attr-defined]
        sent: dict[str, object] = {}

        async def fake_send(payload: dict[str, object]) -> None:
            sent.update(payload)
            worker._dispatch({
                "type": "simulate_path_result",
                "request_id": payload["request_id"],
                "status": "complete",
                "complete": True,
            })

        worker._send = fake_send  # type: ignore[method-assign]
        result = await worker.simulate_path(
            request_id="sim-request",
            snapshot_token="snapshot-token",
            legs=({"leg_id": "leg", "mode": "exact_in"},),
            initial_balances=({"asset_id": "asset-a", "amount_raw": 10**30},),
            deadline_monotonic_ns="9007199254740993123",
        )
        self.assertTrue(result["complete"])
        self.assertEqual(sent["deadline_monotonic_ns"], "9007199254740993123")
        self.assertEqual(sent["initial_balances"], [{"asset_id": "asset-a", "amount_raw": "1000000000000000000000000000000"}])

    async def test_simulate_path_rejects_missing_or_duplicate_balances(self) -> None:
        worker = RaydiumLocalQuoteWorker(
            rpc_http_url="https://rpc.example",
            rpc_ws_url="wss://rpc.example",
            pools=(QuoteWorkerPool("pool-one", "POOL"),),
            event_capacity=64,
        )
        worker._process = SimpleNamespace(returncode=None)  # type: ignore[attr-defined]
        with self.assertRaisesRegex(ValueError, "non-empty initial_balances"):
            await worker.simulate_path(
                request_id="missing",
                snapshot_token="snapshot-token",
                legs=({"leg_id": "leg"},),
                initial_balances=(),
            )
        with self.assertRaisesRegex(ValueError, "duplicate asset_id"):
            await worker.simulate_path(
                request_id="duplicate",
                snapshot_token="snapshot-token",
                legs=({"leg_id": "leg"},),
                initial_balances=(
                    {"asset_id": "asset-a", "amount_raw": "1"},
                    {"asset_id": "asset-a", "amount_raw": "2"},
                ),
            )


if __name__ == "__main__":
    unittest.main()
