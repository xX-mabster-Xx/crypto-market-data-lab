from __future__ import annotations

import asyncio
import unittest

from market_data_lab.solana_quote_worker import _BoundedWorkerEventQueue


def pool_state(pool_id: str, sequence: int) -> dict[str, object]:
    return {
        "type": "pool_state",
        "protocol": "raydium_clmm",
        "pool_id": pool_id,
        "sequence": sequence,
    }


class BoundedWorkerEventQueueTest(unittest.IsolatedAsyncioTestCase):
    async def test_hot_pool_cannot_evict_single_cold_pool_update(self) -> None:
        queue = _BoundedWorkerEventQueue(capacity=2)
        await queue.put(pool_state("cold", 1))
        await queue.put(pool_state("hot", 1))

        for sequence in range(2, 1_001):
            await queue.put(pool_state("hot", sequence))

        self.assertEqual(queue.qsize(), 2)
        cold = await queue.get()
        hot = await queue.get()
        self.assertEqual(cold["pool_id"], "cold")
        self.assertEqual(cold["sequence"], 1)
        self.assertEqual(hot["pool_id"], "hot")
        self.assertEqual(hot["sequence"], 1_000)

    async def test_new_distinct_key_backpressures_until_capacity_is_free(self) -> None:
        queue = _BoundedWorkerEventQueue(capacity=1)
        await queue.put(pool_state("A", 1))

        blocked = asyncio.create_task(queue.put(pool_state("B", 1)))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())

        first = await queue.get()
        self.assertEqual(first["pool_id"], "A")
        await asyncio.wait_for(blocked, timeout=0.5)
        second = await queue.get()
        self.assertEqual(second["pool_id"], "B")

    async def test_refresh_health_is_latest_only(self) -> None:
        queue = _BoundedWorkerEventQueue(capacity=1)
        await queue.put({"type": "refresh_health", "status": "degraded"})
        await queue.put({"type": "refresh_health", "status": "healthy"})

        self.assertEqual(queue.qsize(), 1)
        item = await queue.get()
        self.assertEqual(item["status"], "healthy")

    async def test_unkeyed_messages_are_lossless_and_backpressured(self) -> None:
        queue = _BoundedWorkerEventQueue(capacity=1)
        first = RuntimeError("first")
        second = RuntimeError("second")
        await queue.put(first)

        blocked = asyncio.create_task(queue.put(second))
        await asyncio.sleep(0)
        self.assertFalse(blocked.done())

        self.assertIs(await queue.get(), first)
        await asyncio.wait_for(blocked, timeout=0.5)
        self.assertIs(await queue.get(), second)


if __name__ == "__main__":
    unittest.main()


class WorkerDispatchCompatibilityTest(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _worker(*, capacity: int = 2):
        from market_data_lab.solana_quote_worker import QuoteWorkerPool
        from market_data_lab.solana_quote_worker import RaydiumLocalQuoteWorker

        return RaydiumLocalQuoteWorker(
            rpc_http_url="http://localhost",
            rpc_ws_url="ws://localhost",
            pools=(QuoteWorkerPool("pool-a", "Pool A"),),
            event_capacity=capacity,
        )

    async def test_control_dispatch_keeps_synchronous_contract(self) -> None:
        worker = self._worker()
        worker._dispatch({"type": "worker_stats", "memory": {"rss_bytes": 123}})
        self.assertEqual(worker.latest_worker_stats["memory"]["rss_bytes"], 123)

        future = asyncio.get_running_loop().create_future()
        worker._pending_simulations["sim-1"] = future
        result = worker._dispatch(
            {"type": "simulate_path_result", "request_id": "sim-1", "status": "ok"}
        )
        self.assertIsNone(result)
        self.assertTrue(future.done())
        self.assertEqual(future.result()["status"], "ok")

    async def test_dispatch_backpressures_only_for_new_distinct_key(self) -> None:
        worker = self._worker(capacity=1)
        self.assertIsNone(worker._dispatch(pool_state("A", 1)))

        blocked = worker._dispatch(pool_state("B", 1))
        self.assertIsInstance(blocked, asyncio.Task)
        self.assertFalse(blocked.done())

        first = await worker.next_event()
        self.assertEqual(first["pool_id"], "A")
        await asyncio.wait_for(blocked, timeout=0.5)
        second = await worker.next_event()
        self.assertEqual(second["pool_id"], "B")
