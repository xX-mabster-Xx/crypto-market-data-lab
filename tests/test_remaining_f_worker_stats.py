"""Agent F -- worker_stats observability integration tests.

Verifies:
- Python wrapper stores latest validated stats under a stable key.
- Latest cardinality is 1 (repeated stats replace previous).
- Stats message contains all required fields with correct types.
- Stale instance stats cleared on close/restart.
- Legacy absent/malformed stats are handled gracefully.
- worker_stats_result routed to pending simulation future.
"""
from __future__ import annotations

import asyncio
import unittest
from market_data_lab.solana_quote_worker import QuoteWorkerPool


def _make_worker(pools=None):
    """Create a RaydiumLocalQuoteWorker without starting a subprocess."""
    from market_data_lab.solana_quote_worker import RaydiumLocalQuoteWorker

    if pools is None:
        pools = (QuoteWorkerPool("pool-1", "Pool 1"),)
    worker = RaydiumLocalQuoteWorker(
        rpc_http_url="https://example.com/rpc",
        rpc_ws_url="",
        pools=pools,
    )
    worker._closed = True
    return worker


class WorkerStatsDispatchTest(unittest.TestCase):
    """Verify _dispatch routes worker_stats messages to latest_stats."""

    def test_f01_worker_stats_stored_as_latest(self) -> None:
        worker = _make_worker()
        message = {
            "type": "worker_stats",
            "boot_id": "boot-f",
            "source_epoch": 1,
            "memory": {
                "rss_bytes": 1000,
                "heap_total_bytes": 2000,
                "heap_used_bytes": 1500,
                "external_bytes": 500,
                "array_buffers_bytes": 100,
                "uptime_seconds": 60,
            },
            "stdout": {
                "blocked": False,
                "lossless_queue_size": 0,
                "state_pending_keys": 0,
                "state_coalesced_total": 0,
            },
            "rpc": {
                "queue_total": 0,
                "queue_interactive": 0,
                "queue_bootstrap": 0,
                "queue_refresh": 0,
                "active": 0,
                "queue_high_watermark": 0,
            },
            "pools": {},
            "sampled_at_iso": "2026-01-01T00:00:00.000Z",
        }
        worker._dispatch(message)
        assert worker.latest_worker_stats is not None
        assert isinstance(worker.latest_worker_stats, dict)

    def test_f02_repeated_stats_replace_previous_latest(self) -> None:
        worker = _make_worker()
        for i in range(100):
            worker._dispatch({
                "type": "worker_stats",
                "boot_id": "boot-f",
                "source_epoch": i,
                "memory": {
                    "rss_bytes": i,
                    "heap_total_bytes": 0,
                    "heap_used_bytes": 0,
                    "external_bytes": 0,
                    "array_buffers_bytes": 0,
                    "uptime_seconds": 0,
                },
                "stdout": {
                    "blocked": False,
                    "lossless_queue_size": 0,
                    "state_pending_keys": 0,
                    "state_coalesced_total": 0,
                },
                "rpc": {
                    "queue_total": 0,
                    "queue_interactive": 0,
                    "queue_bootstrap": 0,
                    "queue_refresh": 0,
                    "active": 0,
                    "queue_high_watermark": 0,
                },
                "pools": {},
                "sampled_at_iso": "2026-01-01T00:00:00.000Z",
            })
        # Only the latest should be stored — cardinality is 1
        assert worker.latest_worker_stats is not None
        assert worker.latest_worker_stats["source_epoch"] == 99

    def test_f03_missing_stats_field_does_not_crash(self) -> None:
        worker = _make_worker()
        worker._dispatch({"type": "worker_stats"})
        assert worker.latest_worker_stats is None

    def test_f04_stale_stats_cleared_on_close(self) -> None:
        worker = _make_worker()
        worker._dispatch({
            "type": "worker_stats",
            "boot_id": "boot-f",
            "source_epoch": 1,
            "memory": {
                "rss_bytes": 1,
                "heap_total_bytes": 0,
                "heap_used_bytes": 0,
                "external_bytes": 0,
                "array_buffers_bytes": 0,
                "uptime_seconds": 0,
            },
            "stdout": {
                "blocked": False,
                "lossless_queue_size": 0,
                "state_pending_keys": 0,
                "state_coalesced_total": 0,
            },
            "rpc": {
                "queue_total": 0,
                "queue_interactive": 0,
                "queue_bootstrap": 0,
                "queue_refresh": 0,
                "active": 0,
                "queue_high_watermark": 0,
            },
            "pools": {},
            "sampled_at_iso": "2026-01-01T00:00:00.000Z",
        })
        # close() early-returns if _closed is already True, so unset it
        worker._closed = False
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(worker.close())
        finally:
            loop.close()
        assert worker._latest_worker_stats is None

    def test_f05_worker_stats_result_routed_to_pending_future(self) -> None:
        worker = _make_worker()
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            worker._pending_simulations["stats-req-1"] = future
            worker._dispatch({
                "type": "worker_stats_result",
                "request_id": "stats-req-1",
                "status": "ok",
                "stats": {"type": "worker_stats", "test": True},
            })
            assert future.done()
            result = loop.run_until_complete(future)
            assert result["status"] == "ok"
        finally:
            loop.close()

    def test_f06_worker_stats_contains_all_required_fields(self) -> None:
        worker = _make_worker()
        stats = {
            "type": "worker_stats",
            "boot_id": "boot-f",
            "source_epoch": 1,
            "memory": {
                "rss_bytes": 1000,
                "heap_total_bytes": 2000,
                "heap_used_bytes": 1500,
                "external_bytes": 500,
                "array_buffers_bytes": 100,
                "uptime_seconds": 60,
            },
            "stdout": {
                "blocked": False,
                "lossless_queue_size": 0,
                "state_pending_keys": 0,
                "state_coalesced_total": 0,
            },
            "rpc": {
                "queue_total": 0,
                "queue_interactive": 0,
                "queue_bootstrap": 0,
                "queue_refresh": 0,
                "active": 0,
                "queue_high_watermark": 0,
            },
            "pools": {},
            "sampled_at_iso": "2026-01-01T00:00:00.000Z",
        }
        worker._dispatch(stats)
        stored = worker.latest_worker_stats
        assert stored is not None
        # Verify memory fields
        for field in ("rss_bytes", "heap_total_bytes", "heap_used_bytes",
                       "external_bytes", "array_buffers_bytes", "uptime_seconds"):
            assert field in stored["memory"]
            assert isinstance(stored["memory"][field], (int, float))
        # Verify stdout fields
        for field in ("blocked", "lossless_queue_size", "state_pending_keys",
                       "state_coalesced_total"):
            assert field in stored["stdout"]
        # Verify rpc fields
        for field in ("queue_total", "queue_interactive", "queue_bootstrap",
                       "queue_refresh", "active", "queue_high_watermark"):
            assert field in stored["rpc"]
        assert isinstance(stored["stdout"]["blocked"], bool)
        assert isinstance(stored["pools"], dict)


if __name__ == "__main__":
    unittest.main()
