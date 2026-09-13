from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import RollingStateStore


class _OneEventSource:
    name = "test:source"

    def describe(self) -> dict[str, Any]:
        return {"source": self.name, "mode": "test"}

    async def run(self, publish: Any, stop_event: asyncio.Event) -> None:
        now_realtime = time.time_ns()
        await publish(
            MarketEvent(
                source=self.name,
                key="test:key",
                kind="test_state",
                value={"not_serialized": True},
                summary={"safe": "summary"},
                received_realtime_ns=now_realtime,
                received_monotonic_ns=time.monotonic_ns(),
                chain_position=42,
            ),
        )
        await stop_event.wait()


class _FailingSource:
    name = "test:failing"
    supervisor_retry_initial_seconds = 1.0
    supervisor_retry_max_seconds = 2.0

    def describe(self) -> dict[str, Any]:
        return {"source": self.name, "mode": "test"}

    async def run(self, publish: Any, stop_event: asyncio.Event) -> None:
        raise ConnectionError("test endpoint unavailable")


class RealtimeScannerTest(unittest.IsolatedAsyncioTestCase):
    async def test_persists_only_bounded_status_not_raw_event_value(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "run"
            scanner = RealtimeScanner(
                sources=[_OneEventSource()],
                output_directory=output,
                retention_seconds=60,
                max_events_per_key=8,
                event_bus_capacity=4,
                status_flush_seconds=0.01,
            )
            manifest = await scanner.run(duration_seconds=0.04)

            self.assertEqual(manifest["status"], "completed")
            status = (output / "status.json").read_text(encoding="utf-8")
            self.assertIn('"raw_market_data_persisted": false', status)
            self.assertIn('"safe": "summary"', status)
            self.assertNotIn("not_serialized", status)
            snapshot = scanner.snapshot(status="completed")
            self.assertEqual(snapshot["state"]["states"]["test:key"]["chain_position"], 42)
            self.assertIsNotNone(snapshot["state"]["states"]["test:key"]["event_id"])
            self.assertEqual(snapshot["state"]["states"]["test:key"]["source_epoch"], 1)
            self.assertGreater(snapshot["state"]["states"]["test:key"]["state_version"], 0)
            self.assertEqual(snapshot["state"]["boot_id"], snapshot["boot_id"])
            self.assertEqual(snapshot["sources"]["test:source"]["updates"], 1)
            self.assertEqual(snapshot["sources"]["test:source"]["source_epoch"], 1)

    async def test_source_can_set_a_longer_retry_backoff_without_affecting_defaults(self) -> None:
        source = _FailingSource()
        scanner = RealtimeScanner(
            sources=[source],
            output_directory=Path(tempfile.gettempdir()) / "unused-scanner-output",
        )
        observed_timeouts: list[float] = []

        async def fake_wait_for(awaitable: Any, *, timeout: float) -> bool:
            observed_timeouts.append(timeout)
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            if len(observed_timeouts) == 3:
                scanner.stop_event.set()
                return True
            raise TimeoutError

        with patch("market_data_lab.realtime_scanner.asyncio.wait_for", new=fake_wait_for):
            await scanner._source_supervisor(source)

        self.assertEqual(observed_timeouts, [1.0, 2.0, 2.0])
        self.assertEqual(scanner.snapshot(status="running")["sources"][source.name]["restarts"], 3)


class RollingStateStoreTest(unittest.TestCase):
    def test_history_can_coalesce_without_delaying_latest_state(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            history_minimum_interval_ms=10,
        )
        first = MarketEvent("feed", "key", "quote", "first", {}, 1, 1)
        second = MarketEvent("feed", "key", "quote", "second", {}, 2, 2_000_000)
        store.add(first)
        store.add(second)
        self.assertEqual(store.latest("key"), second)
        self.assertEqual(store.recent("key"), (first,))
        self.assertEqual(store.coalesced_by_interval, 1)

    def test_out_of_order_source_sequence_does_not_replace_latest(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        newer = MarketEvent(
            "feed",
            "key",
            "quote",
            "new",
            {},
            1_000,
            1_000,
            chain_position=20,
            event_id="new",
            source_epoch=1,
            boot_id="boot-a",
        )
        older = MarketEvent(
            "feed",
            "key",
            "quote",
            "old",
            {},
            2_000,
            2_000,
            chain_position=19,
            event_id="old",
            source_epoch=1,
            boot_id="boot-a",
        )

        self.assertTrue(store.add(newer))
        self.assertFalse(store.add(older))
        self.assertIs(store.latest("key"), newer)
        self.assertEqual(
            store.snapshot(now_monotonic_ns=2_000)[
                "rejected_out_of_order_or_duplicate"
            ],
            1,
        )

    def test_source_epoch_invalidates_untouched_latest_state(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        for key in ("book:A", "book:B"):
            self.assertTrue(
                store.add(
                    MarketEvent(
                        "feed",
                        key,
                        "book",
                        key,
                        {},
                        1_000,
                        1_000,
                        chain_position=1,
                        event_id=f"{key}:1",
                        source_epoch=1,
                        boot_id="boot-a",
                    ),
                ),
            )
        replacement = MarketEvent(
            "feed",
            "book:A",
            "book",
            "new-a",
            {},
            2_000,
            2_000,
            chain_position=1,
            event_id="book:A:2",
            source_epoch=2,
            boot_id="boot-a",
        )

        self.assertTrue(store.add(replacement))
        self.assertIs(store.latest("book:A"), replacement)
        self.assertIsNone(store.latest("book:B"))
        self.assertEqual(store.recent("book:B")[-1].value, "book:B")


if __name__ == "__main__":
    unittest.main()
