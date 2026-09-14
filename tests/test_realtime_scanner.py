from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
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

    async def test_stable_run_resets_retry_backoff(self) -> None:
        source = _FailingSource()
        source.supervisor_retry_initial_seconds = 0.25
        source.supervisor_retry_max_seconds = 4.0
        scanner = RealtimeScanner(
            sources=[source],
            output_directory=Path(tempfile.gettempdir()) / "unused-scanner-output",
            supervisor_retry_reset_after_seconds=30.0,
        )
        observed_timeouts: list[float] = []

        async def fake_wait_for(awaitable: Any, *, timeout: float) -> bool:
            observed_timeouts.append(timeout)
            close = getattr(awaitable, "close", None)
            if callable(close):
                close()
            if len(observed_timeouts) == 2:
                scanner.stop_event.set()
                return True
            raise TimeoutError

        # First failure is rapid.  The second failure follows a stable
        # 31-second run and must therefore use the initial delay again.
        monotonic_values = iter((0.0, 0.1, 0.1, 31.1))
        with (
            patch("market_data_lab.realtime_scanner.time.monotonic", side_effect=monotonic_values),
            patch("market_data_lab.realtime_scanner.asyncio.wait_for", new=fake_wait_for),
        ):
            await scanner._source_supervisor(source)

        self.assertEqual(observed_timeouts, [0.25, 0.25])


class RollingStateStoreTest(unittest.TestCase):
    @staticmethod
    def _event(key: str, *, monotonic_ns: int, source_epoch: int = 1) -> MarketEvent:
        return MarketEvent(
            "feed",
            key,
            "quote",
            {"key": key, "monotonic_ns": monotonic_ns},
            {},
            monotonic_ns,
            monotonic_ns,
            event_id=f"{key}:{monotonic_ns}",
            source_epoch=source_epoch,
            boot_id="boot-a",
        )

    def test_idle_key_is_physically_retired_by_sweep(self) -> None:
        store = RollingStateStore(
            retention_seconds=1,
            max_events_per_key=8,
            max_state_keys=8,
            boot_id="boot-a",
        )
        self.assertTrue(store.add(self._event("idle", monotonic_ns=1_000_000_000)))

        retired = store.sweep(now_monotonic_ns=2_000_000_001)

        self.assertEqual(retired, ("idle",))
        self.assertIsNone(store.latest("idle"))
        self.assertEqual(store.recent("idle"), ())
        self.assertIsNone(store._versioned.latest("idle"))
        self.assertEqual(store._versioned.snapshot()["states"], 0)

    def test_max_state_keys_is_enforced_on_admission_and_is_deterministic(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            max_state_keys=2,
            boot_id="boot-a",
        )

        for index, key in enumerate(("old", "middle", "new"), start=1):
            self.assertTrue(store.add(self._event(key, monotonic_ns=index)))

        self.assertEqual(set(store._latest), {"middle", "new"})
        self.assertIsNone(store.latest("old"))
        self.assertEqual(store._versioned.snapshot()["states"], 2)
        self.assertEqual(store.snapshot(now_monotonic_ns=3)["capacity_evictions"], 1)

    def test_capacity_eviction_keeps_recent_active_key(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            max_state_keys=3,
            boot_id="boot-a",
        )
        for key, timestamp in (("old-a", 1), ("old-b", 2), ("active", 100)):
            self.assertTrue(store.add(self._event(key, monotonic_ns=timestamp)))

        self.assertTrue(store.add(self._event("new", monotonic_ns=101)))

        self.assertIsNotNone(store.latest("active"))
        self.assertIsNone(store.latest("old-a"))
        self.assertEqual(len(store._latest), 3)

    def test_health_counters_only_advance_on_accepted_state(self) -> None:
        async def exercise() -> None:
            scanner = RealtimeScanner(
                sources=[_OneEventSource()],
                output_directory=Path(tempfile.gettempdir()) / "unused-scanner-output",
            )
            first = MarketEvent(
                source="test:source",
                key="health:key",
                kind="quote",
                value="first",
                summary={},
                received_realtime_ns=1,
                received_monotonic_ns=1,
                event_id="health:event:1",
                chain_position=2,
            )
            duplicate = replace(first)

            self.assertTrue((await scanner._publish(first)).accepted)
            scanner._health[first.source].record_error("transport failure")
            self.assertFalse((await scanner._publish(duplicate)).accepted)
            out_of_order = replace(
                first,
                event_id="health:event:older",
                chain_position=1,
                received_realtime_ns=2,
                received_monotonic_ns=2,
            )
            self.assertFalse((await scanner._publish(out_of_order)).accepted)

            health = scanner.snapshot(status="running")["sources"][first.source]
            self.assertEqual(health["received_events"], 3)
            self.assertEqual(health["accepted_events"], 1)
            self.assertEqual(health["rejected_events"], 2)
            self.assertEqual(health["last_error"], "transport failure")

            accepted_after_error = replace(
                first,
                event_id="health:event:2",
                value="second",
                received_realtime_ns=3,
                received_monotonic_ns=3,
                chain_position=3,
            )
            self.assertTrue((await scanner._publish(accepted_after_error)).accepted)
            health = scanner.snapshot(status="running")["sources"][first.source]
            self.assertEqual(health["accepted_events"], 2)
            self.assertIsNone(health["last_error"])

        asyncio.run(exercise())
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
        self.assertEqual(store.recent("book:B"), ())


if __name__ == "__main__":
    unittest.main()
