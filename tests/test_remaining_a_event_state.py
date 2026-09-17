from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import replace
from pathlib import Path
import unittest
from unittest.mock import patch

from market_data_lab.realtime_scanner import CoalescingEventBus
from market_data_lab.realtime_scanner import EventBusClosed
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import RollingStateStore
from market_data_lab.solana_realtime_scanner import RaydiumLocalQuoteStateSource
from market_data_lab.versioned_market_state import SourceStateRevision


def _event(key: str, sequence: int, *, received: int | None = None) -> MarketEvent:
    timestamp = sequence if received is None else received
    return MarketEvent(
        source="feed",
        key=key,
        kind="quote",
        value=sequence,
        summary={},
        received_realtime_ns=timestamp,
        received_monotonic_ns=timestamp,
        chain_position=sequence,
        event_id=f"{key}:{sequence}:{timestamp}",
        source_epoch=1,
        boot_id="boot-a",
    )


class _SequenceWorker:
    messages: tuple[dict[str, object], ...] = ()

    def __init__(self, **_: object) -> None:
        self._messages = deque(self.messages)

    async def start(self, **_: object) -> dict[str, object]:
        return {"type": "ready"}

    async def next_event(self) -> dict[str, object]:
        if self._messages:
            return self._messages.popleft()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self) -> None:
        return None


def _pool_message(*, core: int, dependency: int, generation: int) -> dict[str, object]:
    return {
        "type": "pool_state",
        "protocol": "raydium_clmm",
        "pool_id": "pool-1",
        "label": "SOL/USDC",
        "slot": core,
        "core_state_slot": core,
        "dependency_slot_min": dependency,
        "dependency_slot_max": dependency,
        "dependency_generation": generation,
        "token_a_mint": "mint-a",
        "token_b_mint": "mint-b",
        "token_a_decimals": 9,
        "token_b_decimals": 6,
        "tick_current": generation,
        "sqrt_price_x64": str(100 + generation),
        "tick_cache_age_ms": 0,
    }


class RemainingAEventBusAndAdmissionTest(unittest.IsolatedAsyncioTestCase):
    async def _wait_for_publishers(
        self,
        bus: CoalescingEventBus,
        expected: int,
    ) -> None:
        async with asyncio.timeout(1):
            async with bus._condition:
                while bus.waiting_publishers != expected:
                    await bus._condition.wait()

    async def test_waiting_same_key_publishers_do_not_duplicate_queue_position(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=2)
        await bus.publish(_event("A", 1))
        await bus.publish(_event("C", 2))

        older = asyncio.create_task(bus.publish(_event("B", 3)))
        newer = asyncio.create_task(bus.publish(_event("B", 4)))
        await self._wait_for_publishers(bus, 2)

        self.assertEqual((await bus.next_event()).key, "A")
        self.assertEqual((await bus.next_event()).key, "C")
        await asyncio.gather(older, newer)

        self.assertEqual(list(bus._pending_order), ["B"])
        self.assertEqual((await bus.next_event()).value, 4)
        self.assertEqual(bus.coalesced_updates, 1)
        await bus.publish(_event("D", 5))
        self.assertEqual((await bus.next_event()).key, "D")

    async def test_cancelled_newer_waiter_does_not_make_older_payload_win(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=1)
        await bus.publish(_event("A", 1))
        older = asyncio.create_task(bus.publish(_event("B", 3)))
        newer = asyncio.create_task(bus.publish(_event("B", 4)))
        await self._wait_for_publishers(bus, 2)

        newer.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await newer
        self.assertEqual((await bus.next_event()).key, "A")
        result = await asyncio.wait_for(older, timeout=1)

        self.assertTrue(result.coalesced)
        self.assertEqual((await bus.next_event()).value, 4)
        self.assertEqual(bus.waiting_publishers, 0)

    async def test_cancelled_older_waiter_leaves_newer_waiter_consistent(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=1)
        await bus.publish(_event("A", 1))
        older = asyncio.create_task(bus.publish(_event("B", 3)))
        newer = asyncio.create_task(bus.publish(_event("B", 4)))
        await self._wait_for_publishers(bus, 2)

        older.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await older
        self.assertEqual((await bus.next_event()).key, "A")
        await asyncio.wait_for(newer, timeout=1)

        self.assertEqual((await bus.next_event()).value, 4)
        self.assertEqual(bus.waiting_publishers, 0)

    async def test_close_wakes_publishers_and_consumers_without_hanging(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=1)
        await bus.publish(_event("A", 1))
        waiter = asyncio.create_task(bus.publish(_event("B", 2)))
        await self._wait_for_publishers(bus, 1)

        await bus.close()

        with self.assertRaises(EventBusClosed):
            await asyncio.wait_for(waiter, timeout=1)
        self.assertEqual((await bus.next_event()).key, "A")
        with self.assertRaises(EventBusClosed):
            await asyncio.wait_for(bus.next_event(), timeout=1)
        self.assertEqual(bus.waiting_publishers, 0)

    async def test_hot_key_coalesces_without_displacing_cold_key(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=2)
        await bus.publish(_event("A", 1))
        await bus.publish(_event("B", 1))
        for sequence in range(2, 1_001):
            result = await bus.publish(_event("A", sequence))
            self.assertTrue(result.coalesced)

        self.assertEqual(bus.queued, 2)
        self.assertEqual((await bus.next_event()).value, 1_000)
        self.assertEqual((await bus.next_event()).key, "B")
        self.assertEqual(bus.queue_high_watermark, 2)
        self.assertEqual(bus.dropped_events, 0)

    async def test_rejected_state_never_changes_pending_payload(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        bus = CoalescingEventBus(store, capacity=1)
        accepted = await bus.publish(_event("A", 2))
        rejected = await bus.publish(_event("A", 1, received=3))

        self.assertTrue(accepted.accepted)
        self.assertFalse(rejected.accepted)
        self.assertEqual((await bus.next_event()).value, 2)
        self.assertEqual(bus.rejected_state_events, 1)

    async def test_distinct_key_backpressure_preserves_fifo_at_capacities_one_and_two(self) -> None:
        for capacity in (1, 2):
            with self.subTest(capacity=capacity):
                store = RollingStateStore(
                    retention_seconds=60,
                    max_events_per_key=8,
                    boot_id="boot-a",
                )
                bus = CoalescingEventBus(store, capacity=capacity)
                initial = [chr(ord("A") + index) for index in range(capacity)]
                for index, key in enumerate(initial, start=1):
                    await bus.publish(_event(key, index))
                cold = asyncio.create_task(bus.publish(_event("Z", 100)))
                await self._wait_for_publishers(bus, 1)
                consumed = [await bus.next_event() for _ in initial]
                await asyncio.wait_for(cold, timeout=1)

                self.assertEqual([event.key for event in consumed], initial)
                self.assertEqual((await bus.next_event()).key, "Z")
                self.assertEqual(bus.dropped_events, 0)

    async def test_dependency_only_pool_update_is_admitted_by_real_source_and_store(self) -> None:
        _SequenceWorker.messages = (
            _pool_message(core=100, dependency=110, generation=1),
            _pool_message(core=100, dependency=120, generation=2),
        )
        source = RaydiumLocalQuoteStateSource(
            pools=(),
            raydium_standard_pools=(),
            meteora_pools=(),
            orca_pools=(),
            http_url="https://rpc.example",
            ws_url="wss://rpc.example",
            timeout_seconds=1,
            tick_cache_max_age_ms=300_000,
            state_snapshot_refresh_interval_ms=15_000,
            rpc_http_min_request_interval_ms=200,
        )
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        stop = asyncio.Event()
        admitted: list[bool] = []

        async def publish(event: MarketEvent) -> None:
            admitted.append(store.add(event))
            if len(admitted) == 2:
                stop.set()

        with patch(
            "market_data_lab.solana_realtime_scanner.RaydiumLocalQuoteWorker",
            _SequenceWorker,
        ):
            await source.run(publish, stop)

        self.assertEqual(admitted, [True, True])
        latest = store.latest("solana:raydium-clmm:pool-1")
        assert latest is not None
        self.assertEqual(latest.summary["dependency_generation"], 2)

    async def test_pool_revision_order_runs_source_through_bus_and_callback(self) -> None:
        _SequenceWorker.messages = (
            _pool_message(core=100, dependency=110, generation=1),
            _pool_message(core=100, dependency=120, generation=2),
            _pool_message(core=100, dependency=120, generation=2),
            _pool_message(core=100, dependency=115, generation=1),
            _pool_message(core=105, dependency=120, generation=2),
            _pool_message(core=104, dependency=130, generation=3),
        )
        source = RaydiumLocalQuoteStateSource(
            pools=(),
            raydium_standard_pools=(),
            meteora_pools=(),
            orca_pools=(),
            http_url="https://rpc.example",
            ws_url="wss://rpc.example",
            timeout_seconds=1,
            tick_cache_max_age_ms=300_000,
            state_snapshot_refresh_interval_ms=15_000,
            rpc_http_min_request_interval_ms=200,
        )
        delivered: list[SourceStateRevision] = []
        delivered_condition = asyncio.Condition()

        async def callback(event: MarketEvent) -> None:
            assert event.source_revision is not None
            async with delivered_condition:
                delivered.append(event.source_revision)
                delivered_condition.notify_all()

        scanner = RealtimeScanner(
            sources=[source],
            output_directory=Path("/tmp/unused-agent-a-scanner"),
            event_bus_capacity=2,
            event_handler=callback,
        )
        consumer = asyncio.create_task(scanner._event_consumer())
        admission: list[bool] = []

        async def publish(event: MarketEvent) -> None:
            result = await scanner._publish(event)
            admission.append(result.accepted)
            if result.accepted:
                assert event.source_revision is not None
                async with asyncio.timeout(1):
                    async with delivered_condition:
                        while event.source_revision not in delivered:
                            await delivered_condition.wait()

        try:
            with patch(
                "market_data_lab.solana_realtime_scanner.RaydiumLocalQuoteWorker",
                _SequenceWorker,
            ):
                source_task = asyncio.create_task(source.run(publish, scanner.stop_event))
                async with asyncio.timeout(1):
                    while len(admission) != len(_SequenceWorker.messages):
                        await asyncio.sleep(0)
                scanner.stop_event.set()
                await source_task
        finally:
            await scanner._bus.close()
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

        self.assertEqual(admission, [True, True, False, False, True, False])
        self.assertEqual(
            delivered,
            [
                SourceStateRevision(100, 1),
                SourceStateRevision(100, 2),
                SourceStateRevision(105, 2),
            ],
        )
        latest = scanner.store.latest("solana:raydium-clmm:pool-1")
        assert latest is not None
        self.assertEqual(latest.chain_position, 105)
        self.assertEqual(latest.summary["dependency_slot_max"], 120)
        health = scanner.snapshot(status="running")["sources"][source.name]
        self.assertEqual(health["received_events"], 6)
        self.assertEqual(health["accepted_events"], 3)
        self.assertEqual(health["rejected_events"], 3)
        counts = scanner.store._versioned.snapshot()["counts"]
        self.assertEqual(counts["duplicate_source_revision"], 1)
        self.assertEqual(counts["out_of_order_source_revision"], 2)


class RemainingARevisionContractTest(unittest.TestCase):
    @staticmethod
    def _pool_event(
        *,
        core: int,
        generation: int,
        epoch: int,
        timestamp: int,
    ) -> MarketEvent:
        return MarketEvent(
            source="solana:local-exact-pools",
            key="solana:raydium-clmm:pool-1",
            kind="pool_state",
            value={"core": core, "generation": generation},
            summary={},
            received_realtime_ns=timestamp,
            received_monotonic_ns=timestamp,
            chain_position=core,
            source_revision=SourceStateRevision(core, generation),
            event_id=f"event:{epoch}:{core}:{generation}:{timestamp}",
            source_epoch=epoch,
            boot_id="boot-a",
        )

    def test_epoch_reset_accepts_lower_generation_and_stale_epoch_cannot_return(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=8,
            boot_id="boot-a",
        )
        self.assertTrue(store.add(self._pool_event(core=100, generation=9, epoch=1, timestamp=1)))
        self.assertTrue(store.add(self._pool_event(core=90, generation=0, epoch=2, timestamp=2)))
        self.assertFalse(
            store.add(self._pool_event(core=200, generation=99, epoch=1, timestamp=3)),
        )

        latest = store.latest("solana:raydium-clmm:pool-1")
        assert latest is not None
        self.assertEqual(latest.source_epoch, 2)
        self.assertEqual(latest.source_revision, SourceStateRevision(90, 0))

    def test_revision_requires_real_primary_chain_position(self) -> None:
        event = self._pool_event(core=100, generation=1, epoch=1, timestamp=1)
        with self.assertRaisesRegex(ValueError, "primary_sequence"):
            replace(
                event,
                chain_position=101,
                source_revision=SourceStateRevision(100, 1),
            ).envelope(default_boot_id="boot-a")


class _ScanCountingDict(dict[str, str]):
    scanned_entries = 0

    def items(self):  # type: ignore[no-untyped-def]
        self.scanned_entries += len(self)
        return super().items()

    def values(self):  # type: ignore[no-untyped-def]
        self.scanned_entries += len(self)
        return super().values()


class RemainingAStoreIndexTest(unittest.TestCase):
    def test_hot_update_does_not_scan_unrelated_state_keys(self) -> None:
        for key_count in (10, 1_000, 10_000):
            with self.subTest(key_count=key_count):
                store = RollingStateStore(
                    retention_seconds=60,
                    max_events_per_key=2,
                    max_state_keys=key_count + 1,
                    boot_id="boot-a",
                )
                for index in range(key_count):
                    self.assertTrue(store.add(_event(f"key-{index}", index + 1)))
                counting = _ScanCountingDict(store._state_key_by_event_key)
                store._state_key_by_event_key = counting

                self.assertTrue(store.add(_event("key-0", key_count + 100)))

                self.assertEqual(counting.scanned_entries, 0)
                self.assertEqual(len(store._event_keys_by_state_key), key_count)

    def test_retirement_epoch_capacity_and_key_reuse_clean_every_index(self) -> None:
        store = RollingStateStore(
            retention_seconds=1,
            max_events_per_key=2,
            max_state_keys=2,
            boot_id="boot-a",
        )
        first = _event("event-a", 1)
        first = replace(first, instrument_or_pool_id="state-shared")
        second = _event("event-b", 2)
        second = replace(second, instrument_or_pool_id="state-shared")
        self.assertTrue(store.add(first))
        self.assertTrue(store.add(second))
        self.assertIsNone(store.latest("event-a"))
        self.assertEqual(store._state_key_by_event_key, {"event-b": "state-shared"})
        self.assertEqual(store._event_keys_by_state_key, {"state-shared": {"event-b"}})

        self.assertTrue(store.add(_event("event-c", 3)))
        self.assertTrue(store.add(_event("event-d", 4)))
        self.assertNotIn("event-b", store._state_key_by_event_key)
        self.assertNotIn("state-shared", store._event_keys_by_state_key)
        self.assertIsNone(store._versioned.latest("state-shared"))

        self.assertEqual(store.sweep(now_monotonic_ns=1_000_000_005), ("event-c", "event-d"))
        self.assertEqual(store._state_key_by_event_key, {})
        self.assertEqual(store._event_keys_by_state_key, {})
        self.assertEqual(store._versioned.snapshot()["states"], 0)

        self.assertTrue(store.add(_event("epoch-a", 10)))
        self.assertTrue(store.add(_event("epoch-b", 11)))
        self.assertEqual(set(store.advance_source_epoch("feed", 2)), {"epoch-a", "epoch-b"})
        self.assertEqual(store._state_key_by_event_key, {})
        self.assertEqual(store._event_keys_by_state_key, {})
        self.assertEqual(store._versioned.snapshot()["states"], 0)

    def test_epoch_retirement_work_is_linear_in_affected_keys(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=2,
            max_state_keys=2_000,
            boot_id="boot-a",
        )
        for index in range(1_000):
            self.assertTrue(store.add(_event(f"epoch-{index}", index + 1)))
        counting = _ScanCountingDict(store._state_key_by_event_key)
        store._state_key_by_event_key = counting

        invalidated = store.advance_source_epoch("feed", 2)

        self.assertEqual(len(invalidated), 1_000)
        self.assertEqual(counting.scanned_entries, 0)
        self.assertEqual(store._state_key_by_event_key, {})
        self.assertEqual(store._event_keys_by_state_key, {})
        self.assertEqual(store._versioned.snapshot()["states"], 0)

    def test_one_hundred_thousand_dynamic_values_keep_two_logical_slots(self) -> None:
        store = RollingStateStore(
            retention_seconds=60,
            max_events_per_key=4,
            max_state_keys=4,
            boot_id="boot-a",
        )
        for index in range(100_000):
            key = f"notional-slot:{index % 2}"
            self.assertTrue(store.add(_event(key, index + 1)))

        self.assertEqual(store.total_updates, 100_000)
        self.assertEqual(set(store._latest), {"notional-slot:0", "notional-slot:1"})
        self.assertEqual(len(store._state_key_by_event_key), 2)
        self.assertEqual(len(store._event_keys_by_state_key), 2)
        self.assertEqual(store._versioned.snapshot()["states"], 2)
        self.assertTrue(all(len(store.recent(key)) == 4 for key in store._latest))


if __name__ == "__main__":
    unittest.main()
