from __future__ import annotations

import unittest

from market_data_lab.versioned_market_state import DependencyIndex
from market_data_lab.versioned_market_state import EventEnvelope
from market_data_lab.versioned_market_state import VersionedMarketState


def _event(
    *,
    event_id: str,
    key: str = "book:BTCUSDT",
    source: str = "cex:TEST:spot",
    epoch: int = 1,
    sequence: int | None = 1,
    realtime_ns: int = 1_000,
    monotonic_ns: int = 1_000,
    boot_id: str = "boot-a",
    payload: object = "value",
) -> EventEnvelope:
    return EventEnvelope(
        event_id=event_id,
        event_type="order_book",
        schema_version=2,
        source_id=source,
        source_epoch=epoch,
        instrument_or_pool_id=key,
        received_realtime_ns=realtime_ns,
        received_monotonic_ns=monotonic_ns,
        boot_id=boot_id,
        source_sequence=sequence,
        payload=payload,
        quality_flags=("public",),
        provenance="synthetic_test",
    )


class VersionedMarketStateTest(unittest.TestCase):
    def test_out_of_order_sequence_cannot_regress_latest_state(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        newer = _event(event_id="new", sequence=20, payload="new")
        older = _event(
            event_id="old",
            sequence=19,
            realtime_ns=2_000,
            monotonic_ns=2_000,
            payload="old",
        )

        self.assertTrue(store.put(newer, ttl_ns=1_000).accepted)
        rejected = store.put(older, ttl_ns=1_000)

        self.assertFalse(rejected.accepted)
        self.assertEqual(rejected.status, "out_of_order_source_sequence")
        latest = store.latest(newer.state_key)
        assert latest is not None
        self.assertEqual(latest.event.payload, "new")

    def test_same_sequence_is_idempotent_and_not_a_new_version(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        first = _event(event_id="first", sequence=7)
        duplicate_sequence = _event(
            event_id="second-id",
            sequence=7,
            realtime_ns=2_000,
            monotonic_ns=2_000,
        )
        accepted = store.put(first, ttl_ns=1_000)
        duplicate = store.put(duplicate_sequence, ttl_ns=1_000)

        self.assertTrue(accepted.accepted)
        self.assertEqual(duplicate.status, "duplicate_source_sequence")
        assert duplicate.record is not None and accepted.record is not None
        self.assertEqual(duplicate.record.state_version, accepted.record.state_version)

    def test_new_source_epoch_invalidates_other_keys_before_repopulation(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        store.put(_event(event_id="a1", key="book:A", sequence=1), ttl_ns=10_000)
        store.put(_event(event_id="b1", key="book:B", sequence=1), ttl_ns=10_000)

        result = store.put(
            _event(event_id="a2", key="book:A", epoch=2, sequence=1),
            ttl_ns=10_000,
        )

        self.assertTrue(result.accepted)
        self.assertEqual(result.invalidated_keys, ("book:A", "book:B"))
        view = store.view(("book:A", "book:B"), now_monotonic_ns=2_000)
        self.assertFalse(view.consistent)
        self.assertNotIn("book:A", view.unusable_reasons)
        self.assertEqual(view.unusable_reasons["book:B"], "source_epoch_advanced")
        self.assertEqual(view.records["book:A"].event.source_epoch, 2)

    def test_stale_prior_epoch_event_is_rejected(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        store.put(_event(event_id="new", epoch=2, sequence=1), ttl_ns=1_000)
        stale = store.put(
            _event(
                event_id="stale",
                epoch=1,
                sequence=100,
                realtime_ns=3_000,
                monotonic_ns=3_000,
            ),
            ttl_ns=1_000,
        )

        self.assertFalse(stale.accepted)
        self.assertEqual(stale.status, "stale_source_epoch")

    def test_ttl_and_boot_id_never_make_old_state_fresh(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        event = _event(event_id="one", sequence=None, monotonic_ns=1_000)
        store.put(event, ttl_ns=100)

        stale_view = store.view((event.state_key,), now_monotonic_ns=1_100)
        self.assertFalse(stale_view.consistent)
        self.assertEqual(stale_view.unusable_reasons[event.state_key], "state_stale")
        expired = store.expire(now_monotonic_ns=1_100)
        self.assertEqual(expired, (event.state_key,))
        latest = store.latest(event.state_key)
        assert latest is not None
        self.assertEqual(latest.invalid_reason, "state_stale")

        wrong_boot = store.put(
            _event(event_id="two", sequence=None, boot_id="boot-b"),
            ttl_ns=100,
        )
        self.assertEqual(wrong_boot.status, "boot_id_mismatch")

    def test_view_keeps_old_immutable_version_after_new_update(self) -> None:
        store = VersionedMarketState(boot_id="boot-a")
        store.put(_event(event_id="one", sequence=1), ttl_ns=10_000)
        first_view = store.view(("book:BTCUSDT",), now_monotonic_ns=1_000)
        store.put(
            _event(
                event_id="two",
                sequence=2,
                realtime_ns=2_000,
                monotonic_ns=2_000,
            ),
            ttl_ns=10_000,
        )
        second_view = store.view(("book:BTCUSDT",), now_monotonic_ns=2_000)

        self.assertNotEqual(first_view.version_vector, second_view.version_vector)
        self.assertEqual(first_view.records["book:BTCUSDT"].event.event_id, "one")
        self.assertEqual(second_view.records["book:BTCUSDT"].event.event_id, "two")


class DependencyIndexTest(unittest.TestCase):
    def test_dependency_change_marks_only_affected_groups(self) -> None:
        index = DependencyIndex()
        index.register("route:btc", ("book:BTC", "quote:BTC"))
        index.register("route:eth", ("book:ETH", "quote:ETH"))

        self.assertEqual(index.mark_dirty("book:BTC"), ("route:btc",))
        self.assertEqual(index.take_dirty(limit=10), ("route:btc",))
        self.assertEqual(index.take_dirty(limit=10), ())

    def test_redirty_after_take_is_retained_for_next_pass(self) -> None:
        index = DependencyIndex()
        index.register("route:btc", ("book:BTC", "quote:BTC"))
        index.mark_dirty("book:BTC")

        self.assertEqual(index.take_dirty(limit=1), ("route:btc",))
        index.mark_dirty("quote:BTC")
        self.assertEqual(index.take_dirty(limit=1), ("route:btc",))


if __name__ == "__main__":
    unittest.main()
