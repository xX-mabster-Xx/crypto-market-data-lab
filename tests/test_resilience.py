from __future__ import annotations

import time
import unittest

from market_data_lab.resilience import (
    BackoffManager,
    CircuitBreaker,
    CircuitBreakerConfig,
    ErrorClass,
    OverloadHandler,
    RecoveryProfile,
    SourceHealth,
    SourceStatus,
    SourceStatusTracker,
    SourceTransport,
    SourceUsability,
)


class CircuitBreakerTest(unittest.TestCase):
    def test_initial_state_closed(self) -> None:
        cb = CircuitBreaker()
        self.assertEqual(cb.state, "closed")
        self.assertTrue(cb.can_execute())

    def test_opens_after_threshold(self) -> None:
        cb = CircuitBreaker(config=CircuitBreakerConfig(failure_threshold=3))
        for _ in range(3):
            cb.record_failure()
        self.assertEqual(cb.state, "open")
        self.assertFalse(cb.can_execute())

    def test_half_open_after_timeout(self) -> None:
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout_ns=1)
        )
        cb.record_failure()
        time.sleep(0.01)
        self.assertEqual(cb.state, "half_open")

    def test_closes_after_success(self) -> None:
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout_ns=1, half_open_max_calls=1)
        )
        cb.record_failure()
        time.sleep(0.01)
        cb.record_success()
        self.assertEqual(cb.state, "closed")


class SourceStatusTrackerTest(unittest.TestCase):
    def test_register_and_get(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        status = tracker.get("source-1")
        self.assertIsNotNone(status)
        self.assertEqual(status.source_id, "source-1")

    def test_record_success(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.record_success("source-1")
        status = tracker.get("source-1")
        self.assertEqual(status.transport, "live")
        self.assertEqual(status.usability, "valid")

    def test_record_transport_error(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.record_error("source-1", "transport_error", "timeout")
        status = tracker.get("source-1")
        self.assertEqual(status.transport, "backoff")
        self.assertEqual(status.usability, "stale")

    def test_record_auth_error(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.record_error("source-1", "auth_error", "invalid key")
        status = tracker.get("source-1")
        self.assertEqual(status.transport, "disabled")
        self.assertEqual(status.usability, "unsupported")

    def test_record_liquidity_error(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.record_error("source-1", "liquidity_error", "insufficient liquidity")
        status = tracker.get("source-1")
        self.assertEqual(status.usability, "gapped")

    def test_get_health(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.record_success("source-1")
        health = tracker.get_health("source-1")
        self.assertTrue(health.is_healthy)

    def test_get_all_healthy(self) -> None:
        tracker = SourceStatusTracker()
        tracker.register("source-1")
        tracker.register("source-2")
        tracker.record_success("source-1")
        tracker.record_error("source-2", "transport_error", "fail")
        healthy = tracker.get_all_healthy()
        self.assertEqual(healthy, ["source-1"])


class BackoffManagerTest(unittest.TestCase):
    def test_exponential_backoff(self) -> None:
        profile = RecoveryProfile(base_delay_ns=1_000_000, max_delay_ns=100_000_000)
        manager = BackoffManager(profile=profile)

        manager.record_attempt("source-1")
        delay1 = manager.get_delay_ns("source-1")

        manager.record_attempt("source-1")
        delay2 = manager.get_delay_ns("source-1")

        self.assertGreater(delay2, delay1)

    def test_max_delay_cap(self) -> None:
        profile = RecoveryProfile(base_delay_ns=1_000_000_000, max_delay_ns=5_000_000_000)
        manager = BackoffManager(profile=profile)

        for _ in range(20):
            manager.record_attempt("source-1")

        delay = manager.get_delay_ns("source-1")
        self.assertLessEqual(delay, 5_000_000_000)

    def test_can_retry(self) -> None:
        manager = BackoffManager()
        manager.schedule_retry("source-1")
        self.assertFalse(manager.can_retry("source-1"))

    def test_reset(self) -> None:
        manager = BackoffManager()
        manager.record_attempt("source-1")
        manager.record_attempt("source-1")
        manager.reset("source-1")
        self.assertEqual(manager.get_delay_ns("source-1"), manager.profile.base_delay_ns)


class OverloadHandlerTest(unittest.TestCase):
    def test_not_overloaded_initially(self) -> None:
        handler = OverloadHandler()
        self.assertFalse(handler.is_overloaded())
        self.assertTrue(handler.can_accept_signal())

    def test_overloaded_queue(self) -> None:
        handler = OverloadHandler(max_queue_depth=100)
        handler.record_queued(100)
        self.assertTrue(handler.is_overloaded())
        self.assertFalse(handler.can_accept_signal())

    def test_overloaded_ram(self) -> None:
        handler = OverloadHandler(max_ram_bytes=1000)
        handler.update_ram_usage(1000)
        self.assertTrue(handler.is_overloaded())

    def test_analytics_lower_priority(self) -> None:
        handler = OverloadHandler(max_queue_depth=100)
        handler.record_queued(85)
        self.assertTrue(handler.can_accept_signal())
        self.assertFalse(handler.can_accept_analytics())

    def test_degradation_actions(self) -> None:
        handler = OverloadHandler(max_queue_depth=100, max_ram_bytes=10000)
        handler.record_queued(85)
        handler.update_ram_usage(8500)
        actions = handler.get_degradation_actions()
        self.assertIn("reduce_cold_discovery", actions)
        self.assertIn("flush_old_history", actions)

    def test_record_dropped(self) -> None:
        handler = OverloadHandler()
        handler.record_dropped_signal()
        handler.record_dropped_signal()
        handler.record_dropped_analytics()
        self.assertEqual(handler._dropped_signals, 2)
        self.assertEqual(handler._dropped_analytics, 1)


if __name__ == "__main__":
    unittest.main()
