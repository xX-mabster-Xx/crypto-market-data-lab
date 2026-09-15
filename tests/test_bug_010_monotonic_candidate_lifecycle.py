from decimal import Decimal
import unittest

from market_data_lab.solana_route_evaluator import _CandidateLedger


class CandidateLedgerMonotonicTimeTest(unittest.TestCase):
    @staticmethod
    def _positive_cycle() -> dict[str, object]:
        return {
            "route_id": "route",
            "direction": "buy_dex_base_sell_cex_base",
            "status": "ok",
            "timing_valid": True,
            "positive_after_network_floor": True,
            "net_edge_after_network_floor_bps": "5",
            "net_pnl_after_network_floor_settlement": "1",
        }

    def test_wall_clock_jump_forward_does_not_inflate_duration(self) -> None:
        ledger = _CandidateLedger(
            minimum_edge_bps=Decimal("0"),
            improvement_bps=Decimal("1"),
        )
        cycle = self._positive_cycle()

        started = ledger.observe(
            cycle,
            observed_realtime_ns=10_000_000_000,
            observed_monotonic_ns=100_000_000_000,
        )
        invalid = {
            **cycle,
            "status": "cex_book_stale",
            "timing_valid": False,
            "positive_after_network_floor": False,
        }
        closed = ledger.observe(
            invalid,
            observed_realtime_ns=3_610_000_000_000,
            observed_monotonic_ns=102_000_000_000,
        )

        self.assertEqual(started[0]["duration_seconds"], 0.0)
        self.assertEqual(closed[0]["event"], "candidate_closed")
        self.assertEqual(closed[0]["duration_seconds"], 2.0)

    def test_wall_clock_jump_backward_does_not_shrink_close_all_duration(self) -> None:
        ledger = _CandidateLedger(
            minimum_edge_bps=Decimal("0"),
            improvement_bps=Decimal("1"),
        )
        cycle = self._positive_cycle()

        ledger.observe(
            cycle,
            observed_realtime_ns=2_000_000_000_000,
            observed_monotonic_ns=5_000_000_000,
        )
        closed = ledger.close_all(
            observed_realtime_ns=1_000_000_000_000,
            observed_monotonic_ns=8_500_000_000,
            reason="scanner_stopped",
        )

        self.assertEqual(closed[0]["event"], "candidate_closed")
        self.assertEqual(closed[0]["duration_seconds"], 3.5)
        self.assertEqual(closed[0]["close_reason"], "scanner_stopped")

    def test_improvement_uses_monotonic_elapsed_time(self) -> None:
        ledger = _CandidateLedger(
            minimum_edge_bps=Decimal("0"),
            improvement_bps=Decimal("1"),
        )
        cycle = self._positive_cycle()

        ledger.observe(
            cycle,
            observed_realtime_ns=100_000_000_000,
            observed_monotonic_ns=1_000_000_000,
        )
        improved = ledger.observe(
            {
                **cycle,
                "net_edge_after_network_floor_bps": "7",
                "net_pnl_after_network_floor_settlement": "2",
            },
            observed_realtime_ns=90_000_000_000,
            observed_monotonic_ns=1_250_000_000,
        )

        self.assertEqual(improved[0]["event"], "candidate_improved")
        self.assertEqual(improved[0]["duration_seconds"], 0.25)


if __name__ == "__main__":
    unittest.main()
