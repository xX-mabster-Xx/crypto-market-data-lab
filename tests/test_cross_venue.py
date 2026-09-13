from __future__ import annotations

import unittest

from market_data_lab.cross_venue import BookState
from market_data_lab.cross_venue import analyze_pair
from market_data_lab.cross_venue import calculate_execution


def _state(
    venue: str,
    ts_init_ns: int,
    *,
    bids: tuple[tuple[float, float], ...],
    asks: tuple[tuple[float, float], ...],
) -> BookState:
    return BookState(
        venue=venue,
        instrument_id=f"TEST.{venue}",
        ts_init_ns=ts_init_ns,
        ts_event_ns=ts_init_ns,
        sequence=ts_init_ns,
        bids=bids,
        asks=asks,
    )


class CrossVenueExecutionTest(unittest.TestCase):
    def test_execution_walks_depth_and_uses_same_base_quantity(self) -> None:
        buy = _state(
            "BYBIT",
            1,
            bids=((99.0, 20.0),),
            asks=((100.0, 5.0), (101.0, 10.0)),
        )
        sell = _state(
            "OKX",
            1,
            bids=((102.0, 4.0), (101.0, 10.0)),
            asks=((103.0, 20.0),),
        )

        result = calculate_execution(buy, sell, 1_000.0, 10.0, 10.0)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertAlmostEqual(result.base_quantity, 5.0 + 500.0 / 101.0)
        self.assertAlmostEqual(result.gross_buy_quote, 1_000.0)
        self.assertAlmostEqual(result.gross_sell_quote, 1_009.0)
        self.assertAlmostEqual(result.gross_pnl_quote, 9.0)
        self.assertAlmostEqual(result.net_pnl_quote, 6.991)
        self.assertAlmostEqual(result.gross_edge_bps, 90.0)

    def test_execution_rejects_insufficient_depth_on_either_leg(self) -> None:
        shallow_buy = _state(
            "BYBIT",
            1,
            bids=((99.0, 1.0),),
            asks=((100.0, 1.0),),
        )
        deep_sell = _state(
            "OKX",
            1,
            bids=((101.0, 20.0),),
            asks=((102.0, 20.0),),
        )
        self.assertIsNone(calculate_execution(shallow_buy, deep_sell, 1_000.0, 0.0, 0.0))

        deep_buy = _state(
            "BYBIT",
            1,
            bids=((99.0, 20.0),),
            asks=((100.0, 20.0),),
        )
        shallow_sell = _state(
            "OKX",
            1,
            bids=((101.0, 1.0),),
            asks=((102.0, 20.0),),
        )
        self.assertIsNone(calculate_execution(deep_buy, shallow_sell, 1_000.0, 0.0, 0.0))

    def test_signal_is_as_of_event_and_latency_reprices_later_books(self) -> None:
        bybit = [
            _state("BYBIT", 1_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
            _state("BYBIT", 3_000_000, bids=((101.0, 20.0),), asks=((102.0, 20.0),)),
            _state("BYBIT", 4_000_000, bids=((101.0, 20.0),), asks=((102.0, 20.0),)),
        ]
        okx = [
            _state("OKX", 1_000_000, bids=((101.0, 20.0),), asks=((102.0, 20.0),)),
            _state("OKX", 2_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
            _state("OKX", 4_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
        ]

        report = analyze_pair(bybit, okx, [100.0], [0.0, 2.0], 0.0, 0.0)
        forward = report["results_by_target_quote_notional"]["100"][
            "buy_bybit_sell_okx"
        ]

        self.assertEqual(forward["opportunity_episodes"]["episodes"], 1)
        self.assertEqual(forward["opportunity_episodes"]["duration_ms"]["mean"], 1.0)
        self.assertEqual(forward["first_episode_starts"][0]["ts_init_ns"], 1_000_000)
        self.assertAlmostEqual(
            forward["first_episode_starts"][0]["signal"]["net_edge_bps"],
            100.0,
        )
        self.assertAlmostEqual(
            forward["latency_scenarios"]["0"]["delayed_net_edge_bps"]["mean"],
            100.0,
        )
        self.assertLess(
            forward["latency_scenarios"]["2"]["delayed_net_edge_bps"]["mean"],
            0.0,
        )

    def test_stale_opposite_book_can_be_excluded(self) -> None:
        bybit = [
            _state("BYBIT", 1_000_000, bids=((101.0, 20.0),), asks=((102.0, 20.0),)),
            _state("BYBIT", 3_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
            _state("BYBIT", 5_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
        ]
        okx = [
            _state("OKX", 1_000_000, bids=((101.0, 20.0),), asks=((102.0, 20.0),)),
            _state("OKX", 4_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
            _state("OKX", 5_000_000, bids=((99.0, 20.0),), asks=((100.0, 20.0),)),
        ]

        unfiltered = analyze_pair(bybit, okx, [100.0], [0.0], 0.0, 0.0)
        filtered = analyze_pair(
            bybit,
            okx,
            [100.0],
            [0.0],
            0.0,
            0.0,
            max_book_age_ms=1.0,
        )

        unfiltered_forward = unfiltered["results_by_target_quote_notional"]["100"][
            "buy_bybit_sell_okx"
        ]
        filtered_forward = filtered["results_by_target_quote_notional"]["100"][
            "buy_bybit_sell_okx"
        ]
        self.assertEqual(unfiltered_forward["opportunity_episodes"]["episodes"], 1)
        self.assertEqual(filtered_forward["opportunity_episodes"]["episodes"], 0)
        self.assertEqual(filtered["book_freshness"]["stale_observations_skipped"], 1)


if __name__ == "__main__":
    unittest.main()
