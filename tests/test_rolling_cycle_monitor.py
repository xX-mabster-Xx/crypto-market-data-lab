from __future__ import annotations

import asyncio
import contextlib
import io
import json
import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.rolling_cycle_monitor import CandidateTracker
from market_data_lab.rolling_cycle_monitor import MAXIMUM_COVERAGE_MARKETS
from market_data_lab.rolling_cycle_monitor import MAXIMUM_COVERAGE_PROFILES
from market_data_lab.rolling_cycle_monitor import RecentWindow
from market_data_lab.rolling_cycle_monitor import record_rolling_cycle_monitor


MARKET = CycleMarket(
    name="TEST_ROLLING",
    provider="TEST_PROVIDER",
    chain="base",
    dex_pair="TEST/USDC",
    cex_symbol="TESTUSDC",
    cex_base_symbol="TEST",
    quote_symbol="USDC",
    asset_equivalence="test",
)


def _cycle(*, edge: str, positive: bool = True) -> dict[str, object]:
    return {
        "status": "ok",
        "timing_valid": True,
        "positive_after_minimum_network": positive,
        "net_edge_after_minimum_network_bps": edge,
        "net_pnl_after_minimum_network_quote": "0.25",
        "cex_venue": "BYBIT",
        "market": "TEST_ROLLING",
        "cycle_direction": "buy_dex_sell_cex",
        "requested_notional_quote": "100",
    }


class RollingRetentionTest(unittest.TestCase):
    def test_recent_window_discards_rows_older_than_retention(self) -> None:
        now = time.time_ns()
        window = RecentWindow(1)
        window.append({"id": "old"}, observed_realtime_ns=now - 2_000_000_000)
        window.append({"id": "current"}, observed_realtime_ns=now)

        window.evict(now_realtime_ns=now)

        self.assertEqual([row["id"] for row in window.rows()], ["current"])

    def test_candidate_tracker_records_start_improvement_and_close(self) -> None:
        tracker = CandidateTracker(Decimal("0"))
        start = 1_000_000_000

        started = tracker.observe(_cycle(edge="5"), observed_realtime_ns=start)
        improved = tracker.observe(_cycle(edge="7"), observed_realtime_ns=start + 1_000_000_000)
        closed = tracker.observe(
            _cycle(edge="-1", positive=False),
            observed_realtime_ns=start + 3_000_000_000,
        )

        self.assertEqual(started[0]["event"], "candidate_started")
        self.assertEqual(improved[0]["event"], "candidate_improved")
        self.assertEqual(closed[0]["event"], "candidate_closed")
        self.assertEqual(closed[0]["duration_seconds"], 3.0)
        self.assertEqual(closed[0]["max_net_edge_after_minimum_network_bps"], "7")
        self.assertEqual(tracker.active, {})

    def test_maximum_profiles_are_a_disjoint_44_route_universe(self) -> None:
        listed = [
            market
            for profile in MAXIMUM_COVERAGE_PROFILES.values()
            for market in profile.markets
        ]

        self.assertEqual(len(MAXIMUM_COVERAGE_MARKETS), 44)
        self.assertEqual(len(listed), len(set(listed)))
        self.assertEqual(set(listed), set(MAXIMUM_COVERAGE_MARKETS))


class RollingMonitorIntegrationTest(unittest.TestCase):
    def test_monitor_keeps_recent_window_and_persists_candidate_lifecycle(self) -> None:
        class FakeProvider:
            name = "TEST_PROVIDER"

            async def quote_round(
                self,
                round_id: int,
                notionals: list[Decimal],
            ) -> list[dict[str, object]]:
                received = time.time_ns()
                return [
                    {
                        "schema_version": 1,
                        "round_id": round_id,
                        "status": "ok",
                        "direction": "buy_base",
                        "requested_notional_quote": "100",
                        "base_amount": "1",
                        "quote_amount": "100",
                        "average_price_quote_per_base": "100",
                        "response_received_realtime_ns": received,
                        "request_rtt_ms": 1,
                    },
                    {
                        "schema_version": 1,
                        "round_id": round_id,
                        "status": "ok",
                        "direction": "sell_base",
                        "requested_notional_quote": "100",
                        "base_amount": "1",
                        "quote_amount": "100",
                        "average_price_quote_per_base": "100",
                        "response_received_realtime_ns": received,
                        "request_rtt_ms": 1,
                    },
                ]

            def config(self) -> dict[str, object]:
                return {"provider": self.name, "api_credentials_used": False}

        def fake_fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            self.assertIn("api.bybit.com", url)
            return {
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "b": [["102", "10"]],
                    "a": [["103", "10"]],
                    "ts": 1,
                    "cts": 1,
                    "u": 1,
                    "seq": 1,
                },
            }

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "rolling"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                manifest = asyncio.run(
                    record_rolling_cycle_monitor(
                        [MARKET],
                        {"TEST_PROVIDER": FakeProvider()},
                        notionals=[Decimal("100")],
                        duration_seconds=0.001,
                        interval_seconds=1,
                        cex_venues=("BYBIT",),
                        cex_taker_fees={"BYBIT": Decimal("0")},
                        network_cost_floors={"base": Decimal("0")},
                        max_response_skew_ms=Decimal("1000"),
                        retention_seconds=60,
                        output_directory=output,
                        proxy_url=None,
                        timeout_seconds=1,
                        use_mexc_websocket=False,
                        stdout_candidates=True,
                        fetch_json=fake_fetch,
                        account_fee_rates={
                            ("BYBIT", "TESTUSDC"): SpotFeeRate(
                                venue="BYBIT",
                                symbol="TESTUSDC",
                                maker_buy_bps=Decimal("0"),
                                maker_sell_bps=Decimal("0"),
                                taker_buy_bps=Decimal("0"),
                                taker_sell_bps=Decimal("0"),
                                account_verified=True,
                                source="test_account_fee_api",
                            ),
                        },
                    ),
                )

            events = [
                json.loads(line)
                for line in (output / "candidate_events.jsonl").read_text().splitlines()
            ]
            recent = [json.loads(line) for line in (output / "recent.jsonl").read_text().splitlines()]
            stats = json.loads((output / "stats.json").read_text())

            self.assertEqual(manifest["status"], "completed")
            self.assertEqual([event["event"] for event in events], ["candidate_started", "candidate_closed"])
            self.assertTrue(recent)
            self.assertEqual(stats["candidate_lifecycle"]["started"], 1)
            self.assertEqual(stats["candidate_lifecycle"]["closed"], 1)
            self.assertGreater(stats["positive_with_account_verified_fee_observations"], 0)
            console_events = [json.loads(line) for line in stdout.getvalue().splitlines()]
            self.assertEqual(
                [event["event"] for event in console_events],
                ["candidate_started", "candidate_closed"],
            )


if __name__ == "__main__":
    unittest.main()
