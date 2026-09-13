from __future__ import annotations

import json
import time
import unittest
from decimal import Decimal

from market_data_lab.cex_book_streams import BybitLinearTicker
from market_data_lab.cex_book_streams import BybitLinearOrderBookStream
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.perp_dex_monitor import BybitLinearInstrument
from market_data_lab.perp_dex_monitor import BasisCandidateTracker
from market_data_lab.perp_dex_monitor import PerpFeeRate
from market_data_lab.perp_dex_monitor import _bybit_hmac_headers
from market_data_lab.perp_dex_monitor import calculate_perp_dex_basis
from market_data_lab.perp_dex_monitor import exact_perp_step_targets
from market_data_lab.perp_dex_monitor import parse_bybit_fee_rate
from market_data_lab.perp_dex_monitor import parse_bybit_linear_instruments


MARKET = CycleMarket(
    name="BTC_TEST",
    provider="TEST",
    chain="base",
    dex_pair="cbBTC/USDC",
    cex_symbol="BTCUSDC",
    cex_base_symbol="BTC",
    quote_symbol="USDT",
    asset_equivalence="test wrapper basis",
)
INSTRUMENT = BybitLinearInstrument(
    symbol="BTCUSDT",
    base_coin="BTC",
    quote_coin="USDT",
    settle_coin="USDT",
    contract_type="LinearPerpetual",
    status="Trading",
    min_order_qty=Decimal("0.001"),
    qty_step=Decimal("0.001"),
    max_market_order_qty=Decimal("100"),
    min_notional_value=Decimal("5"),
    tick_size=Decimal("0.1"),
    funding_interval_minutes=480,
    upper_funding_rate=Decimal("0.01"),
    lower_funding_rate=Decimal("-0.01"),
)
FEE = PerpFeeRate(
    symbol="BTCUSDT",
    taker_fee_bps=Decimal("5"),
    maker_fee_bps=Decimal("2"),
    source="test",
    account_verified=True,
)


def _book(*, category: str = "linear", bid: str = "105", ask: str = "106") -> BookSnapshot:
    now_real = time.time_ns()
    now_mono = time.monotonic_ns()
    return BookSnapshot(
        symbol="BTCUSDT" if category == "linear" else "USDCUSDT",
        category=category,
        status="ok",
        error=None,
        bids=((Decimal(bid), Decimal("10")),),
        asks=((Decimal(ask), Decimal("10")),),
        exchange_system_time_ms=None,
        matching_engine_time_ms=None,
        update_id=1,
        cross_sequence=1,
        response=TimedResponse(
            payload=None,
            error=None,
            sent_realtime_ns=now_real,
            received_realtime_ns=now_real,
            sent_monotonic_ns=now_mono,
            received_monotonic_ns=now_mono,
        ),
        source="test",
    )


def _ticker() -> BybitLinearTicker:
    return BybitLinearTicker(
        symbol="BTCUSDT",
        funding_rate=Decimal("0.0001"),
        next_funding_time_ms=1_800_000_000_000,
        mark_price=Decimal("100"),
        index_price=Decimal("100"),
        received_realtime_ns=time.time_ns(),
        received_monotonic_ns=time.monotonic_ns(),
        exchange_system_time_ms=None,
    )


def _dex_record(direction: str, quote_amount: str) -> dict[str, object]:
    return {
        "status": "ok",
        "direction": direction,
        "requested_notional_quote": "100",
        "base_amount": "1",
        "quote_amount": quote_amount,
        "response_received_realtime_ns": time.time_ns(),
        "request_rtt_ms": 1,
    }


class PerpDexModelTest(unittest.TestCase):
    def test_candidate_tracker_can_label_public_fee_schedule_candidates(self) -> None:
        row = {
            "market": "BTC_TEST",
            "bybit_symbol": "BTCUSDT",
            "dex_direction": "buy_base",
            "requested_notional_quote": "100",
            "status": "ok",
            "timing_valid": True,
            "positive_after_modeled_costs": True,
            "perp_fee_account_verified": False,
            "modeled_entry_basis_after_one_current_funding_interval_usdt": "1.25",
        }

        strict = BasisCandidateTracker()
        reconnaissance = BasisCandidateTracker(require_account_verified_fee=False)

        self.assertEqual(strict.observe(row, observed_ns=time.time_ns()), [])
        events = reconnaissance.observe(row, observed_ns=time.time_ns())
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event"], "candidate_started")

    def test_linear_stream_keeps_book_and_delta_ticker_in_memory(self) -> None:
        stream = BybitLinearOrderBookStream(("BTCUSDT",), timeout_seconds=1, proxy_url=None)
        stream._handle_raw(
            json.dumps(
                {
                    "topic": "orderbook.50.BTCUSDT",
                    "type": "snapshot",
                    "ts": 10,
                    "seq": 1,
                    "data": {"s": "BTCUSDT", "b": [["100", "1"]], "a": [["101", "1"]], "u": 1, "cts": 9},
                },
            ),
        )
        stream._handle_raw(
            json.dumps(
                {
                    "topic": "tickers.BTCUSDT",
                    "type": "snapshot",
                    "ts": 11,
                    "data": {"symbol": "BTCUSDT", "markPrice": "100.5", "indexPrice": "100", "fundingRate": "0.0001", "nextFundingTime": "1000"},
                },
            ),
        )
        stream._handle_raw(
            json.dumps(
                {"topic": "tickers.BTCUSDT", "type": "delta", "ts": 12, "data": {"fundingRate": "0.0002"}},
            ),
        )
        book = stream.nearest_snapshot("BTCUSDT", time.time_ns())
        ticker = stream.perp_ticker("BTCUSDT")
        self.assertIsNotNone(book)
        self.assertEqual(book.category, "linear")
        self.assertIsNotNone(ticker)
        assert ticker is not None
        self.assertEqual(ticker.funding_rate, Decimal("0.0002"))
        self.assertEqual(ticker.mark_price, Decimal("100.5"))

    def test_parser_filters_to_usdt_linear_perpetuals(self) -> None:
        payload = {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "contractType": "LinearPerpetual",
                        "status": "Trading",
                        "fundingInterval": 480,
                        "upperFundingRate": "0.003",
                        "lowerFundingRate": "-0.003",
                        "lotSizeFilter": {
                            "minOrderQty": "0.001",
                            "qtyStep": "0.001",
                            "maxMktOrderQty": "100",
                            "minNotionalValue": "5",
                        },
                        "priceFilter": {"tickSize": "0.1"},
                    },
                    {
                        "symbol": "BTCUSD",
                        "baseCoin": "BTC",
                        "quoteCoin": "USD",
                        "settleCoin": "BTC",
                        "contractType": "InversePerpetual",
                        "status": "Trading",
                        "lotSizeFilter": {},
                        "priceFilter": {},
                    },
                ],
            },
        }
        parsed = parse_bybit_linear_instruments(payload)
        self.assertEqual(tuple(parsed), ("BTCUSDT",))
        self.assertEqual(parsed["BTCUSDT"].qty_step, Decimal("0.001"))

    def test_short_hedges_dex_buy_and_reserves_both_perp_fees(self) -> None:
        row = calculate_perp_dex_basis(
            market=MARKET,
            dex_record=_dex_record("buy_base", "100"),
            perp_book=_book(),
            stable_book=None,
            instrument=INSTRUMENT,
            fee_rate=FEE,
            ticker=_ticker(),
            network_cost_floor_usdt=Decimal("0"),
            max_response_skew_ms=Decimal("1000"),
            max_ticker_age_ms=Decimal("1000"),
            max_hedge_residual_bps=Decimal("1"),
        )
        self.assertEqual(row["perp_side"], "short")
        self.assertEqual(row["perp_open_fee_usdt"], "0.0525")
        self.assertEqual(row["perp_close_fee_reserve_usdt"], "0.053")
        self.assertEqual(row["funding_pnl_if_held_one_current_interval_usdt"], "0.01")
        self.assertTrue(row["positive_after_modeled_costs"])
        self.assertTrue(row["perp_fee_account_verified"])

    def test_long_hedges_dex_sell(self) -> None:
        row = calculate_perp_dex_basis(
            market=MARKET,
            dex_record=_dex_record("sell_base", "110"),
            perp_book=_book(),
            stable_book=None,
            instrument=INSTRUMENT,
            fee_rate=FEE,
            ticker=_ticker(),
            network_cost_floor_usdt=Decimal("0"),
            max_response_skew_ms=Decimal("1000"),
            max_ticker_age_ms=Decimal("1000"),
            max_hedge_residual_bps=Decimal("1"),
        )
        self.assertEqual(row["perp_side"], "long")
        self.assertEqual(row["funding_pnl_if_held_one_current_interval_usdt"], "-0.01")
        self.assertTrue(row["positive_after_modeled_costs"])

    def test_fee_parser_and_read_only_signature_are_deterministic(self) -> None:
        fee = parse_bybit_fee_rate(
            {
                "retCode": 0,
                "result": {
                    "list": [{"symbol": "BTCUSDT", "takerFeeRate": "0.00055", "makerFeeRate": "0.0002"}],
                },
            },
            symbol="BTCUSDT",
        )
        self.assertEqual(fee.taker_fee_bps, Decimal("5.50000"))
        headers = _bybit_hmac_headers(
            api_key="key",
            api_secret="secret",
            query="category=linear&symbol=BTCUSDT",
            timestamp_ms=1_700_000_000_000,
        )
        self.assertEqual(headers["X-BAPI-TIMESTAMP"], "1700000000000")
        self.assertEqual(len(headers["X-BAPI-SIGN"]), 64)

    def test_exact_step_targets_floor_to_bybit_contract_step(self) -> None:
        mid, targets = exact_perp_step_targets(
            [Decimal("100"), Decimal("250")],
            book=_book(bid="100", ask="100"),
            instrument=INSTRUMENT,
        )

        self.assertEqual(mid, Decimal("100"))
        self.assertEqual(targets, [(Decimal("100"), Decimal("1")), (Decimal("250"), Decimal("2.5"))])


if __name__ == "__main__":
    unittest.main()
