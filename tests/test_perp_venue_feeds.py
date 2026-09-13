from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.perp_venue_feeds import parse_hyperliquid_active_asset_context
from market_data_lab.perp_venue_feeds import parse_hyperliquid_l2_book
from market_data_lab.perp_venue_feeds import parse_hyperliquid_meta_and_contexts
from market_data_lab.perp_venue_feeds import parse_dydx_perp_markets
from market_data_lab.perp_venue_feeds import _uncross_dydx_book


class HyperliquidParsingTest(unittest.TestCase):
    def test_l2_book_uses_executable_sides_and_common_linear_shape(self) -> None:
        book = parse_hyperliquid_l2_book(
            {
                "channel": "l2Book",
                "data": {
                    "coin": "SOL",
                    "time": 123,
                    "levels": [
                        [{"px": "150", "sz": "2"}, {"px": "149", "sz": "3"}],
                        [{"px": "151", "sz": "4"}, {"px": "152", "sz": "5"}],
                    ],
                },
            },
        )
        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual(book.symbol, "SOL")
        self.assertEqual(book.category, "linear")
        self.assertEqual(book.bids[0], (Decimal("150"), Decimal("2")))
        self.assertEqual(book.asks[0], (Decimal("151"), Decimal("4")))

    def test_context_and_meta_do_not_guess_price_tick_or_fees(self) -> None:
        context = parse_hyperliquid_active_asset_context(
            {
                "channel": "activeAssetCtx",
                "data": {
                    "coin": "SOL",
                    "time": 456,
                    "ctx": {
                        "funding": "0.0001",
                        "markPx": "150.2",
                        "oraclePx": "150.1",
                        "openInterest": "42",
                    },
                },
            },
        )
        self.assertIsNotNone(context)
        assert context is not None
        self.assertEqual(context[1].funding_rate, Decimal("0.0001"))
        self.assertEqual(context[1].mark_price, Decimal("150.2"))

        contracts = parse_hyperliquid_meta_and_contexts(
            [{"universe": [{"name": "SOL", "szDecimals": 2}]}, [{}]],
            coins=("SOL",),
        )
        contract = contracts["SOL"]
        self.assertEqual(contract.quantity_step, Decimal("0.01"))
        self.assertIsNone(contract.tick_size)
        self.assertIsNone(contract.public_taker_fee_bps)

    def test_dydx_keeps_next_funding_semantics_and_uncrosses_by_offset(self) -> None:
        markets = parse_dydx_perp_markets(
            {
                "markets": {
                    "SOL-USD": {
                        "ticker": "SOL-USD",
                        "status": "ACTIVE",
                        "tickSize": "0.01",
                        "stepSize": "0.1",
                        "oraclePrice": "150",
                        "nextFundingRate": "0.0002",
                        "openInterest": "10",
                    },
                },
            },
            bases=("SOL",),
        )
        contract, context = markets["SOL-USD"]
        self.assertEqual(contract.tick_size, Decimal("0.01"))
        self.assertEqual(context.funding_rate, Decimal("0.0002"))
        self.assertEqual(context.funding_rate_kind, "next_hourly_rate")

        bids = {Decimal("101"): (Decimal("3"), 1)}
        asks = {Decimal("100"): (Decimal("2"), 2)}
        _uncross_dydx_book(bids, asks)
        self.assertEqual(bids, {})
        self.assertEqual(asks, {Decimal("100"): (Decimal("2"), 2)})


if __name__ == "__main__":
    unittest.main()
