from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.execution_cost import cost_to_acquire
from market_data_lab.execution_cost import proceeds_from_sell


class OrderBookExecutionCostTest(unittest.TestCase):
    def test_t01_quote_fee_and_gas_produce_independent_expected_pnl(self) -> None:
        estimate = cost_to_acquire(
            Decimal("100"),
            (
                (Decimal("1"), Decimal("50")),
                (Decimal("1.02"), Decimal("50")),
            ),
            fee_bps=Decimal("10"),
            fee_currency="quote",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic_public_fee",
            fee_quality="scenario",
        )

        self.assertTrue(estimate.complete)
        self.assertEqual(estimate.gross_book_quote_amount, Decimal("101.00"))
        self.assertEqual(estimate.fee_amount, Decimal("0.10100"))
        self.assertEqual(estimate.net_quote_movement, Decimal("-101.10100"))
        self.assertEqual(estimate.net_base_movement, Decimal("100"))
        self.assertEqual(estimate.levels_consumed, 2)
        # DEX output 104 already includes its pool fee; subtracting the CEX
        # cash requirement and gas must not apply that pool fee a second time.
        pnl_after_gas = Decimal("104") + estimate.net_quote_movement - Decimal("0.2")
        self.assertEqual(pnl_after_gas, Decimal("2.69900"))

    def test_t02_base_fee_walks_gross_quantity_through_next_level(self) -> None:
        estimate = cost_to_acquire(
            Decimal("100"),
            (
                (Decimal("1"), Decimal("100")),
                (Decimal("2"), Decimal("1")),
            ),
            fee_bps=Decimal("10"),
            fee_currency="base",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic_base_fee",
            fee_quality="scenario",
        )

        expected_gross_base = Decimal("100") / Decimal("0.999")
        expected_cost = Decimal("100") + (expected_gross_base - Decimal("100")) * 2
        self.assertTrue(estimate.complete)
        self.assertEqual(estimate.requested_book_base_quantity, expected_gross_base)
        self.assertEqual(estimate.filled_book_base_quantity, expected_gross_base)
        self.assertEqual(estimate.net_base_movement, Decimal("100"))
        self.assertEqual(estimate.gross_book_quote_amount, expected_cost)
        self.assertEqual(estimate.levels_consumed, 2)
        self.assertEqual(estimate.marginal_price, Decimal("2"))

    def test_missing_depth_returns_partial_evidence_without_extrapolation(self) -> None:
        estimate = cost_to_acquire(
            Decimal("2"),
            ((Decimal("100"), Decimal("1")),),
            fee_bps=Decimal("0"),
            fee_currency="quote",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic",
            fee_quality="scenario",
        )

        self.assertFalse(estimate.complete)
        self.assertEqual(estimate.status, "insufficient_known_depth")
        self.assertEqual(estimate.filled_book_base_quantity, Decimal("1"))
        self.assertEqual(estimate.unfilled_book_base_quantity, Decimal("1"))
        self.assertEqual(estimate.gross_book_quote_amount, Decimal("100"))

    def test_sell_walk_applies_quote_fee_to_actual_depth_proceeds(self) -> None:
        estimate = proceeds_from_sell(
            Decimal("3"),
            (
                (Decimal("101"), Decimal("2")),
                (Decimal("100"), Decimal("2")),
            ),
            fee_bps=Decimal("10"),
            fee_currency="quote",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic",
            fee_quality="scenario",
        )

        self.assertTrue(estimate.complete)
        self.assertEqual(estimate.gross_book_quote_amount, Decimal("302"))
        self.assertEqual(estimate.fee_amount, Decimal("0.302"))
        self.assertEqual(estimate.net_quote_movement, Decimal("301.698"))
        self.assertEqual(estimate.net_base_movement, Decimal("-3"))
        self.assertEqual(estimate.average_price, Decimal("302") / Decimal("3"))

    def test_unordered_or_nonpositive_book_is_invalid(self) -> None:
        unordered = cost_to_acquire(
            Decimal("1"),
            (
                (Decimal("101"), Decimal("1")),
                (Decimal("100"), Decimal("1")),
            ),
            fee_bps=Decimal("0"),
            fee_currency="quote",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic",
            fee_quality="scenario",
        )
        nonpositive = proceeds_from_sell(
            Decimal("1"),
            ((Decimal("0"), Decimal("1")),),
            fee_bps=Decimal("0"),
            fee_currency="quote",
            base_currency="BASE",
            quote_currency="USDT",
            fee_source="synthetic",
            fee_quality="scenario",
        )

        self.assertEqual(unordered.status, "book_invalid")
        self.assertEqual(unordered.filled_book_base_quantity, Decimal("0"))
        self.assertEqual(nonpositive.status, "book_invalid")

    def test_invalid_request_or_fee_contract_raises(self) -> None:
        common = {
            "asks": ((Decimal("1"), Decimal("1")),),
            "base_currency": "BASE",
            "quote_currency": "USDT",
            "fee_source": "synthetic",
            "fee_quality": "scenario",
        }
        with self.assertRaises(ValueError):
            cost_to_acquire(
                Decimal("0"),
                fee_bps=Decimal("0"),
                fee_currency="quote",
                **common,
            )
        with self.assertRaises(ValueError):
            cost_to_acquire(
                Decimal("1"),
                fee_bps=Decimal("10000"),
                fee_currency="quote",
                **common,
            )


if __name__ == "__main__":
    unittest.main()
