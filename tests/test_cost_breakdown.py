from __future__ import annotations
import unittest
from decimal import Decimal

from market_data_lab.cost_breakdown import (
    CapitalCharge,
    CostCalculator,
    CostCategory,
    CostRecord,
    DepositKind,
    FeeTier,
    GasEstimate,
    ReturnableDeposit,
    calc_capital_charge,
    calc_failed_tx_cost,
    calc_gas,
    calc_returnable_deposit,
    calc_trading_fee,
    aggregate_costs,
    calculate_total_cost,
    calculate_break_even,
)


class CostRecordTest(unittest.TestCase):
    def test_cost_record_as_dict(self) -> None:
        record = CostRecord(
            record_id="test-1",
            category="trading_fee",
            native_amount=Decimal("10.50"),
            native_currency="USDT",
            price_conversion=Decimal("1"),
            quote_currency="USDT",
            source="public",
            quality="verified",
        )
        self.assertEqual(record.amount_in_quote, Decimal("10.50"))
        self.assertFalse(record.is_returnable)

    def test_returnable_deposit_not_mixed_with_fee(self) -> None:
        deposit = ReturnableDeposit(
            deposit_kind="returnable_rent",
            amount=Decimal("0.002"),
            currency="SOL",
        )
        record = calc_returnable_deposit("deposit", deposit)
        self.assertTrue(record.is_returnable)
        self.assertEqual(record.category, "token_account_setup")

    def test_gas_estimate_total(self) -> None:
        gas = GasEstimate(
            base_fee=Decimal("0.001"),
            priority_fee=Decimal("0.0001"),
            currency="SOL",
            tx_count=2,
        )
        self.assertEqual(gas.total, Decimal("0.0011"))

    def test_capital_charge_calculation(self) -> None:
        charge = calc_capital_charge(
            annual_rate_bps=Decimal("500"),
            horizon_seconds=Decimal("86400"),
            capital_amount=Decimal("100000"),
        )
        self.assertGreater(charge.charge_amount, Decimal("0"))


class CostCalculatorTest(unittest.TestCase):
    def test_round_trip_cost(self) -> None:
        tier = FeeTier(name="test", taker_bps=Decimal("10"))
        calc = CostCalculator(fee_tier=tier)
        entry, exit_ = calc.round_trip_cost(Decimal("1000"), Decimal("100"))
        self.assertEqual(entry.amount_in_quote, Decimal("100.000"))
        self.assertEqual(exit_.amount_in_quote, Decimal("100.000"))

    def test_total_round_trip_cost_quote(self) -> None:
        tier = FeeTier(name="test", taker_bps=Decimal("10"))
        calc = CostCalculator(fee_tier=tier)
        total = calc.total_round_trip_cost_quote(Decimal("1000"), Decimal("100"))
        self.assertEqual(total, Decimal("200.000"))

    def test_with_gas(self) -> None:
        tier = FeeTier(name="test", taker_bps=Decimal("10"))
        gas = GasEstimate(base_fee=Decimal("0.001"), priority_fee=Decimal("0.0001"), currency="SOL")
        calc = CostCalculator(fee_tier=tier, gas_estimate=gas)
        entry, exit_, gas_record = calc.with_gas(Decimal("1000"), Decimal("100"))
        self.assertIsNotNone(gas_record)
        self.assertEqual(gas_record.category, "gas")


class CostBreakdownTest(unittest.TestCase):
    def test_aggregate_costs(self) -> None:
        records = [
            calc_trading_fee("entry", Decimal("1000"), Decimal("100")),
            calc_trading_fee("exit", Decimal("1000"), Decimal("100")),
            calc_gas("gas", GasEstimate(base_fee=Decimal("0.001"), priority_fee=Decimal("0"), currency="SOL")),
        ]
        breakdown = aggregate_costs("leg-test", records)
        self.assertEqual(len(breakdown.records), 3)
        self.assertGreater(breakdown.total_fees_quote, Decimal("0"))

    def test_total_returnable_vs_non_returnable(self) -> None:
        deposit = ReturnableDeposit(
            deposit_kind="returnable_rent",
            amount=Decimal("0.002"),
            currency="SOL",
        )
        records = [
            calc_trading_fee("fee", Decimal("1000"), Decimal("100")),
            calc_returnable_deposit("deposit", deposit),
        ]
        breakdown = aggregate_costs("leg-test", records)
        self.assertGreater(breakdown.total_non_returnable_quote, Decimal("0"))
        self.assertGreater(breakdown.total_returnable_quote, Decimal("0"))
        self.assertEqual(breakdown.total_returnable_quote, Decimal("0.002"))

    def test_by_category(self) -> None:
        records = [
            calc_trading_fee("entry", Decimal("1000"), Decimal("100")),
            calc_gas("gas", GasEstimate(base_fee=Decimal("0.001"), priority_fee=Decimal("0"), currency="SOL")),
        ]
        breakdown = aggregate_costs("leg-test", records)
        by_cat = breakdown.by_category()
        self.assertIn("trading_fee", by_cat)
        self.assertIn("gas", by_cat)

    def test_calculate_total_cost(self) -> None:
        records = [
            calc_trading_fee("entry", Decimal("1000"), Decimal("100")),
            calc_trading_fee("exit", Decimal("1000"), Decimal("100")),
        ]
        breakdown = aggregate_costs("leg-test", records)
        total = calculate_total_cost(breakdown)
        self.assertEqual(total, Decimal("200.000"))

    def test_calculate_break_even(self) -> None:
        break_even = calculate_break_even(Decimal("1000"), Decimal("100"), Decimal("20"))
        self.assertEqual(break_even, Decimal("100.02"))


class FailedTxCostTest(unittest.TestCase):
    def test_failed_tx_cost(self) -> None:
        gas = GasEstimate(
            base_fee=Decimal("0.001"),
            priority_fee=Decimal("0.0001"),
            currency="SOL",
            failed_tx_probability=Decimal("0.1"),
            failed_tx_cost=Decimal("0.0005"),
        )
        record = calc_failed_tx_cost("failed", gas)
        self.assertEqual(record.category, "failed_tx_cost")
        self.assertEqual(record.native_amount, Decimal("0.0005"))


if __name__ == "__main__":
    unittest.main()
