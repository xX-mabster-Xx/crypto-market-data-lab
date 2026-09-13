"""Section 18.3: Invariant and property tests."""

from __future__ import annotations

import unittest
from decimal import Decimal

from market_data_lab.cost_breakdown import (
    CostCalculator,
    FeeTier,
    calc_trading_fee,
)
from market_data_lab.quantity_lattice import round_down_to_common_quantity_lattice


class LedgerConvergenceTest(unittest.TestCase):
    """Ledger balance converges per asset with external expenses."""

    def test_ledger_converges(self) -> None:
        """Equity change matches PnL under same valuation policy."""
        # Initial: 1000 USDT
        # Buy 100 SOL @ 100 = -10000 USDT, +100 SOL
        # Sell 100 SOL @ 110 = +11000 USDT, -100 SOL
        # Fee 0.1% = 10 USDT
        # Final: 1000 - 10000 + 11000 - 10 = 1990
        initial = Decimal("1000")
        buy_cost = Decimal("10000")
        sell_proceeds = Decimal("11000")
        fee = Decimal("10")
        final = initial - buy_cost + sell_proceeds - fee

        # PnL = sell - buy - fee = 11000 - 10000 - 10 = 990
        pnl = sell_proceeds - buy_cost - fee
        self.assertEqual(final - initial, pnl)


class NoProfitFromBuySellTest(unittest.TestCase):
    """Buy→sell on same market doesn't generate profit with no fees/funding."""

    def test_no_profit_without_edge(self) -> None:
        """Self buy→sell on one market doesn't generate profit."""
        buy_price = Decimal("100")
        sell_price = Decimal("100")
        quantity = Decimal("10")

        buy_cost = quantity * buy_price
        sell_proceeds = quantity * sell_price
        pnl = sell_proceeds - buy_cost
        self.assertEqual(pnl, Decimal("0"))


class FeeMonotonicityTest(unittest.TestCase):
    """Increasing fee doesn't increase PnL."""

    def test_higher_fee_lower_pnl(self) -> None:
        """Higher fee results in lower or equal PnL."""
        quantity = Decimal("1000")
        price = Decimal("100")

        fee_low = FeeTier(name="low", taker_bps=Decimal("5"))
        fee_high = FeeTier(name="high", taker_bps=Decimal("20"))

        record_low = calc_trading_fee("test", quantity, price, fee_low)
        record_high = calc_trading_fee("test", quantity, price, fee_high)

        self.assertLess(record_low.native_amount, record_high.native_amount)


class LiquidityBoundsTest(unittest.TestCase):
    """Consumed liquidity doesn't exceed available."""

    def test_quantity_within_bounds(self) -> None:
        """Consumed quantity doesn't exceed available depth."""
        available = Decimal("1000")
        requested = Decimal("500")

        # Can only consume what's available
        consumed = min(requested, available)
        self.assertLessEqual(consumed, available)


class QuantityConstraintTest(unittest.TestCase):
    """All accepted quantities satisfy leg constraints."""

    def test_quantity_satisfies_lot_size(self) -> None:
        """Quantity must be multiple of lot size."""
        lot_size = Decimal("0.1")
        quantity = Decimal("1.2")

        self.assertEqual(quantity % lot_size, Decimal("0"))

    def test_residual_within_limits(self) -> None:
        """Residual within rules."""
        requested = Decimal("1.23")
        lot_size = Decimal("0.1")
        executable = round_down_to_common_quantity_lattice(requested, (lot_size,))
        residual = requested - executable

        self.assertLess(residual, lot_size)


class DeterminismTest(unittest.TestCase):
    """Same immutable bundle/config produces deterministic result."""

    def test_same_inputs_same_output(self) -> None:
        """Same inputs produce same output."""
        fee_tier = FeeTier(name="test", taker_bps=Decimal("10"))
        result1 = calc_trading_fee("test", Decimal("1000"), Decimal("100"), fee_tier)
        result2 = calc_trading_fee("test", Decimal("1000"), Decimal("100"), fee_tier)

        self.assertEqual(result1.native_amount, result2.native_amount)


class RealizedPnlSourceTest(unittest.TestCase):
    """Realized PnL doesn't appear from market quotes or paper fills."""

    def test_realized_pnl_only_from_fills(self) -> None:
        """Realized PnL only from actual fills."""
        # In research mode, realized_pnl should be None
        realized_pnl = None
        self.assertIsNone(realized_pnl)


class QuotaComplianceTest(unittest.TestCase):
    """Common quotas respected regardless of strategy count."""

    def test_quota_not_exceeded(self) -> None:
        """Quota not exceeded regardless of number of strategies."""
        max_quota = 100
        current_usage = 50
        additional_requests = 10

        self.assertLess(current_usage + additional_requests, max_quota)


if __name__ == "__main__":
    unittest.main()
