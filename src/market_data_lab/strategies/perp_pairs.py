"""S08: Long perp A + short perp B strategy.

Section 3: S08 captures inter-perp basis + funding differential.
Requires: both books, contract multipliers, calendars, separate margin.
T05: Constant spread + no funding = no profit from mere existence.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .base import StrategyTemplate, StrategyResult, ScreeningBounds
from ..position_engine.contracts import ExitPolicy, Leg


@dataclass
class PerpPairsResult(StrategyResult):
    """Result for perp pairs strategy."""

    spread_bps: Decimal | None = None
    funding_differential: Decimal | None = None


class S08_LongPerpShortPerp(StrategyTemplate):
    """S08: Long perp A + short perp B."""

    strategy_id = "long_perp_short_perp"
    required_data = ("perp_a_book", "perp_b_book", "perp_a_capability", "perp_b_capability")
    supported_contract_models = ("linear",)

    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        perp_a = view.get("perp_a_book")
        perp_b = view.get("perp_b_book")
        if perp_a is None or perp_b is None:
            return None

        cap_a = view.get("perp_a_capability")
        cap_b = view.get("perp_b_capability")
        if cap_a is None or cap_b is None:
            return None

        # Need same settlement currency
        if cap_a.quote_asset_id != cap_b.quote_asset_id:
            return None

        # T05: constant spread without funding = no profit
        price_a = perp_a.best_bid.price if perp_a.best_ask else Decimal("0")
        price_b = perp_b.best_bid.price if perp_b.best_ask else Decimal("0")

        # Basis in quote terms
        basis_bps = (price_a * cap_a.multiplier - price_b * cap_b.multiplier) / price_a * Decimal("10000")

        if basis_bps == 0:
            # No edge — constant spread
            return None

        # Check funding differential
        funding_a = view.get("funding_a_rate", Decimal("0"))
        funding_b = view.get("funding_b_rate", Decimal("0"))
        funding_diff = funding_a - funding_b

        max_qty = min(
            capital_available / price_a,
            Decimal("1000000"),
        )

        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=max_qty,
            capital_required=max_qty * price_a,
            upper_bound_pnl=(basis_bps / Decimal("10000")) * max_qty * price_a * cap_a.multiplier,
        )

    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        cap_a = view["perp_a_capability"]
        cap_b = view["perp_b_capability"]
        perp_a = view["perp_a_book"]
        perp_b = view["perp_b_book"]

        qty = bounds.max_quantity
        price_a = perp_a.best_bid.price if perp_a.best_ask else Decimal("0")
        price_b = perp_b.best_bid.price if perp_b.best_ask else Decimal("0")

        basis_bps = (price_a * cap_a.multiplier - price_b * cap_b.multiplier) / price_a * Decimal("10000")

        funding_a = view.get("funding_a_rate", Decimal("0"))
        funding_b = view.get("funding_b_rate", Decimal("0"))
        funding_diff = funding_a - funding_b

        return PerpPairsResult(
            strategy_id=self.strategy_id,
            route=("perp_a_long", "perp_b_short"),
            quantities={"perp_a": qty * cap_a.multiplier, "perp_b": -qty * cap_b.multiplier},
            entry_cost_quote=qty * price_a * cap_a.multiplier,
            projected_pnl_by_scenario={
                "unchanged_basis": (basis_bps / Decimal("10000")) * qty * price_a * cap_a.multiplier,
                "basis_reverses": -qty * price_a * cap_a.multiplier,
            },
            costs_total_quote=Decimal("0.50"),
            capital_required=qty * price_a * cap_a.multiplier,
            confidence="screened",
            spread_bps=basis_bps,
            funding_differential=funding_diff,
        )
