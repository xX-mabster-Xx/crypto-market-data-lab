"""S10: Hedge transfer strategy.

Section 3: S10 closes old perp leg and opens new; compares keep vs switch.
POS-05: Compare future cost of keep vs switch on same horizon.
Historical entry fees not re-deducted from incremental effect.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .base import StrategyTemplate, StrategyResult, ScreeningBounds
from ..position_engine.contracts import ExitPolicy


@dataclass
class HedgeSwitchResult(StrategyResult):
    """Result for hedge transfer."""

    keep_cost: Decimal | None = None
    switch_cost: Decimal | None = None
    incremental_effect: Decimal | None = None


class S10_HedgeTransfer(StrategyTemplate):
    """S10: Transfer existing hedge from old perp to new perp."""

    strategy_id = "hedge_transfer"
    required_data = ("old_perp_book", "new_perp_book", "exit_costs", "entry_costs")
    supported_contract_models = ("linear",)

    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        old_price = view.get("old_perp_price", Decimal("0"))
        new_price = view.get("new_perp_price", Decimal("0"))

        if old_price == 0 or new_price == 0:
            return None

        basis_improvement = view.get("basis_improvement", Decimal("0"))
        if basis_improvement <= 0:
            return None

        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=capital_available / old_price,
            capital_required=capital_available,
        )

    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        qty = bounds.max_quantity
        old_price = view.get("old_perp_price", Decimal("0"))
        new_price = view.get("new_perp_price", Decimal("0"))

        # POS-05: Don't re-deduct old entry fees from incremental effect
        old_exit_cost = view.get("exit_cost_old", Decimal("0"))
        new_entry_cost = view.get("entry_cost_new", Decimal("0"))
        switch_costs = old_exit_cost + new_entry_cost

        # Keep cost: remaining funding obligation on old position
        keep_cost = view.get("keep_cost", Decimal("0"))

        incremental = keep_cost - switch_costs - qty * (old_price - new_price)

        return HedgeSwitchResult(
            strategy_id=self.strategy_id,
            route=("close_old_perp", "open_new_perp"),
            quantities={"perp_base": qty},
            entry_cost_quote=switch_costs,
            projected_pnl_by_scenario={
                "switch_vs_keep": incremental,
            },
            costs_total_quote=switch_costs,
            capital_required=qty * new_price,
            confidence="screened",
            keep_cost=keep_cost,
            switch_cost=switch_costs,
            incremental_effect=incremental,
        )
