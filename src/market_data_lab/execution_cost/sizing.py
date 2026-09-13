"""Quantity lattice and size optimization.

Section 8.5: SIZE-01 — quantities on common lattice via integer scale and LCM.
SIZE-02 — exact_neutral and bounded_residual modes.
Section 10.5: Size optimization via grid evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from typing import Sequence

from ..quantity_lattice import common_quantity_step, round_down_to_common_quantity_lattice


@dataclass(frozen=True, slots=True)
class SizingPlan:
    """A sizing plan result for a candidate."""

    quantity: Decimal
    mode: str  # "exact_neutral" | "bounded_residual"
    residual: Decimal = Decimal("0")
    residual_asset_id: str | None = None
    capital_required: Decimal = Decimal("0")
    reason: str = ""
    confidence: str = "verified"


@dataclass
class _GridResult:
    quantity: Decimal
    pnL: Decimal
    capital_required: Decimal


def compute_quantity_lattice(
    quantity: Decimal,
    steps: Sequence[Decimal | None],
) -> Decimal | None:
    """Compute common lattice step and round down.

    T10: Contract steps 0.03 and 0.02 -> common lattice 0.06; 0.04 rejected.
    """
    return round_down_to_common_quantity_lattice(quantity, steps)


def _evaluate_plan_cost(
    quantity: Decimal,
    avg_entry_price: Decimal,
    avg_exit_price: Decimal,
    total_cost: Decimal,
) -> Decimal:
    """Evaluate PnL for a given quantity."""
    entry_value = quantity * avg_entry_price
    exit_value = quantity * avg_exit_price
    return exit_value - entry_value - total_cost


def find_best_size_on_grid(
    steps_bps: Sequence[int],
    max_quantity: Decimal,
    avg_entry_price: Decimal,
    avg_exit_price: Decimal,
    cost_per_unit: Decimal,
    constraints: dict[str, Decimal | None] = None,
) -> SizingPlan:
    """Find the best size on a notional grid.

    Section 10.5: Evaluate grid of notional values, find PnL-maximizing size.
    """

    notional_grid = steps_bps  # e.g. [25, 50, 100, 250, 500, 1000]
    best_result: _GridResult | None = None

    min_qty = constraints.get("min_quantity", Decimal("0")) if constraints else Decimal("0")
    max_qty = constraints.get("max_quantity", max_quantity) if constraints else max_quantity

    for notional in notional_grid:
        if avg_entry_price <= 0:
            continue
        qty = Decimal(str(notional)) / avg_entry_price
        if qty < min_qty or qty > max_qty:
            continue

        total_cost = qty * cost_per_unit
        pnl = _evaluate_plan_cost(qty, avg_entry_price, avg_exit_price, total_cost)
        capital_required = qty * avg_entry_price

        result = _GridResult(
            quantity=qty,
            pnL=pnl,
            capital_required=capital_required,
        )

        if best_result is None or pnl > best_result.pnL:
            best_result = result

    if best_result is None:
        return SizingPlan(
            quantity=Decimal("0"),
            mode="bounded_residual",
            reason="no_feasible_size_on_grid",
        )

    return SizingPlan(
        quantity=best_result.quantity,
        mode="exact_neutral",
        capital_required=best_result.capital_required,
        confidence="verified" if best_result.pnL > 0 else "screened",
    )


def optimize_size_for_pnl(
    quantity_candidates: Sequence[Decimal],
    avg_entry_price: Decimal,
    avg_exit_price: Decimal,
    total_cost: Decimal,
    capital_limit: Decimal,
) -> SizingPlan:
    """Refine to the best PnL within capital limits (Section 10.5.4)."""

    best_qty = Decimal("0")
    best_pnl = Decimal("-999999999")
    best_capital = Decimal("0")

    for qty in quantity_candidates:
        entry_value = qty * avg_entry_price
        if entry_value > capital_limit:
            continue
        pnl = _evaluate_plan_cost(qty, avg_entry_price, avg_exit_price, total_cost)
        if pnl > best_pnl:
            best_pnl = pnl
            best_qty = qty
            best_capital = entry_value

    if best_qty == 0:
        return SizingPlan(
            quantity=Decimal("0"),
            mode="bounded_residual",
            reason="capital_limit_exceeded",
        )

    return SizingPlan(
        quantity=best_qty,
        mode="exact_neutral",
        capital_required=best_capital,
        confidence="verified",
    )
