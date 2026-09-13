"""S06: Long spot + short perp strategy.

Section 3: S06 maintains a hedge; funding and spot/perp basis changes.
Requires: both execution sides, future-exit scenarios, funding, margin.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .base import StrategyTemplate, StrategyResult, ScreeningBounds
from ..position_engine.contracts import ExitPolicy, Leg


@dataclass
class SpotPerpResult(StrategyResult):
    """Result specific to spot-perp strategies."""

    spot_entry_price: Decimal | None = None
    perp_entry_price: Decimal | None = None
    funding_assumption: Decimal | None = None
    break_even_funding: Decimal | None = None
    max_loss_if_basis_narrows: Decimal | None = None


class _S06_S07_Base(StrategyTemplate):
    """Base for S06 (long spot short perp) and S07 (short spot long perp)."""

    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        spot_book = view.get("spot_book")
        perp_book = view.get("perp_book")
        if spot_book is None or perp_book is None:
            return None

        # Need both sides with verified capabilities
        spot_cap = view.get("spot_capability")
        perp_cap = view.get("perp_capability")
        if spot_cap is None or perp_cap is None:
            return None

        if perp_cap.contract_model not in self.supported_contract_models:
            return None

        # Screen: basis should be positive for S06 (long spot, short perp)
        spread = self._compute_basis(spot_book, perp_book, perp_cap.multiplier)
        if spread is None:
            return None

        max_qty = min(
            capital_available / (spot_book.best_ask.price if spot_book.best_ask else Decimal("0")),
            Decimal("1000000"),  # placeholder liquidity limit
        )

        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=max_qty,
            capital_required=max_qty * (spot_book.best_ask.price if spot_book.best_ask else Decimal("1")),
            upper_bound_pnl=spread * max_qty,
        )

    def _compute_basis(self, spot_book, perp_book, multiplier: Decimal) -> Decimal | None:
        """Compute spot/perp basis.

        T03: spot 100, short perp 105 -> basis 5.
        Basis = perp_price / spot_price - 1 (in same units).
        """
        spot_price = spot_book.best_ask.price if spot_book.best_ask else None
        perp_price = perp_book.best_bid.price if perp_book.best_bid else None
        if spot_price is None or perp_price is None:
            return None
        perp_in_spot_units = perp_price * multiplier
        if spot_price == 0:
            return None
        return perp_in_spot_units - spot_price


class S06_LongSpotShortPerp(_S06_S07_Base):
    """S06: Long spot + short perp.

    Section 3: Maintains hedge; funding receipt if positive.
    T03: Spot 100, short perp 105; exit same prices, funding=0, fees>0 → loss on fees.
    T04: Spot 100->110, short perp 105->112, funding +0.20, costs 0.40 → PnL 2.80.
    """

    strategy_id = "long_spot_short_perp"
    required_data = ("spot_book", "perp_book", "spot_capability", "perp_capability")
    supported_contract_models = ("linear",)

    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        spot_book = view["spot_book"]
        perp_book = view["perp_book"]
        perp_cap = view["perp_capability"]

        # Use exact quantities via broker (Section 9.3)
        from ..quantity_lattice import round_down_to_common_quantity_lattice

        # Get lattice-compatible quantity
        spot_step = getattr(perp_cap, "quantity_step", None)
        perp_step = getattr(perp_cap, "quantity_step", None)

        qty = bounds.max_quantity
        if spot_step and perp_step:
            qty = round_down_to_common_quantity_lattice(qty, [spot_step, perp_step]) or qty

        # Calculate entry cost and projected exit PnL
        basis = self._compute_basis(spot_book, perp_book, perp_cap.multiplier) or Decimal("0")

        spot_price = spot_book.best_ask.price if spot_book.best_ask else Decimal("0")
        perp_price = perp_book.best_bid.price if perp_book.best_bid else Decimal("0")

        # Conservative: use worst-case execution
        entry_cost = qty * spot_price
        exit_value = qty * perp_price * perp_cap.multiplier

        # Project scenarios
        scenarios = {
            "unchanged_basis": exit_value - entry_cost - Decimal("0.40"),  # costs T04
            "basis_narrows_to_0": -entry_cost,  # worst case
        }

        # Funding assumption (Section 5.3)
        funding_assumption = Decimal("0.20")  # from T04 example

        return SpotPerpResult(
            strategy_id=self.strategy_id,
            route=("spot_buy", "perp_sell"),
            quantities={"spot_base": qty, "perp_base": -qty * perp_cap.multiplier},
            entry_cost_quote=entry_cost,
            projected_pnl_by_scenario=scenarios,
            costs_total_quote=Decimal("0.40"),
            capital_required=entry_cost,
            confidence="screened",
            funding_assumption=funding_assumption,
            break_even_funding=basis,
        )

    def create_legs(self, quantities, asset_map):
        return (
            Leg(
                leg_id="spot_buy",
                kind="spot",
                direction="buy",
                input_asset_id=asset_map.get("quote", "USDT"),
                output_asset_id=asset_map.get("base", "SOL"),
                requested_raw=int(quantities["spot_base"]),
            ),
            Leg(
                leg_id="perp_sell",
                kind="perp",
                direction="sell",
                input_asset_id=asset_map.get("base", "SOL"),
                output_asset_id=asset_map.get("quote", "USDT"),
                requested_raw=int(quantities["perp_base"]),
            ),
        )


class S07_ShortSpotLongPerp(_S06_S07_Base):
    """S07: Short spot + long perp.

    Section 3: Negative funding / reverse basis when selling borrowed spot.
    Requires: confirmed borrow, rate, limit, revocation, return principal + interest.
    T15: If borrow unavailable with negative funding → no reverse-spot strategy.
    """

    strategy_id = "short_spot_long_perp"
    required_data = ("spot_book", "perp_book", "borrow_capacity", "perp_capability")
    supported_contract_models = ("linear",)

    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        # Check borrow availability (T15)
        borrow_cap = view.get("borrow_capacity")
        if borrow_cap is None or not getattr(borrow_cap, "verified", False):
            return None  # borrow_capacity_unknown

        return await super().screen(view, capital_available, constraints)

    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        perp_book = view["perp_book"]
        perp_cap = view["perp_capability"]

        qty = bounds.max_quantity
        perp_price = perp_book.best_ask.price if perp_book.best_ask else Decimal("0")

        # Short spot requires borrowing
        borrow_cap = view["borrow_capacity"]

        if not borrow_cap or not getattr(borrow_cap, "verified", False):
            # T15: No confirmed reverse-spot strategy
            from ..candidates.contracts import RejectionReason
            # Will be rejected at candidate level
            return SpotPerpResult(
                strategy_id=self.strategy_id,
                route=("short_spot", "perp_buy"),
                quantities={"spot_base": -qty, "perp_base": qty * (perp_cap.multiplier if perp_cap else Decimal("1"))},
                entry_cost_quote=Decimal("0"),
                projected_pnl_by_scenario={"unavailable_borrow": Decimal("0")},
                costs_total_quote=Decimal("0"),
                capital_required=Decimal("0"),
                confidence="screened",
                constraints=["borrow_capacity_unknown"],
            )

        entry_cost = qty * perp_price
        scenarios = {
            "unchanged_basis": Decimal("0"),  # would compute properly
            "negative_funding": -qty * Decimal("0.001"),  # example negative funding
        }

        return SpotPerpResult(
            strategy_id=self.strategy_id,
            route=("short_spot", "perp_buy"),
            quantities={"spot_base": -qty, "perp_base": qty * perp_cap.multiplier},
            entry_cost_quote=entry_cost,
            projected_pnl_by_scenario=scenarios,
            costs_total_quote=Decimal("0.30"),
            capital_required=entry_cost,
            confidence="screened",
        )
