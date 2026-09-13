"""S01-S05: Spot path strategies.

Section 3: S01-S05 are the mandatory spot-path catalog for first release.
- S01: CEX spot <-> CEX spot (cross-venue spread)
- S02: CEX spot <-> DEX spot (cross-venue, DEX exact quote)
- S03: DEX <-> DEX same chain (sequential swaps)
- S04: Short multi-hop spot route (3-4 swaps via bridge asset)
- S05: Cross-chain/cross-venue inventory (synchronous swaps, bridge not instant)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from .base import StrategyTemplate, StrategyResult, ScreeningBounds
from ..position_engine.contracts import ExitPolicy


@dataclass
class SpotPathResult(StrategyResult):
    """Result for spot path strategies."""

    route_sequence: list[str] | None = None
    post_trade_simulation: bool = False


class S01_CexSpotToCexSpot(StrategyTemplate):
    """S01: CEX spot -> CEX spot (cross-venue exchange spread).

    Section 3: Buy base on A, sell same net-base on B.
    Requires: depth of both books, commissions, inventory placement, FX, rebalance.
    T01: Buy 100 base split at different prices, fee in quote, DEX sell after
    pool fee.
    """

    strategy_id = "cex_spot_to_cex_spot"
    required_data = ("book_a", "book_b", "fee_a", "fee_b")
    supported_contract_models = ("spot",)

    async def screen(
        self,
        view: Mapping[str, object],
        capital_available: Decimal,
        constraints: Mapping[str, Decimal | None],
    ) -> ScreeningBounds | None:
        book_a = view.get("book_a")
        book_b = view.get("book_b")
        if book_a is None or book_b is None:
            return None

        best_bid_b = book_b.best_bid.price if book_b.best_bid else None
        best_ask_a = book_a.best_ask.price if book_a.best_ask else None

        if best_bid_b is None or best_ask_a is None:
            return None

        if best_bid_b <= best_ask_a:
            return None

        max_qty = min(
            book_a.asks[0].size if book_a.asks else Decimal("0"),
            book_b.bids[0].size if book_b.bids else Decimal("0"),
            capital_available / best_ask_a,
        )

        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=max_qty,
            capital_required=max_qty * best_ask_a,
            upper_bound_pnl=(best_bid_b - best_ask_a) * max_qty,
        )

    async def evaluate(
        self,
        bounds: ScreeningBounds,
        view: Mapping[str, object],
        exit_policy: ExitPolicy,
    ) -> StrategyResult:
        book_a = view["book_a"]
        book_b = view["book_b"]

        qty = bounds.max_quantity
        cost = qty * book_a.best_ask.price
        fee_a = cost * Decimal("0.001")
        total_cost = cost + fee_a

        proceeds = qty * book_b.best_bid.price
        fee_b = proceeds * Decimal("0.001")
        net_proceeds = proceeds - fee_b

        pnL = net_proceeds - total_cost

        return SpotPathResult(
            strategy_id=self.strategy_id,
            route=("buy_A", "sell_B"),
            quantities={"base": qty},
            entry_cost_quote=total_cost,
            projected_pnl_by_scenario={"unchanged_prices": pnL},
            costs_total_quote=fee_a + fee_b,
            capital_required=total_cost,
            confidence="screened",
        )


class S02_CexSpotToDexSpot(StrategyTemplate):
    """S02: CEX spot -> DEX spot.

    Section 3: Requires DEX exact quote/local simulation, CEX depth, network, tokens.
    """

    strategy_id = "cex_spot_to_dex_spot"
    required_data = ("cex_book", "dex_pool", "dex_quote", "fee_tier")
    supported_contract_models = ("spot",)

    async def screen(
        self, view, capital_available, constraints
    ) -> ScreeningBounds | None:
        return await S01_CexSpotToCexSpot.screen(self, view, capital_available, constraints)

    async def evaluate(self, bounds, view, exit_policy) -> StrategyResult:
        return SpotPathResult(
            strategy_id=self.strategy_id,
            route=("buy_CEX", "sell_DEX"),
            quantities={"base": bounds.max_quantity},
            entry_cost_quote=bounds.capital_required,
            projected_pnl_by_scenario={"unchanged_prices": Decimal("0")},
            costs_total_quote=Decimal("0"),
            capital_required=bounds.capital_required,
            confidence="screened",
        )


class S03_DexToDexSameChain(StrategyTemplate):
    """S03: DEX -> DEX, same chain (sequential swaps).

    Section 3: Result after all swaps and gas.
    Section 8.4: If same pool used twice, second operates on virtual post-state.
    """

    strategy_id = "dex_to_dex_same_chain"
    required_data = ("dex_a_pool", "dex_b_pool")
    supported_contract_models = ("spot",)

    async def screen(self, view, capital_available, constraints) -> ScreeningBounds | None:
        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=capital_available,
            capital_required=capital_available,
        )

    async def evaluate(self, bounds, view, exit_policy) -> StrategyResult:
        return SpotPathResult(
            strategy_id=self.strategy_id,
            route=("swap_A_to_bridge", "swap_bridge_to_B"),
            quantities={"base": bounds.max_quantity},
            entry_cost_quote=Decimal("0"),
            projected_pnl_by_scenario={"unchanged": Decimal("0")},
            costs_total_quote=Decimal("0"),
            capital_required=bounds.capital_required,
            confidence="screened",
        )


class S04_ShortMultiHopSpot(StrategyTemplate):
    """S04: Short multi-hop spot route (3-4 swaps via bridge asset)."""

    strategy_id = "short_multi_hop_spot"
    required_data = ("hop_pools",)
    supported_contract_models = ("spot",)

    async def screen(self, view, capital_available, constraints) -> ScreeningBounds | None:
        return ScreeningBounds(
            min_quantity=Decimal("0"),
            max_quantity=capital_available,
            capital_required=capital_available,
        )

    async def evaluate(self, bounds, view, exit_policy) -> StrategyResult:
        return SpotPathResult(
            strategy_id=self.strategy_id,
            route=("hop1", "hop2", "hop3", "settlement"),
            quantities={"base": bounds.max_quantity},
            entry_cost_quote=Decimal("0"),
            projected_pnl_by_scenario={"unchanged": Decimal("0")},
            costs_total_quote=Decimal("0"),
            capital_required=bounds.capital_required,
            confidence="screened",
        )


class S05_CrossChainInventory(StrategyTemplate):
    """S05: Cross-chain/cross-venue inventory.

    Section 3: Bridge is NOT instant; requires separate inventory recovery model.
    """

    strategy_id = "cross_chain_inventory"
    required_data = ("inventory_states", "bridge_models")
    supported_contract_models = ("spot",)

    async def screen(self, view, capital_available, constraints) -> ScreeningBounds | None:
        return None

    async def evaluate(self, bounds, view, exit_policy) -> StrategyResult:
        return SpotPathResult(
            strategy_id=self.strategy_id,
            route=(),
            quantities={},
            entry_cost_quote=Decimal("0"),
            projected_pnl_by_scenario={"unchanged": Decimal("0")},
            costs_total_quote=Decimal("0"),
            capital_required=Decimal("0"),
            confidence="screened",
            constraints=["cross_chain_bridge_not_instant"],
        )
