"""Execution cost engine package.

This package provides both the typed execution-cost engine (orderbook, AMM,
sizing, currency) and the legacy ``cost_to_acquire`` / ``proceeds_from_sell``
functions that the cycle monitors call with the new keyword contract.

The legacy function signatures live in ``execution_cost.py`` at the
``market_data_lab`` package root.  Because this directory package shadows
that module file, we load it explicitly via :mod:`importlib` so that the
package-level names refer to the canonical implementation used by
``cex_dex_cycles`` and ``triangle_cycle_monitor``.
"""

from __future__ import annotations

import importlib.util
import os

# Load the canonical execution_cost.py module that lives next to this
# package.  The package directory shadows it on sys.path, so a normal
# relative import would be ambiguous; importlib bypasses the shadowing.
_legacy_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "execution_cost.py",
)
_spec = importlib.util.spec_from_file_location(
    "market_data_lab._execution_cost_legacy", _legacy_path
)
_legacy_mod = importlib.util.module_from_spec(_spec)
_sys = __import__("sys")
_sys.modules["market_data_lab._execution_cost_legacy"] = _legacy_mod
_spec.loader.exec_module(_legacy_mod)  # type: ignore[arg-type]


# --- Package-level exports (submodules) ---
from .contracts import (
    ExecutionEstimate as EngineExecutionEstimate,
    DepthLevel,
    OrderBook,
    OrderBookSnapshot,
    AMMQuality,
    AMMState,
    CapacityBounds,
    ExecutionResult,
    ExecutionStatus,
)
from .orderbook import (
    L2OrderBook,
    depth_cost_to_acquire,
    depth_proceeds_from_sell,
    apply_virtual_fill_to_book,
)
from .amm import (
    CPMMSimulator,
    ClmmSimulator,
    simulate_cpmm_swap,
    simulate_reverse_swap,
)
from .sizing import (
    SizingPlan,
    compute_quantity_lattice,
    find_best_size_on_grid,
    optimize_size_for_pnl,
)
from .currency import (
    FXRate,
    FXConversion,
    convert_currency,
    estimate_capital_charge,
)
from .engine import ExecutionCostEngine

# --- Canonical legacy functions and their types ---
# These are the *only* definitions of ``cost_to_acquire`` and
# ``proceeds_from_sell`` with the current keyword contract
# (fee_source, fee_quality, base_currency, quote_currency, state_version).
cost_to_acquire = _legacy_mod.cost_to_acquire
proceeds_from_sell = _legacy_mod.proceeds_from_sell
FeeCurrency = _legacy_mod.FeeCurrency

# The legacy ``ExecutionEstimate`` has a different shape than the package
# engine's ``EngineExecutionEstimate``.  Callers that use
# ``cost_to_acquire`` / ``proceeds_from_sell`` rely on the legacy shape
# (e.g. ``gross_book_quote_amount``, ``net_quote_movement``, ``fee_amount``).
ExecutionEstimate = _legacy_mod.ExecutionEstimate

# --- Backward-compatible synonyms ---
# Old code that imported the compatibility wrappers still works.  They share
# the same module object as the legacy functions above, so there is no
# second definition with a different signature.
LegacyExecutionEstimate = _legacy_mod.ExecutionEstimate


__all__ = [
    "ExecutionEstimate",
    "DepthLevel",
    "OrderBook",
    "OrderBookSnapshot",
    "AMMQuality",
    "AMMState",
    "CapacityBounds",
    "ExecutionResult",
    "ExecutionStatus",
    "L2OrderBook",
    "depth_cost_to_acquire",
    "depth_proceeds_from_sell",
    "apply_virtual_fill_to_book",
    "CPMMSimulator",
    "ClmmSimulator",
    "simulate_cpmm_swap",
    "simulate_reverse_swap",
    "SizingPlan",
    "compute_quantity_lattice",
    "find_best_size_on_grid",
    "optimize_size_for_pnl",
    "FXRate",
    "FXConversion",
    "convert_currency",
    "estimate_capital_charge",
    "ExecutionCostEngine",
    # Legacy backward-compatible
    "FeeCurrency",
    "LegacyExecutionEstimate",
    "cost_to_acquire",
    "proceeds_from_sell",
]
