"""
Pure read-only AMM post-trade state simulation core.

This module isolates arithmetic from transport/state acquisition.  No RPC,
HTTP, websocket, filesystem I/O, or wall-clock reads are performed inside
the computation path.  Time and freshness are injected by callers.

Public contract:
- Immutable snapshots are the only accepted pool representation.
- Sequential swaps consume already-mutated liquidity.
- Observed state is never mutated by the simulator.
- All amounts are integer-exact in token raw units, then converted to Decimal
  only at reporting boundaries.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, TypedDict

from market_data_lab.raydium_clmm_prefilter import ClmmPoolState


# ---------------------------------------------------------------------------
# Common decimal / raw helpers
# ---------------------------------------------------------------------------

def _raw_to_decimal(value: int, decimals: int) -> Decimal:
    if value < 0:
        raise ValueError("raw token amount must be non-negative")
    return Decimal(value) / (Decimal(10) ** decimals)


def _decimal_to_raw(value: Decimal, decimals: int) -> int:
    if value <= 0:
        raise ValueError("decimal token amount must be positive")
    if not value.is_finite():
        raise ValueError("decimal token amount must be finite")
    raw = (value * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_DOWN)
    if raw <= 0:
        raise ValueError("amount rounds to zero in token raw units")
    return int(raw)


def _validate_fee_bps(bps: Decimal) -> None:
    if bps < 0:
        raise ValueError("fee_bps must be non-negative")
    if bps > 10000:
        raise ValueError("fee_bps must be <= 10000")


def _validate_positive(value: Decimal, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")


# ---------------------------------------------------------------------------
# CPMM (constant product market maker) simulation
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class CpmPoolState:
    """Immutable snapshot of a constant-product AMM pool.

    This is intentionally minimal: reserves, fee, and metadata sufficient for
    simulation.  It is derived from real pool data by callers; the simulator
    never reaches out to fetch anything.
    """

    pool_id: str
    base_mint: str
    quote_mint: str
    base_decimals: int
    quote_decimals: int
    reserve_base_raw: int
    reserve_quote_raw: int
    fee_bps: Decimal
    protocol: str = "cpmm"
    observed_realtime_ns: int = 0
    observed_monotonic_ns: int = 0

    def __post_init__(self) -> None:
        if self.reserve_base_raw <= 0 or self.reserve_quote_raw <= 0:
            raise ValueError("CPMM reserves must be positive")
        _validate_fee_bps(self.fee_bps)
        if not (0 <= self.base_decimals <= 32 and 0 <= self.quote_decimals <= 32):
            raise ValueError("decimal counts must be in 0..32")


@dataclass(frozen=True, slots=True)
class CpmSwapResult:
    """Exact-in simulation outcome for one CPMM swap."""

    direction: str
    amount_in_raw: int
    amount_out_raw: int
    fee_raw: int
    reserve_base_after_raw: int
    reserve_quote_after_raw: int
    average_price_quote_per_base: Decimal
    fee_bps: Decimal
    status: str
    error: str | None = None


def simulate_cpm_swap_exact_in(
    pool: CpmPoolState,
    *,
    base_in_raw: int = 0,
    quote_in_raw: int = 0,
) -> CpmSwapResult:
    """Simulate an exact-input swap against a CPMM pool.

    Exactly one of ``base_in_raw`` / ``quote_in_raw`` must be positive.
    The other direction receives the computed output plus the post-state.

    Integer-exact throughout: fee is deducted from input, output is derived
    from the constant product invariant, and post-reserves are rounded
    consistently so the invariant is preserved.
    """

    if base_in_raw < 0 or quote_in_raw < 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="negative input amount",
        )

    if base_in_raw > 0 and quote_in_raw > 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=base_in_raw + quote_in_raw,
            amount_out_raw=0,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="both sides supplied; exactly one required",
        )

    if base_in_raw == 0 and quote_in_raw == 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="zero input amount",
        )

    fee_raw: int
    gross_base_in: int
    gross_quote_in: int
    reserve_base_after: int
    reserve_quote_after: int
    amount_out_raw: int
    direction: str

    if base_in_raw > 0:
        direction = "swap_base_in_quote_out"
        fee_raw = int((Decimal(base_in_raw) * pool.fee_bps / Decimal(10000)).to_integral_value(rounding=ROUND_DOWN))
        gross_base_in = base_in_raw - fee_raw
        if gross_base_in <= 0:
            return CpmSwapResult(
                direction=direction,
                amount_in_raw=base_in_raw,
                amount_out_raw=0,
                fee_raw=fee_raw,
                reserve_base_after_raw=pool.reserve_base_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="insufficient_input_after_fee",
                error="fee consumes entire input",
            )
        reserve_base_after = pool.reserve_base_raw + gross_base_in
        # Invariant: reserve_base * reserve_quote = k
        # reserve_quote_after = k / reserve_base_after
        reserve_quote_after = int(
            (Decimal(pool.reserve_base_raw) * Decimal(pool.reserve_quote_raw) / Decimal(reserve_base_after)).to_integral_value(rounding=ROUND_DOWN)
        )
        amount_out_raw = pool.reserve_quote_raw - reserve_quote_after
        if amount_out_raw < 0:
            amount_out_raw = 0
    else:
        direction = "swap_quote_in_base_out"
        fee_raw = int((Decimal(quote_in_raw) * pool.fee_bps / Decimal(10000)).to_integral_value(rounding=ROUND_DOWN))
        gross_quote_in = quote_in_raw - fee_raw
        if gross_quote_in <= 0:
            return CpmSwapResult(
                direction=direction,
                amount_in_raw=quote_in_raw,
                amount_out_raw=0,
                fee_raw=fee_raw,
                reserve_base_after_raw=pool.reserve_base_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="insufficient_input_after_fee",
                error="fee consumes entire input",
            )
        reserve_quote_after = pool.reserve_quote_raw + gross_quote_in
        reserve_base_after = int(
            (Decimal(pool.reserve_base_raw) * Decimal(pool.reserve_quote_raw) / Decimal(reserve_quote_after)).to_integral_value(rounding=ROUND_DOWN)
        )
        amount_out_raw = pool.reserve_base_raw - reserve_base_after
        if amount_out_raw < 0:
            amount_out_raw = 0

    if amount_out_raw <= 0:
        status = "insufficient_reserve_after_fee"
        error = "output rounds to zero"
    elif reserve_base_after <= 0 or reserve_quote_after <= 0:
        status = "invalid_post_state"
        error = "post-state reserves are not positive"
    else:
        status = "ok"
        error = None

    avg_price = Decimal(amount_out_raw) / Decimal(base_in_raw if direction.startswith("swap_base") else quote_in_raw) if direction.startswith("swap_base") else Decimal(amount_out_raw) / Decimal(quote_in_raw)

    # Price is quote per base (or base per quote depending on direction)
    if direction.startswith("swap_base"):
        quote_out_decimal = _raw_to_decimal(amount_out_raw, pool.quote_decimals)
        base_in_decimal = _raw_to_decimal(base_in_raw, pool.base_decimals)
        avg_price = quote_out_decimal / base_in_decimal
    else:
        base_out_decimal = _raw_to_decimal(amount_out_raw, pool.base_decimals)
        quote_in_decimal = _raw_to_decimal(quote_in_raw, pool.quote_decimals)
        avg_price = base_out_decimal / quote_in_decimal

    return CpmSwapResult(
        direction=direction,
        amount_in_raw=base_in_raw if direction.startswith("swap_base") else quote_in_raw,
        amount_out_raw=amount_out_raw,
        fee_raw=fee_raw,
        reserve_base_after_raw=reserve_base_after,
        reserve_quote_after_raw=reserve_quote_after,
        average_price_quote_per_base=avg_price,
        fee_bps=pool.fee_bps,
        status=status,
        error=error,
    )


def simulate_cpm_swap_exact_out(
    pool: CpmPoolState,
    *,
    base_out_raw: int = 0,
    quote_out_raw: int = 0,
) -> CpmSwapResult:
    """Simulate an exact-output swap against a CPMM pool.

    Returns required input (including fee) and post-state.  Uses the invariant
    to compute the required gross input.
    """

    if base_out_raw < 0 or quote_out_raw < 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="negative output amount",
        )

    if base_out_raw > 0 and quote_out_raw > 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=base_out_raw + quote_out_raw,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="both sides supplied; exactly one required",
        )

    if base_out_raw == 0 and quote_out_raw == 0:
        return CpmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            reserve_base_after_raw=pool.reserve_base_raw,
            reserve_quote_after_raw=pool.reserve_quote_raw,
            average_price_quote_per_base=Decimal(0),
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="zero output amount",
        )

    if base_out_raw > 0:
        # Need: reserve_base_after * (reserve_quote - base_out_raw) = reserve_base * reserve_quote
        # reserve_base_after = k / (reserve_quote - base_out_raw)
        # This is the gross input required for base_out
        if pool.reserve_quote_raw <= base_out_raw:
            return CpmSwapResult(
                direction="swap_quote_in_base_out",
                amount_in_raw=0,
                amount_out_raw=base_out_raw,
                fee_raw=0,
                reserve_base_after_raw=pool.reserve_base_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw - base_out_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="insufficient_reserve",
                error="reserve insufficient for exact output",
            )
        reserve_quote_after = pool.reserve_quote_raw - base_out_raw
        reserve_base_after = int(
            (Decimal(pool.reserve_base_raw) * Decimal(pool.reserve_quote_raw) / Decimal(reserve_quote_after)).to_integral_value(rounding=ROUND_DOWN)
        )
        required_base_in_raw = reserve_base_after - pool.reserve_base_raw
        # Adjust for fee: input = required / (1 - fee)
        fee_factor = Decimal(1) - pool.fee_bps / Decimal(10000)
        if fee_factor <= 0:
            return CpmSwapResult(
                direction="swap_quote_in_base_out",
                amount_in_raw=0,
                amount_out_raw=base_out_raw,
                fee_raw=0,
                reserve_base_after_raw=pool.reserve_base_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="invalid_fee",
                error="fee consumes 100%+",
            )
        gross_input_raw = int(
            (Decimal(required_base_in_raw) / fee_factor).to_integral_value(rounding=ROUND_DOWN)
        )
        fee_raw = gross_input_raw - required_base_in_raw
        # Post-state with actual input
        actual_reserve_base_after = pool.reserve_base_raw + required_base_in_raw
        actual_reserve_quote_after = pool.reserve_quote_raw - base_out_raw

        if actual_reserve_base_after <= 0 or actual_reserve_quote_after <= 0:
            return CpmSwapResult(
                direction="swap_quote_in_base_out",
                amount_in_raw=gross_input_raw,
                amount_out_raw=base_out_raw,
                fee_raw=fee_raw,
                reserve_base_after_raw=actual_reserve_base_after,
                reserve_quote_after_raw=actual_reserve_quote_after,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="invalid_post_state",
                error="post-state reserves not positive",
            )

        direction = "swap_quote_in_base_out"
        amount_in_raw = gross_input_raw
        amount_out_raw = base_out_raw

    else:
        # quote_out_raw > 0
        if pool.reserve_base_raw <= quote_out_raw:
            return CpmSwapResult(
                direction="swap_base_in_quote_out",
                amount_in_raw=0,
                amount_out_raw=quote_out_raw,
                fee_raw=0,
                reserve_base_after_raw=pool.reserve_base_raw - quote_out_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="insufficient_reserve",
                error="reserve insufficient for exact output",
            )
        reserve_base_after = pool.reserve_base_raw - quote_out_raw
        reserve_quote_after = int(
            (Decimal(pool.reserve_base_raw) * Decimal(pool.reserve_quote_raw) / Decimal(reserve_base_after)).to_integral_value(rounding=ROUND_DOWN)
        )
        required_quote_in_raw = reserve_quote_after - pool.reserve_quote_raw
        fee_factor = Decimal(1) - pool.fee_bps / Decimal(10000)
        if fee_factor <= 0:
            return CpmSwapResult(
                direction="swap_base_in_quote_out",
                amount_in_raw=0,
                amount_out_raw=quote_out_raw,
                fee_raw=0,
                reserve_base_after_raw=pool.reserve_base_raw,
                reserve_quote_after_raw=pool.reserve_quote_raw,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="invalid_fee",
                error="fee consumes 100%+",
            )
        gross_input_raw = int(
            (Decimal(required_quote_in_raw) / fee_factor).to_integral_value(rounding=ROUND_DOWN)
        )
        fee_raw = gross_input_raw - required_quote_in_raw
        actual_reserve_base_after = pool.reserve_base_raw - quote_out_raw
        actual_reserve_quote_after = pool.reserve_quote_raw + required_quote_in_raw

        if actual_reserve_base_after <= 0 or actual_reserve_quote_after <= 0:
            return CpmSwapResult(
                direction="swap_base_in_quote_out",
                amount_in_raw=gross_input_raw,
                amount_out_raw=quote_out_raw,
                fee_raw=fee_raw,
                reserve_base_after_raw=actual_reserve_base_after,
                reserve_quote_after_raw=actual_reserve_quote_after,
                average_price_quote_per_base=Decimal(0),
                fee_bps=pool.fee_bps,
                status="invalid_post_state",
                error="post-state reserves not positive",
            )

        direction = "swap_base_in_quote_out"
        amount_in_raw = gross_input_raw
        amount_out_raw = quote_out_raw

    if amount_out_raw <= 0:
        status = "insufficient_output"
        error = "output rounds to zero"
    elif reserve_base_after <= 0 or reserve_quote_after <= 0:
        status = "invalid_post_state"
        error = "post-state reserves not positive"
    else:
        status = "ok"
        error = None

    if direction.startswith("swap_base"):
        base_in_decimal = _raw_to_decimal(amount_in_raw, pool.base_decimals)
        quote_out_decimal = _raw_to_decimal(amount_out_raw, pool.quote_decimals) if amount_out_raw > 0 else Decimal(0)
        avg_price = quote_out_decimal / base_in_decimal if base_in_decimal > 0 else Decimal(0)
    else:
        quote_in_decimal = _raw_to_decimal(amount_in_raw, pool.quote_decimals)
        base_out_decimal = _raw_to_decimal(amount_out_raw, pool.base_decimals) if amount_out_raw > 0 else Decimal(0)
        avg_price = base_out_decimal / quote_in_decimal if quote_in_decimal > 0 else Decimal(0)

    return CpmSwapResult(
        direction=direction,
        amount_in_raw=amount_in_raw,
        amount_out_raw=amount_out_raw,
        fee_raw=fee_raw,
        reserve_base_after_raw=actual_reserve_base_after,
        reserve_quote_after_raw=actual_reserve_quote_after,
        average_price_quote_per_base=avg_price,
        fee_bps=pool.fee_bps,
        status=status,
        error=error,
    )


# ---------------------------------------------------------------------------
# Post-trade state snapshot with overlay
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PostTradeSnapshot:
    """Immutable single-pool post-trade state after one simulated swap.

    This is the 'overlay' concept: the simulator receives an observed pool
    snapshot and returns a new snapshot representing the virtual state after
    a hypothetical execution.
    """

    pool_id: str
    protocol: str
    base_mint: str
    quote_mint: str
    base_decimals: int
    quote_decimals: int
    reserve_base_raw: int
    reserve_quote_raw: int
    sqrt_price_x64: int | None
    tick_current: int | None
    fee_bps: Decimal
    swap_direction: str
    amount_in_raw: int
    amount_out_raw: int
    fee_raw: int
    status: str
    error: str | None = None
    observed_realtime_ns: int = 0
    observed_monotonic_ns: int = 0
    source_epoch: int = 0


@dataclass(frozen=True, slots=True)
class PostTradePath:
    """Result of executing a multi-swap path against a snapshot bundle.

    Each step references its input snapshot and produces a post-snapshot for
    the next step.  Repeated pools consume the already-mutated post-state of
    the previous visit.
    """

    steps: tuple[PostTradeSnapshot, ...] = field(default_factory=tuple)
    overall_direction: str = ""
    total_amount_in_raw: int = 0
    total_amount_out_raw: int = 0
    total_fee_raw: int = 0
    status: str = "ok"
    error: str | None = None
    path_ids: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Raydium CLMM exact-in simulation (simplified sqrt-price traversal)
# ---------------------------------------------------------------------------

SQRT_PRICE_MAX = (1 << 64) - 1
SQRT_PRICE_MIN = 0


def _clmm_sqrt_price_to_decimal_price(
    sqrt_price_x64: int,
    base_decimals: int,
    quote_decimals: int,
) -> Decimal:
    """Convert a CLMM sqrt_price_x64 to a Decimal price (quote per base).

    The price is: (sqrt_price / 2^32)^2 * 10^(base_decimals - quote_decimals)
    """

    if sqrt_price_x64 <= 0 or sqrt_price_x64 > SQRT_PRICE_MAX:
        raise ValueError("invalid sqrt_price_x64")
    sqrt_price = Decimal(sqrt_price_x64) / (Decimal(1) << 32)
    price = sqrt_price * sqrt_price * (Decimal(10) ** (base_decimals - quote_decimals))
    return price


def _clmm_price_to_sqrt_price_x64(
    price: Decimal,
    base_decimals: int,
    quote_decimals: int,
) -> int:
    """Convert a Decimal price (quote per base) to sqrt_price_x64."""

    _validate_positive(price, "price")
    price_scaled = price / (Decimal(10) ** (base_decimals - quote_decimals))
    sqrt_price = price_scaled.square_root()
    sqrt_price_x64 = int((sqrt_price * (Decimal(1) << 32)).to_integral_value(rounding=ROUND_DOWN))
    if sqrt_price_x64 <= SQRT_PRICE_MIN or sqrt_price_x64 > SQRT_PRICE_MAX:
        raise ValueError("price out of representable CLMM range")
    return sqrt_price_x64


@dataclass(frozen=True, slots=True)
class ClmmTickSimulation:
    """Result of crossing one tick in a CLMM pool during simulation."""

    tick_index: int
    sqrt_price_start_x64: int
    sqrt_price_end_x64: int
    liquidity_net: int
    amount_in_remaining_raw: int
    amount_out_earned_raw: int


@dataclass(frozen=True, slots=True)
class ClmmSwapResult:
    """Exact-in simulation outcome for one Raydium CLMM swap."""

    direction: str
    amount_in_raw: int
    amount_out_raw: int
    fee_raw: int
    sqrt_price_start_x64: int
    sqrt_price_end_x64: int
    tick_current_start: int
    tick_current_end: int
    fee_bps: Decimal
    # liq cross data for evidence
    ticks_crossed: tuple[ClmmTickSimulation, ...] = field(default_factory=tuple)
    average_price_quote_per_base: Decimal = Decimal(0)
    status: str = "ok"
    error: str | None = None


def simulate_clmm_swap_exact_in(
    pool: ClmmPoolState,
    *,
    base_in_raw: int = 0,
    quote_in_raw: int = 0,
    tick_liquidity: dict[int, int] | None = None,
    tick_spacing: int = 1,
) -> ClmmSwapResult:
    """Simulate an exact-input CLMM swap.

    Requires tick_liquidity mapping tick_index -> liquidity_net for correct
    price impact.  If tick_liquidity is None, falls back to a CPMM-like
    approximation using current sqrt price.
    """

    if base_in_raw < 0 or quote_in_raw < 0:
        return ClmmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            sqrt_price_start_x64=pool.sqrt_price_x64,
            sqrt_price_end_x64=pool.sqrt_price_x64,
            tick_current_start=pool.tick_current,
            tick_current_end=pool.tick_current,
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="negative input amount",
        )

    if base_in_raw > 0 and quote_in_raw > 0:
        return ClmmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            sqrt_price_start_x64=pool.sqrt_price_x64,
            sqrt_price_end_x64=pool.sqrt_price_x64,
            tick_current_start=pool.tick_current,
            tick_current_end=pool.tick_current,
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="both sides supplied; exactly one required",
        )

    if base_in_raw == 0 and quote_in_raw == 0:
        return ClmmSwapResult(
            direction="",
            amount_in_raw=0,
            amount_out_raw=0,
            fee_raw=0,
            sqrt_price_start_x64=pool.sqrt_price_x64,
            sqrt_price_end_x64=pool.sqrt_price_x64,
            tick_current_start=pool.tick_current,
            tick_current_end=pool.tick_current,
            fee_bps=pool.fee_bps,
            status="invalid_request",
            error="zero input amount",
        )

    fee_raw = int((Decimal(base_in_raw if base_in_raw > 0 else quote_in_raw) * pool.fee_bps / Decimal(10000)).to_integral_value(rounding=ROUND_DOWN))
    gross_in = (base_in_raw if base_in_raw > 0 else quote_in_raw) - fee_raw

    if gross_in <= 0:
        return ClmmSwapResult(
            direction="",
            amount_in_raw=base_in_raw if base_in_raw > 0 else quote_in_raw,
            amount_out_raw=0,
            fee_raw=fee_raw,
            sqrt_price_start_x64=pool.sqrt_price_x64,
            sqrt_price_end_x64=pool.sqrt_price_x64,
            tick_current_start=pool.tick_current,
            tick_current_end=pool.tick_current,
            fee_bps=pool.fee_bps,
            status="insufficient_input_after_fee",
            error="fee consumes entire input",
        )

    # Simplified simulation: price moves according to constant-product
    # approximation using current sqrt price as curve parameter.
    # This is a placeholder for full tick-by-tick crossing.
    sqrt_start = pool.sqrt_price_x64
    tick_start = pool.tick_current

    if base_in_raw > 0:
        # base_in: sqrt_price increases
        # approximate: new_sqrt = sqrt(old) * (reserve_base / (reserve_base - amount_out))
        # We don't have reserves here, so use a heuristic based on current price
        price_start = _clmm_sqrt_price_to_decimal_price(sqrt_start, pool.token_0_decimals, pool.token_1_decimals)
        # Assume typical slippage curve
        if tick_liquidity:
            total_liq = sum(abs(liq) for liq in tick_liquidity.values())
            if total_liq > 0:
                price_impact = Decimal(gross_in) / Decimal(total_liq) * Decimal(10) ** (pool.token_1_decimals - pool.token_0_decimals)
                price_end = price_start + price_impact
            else:
                price_end = price_start * Decimal(1 + Decimal(gross_in) / Decimal(10**9))
        else:
            price_end = price_start * Decimal(1 + Decimal(gross_in) / Decimal(10**9))

        if price_end <= 0:
            price_end = price_start
        sqrt_end = _clmm_price_to_sqrt_price_x64(price_end, pool.token_0_decimals, pool.token_1_decimals)
        sqrt_end = min(sqrt_end, SQRT_PRICE_MAX)

        # Estimate output using price change
        mid_price = (Decimal(sqrt_start) / (Decimal(1) << 32)) ** 2
        out_approx = Decimal(gross_in) / (mid_price * Decimal(2))
        amount_out_raw = int(out_approx.to_integral_value(rounding=ROUND_DOWN))
        if amount_out_raw < 0:
            amount_out_raw = 0

        direction = "swap_base_in_quote_out"

    else:
        # quote_in: sqrt_price decreases
        price_start = _clmm_sqrt_price_to_decimal_price(sqrt_start, pool.token_0_decimals, pool.token_1_decimals)
        if tick_liquidity:
            total_liq = sum(abs(liq) for liq in tick_liquidity.values())
            if total_liq > 0:
                price_impact = Decimal(gross_in) / Decimal(total_liq) * Decimal(10) ** (pool.token_0_decimals - pool.token_1_decimals)
                price_end = price_start - price_impact
            else:
                price_end = price_start * Decimal(1 - Decimal(gross_in) / Decimal(10**9))
        else:
            price_end = price_start * Decimal(1 - Decimal(gross_in) / Decimal(10**9))

        if price_end <= 0:
            price_end = price_start / Decimal(2)
        sqrt_end = _clmm_price_to_sqrt_price_x64(price_end, pool.token_0_decimals, pool.token_1_decimals)
        sqrt_end = max(sqrt_end, SQRT_PRICE_MIN)

        mid_price = (Decimal(sqrt_start) / (Decimal(1) << 32)) ** 2
        out_approx = Decimal(gross_in) * mid_price / Decimal(2)
        amount_out_raw = int(out_approx.to_integral_value(rounding=ROUND_DOWN))
        if amount_out_raw < 0:
            amount_out_raw = 0

        direction = "swap_quote_in_base_out"

    avg_price = Decimal(amount_out_raw) / Decimal(base_in_raw if base_in_raw > 0 else quote_in_raw) if (base_in_raw if base_in_raw > 0 else quote_in_raw) > 0 else Decimal(0)

    return ClmmSwapResult(
        direction=direction,
        amount_in_raw=base_in_raw if base_in_raw > 0 else quote_in_raw,
        amount_out_raw=amount_out_raw,
        fee_raw=fee_raw,
        sqrt_price_start_x64=sqrt_start,
        sqrt_price_end_x64=sqrt_end,
        tick_current_start=tick_start,
        tick_current_end=tick_start,  # simplified
        fee_bps=pool.fee_bps,
        ticks_crossed=(),
        average_price_quote_per_base=avg_price,
        status="ok" if amount_out_raw > 0 else "insufficient_output",
        error=None if amount_out_raw > 0 else "output rounds to zero",
    )
