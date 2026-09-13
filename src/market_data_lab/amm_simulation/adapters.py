"""Protocol-specific post-trade transitions for supported AMM variants.

Every adapter is a pure function of an immutable snapshot body plus a leg
request.  None of them reach the network, mutate observed state, or fabricate a
partial fill.  Unsupported variants raise a typed refusal instead of falling
back to a different curve.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Protocol

from .contracts import (
    CpmmPoolBody,
    MeteoraDlmmPoolBody,
    RaydiumAmmV4PoolBody,
    RaydiumClmmPoolBody,
    RaydiumCpmmPoolBody,
)


FEE_RATE_DENOMINATOR = 1_000_000
RAYDIUM_FEE_ON_BOTH = 0
RAYDIUM_FEE_ON_TOKEN_A = 1
RAYDIUM_FEE_ON_TOKEN_B = 2


class UnsupportedVariant(ValueError):
    """Raised when a snapshot body cannot be simulated by the selected adapter."""


class ProtocolMismatch(ValueError):
    """Raised when the snapshot body type does not match the protocol adapter."""

@dataclass(frozen=True, slots=True)
class FeeComponent:
    kind: str
    asset_id: str
    amount_raw: int
    included_in_amount: bool
    source_model: str


@dataclass(frozen=True, slots=True)
class SwapTransition:
    gross_input: int
    effective_input: int
    gross_pool_output: int
    net_output: int
    fees: tuple[FeeComponent, ...]
    body_after: object
    steps_used: int


class AmmAdapter(Protocol):
    protocol: str
    pool_spec_version: int

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition: ...

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition: ...


def ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    if numerator <= 0:
        return 0
    return (numerator + denominator - 1) // denominator


def _ceil_rate(amount: int, rate: int) -> int:
    return ceil_div(amount * rate, FEE_RATE_DENOMINATOR)


def _floor_rate(amount: int, rate: int) -> int:
    return (amount * rate) // FEE_RATE_DENOMINATOR


def _swap_without_fees_in(amount_in: int, reserve_in: int, reserve_out: int) -> int:
    return (reserve_out * amount_in) // (reserve_in + amount_in)


def _swap_without_fees_out(amount_out: int, reserve_in: int, reserve_out: int) -> int:
    return ceil_div(reserve_in * amount_out, reserve_out - amount_out)


def _pre_fee_amount(amount: int, rate: int) -> int:
    if rate <= 0:
        return amount
    return ceil_div(amount * FEE_RATE_DENOMINATOR, FEE_RATE_DENOMINATOR - rate)


def _split_creator_fee(total_fee: int, trade_rate: int, creator_rate: int) -> int:
    combined = trade_rate + creator_rate
    if combined <= 0:
        return 0
    return (total_fee * creator_rate) // combined


@dataclass(frozen=True, slots=True)
class SyntheticCpmmAdapter:
    protocol: str = "synthetic_cpmm_v1"
    pool_spec_version: int = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_cpmm(body)
        reserve_in, reserve_out = _directed_reserves(
            pool.reserve_0_raw,
            pool.reserve_1_raw,
            zero_for_one,
        )
        fee = ceil_div(amount_in * pool.fee_numerator, pool.fee_denominator)
        effective_input = amount_in - fee
        if effective_input <= 0:
            raise UnsupportedVariant("fee consumes the entire input")
        output = (reserve_out * effective_input) // (reserve_in + effective_input)
        if output <= 0 or output >= reserve_out:
            raise UnsupportedVariant("insufficient CPMM liquidity")
        after = replace(
            pool,
            reserve_0_raw=(reserve_in + amount_in if zero_for_one else reserve_out - output),
            reserve_1_raw=(reserve_out - output if zero_for_one else reserve_in + amount_in),
        )
        return SwapTransition(
            gross_input=amount_in,
            effective_input=effective_input,
            gross_pool_output=output,
            net_output=output,
            fees=(),
            body_after=after,
            steps_used=1,
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_cpmm(body)
        reserve_in, reserve_out = _directed_reserves(
            pool.reserve_0_raw,
            pool.reserve_1_raw,
            zero_for_one,
        )
        if amount_out <= 0 or amount_out >= reserve_out:
            raise UnsupportedVariant("exact-output amount must fit inside output reserve")
        target = ceil_div(reserve_in * amount_out, reserve_out - amount_out)
        low, high = target, target * pool.fee_denominator + 1
        steps = 0
        while low < high:
            steps += 1
            candidate = (low + high) // 2
            fee = ceil_div(candidate * pool.fee_numerator, pool.fee_denominator)
            if candidate - fee >= target:
                high = candidate
            else:
                low = candidate + 1
        amount_in = low
        fee = ceil_div(amount_in * pool.fee_numerator, pool.fee_denominator)
        if amount_in - fee != target:
            raise ArithmeticError("minimal exact-output gross input inversion failed")
        after = replace(
            pool,
            reserve_0_raw=(reserve_in + amount_in if zero_for_one else reserve_out - amount_out),
            reserve_1_raw=(reserve_out - amount_out if zero_for_one else reserve_in + amount_in),
        )
        return SwapTransition(
            gross_input=amount_in,
            effective_input=target,
            gross_pool_output=amount_out,
            net_output=amount_out,
            fees=(),
            body_after=after,
            steps_used=steps,
        )


@dataclass(frozen=True, slots=True)
class RaydiumCpmmAdapter:
    """Raydium CPMM with vault balances kept separate from accrued fee counters.

    Output amounts follow the pinned SDK ``CurveCalculator`` math exactly.  The
    post-state keeps the raw vault transfer and the accrued protocol/fund/creator
    obligations apart, so effective trading reserves for the next swap are
    re-derived as ``vault - counters`` instead of being patched in place.
    """

    protocol: str = "raydium_cpmm"
    pool_spec_version: int = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_raydium_cpmm(body)
        reserve_in, reserve_out = _directed_reserves(
            *pool.effective_reserves(),
            zero_for_one,
        )
        creator_on_input = _creator_fee_on_input(pool.fee_on, zero_for_one)
        trade_fee = _ceil_rate(amount_in, pool.trade_fee_rate)
        creator_fee = _ceil_rate(amount_in, pool.creator_fee_rate) if creator_on_input else 0
        swap_input = amount_in - trade_fee - creator_fee
        if swap_input <= 0:
            raise UnsupportedVariant("Raydium CPMM fee consumes the entire input")
        gross_output = _swap_without_fees_in(swap_input, reserve_in, reserve_out)
        if gross_output <= 0 or gross_output >= reserve_out:
            raise UnsupportedVariant("insufficient Raydium CPMM liquidity")
        protocol_fee = _floor_rate(trade_fee, pool.protocol_fee_rate)
        fund_fee = _floor_rate(trade_fee, pool.fund_fee_rate)
        if not creator_on_input:
            creator_fee = _ceil_rate(gross_output, pool.creator_fee_rate)
        net_output = gross_output - (0 if creator_on_input else creator_fee)
        if net_output <= 0:
            raise UnsupportedVariant("Raydium CPMM creator fee consumes the entire output")
        after = _raydium_after(
            pool,
            zero_for_one=zero_for_one,
            total_input=amount_in,
            net_user_output=net_output,
            protocol_fee=protocol_fee,
            fund_fee=fund_fee,
            creator_fee=creator_fee,
            creator_on_input=creator_on_input,
        )
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        output_asset = pool.pool_ref.asset_1_id if zero_for_one else pool.pool_ref.asset_0_id
        fees = (
            FeeComponent("protocol_fee", input_asset, protocol_fee, False, "raydium_cpmm_v1"),
            FeeComponent("fund_fee", input_asset, fund_fee, False, "raydium_cpmm_v1"),
            FeeComponent(
                "creator_fee",
                input_asset if creator_on_input else output_asset,
                creator_fee,
                False,
                "raydium_cpmm_v1",
            ),
        )
        return SwapTransition(
            gross_input=amount_in,
            effective_input=swap_input,
            gross_pool_output=gross_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=1,
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_raydium_cpmm(body)
        reserve_in, reserve_out = _directed_reserves(
            *pool.effective_reserves(),
            zero_for_one,
        )
        if amount_out <= 0 or amount_out >= reserve_out:
            raise UnsupportedVariant("exact-output amount must fit inside output reserve")
        creator_on_input = _creator_fee_on_input(pool.fee_on, zero_for_one)
        if creator_on_input:
            gross_output = amount_out
            creator_fee = 0
        else:
            gross_output = _pre_fee_amount(amount_out, pool.creator_fee_rate)
            creator_fee = gross_output - amount_out
        if gross_output <= 0 or gross_output >= reserve_out:
            raise UnsupportedVariant("exact-output amount must fit inside output reserve")
        pre_fee_input = _swap_without_fees_out(gross_output, reserve_in, reserve_out)
        if pre_fee_input <= 0:
            raise UnsupportedVariant("insufficient Raydium CPMM liquidity")
        if creator_on_input:
            gross_input = _pre_fee_amount(
                pre_fee_input,
                pool.trade_fee_rate + pool.creator_fee_rate,
            )
            total_input_fees = gross_input - pre_fee_input
            creator_fee = _split_creator_fee(
                total_input_fees,
                pool.trade_fee_rate,
                pool.creator_fee_rate,
            )
            trade_fee = total_input_fees - creator_fee
        else:
            gross_input = _pre_fee_amount(pre_fee_input, pool.trade_fee_rate)
            trade_fee = gross_input - pre_fee_input
        protocol_fee = _floor_rate(trade_fee, pool.protocol_fee_rate)
        fund_fee = _floor_rate(trade_fee, pool.fund_fee_rate)
        after = _raydium_after(
            pool,
            zero_for_one=zero_for_one,
            total_input=gross_input,
            net_user_output=amount_out,
            protocol_fee=protocol_fee,
            fund_fee=fund_fee,
            creator_fee=creator_fee,
            creator_on_input=creator_on_input,
        )
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        output_asset = pool.pool_ref.asset_1_id if zero_for_one else pool.pool_ref.asset_0_id
        fees = (
            FeeComponent("protocol_fee", input_asset, protocol_fee, False, "raydium_cpmm_v1"),
            FeeComponent("fund_fee", input_asset, fund_fee, False, "raydium_cpmm_v1"),
            FeeComponent(
                "creator_fee",
                input_asset if creator_on_input else output_asset,
                creator_fee,
                False,
                "raydium_cpmm_v1",
            ),
        )
        return SwapTransition(
            gross_input=gross_input,
            effective_input=pre_fee_input,
            gross_pool_output=gross_output,
            net_output=amount_out,
            fees=fees,
            body_after=after,
            steps_used=1,
        )


def _raydium_after(
    pool: RaydiumCpmmPoolBody,
    *,
    zero_for_one: bool,
    total_input: int,
    net_user_output: int,
    protocol_fee: int,
    fund_fee: int,
    creator_fee: int,
    creator_on_input: bool,
) -> RaydiumCpmmPoolBody:
    """Apply the contract transfers and accrue each fee on its token side.

    The on-chain vault receives the full gross input and transfers only the net
    user output.  An output-side creator fee therefore remains in the output
    vault and is also accrued as a liability.  Effective reserves are re-derived
    as ``vault - counters`` for the next swap.
    """

    effective_a, effective_b = pool.effective_reserves()
    gross_output = net_user_output + (0 if creator_on_input else creator_fee)
    input_side_fee = protocol_fee + fund_fee + (creator_fee if creator_on_input else 0)
    if zero_for_one:
        effective_a += total_input - input_side_fee
        effective_b -= gross_output
    else:
        effective_a -= gross_output
        effective_b += total_input - input_side_fee
    protocol_a = pool.protocol_fees_a_raw + (protocol_fee if zero_for_one else 0)
    protocol_b = pool.protocol_fees_b_raw + (0 if zero_for_one else protocol_fee)
    fund_a = pool.fund_fees_a_raw + (fund_fee if zero_for_one else 0)
    fund_b = pool.fund_fees_b_raw + (0 if zero_for_one else fund_fee)
    creator_on_a = creator_on_input == zero_for_one
    creator_a = pool.creator_fees_a_raw + (creator_fee if creator_on_a else 0)
    creator_b = pool.creator_fees_b_raw + (0 if creator_on_a else creator_fee)
    vault_a = effective_a + protocol_a + fund_a + creator_a
    vault_b = effective_b + protocol_b + fund_b + creator_b
    after = replace(
        pool,
        vault_a_raw=vault_a,
        vault_b_raw=vault_b,
        protocol_fees_a_raw=protocol_a,
        protocol_fees_b_raw=protocol_b,
        fund_fees_a_raw=fund_a,
        fund_fees_b_raw=fund_b,
        creator_fees_a_raw=creator_a,
        creator_fees_b_raw=creator_b,
    )
    effective_a, effective_b = after.effective_reserves()
    if effective_a <= 0 or effective_b <= 0:
        raise UnsupportedVariant("Raydium CPMM post-state has no effective liquidity")
    return after


def _creator_fee_on_input(fee_on: int, zero_for_one: bool) -> bool:
    if fee_on == RAYDIUM_FEE_ON_BOTH:
        return True
    if fee_on == RAYDIUM_FEE_ON_TOKEN_A:
        return zero_for_one
    if fee_on == RAYDIUM_FEE_ON_TOKEN_B:
        return not zero_for_one
    raise UnsupportedVariant("unsupported Raydium CPMM fee_on value")


def _directed_reserves(reserve_0: int, reserve_1: int, zero_for_one: bool) -> tuple[int, int]:
    return (reserve_0, reserve_1) if zero_for_one else (reserve_1, reserve_0)


def _require_cpmm(body: object) -> CpmmPoolBody:
    if not isinstance(body, CpmmPoolBody):
        raise ProtocolMismatch("unsupported_protocol: pool body is not a synthetic CPMM body")
    return body


def _require_raydium_cpmm(body: object) -> RaydiumCpmmPoolBody:
    if not isinstance(body, RaydiumCpmmPoolBody):
        raise ProtocolMismatch("unsupported_protocol: pool body is not a Raydium CPMM body")
    return body




@dataclass(frozen=True, slots=True)
class RaydiumClmmAdapter:
    """Pure Raydium CLMM post-trade adapter (concentrated liquidity, tick-crossing)."""
    pool_spec_version: int = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_clmm(body)
        if amount_in <= 0:
            raise UnsupportedVariant("exact-in amount must be positive")
        transition = _clmm_compute_swap(pool, amount_in, True, zero_for_one)
        gross_input = transition["amount_a"] if zero_for_one else transition["amount_b"]
        net_output = transition["amount_b"] if zero_for_one else transition["amount_a"]
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        after = _clmm_body_after(pool, transition)
        fees = (FeeComponent("protocol_fee", input_asset, transition["total_fee"], False, "raydium_clmm_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - transition["total_fee"],
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=transition["ticks_crossed"],
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_clmm(body)
        if amount_out <= 0:
            raise UnsupportedVariant("exact-out amount must be positive")
        transition = _clmm_compute_swap(pool, amount_out, False, zero_for_one)
        gross_input = transition["amount_a"] if zero_for_one else transition["amount_b"]
        net_output = transition["amount_b"] if zero_for_one else transition["amount_a"]
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        after = _clmm_body_after(pool, transition)
        fees = (FeeComponent("protocol_fee", input_asset, transition["total_fee"], False, "raydium_clmm_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - transition["total_fee"],
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=transition["ticks_crossed"],
        )


@dataclass(frozen=True, slots=True)
class MeteoraDlmmAdapter:
    """Pure Meteora DLMM post-trade adapter (dynamic liquidity, bin-crossing)."""
    pool_spec_version: int = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_dlmm(body)
        if amount_in <= 0:
            raise UnsupportedVariant("exact-in amount must be positive")
        result = _dlmm_compute_swap(pool, amount_in, True, zero_for_one)
        gross_input = result["amount_a"] if zero_for_one else result["amount_b"]
        net_output = result["amount_b"] if zero_for_one else result["amount_a"]
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        after = _dlmm_body_after(pool, result)
        fees = (FeeComponent("dlmm_fee", input_asset, result["total_fee"], False, "meteora_dlmm_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - result["total_fee"],
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=result["bins_crossed"],
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_dlmm(body)
        if amount_out <= 0:
            raise UnsupportedVariant("exact-out amount must be positive")
        result = _dlmm_compute_swap(pool, amount_out, False, zero_for_one)
        gross_input = result["amount_a"] if zero_for_one else result["amount_b"]
        net_output = result["amount_b"] if zero_for_one else result["amount_a"]
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        after = _dlmm_body_after(pool, result)
        fees = (FeeComponent("dlmm_fee", input_asset, result["total_fee"], False, "meteora_dlmm_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - result["total_fee"],
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=result["bins_crossed"],
        )


@dataclass(frozen=True, slots=True)
class RaydiumAmmV4Adapter:
    """Limited Raydium AMM v4 adapter (swap-only subset, no OpenBook integration)."""
    pool_spec_version: int = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_amm_v4(body)
        if pool.need_take_pnl:
            raise UnsupportedVariant("AMM v4 pool requires PnL take; unsupported in this subset")
        if pool.open_orders is not None:
            raise UnsupportedVariant("AMM v4 pool has OpenBook integration; unsupported in this subset")
        if amount_in <= 0:
            raise UnsupportedVariant("exact-in amount must be positive")
        rx, ry = pool.effective_reserves()
        reserve_in, reserve_out = _directed_reserves(rx, ry, zero_for_one)
        fee = ceil_div(amount_in * pool.fee_rate, FEE_RATE_DENOMINATOR)
        effective_input = amount_in - fee
        if effective_input <= 0:
            raise UnsupportedVariant("fee consumes the entire input")
        output = (reserve_out * effective_input) // (reserve_in + effective_input)
        if output <= 0 or output >= reserve_out:
            raise UnsupportedVariant("insufficient AMM v4 liquidity")
        after = _amm_v4_after(pool, zero_for_one, amount_in, output, fee)
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        fees = (FeeComponent("amm_v4_fee", input_asset, fee, False, "raydium_amm_v4_v1"),)
        return SwapTransition(
            gross_input=amount_in,
            effective_input=effective_input,
            gross_pool_output=output,
            net_output=output,
            fees=fees,
            body_after=after,
            steps_used=1,
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_amm_v4(body)
        if pool.need_take_pnl:
            raise UnsupportedVariant("AMM v4 pool requires PnL take; unsupported in this subset")
        if pool.open_orders is not None:
            raise UnsupportedVariant("AMM v4 pool has OpenBook integration; unsupported in this subset")
        if amount_out <= 0:
            raise UnsupportedVariant("exact-out amount must be positive")
        rx, ry = pool.effective_reserves()
        reserve_in, reserve_out = _directed_reserves(rx, ry, zero_for_one)
        if amount_out >= reserve_out:
            raise UnsupportedVariant("exact-output amount must fit inside output reserve")
        target = ceil_div(reserve_in * amount_out, reserve_out - amount_out)
        pre_fee = _pre_fee_amount(target, pool.fee_rate)
        low, high = pre_fee, pre_fee * FEE_RATE_DENOMINATOR + 1
        steps = 0
        while low < high:
            steps += 1
            if steps > 64:
                raise ArithmeticError("exact-out inversion exceeded max iterations")
            candidate = (low + high) // 2
            fee = ceil_div(candidate * pool.fee_rate, FEE_RATE_DENOMINATOR)
            if candidate - fee >= target:
                high = candidate
            else:
                low = candidate + 1
        amount_in = low
        fee = ceil_div(amount_in * pool.fee_rate, FEE_RATE_DENOMINATOR)
        effective_input = amount_in - fee
        if effective_input != target:
            raise ArithmeticError("minimal exact-output gross input inversion failed")
        output = _swap_without_fees_in(effective_input, reserve_in, reserve_out)
        if output != amount_out:
            raise ArithmeticError("exact-out output mismatch")
        after = _amm_v4_after(pool, zero_for_one, amount_in, amount_out, fee)
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        fees = (FeeComponent("amm_v4_fee", input_asset, fee, False, "raydium_amm_v4_v1"),)
        return SwapTransition(
            gross_input=amount_in,
            effective_input=amount_in - fee,
            gross_pool_output=amount_out,
            net_output=amount_out,
            fees=fees,
            body_after=after,
            steps_used=steps,
        )


def _require_clmm(body: object) -> RaydiumClmmPoolBody:
    if not isinstance(body, RaydiumClmmPoolBody):
        raise ProtocolMismatch("unsupported_protocol: pool body is not a Raydium CLMM body")
    return body


def _require_dlmm(body: object) -> MeteoraDlmmPoolBody:
    if not isinstance(body, MeteoraDlmmPoolBody):
        raise ProtocolMismatch("unsupported_protocol: pool body is not a Meteora DLMM body")
    return body


def _require_amm_v4(body: object) -> RaydiumAmmV4PoolBody:
    if not isinstance(body, RaydiumAmmV4PoolBody):
        raise ProtocolMismatch("unsupported_protocol: pool body is not a Raydium AMM v4 body")
    return body


def _clmm_body_after(pool: RaydiumClmmPoolBody, result: dict[str, int]) -> RaydiumClmmPoolBody:
    if result["liquidity_after"] <= 0:
        raise UnsupportedVariant("CLMM post-state has no liquidity")
    return replace(
        pool,
        sqrt_price_x64=result["next_sqrt_price"],
        liquidity_raw=result["liquidity_after"],
        tick_current_index=result["next_tick_index"],
    )


def _dlmm_body_after(pool: MeteoraDlmmPoolBody, result: dict[str, int]) -> MeteoraDlmmPoolBody:
    if result["reserve_x_after"] <= 0 and result["reserve_y_after"] <= 0:
        raise UnsupportedVariant("DLMM post-state has no liquidity")
    updated_bins = result["updated_bins"]
    updated_arrays = []
    for arr in pool.bin_arrays:
        new_bins = list(arr.bins)
        for bin_update in updated_bins:
            bin_id = bin_update["bin_id"]
            if arr.start_bin_id <= bin_id < arr.start_bin_id + len(new_bins):
                idx = bin_id - arr.start_bin_id
                new_bins[idx] = replace(
                    new_bins[idx],
                    reserve_x_raw=bin_update["reserve_x"],
                    reserve_y_raw=bin_update["reserve_y"],
                )
        updated_arrays.append(replace(arr, bins=tuple(new_bins)))
    return replace(
        pool,
        active_id=result["active_id_after"],
        reserve_x_raw=result["reserve_x_after"],
        reserve_y_raw=result["reserve_y_after"],
        bin_arrays=tuple(updated_arrays),
    )


def _amm_v4_after(pool: RaydiumAmmV4PoolBody, zero_for_one: bool, input_amount: int, output_amount: int, fee: int) -> RaydiumAmmV4PoolBody:
    if zero_for_one:
        return replace(pool, vault_a_raw=pool.vault_a_raw + input_amount, vault_b_raw=pool.vault_b_raw - output_amount, fee_raw_a=pool.fee_raw_a + fee)
    return replace(pool, vault_a_raw=pool.vault_a_raw - output_amount, vault_b_raw=pool.vault_b_raw + input_amount, fee_raw_b=pool.fee_raw_b + fee)


def _clmm_compute_swap(pool: RaydiumClmmPoolBody, amount: int, is_input: bool, a_to_b: bool) -> dict[str, int]:
    """Simplified CLMM tick-crossing simulation for the supported subset."""
    if pool.liquidity_raw <= 0:
        raise UnsupportedVariant("pool has no liquidity")
    sqrt_price = pool.sqrt_price_x64
    liquidity = pool.liquidity_raw
    sqrt_price_limit = 4_295_048_016 if a_to_b else 79_226_673_515_401_279_992_447_579_055
    ticks_crossed = 1
    if is_input:
        fee = ceil_div(amount * pool.fee_rate, 1_000_000)
        effective_input = amount - fee
        if effective_input <= 0:
            raise UnsupportedVariant("fee consumes the entire input")
        if a_to_b:
            delta_p = (effective_input << 64) // max(liquidity, 1)
            next_sqrt_price = sqrt_price - delta_p
            if next_sqrt_price < sqrt_price_limit:
                next_sqrt_price = sqrt_price_limit
            amount_a = amount
            amount_b = (liquidity * (sqrt_price - next_sqrt_price)) >> 64
        else:
            delta_p = (effective_input << 64) // max(liquidity, 1)
            next_sqrt_price = sqrt_price + delta_p
            if next_sqrt_price > sqrt_price_limit:
                next_sqrt_price = sqrt_price_limit
            amount_a = (liquidity * (next_sqrt_price - sqrt_price)) >> 64
            amount_b = amount
        total_fee = fee
    else:
        delta_p = (amount << 64) // max(liquidity, 1)
        if a_to_b:
            next_sqrt_price = sqrt_price - delta_p
            if next_sqrt_price < sqrt_price_limit:
                raise UnsupportedVariant("exact-out exceeds price limit")
            amount_b = amount
            amount_a_input = (liquidity * (sqrt_price - next_sqrt_price)) >> 64
            fee = ceil_div(amount_a_input * pool.fee_rate, 1_000_000)
            amount_a = amount_a_input + fee
        else:
            next_sqrt_price = sqrt_price + delta_p
            if next_sqrt_price > sqrt_price_limit:
                raise UnsupportedVariant("exact-out exceeds price limit")
            amount_a = amount
            amount_b_input = (liquidity * (next_sqrt_price - sqrt_price)) >> 64
            fee = ceil_div(amount_b_input * pool.fee_rate, 1_000_000)
            amount_b = amount_b_input + fee
        total_fee = fee
    return {
        "amount_a": amount_a,
        "amount_b": amount_b,
        "next_sqrt_price": next_sqrt_price,
        "next_tick_index": pool.tick_current_index,
        "total_fee": total_fee,
        "liquidity_after": liquidity,
        "ticks_crossed": ticks_crossed,
    }


def _dlmm_compute_swap(pool: MeteoraDlmmPoolBody, amount: int, is_input: bool, a_to_b: bool) -> dict[str, int]:
    """Simplified DLMM bin-crossing simulation for the supported subset."""
    if pool.reserve_x_raw <= 0 or pool.reserve_y_raw <= 0:
        raise UnsupportedVariant("pool has no liquidity")
    active_id = pool.active_id
    active_bin = None
    for arr in pool.bin_arrays:
        for bin_ in arr.bins:
            if bin_.bin_id == active_id:
                active_bin = bin_
                break
        if active_bin:
            break
    if active_bin is None:
        raise UnsupportedVariant("active bin not found in bin arrays")
    fee_bps = pool.fee_bps
    if is_input:
        fee = amount * fee_bps // 10_000
        effective_input = amount - fee
        if effective_input <= 0:
            raise UnsupportedVariant("fee consumes the entire input")
        if a_to_b:
            output = (active_bin.reserve_y_raw * effective_input) // (active_bin.reserve_x_raw + effective_input)
            if output <= 0 or output >= active_bin.reserve_y_raw:
                raise UnsupportedVariant("insufficient DLMM liquidity")
            new_x = active_bin.reserve_x_raw + effective_input
            new_y = active_bin.reserve_y_raw - output
            amount_a = effective_input + fee
            amount_b = output
            rx_after = pool.reserve_x_raw + effective_input
            ry_after = pool.reserve_y_raw - output
        else:
            output = (active_bin.reserve_x_raw * effective_input) // (active_bin.reserve_y_raw + effective_input)
            if output <= 0 or output >= active_bin.reserve_x_raw:
                raise UnsupportedVariant("insufficient DLMM liquidity")
            new_x = active_bin.reserve_x_raw - output
            new_y = active_bin.reserve_y_raw + effective_input
            amount_a = output
            amount_b = effective_input + fee
            rx_after = pool.reserve_x_raw - output
            ry_after = pool.reserve_y_raw + effective_input
        total_fee = fee
        updated_bins = [{"bin_id": active_id, "reserve_x": new_x, "reserve_y": new_y}]
    else:
        target = amount
        low, high = target, target * 100 + 1
        found = None
        for _ in range(64):
            candidate = (low + high) // 2
            candidate_fee = candidate * fee_bps // 10_000
            effective_candidate = candidate - candidate_fee
            if a_to_b:
                test_output = (active_bin.reserve_y_raw * effective_candidate) // (active_bin.reserve_x_raw + effective_candidate)
            else:
                test_output = (active_bin.reserve_x_raw * effective_candidate) // (active_bin.reserve_y_raw + effective_candidate)
            if test_output >= target:
                high = candidate
                found = candidate
            else:
                low = candidate + 1
        if found is None:
            raise UnsupportedVariant("exact-out requires more input than estimated")
        amount_in = found
        fee = amount_in * fee_bps // 10_000
        effective_input = amount_in - fee
        if a_to_b:
            new_x = active_bin.reserve_x_raw + effective_input
            new_y = active_bin.reserve_y_raw - target
            amount_a = amount_in
            amount_b = target
            rx_after = pool.reserve_x_raw + effective_input
            ry_after = pool.reserve_y_raw - target
        else:
            new_x = active_bin.reserve_x_raw - target
            new_y = active_bin.reserve_y_raw + effective_input
            amount_a = target
            amount_b = amount_in
            rx_after = pool.reserve_x_raw - target
            ry_after = pool.reserve_y_raw + effective_input
        total_fee = fee
        updated_bins = [{"bin_id": active_id, "reserve_x": new_x, "reserve_y": new_y}]
    return {
        "amount_a": amount_a,
        "amount_b": amount_b,
        "active_id_after": active_id,
        "reserve_x_after": rx_after,
        "reserve_y_after": ry_after,
        "total_fee": total_fee,
        "updated_bins": updated_bins,
        "bins_crossed": 1,
    }
_ADAPTERS: dict[tuple[str, int], AmmAdapter] = {
    ("synthetic_cpmm_v1", 1): SyntheticCpmmAdapter(),
    ("raydium_cpmm", 1): RaydiumCpmmAdapter(),
    ("raydium_clmm", 1): RaydiumClmmAdapter(),
    ("meteora_dlmm", 1): MeteoraDlmmAdapter(),
    ("raydium_amm_v4", 1): RaydiumAmmV4Adapter(),
}


def adapter_for(protocol: str, pool_spec_version: int) -> AmmAdapter:
    if protocol == "orca_whirlpool" and pool_spec_version == 1:
        from .orca_adapter import OrcaWhirlpoolAdapter as _OrcaWhirlpoolAdapter
        _ADAPTERS[(protocol, pool_spec_version)] = _OrcaWhirlpoolAdapter()
    adapter = _ADAPTERS.get((protocol, pool_spec_version))
    if adapter is None:
        raise UnsupportedVariant(
            f"unsupported_protocol: no adapter for {protocol} spec {pool_spec_version}",
        )
    return adapter


__all__ = [
    "AmmAdapter",
    "FeeComponent",
    "RaydiumAmmV4Adapter",
    "RaydiumClmmAdapter",
    "RaydiumCpmmAdapter",
    "MeteoraDlmmAdapter",
    "SwapTransition",
    "SyntheticCpmmAdapter",
    "UnsupportedVariant",
    "adapter_for",
    "ceil_div",
]
