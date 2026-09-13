"""Classic Orca Whirlpool post-trade adapter (pure offline core).

This is a faithful port of the pinned ``@orca-so/whirlpools-sdk`` 0.22.0
swap path (``swap-manager`` / ``swap-math`` / ``token-math`` / ``bit-math`` /
``price-math`` / ``tick-array-*``) restricted to classic non-adaptive fee tiers.

It never reaches the network, never mutates the observed body, and returns a
typed ``UnsupportedVariant`` instead of a silent fallback when a swap would
traverse into an uninitialized/absent tick array, exit the allowed price range,
or invert a zero denominator.

Only swap-execution state is projected into the returned body:
``sqrt_price_x64``, ``tick_current_index``, ``liquidity_raw`` and the input-side
``fee_growth_global_*`` / ``protocol_fee_owed_*`` counters that affect subsequent
supported swaps.  LP accounting and byte-identical chain state are out of scope.
"""

from __future__ import annotations

from dataclasses import replace

from .adapters import FeeComponent, SwapTransition, UnsupportedVariant
from .contracts import OrcaWhirlpoolPoolBody


MIN_SQRT_PRICE = 4_295_048_016
MAX_SQRT_PRICE = 79_226_673_515_401_279_992_447_579_055
MIN_TICK_INDEX = -443_636
MAX_TICK_INDEX = 443_636
TICK_ARRAY_SIZE = 88
FEE_RATE_MUL_VALUE = 1_000_000
PROTOCOL_FEE_RATE_MUL_VALUE = 10_000
U64_MAX = 2**64 - 1
BIT_PRECISION = 14
LOG_B_2_X32 = 59_543_866_431_248
LOG_B_P_ERR_MARGIN_LOWER_X64 = 184_467_440_737_095_516
LOG_B_P_ERR_MARGIN_UPPER_X64 = 15_793_534_762_490_258_745

# (factor, shift) per bit index 1..18, copied from the pinned SDK price-math.
_POSITIVE_FACTORS = (
    ("79236085330515764027303304731", 96),
    ("79244008939048815603706035061", 96),
    ("79259858533276714757314932305", 96),
    ("79291567232598584799939703904", 96),
    ("79355022692464371645785046466", 96),
    ("79482085999252804386437311141", 96),
    ("79736823300114093921829183326", 96),
    ("80248749790819932309965073892", 96),
    ("81282483887344747381513967011", 96),
    ("83390072131320151908154831281", 96),
    ("87770609709833776024991924138", 96),
    ("97234110755111693312479820773", 96),
    ("119332217159966728226237229890", 96),
    ("179736315981702064433883588727", 96),
    ("407748233172238350107850275304", 96),
    ("2098478828474011932436660412517", 96),
    ("55581415166113811149459800483533", 96),
    ("38992368544603139932233054999993551", 96),
)
_NEGATIVE_FACTORS = (
    ("18444899583751176498", 64),
    ("18443055278223354162", 64),
    ("18439367220385604838", 64),
    ("18431993317065449817", 64),
    ("18417254355718160513", 64),
    ("18387811781193591352", 64),
    ("18329067761203520168", 64),
    ("18212142134806087854", 64),
    ("17980523815641551639", 64),
    ("17526086738831147013", 64),
    ("16651378430235024244", 64),
    ("15030750278693429944", 64),
    ("12247334978882834399", 64),
    ("8131365268884726200", 64),
    ("3584323654723342297", 64),
    ("696457651847595233", 64),
    ("26294789957452057", 64),
    ("37481735321082", 64),
)


def _mul_div(n0: int, n1: int, d: int) -> int:
    return n0 * n1 // d


def _mul_div_round_up(n0: int, n1: int, d: int) -> int:
    quotient, remainder = divmod(n0 * n1, d)
    return quotient + (1 if remainder else 0)


def _div_round_up(n: int, d: int) -> int:
    quotient, remainder = divmod(n, d)
    return quotient + (1 if remainder else 0)


def _tick_index_to_sqrt_price_x64(tick_index: int) -> int:
    if tick_index > 0:
        ratio = 79_228_162_514_264_337_593_543_950_336 if (tick_index & 1) == 0 else 79_232_123_823_359_799_118_286_999_567
        for bit_index, (factor, shift) in enumerate(_POSITIVE_FACTORS, start=1):
            if (tick_index >> bit_index) & 1:
                ratio = (ratio * int(factor)) >> shift
        return ratio >> 32
    tick = -tick_index
    ratio = 18_446_744_073_709_551_616 if (tick & 1) == 0 else 18_445_821_805_675_392_311
    for bit_index, (factor, shift) in enumerate(_NEGATIVE_FACTORS, start=1):
        if (tick >> bit_index) & 1:
            ratio = (ratio * int(factor)) >> shift
    return ratio


def _sqrt_price_x64_to_tick_index(sqrt_price: int) -> int:
    if sqrt_price > MAX_SQRT_PRICE or sqrt_price < MIN_SQRT_PRICE:
        raise UnsupportedVariant("sqrt price is outside the supported range")
    msb = sqrt_price.bit_length() - 1
    adjusted_msb = msb - 64
    log2p_integer_x32 = adjusted_msb << 32
    bit = 1 << 63
    precision = 0
    log2p_fraction_x64 = 0
    r = sqrt_price >> (msb - 63) if msb >= 64 else sqrt_price << (63 - msb)
    while bit > 0 and precision < BIT_PRECISION:
        r = r * r
        r_more_than_two = r >> 127
        r = r >> (63 + r_more_than_two)
        log2p_fraction_x64 += bit * r_more_than_two
        bit >>= 1
        precision += 1
    log2p_fraction_x32 = log2p_fraction_x64 >> 32
    log2p_x32 = log2p_integer_x32 + log2p_fraction_x32
    log_bp_x64 = log2p_x32 * LOG_B_2_X32
    tick_low = (log_bp_x64 - LOG_B_P_ERR_MARGIN_LOWER_X64) >> 64
    tick_high = (log_bp_x64 + LOG_B_P_ERR_MARGIN_UPPER_X64) >> 64
    if tick_low == tick_high:
        return tick_low
    if _tick_index_to_sqrt_price_x64(tick_high) <= sqrt_price:
        return tick_high
    return tick_low


def _get_amount_delta_a(curr_sqrt: int, target_sqrt: int, liquidity: int, round_up: bool) -> int:
    sqrt_lower, sqrt_upper = sorted((curr_sqrt, target_sqrt))
    numerator = liquidity * (sqrt_upper - sqrt_lower) << 64
    denominator = sqrt_lower * sqrt_upper
    quotient, remainder = divmod(numerator, denominator)
    result = quotient + (1 if round_up and remainder else 0)
    if result > U64_MAX:
        raise UnsupportedVariant("token A delta exceeds u64")
    return result


def _get_amount_delta_b(curr_sqrt: int, target_sqrt: int, liquidity: int, round_up: bool) -> int:
    sqrt_lower, sqrt_upper = sorted((curr_sqrt, target_sqrt))
    n1 = sqrt_upper - sqrt_lower
    if liquidity == 0 or n1 == 0:
        return 0
    product = liquidity * n1
    if product > 2**256 - 1:
        raise UnsupportedVariant("token B delta exceeds u256")
    result = product >> 64
    if round_up and (product & U64_MAX) > 0:
        if result == U64_MAX:
            raise UnsupportedVariant("token B delta overflows u64")
        result += 1
    return result


def _amount_fixed_delta(curr_sqrt: int, target_sqrt: int, liquidity: int, is_input: bool, a_to_b: bool) -> int:
    if a_to_b == is_input:
        return _get_amount_delta_a(curr_sqrt, target_sqrt, liquidity, is_input)
    return _get_amount_delta_b(curr_sqrt, target_sqrt, liquidity, is_input)


def _try_amount_fixed_delta(curr_sqrt: int, target_sqrt: int, liquidity: int, is_input: bool, a_to_b: bool) -> int | None:
    try:
        return _amount_fixed_delta(curr_sqrt, target_sqrt, liquidity, is_input, a_to_b)
    except UnsupportedVariant:
        return None


def _amount_unfixed_delta(curr_sqrt: int, target_sqrt: int, liquidity: int, is_input: bool, a_to_b: bool) -> int:
    if a_to_b == is_input:
        return _get_amount_delta_b(curr_sqrt, target_sqrt, liquidity, not is_input)
    return _get_amount_delta_a(curr_sqrt, target_sqrt, liquidity, not is_input)


def _get_next_sqrt_price_from_a_round_up(sqrt_price: int, liquidity: int, amount: int, is_input: bool) -> int:
    if amount == 0:
        return sqrt_price
    product = sqrt_price * amount
    numerator = (liquidity * sqrt_price) << 64
    if numerator > 2**256 - 1:
        raise UnsupportedVariant("getNextSqrtPriceFromA numerator overflow")
    liquidity_shift_left = liquidity << 64
    if not is_input and liquidity_shift_left <= product:
        raise UnsupportedVariant("unable to divide liquidity by product")
    denominator = liquidity_shift_left + product if is_input else liquidity_shift_left - product
    price = _div_round_up(numerator, denominator)
    if price < MIN_SQRT_PRICE:
        raise UnsupportedVariant("swap price is below min sqrt price")
    if price > MAX_SQRT_PRICE:
        raise UnsupportedVariant("swap price is above max sqrt price")
    return price


def _get_next_sqrt_price_from_b_round_down(sqrt_price: int, liquidity: int, amount: int, is_input: bool) -> int:
    amount_x64 = amount << 64
    quotient, remainder = divmod(amount_x64, liquidity)
    delta = quotient + (1 if (not is_input) and remainder else 0)
    return sqrt_price + delta if is_input else sqrt_price - delta


def _get_next_sqrt_price(sqrt_price: int, liquidity: int, amount: int, is_input: bool, a_to_b: bool) -> int:
    if is_input == a_to_b:
        return _get_next_sqrt_price_from_a_round_up(sqrt_price, liquidity, amount, is_input)
    return _get_next_sqrt_price_from_b_round_down(sqrt_price, liquidity, amount, is_input)


def _compute_swap_step(
    amount_remaining: int,
    fee_rate: int,
    curr_liquidity: int,
    curr_sqrt_price: int,
    target_sqrt_price: int,
    is_input: bool,
    a_to_b: bool,
) -> tuple[int, int, int, int]:
    """Port of SDK ``computeSwapStep`` → (amount_in, amount_out, fee, next_sqrt)."""
    initial_fixed_delta = _try_amount_fixed_delta(
        curr_sqrt_price, target_sqrt_price, curr_liquidity, is_input, a_to_b,
    )
    amount_calc = amount_remaining
    if is_input:
        amount_calc = _mul_div(amount_remaining, FEE_RATE_MUL_VALUE - fee_rate, FEE_RATE_MUL_VALUE)
    if initial_fixed_delta is not None and initial_fixed_delta <= amount_calc:
        next_sqrt_price = target_sqrt_price
    else:
        next_sqrt_price = _get_next_sqrt_price(curr_sqrt_price, curr_liquidity, amount_calc, is_input, a_to_b)
    is_max_swap = next_sqrt_price == target_sqrt_price
    amount_unfixed = _amount_unfixed_delta(curr_sqrt_price, next_sqrt_price, curr_liquidity, is_input, a_to_b)
    if is_max_swap and initial_fixed_delta is not None:
        amount_fixed = initial_fixed_delta
    else:
        amount_fixed = _amount_fixed_delta(curr_sqrt_price, next_sqrt_price, curr_liquidity, is_input, a_to_b)
    amount_in = amount_fixed if is_input else amount_unfixed
    amount_out = amount_unfixed if is_input else amount_fixed
    if not is_input and amount_out > amount_remaining:
        amount_out = amount_remaining
    if is_input and not is_max_swap:
        fee_amount = amount_remaining - amount_in
    else:
        fee_amount = _mul_div_round_up(amount_in, fee_rate, FEE_RATE_MUL_VALUE - fee_rate)
    return amount_in, amount_out, fee_amount, next_sqrt_price


def _calculate_protocol_fee(global_fee: int, protocol_fee_rate: int) -> int:
    # Mirrors the pinned SDK (rate is an integer quotient of 10_000).
    return global_fee * (protocol_fee_rate // PROTOCOL_FEE_RATE_MUL_VALUE)


def _calculate_fees(
    fee_amount: int,
    protocol_fee_rate: int,
    curr_liquidity: int,
    curr_protocol_fee: int,
    curr_fee_growth: int,
) -> tuple[int, int]:
    next_protocol_fee = curr_protocol_fee
    next_fee_growth = curr_fee_growth
    global_fee = fee_amount
    if protocol_fee_rate > 0:
        delta = _calculate_protocol_fee(global_fee, protocol_fee_rate)
        global_fee -= delta
        next_protocol_fee = next_protocol_fee + curr_protocol_fee  # intentional SDK port
    if curr_liquidity > 0:
        next_fee_growth += (global_fee << 64) // curr_liquidity
    return next_protocol_fee, next_fee_growth


class _OrcaTickSequence:
    """In-memory mirror of the SDK ``TickArraySequence``.

    The sequence is ordered along the swap direction starting from the first
    array at the current tick.  Bounds are expressed as a local array index,
    exactly like the pinned SDK port: when the search leaves the supplied
    arrays it returns a clamped boundary tick instead of failing, and a swap
    only fails if it actually needs to cross into a missing array (``get_tick``).
    """

    def __init__(self, body: OrcaWhirlpoolPoolBody, a_to_b: bool) -> None:
        self._spacing = body.tick_spacing
        self._a_to_b = a_to_b
        shift = 0 if a_to_b else body.tick_spacing
        self._first_start = _start_tick_index(body.tick_current_index + shift, body.tick_spacing)
        ordered = [
            a for a in sorted(body.tick_arrays, key=lambda a: a.start_tick_index)
            if (a.start_tick_index >= self._first_start) == a_to_b or a.start_tick_index >= self._first_start - 1
        ]
        if not ordered:
            raise UnsupportedVariant("no tick array is available for the swap direction")
        if a_to_b:
            ordered = [a for a in body.tick_arrays if a.start_tick_index <= self._first_start]
            ordered.sort(key=lambda a: a.start_tick_index, reverse=True)
            base = ordered[0].start_tick_index
        else:
            ordered = [a for a in body.tick_arrays if a.start_tick_index >= self._first_start]
            ordered.sort(key=lambda a: a.start_tick_index)
            base = ordered[0].start_tick_index
        self._sequence = ordered
        self._base_start = base
        self._start_array_index = _array_index(base, body.tick_spacing)

    def _local_array_index(self, tick_index: int) -> int:
        array_index = _array_index(_start_tick_index(tick_index, self._spacing), self._spacing)
        if self._a_to_b:
            return self._start_array_index - array_index
        return array_index - self._start_array_index

    def _is_in_bounds(self, tick_index: int) -> bool:
        local = self._local_array_index(tick_index)
        return 0 <= local < len(self._sequence)

    def get_tick(self, tick_index: int) -> object:
        if not self._is_in_bounds(tick_index):
            raise UnsupportedVariant("tick index is outside the provided tick array sequence")
        array = self._sequence[self._local_array_index(tick_index)]
        start = array.start_tick_index
        offset = (tick_index - start) // self._spacing
        if not (0 <= offset < len(array.ticks)):
            raise UnsupportedVariant("tick index is outside the provided tick array")
        return array.ticks[offset]

    def find_next_initialized_tick_index(self, curr_index: int) -> tuple[int, object | None]:
        search = curr_index if self._a_to_b else curr_index + self._spacing
        if not self._is_in_bounds(search):
            raise UnsupportedVariant("swap input traverses outside the supplied tick arrays")
        tick = search
        guard = 0
        while self._is_in_bounds(tick) and guard < 10_000:
            data = self.get_tick(tick)
            if data.initialized:
                return tick, data
            tick += -self._spacing if self._a_to_b else self._spacing
            guard += 1
        last_index_in_array = max(
            min(
                tick + self._spacing if self._a_to_b else tick - 1,
                MAX_TICK_INDEX,
            ),
            MIN_TICK_INDEX,
        )
        return last_index_in_array, None



def _array_index(start_tick: int, spacing: int) -> int:
    return start_tick // (spacing * TICK_ARRAY_SIZE)


def _start_tick_index(tick: int, spacing: int) -> int:
    real_index = tick // spacing // TICK_ARRAY_SIZE
    return real_index * spacing * TICK_ARRAY_SIZE


def _calculate_est_tokens(amount: int, amount_remaining: int, amount_calculated: int, a_to_b: bool, is_input: bool) -> tuple[int, int]:
    if a_to_b == is_input:
        return amount - amount_remaining, amount_calculated
    return amount_calculated, amount - amount_remaining


def _compute_swap(
    body: OrcaWhirlpoolPoolBody,
    token_amount: int,
    sqrt_price_limit: int,
    is_input: bool,
    a_to_b: bool,
) -> dict[str, int]:
    amount_remaining = token_amount
    amount_calculated = 0
    curr_sqrt_price = body.sqrt_price_x64
    curr_liquidity = body.liquidity_raw
    curr_tick_index = body.tick_current_index
    total_fee_amount = 0
    fee_rate = body.fee_rate
    applied_min: int | None = None
    applied_max: int | None = None
    protocol_fee_rate = body.protocol_fee_rate
    curr_protocol_fee = 0
    curr_fee_growth = (
        body.fee_growth_global_a if a_to_b else body.fee_growth_global_b
    )
    sequence = _OrcaTickSequence(body, a_to_b)
    while amount_remaining > 0 and curr_sqrt_price != sqrt_price_limit:
        next_tick_index, _ = sequence.find_next_initialized_tick_index(curr_tick_index)
        next_tick_price = _tick_index_to_sqrt_price_x64(next_tick_index)
        sqrt_price_target = (
            max(sqrt_price_limit, next_tick_price) if a_to_b else min(sqrt_price_limit, next_tick_price)
        )
        while True:
            total_fee_rate = fee_rate  # classic non-adaptive fee tier
            applied_min = total_fee_rate if applied_min is None else min(applied_min, total_fee_rate)
            applied_max = total_fee_rate if applied_max is None else max(applied_max, total_fee_rate)
            bounded_target = sqrt_price_target  # static fee manager returns the target unchanged
            step_in, step_out, step_fee, next_sqrt_price = _compute_swap_step(
                amount_remaining,
                total_fee_rate,
                curr_liquidity,
                curr_sqrt_price,
                bounded_target,
                is_input,
                a_to_b,
            )
            total_fee_amount += step_fee
            if is_input:
                amount_remaining -= step_in
                amount_remaining -= step_fee
                amount_calculated += step_out
            else:
                amount_remaining -= step_out
                amount_calculated += step_in
                amount_calculated += step_fee
            if amount_remaining < 0:
                raise UnsupportedVariant("amount remaining became negative")
            if amount_calculated > U64_MAX:
                raise UnsupportedVariant("amount calculated exceeds u64")
            next_protocol_fee, next_fee_growth = _calculate_fees(
                step_fee,
                protocol_fee_rate,
                curr_liquidity,
                curr_protocol_fee,
                curr_fee_growth,
            )
            curr_protocol_fee = next_protocol_fee
            curr_fee_growth = next_fee_growth
            if next_sqrt_price == next_tick_price:
                tick = sequence.get_tick(next_tick_index)
                if tick.initialized:
                    curr_liquidity = (
                        curr_liquidity - tick.liquidity_net_raw
                        if a_to_b
                        else curr_liquidity + tick.liquidity_net_raw
                    )
                curr_tick_index = next_tick_index - 1 if a_to_b else next_tick_index
            else:
                curr_tick_index = _sqrt_price_x64_to_tick_index(next_sqrt_price)
            curr_sqrt_price = next_sqrt_price
            if not (amount_remaining > 0 and curr_sqrt_price != sqrt_price_target):
                break
    amount_a, amount_b = _calculate_est_tokens(
        token_amount, amount_remaining, amount_calculated, a_to_b, is_input,
    )
    return {
        "amount_a": amount_a,
        "amount_b": amount_b,
        "next_tick_index": curr_tick_index,
        "next_sqrt_price": curr_sqrt_price,
        "total_fee_amount": total_fee_amount,
        "applied_fee_rate_min": applied_min if applied_min is not None else fee_rate,
        "applied_fee_rate_max": applied_max if applied_max is not None else fee_rate,
        "protocol_fee": curr_protocol_fee,
        "fee_growth_input": curr_fee_growth,
        "liquidity_after": curr_liquidity,
    }


def _effective_reserves(body: OrcaWhirlpoolPoolBody) -> tuple[int, int]:
    token_a = (body.liquidity_raw * body.sqrt_price_x64) >> 64
    token_b = (body.liquidity_raw << 64) // body.sqrt_price_x64
    return token_a, token_b


class OrcaWhirlpoolAdapter:
    protocol = "orca_whirlpool"
    pool_spec_version = 1

    def exact_in(self, body: object, amount_in: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_orca(body)
        if amount_in <= 0:
            raise UnsupportedVariant("exact-in amount must be positive")
        sqrt_price_limit = MIN_SQRT_PRICE if zero_for_one else MAX_SQRT_PRICE
        result = _compute_swap(pool, amount_in, sqrt_price_limit, True, zero_for_one)
        gross_input = result["amount_a"] if zero_for_one else result["amount_b"]
        net_output = result["amount_b"] if zero_for_one else result["amount_a"]
        fee_amount = result["total_fee_amount"]
        if gross_input < amount_in:
            raise UnsupportedVariant("insufficient Whirlpool liquidity for exact-in")
        if amount_in - gross_input > 0 or gross_input <= 0:
            raise UnsupportedVariant("invalid exact-in outcome")
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        output_asset = pool.pool_ref.asset_1_id if zero_for_one else pool.pool_ref.asset_0_id
        after = _body_after(pool, result, zero_for_one)
        fees = (FeeComponent("whirlpool_fee", input_asset, fee_amount, False, "orca_whirlpool_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - fee_amount,
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=1,
        )

    def exact_out(self, body: object, amount_out: int, zero_for_one: bool) -> SwapTransition:
        pool = _require_orca(body)
        if amount_out <= 0:
            raise UnsupportedVariant("exact-out amount must be positive")
        sqrt_price_limit = MIN_SQRT_PRICE if zero_for_one else MAX_SQRT_PRICE
        result = _compute_swap(pool, amount_out, sqrt_price_limit, False, zero_for_one)
        gross_input = result["amount_a"] if zero_for_one else result["amount_b"]
        net_output = result["amount_b"] if zero_for_one else result["amount_a"]
        fee_amount = result["total_fee_amount"]
        if net_output < amount_out:
            raise UnsupportedVariant("insufficient Whirlpool liquidity for exact-out")
        if net_output > amount_out:
            raise UnsupportedVariant("exact-out internal mismatch")
        input_asset = pool.pool_ref.asset_0_id if zero_for_one else pool.pool_ref.asset_1_id
        output_asset = pool.pool_ref.asset_1_id if zero_for_one else pool.pool_ref.asset_0_id
        after = _body_after(pool, result, zero_for_one)
        fees = (FeeComponent("whirlpool_fee", input_asset, fee_amount, False, "orca_whirlpool_v1"),)
        return SwapTransition(
            gross_input=gross_input,
            effective_input=gross_input - fee_amount,
            gross_pool_output=net_output,
            net_output=net_output,
            fees=fees,
            body_after=after,
            steps_used=1,
        )


def _body_after(pool: OrcaWhirlpoolPoolBody, result: dict[str, int], zero_for_one: bool) -> OrcaWhirlpoolPoolBody:
    if not result["liquidity_after"] > 0:
        raise UnsupportedVariant("Whirlpool post-state has no liquidity")
    fee_growth_a = (
        result["fee_growth_input"] if zero_for_one else pool.fee_growth_global_a
    )
    fee_growth_b = (
        pool.fee_growth_global_b if zero_for_one else result["fee_growth_input"]
    )
    protocol_owed_a = (
        pool.protocol_fee_owed_a + result["protocol_fee"] if zero_for_one else pool.protocol_fee_owed_a
    )
    protocol_owed_b = (
        pool.protocol_fee_owed_b if zero_for_one else pool.protocol_fee_owed_b + result["protocol_fee"]
    )
    return replace(
        pool,
        sqrt_price_x64=result["next_sqrt_price"],
        liquidity_raw=result["liquidity_after"],
        tick_current_index=result["next_tick_index"],
        fee_growth_global_a=fee_growth_a,
        fee_growth_global_b=fee_growth_b,
        protocol_fee_owed_a=protocol_owed_a,
        protocol_fee_owed_b=protocol_owed_b,
    )


def _require_orca(body: object) -> OrcaWhirlpoolPoolBody:
    if not isinstance(body, OrcaWhirlpoolPoolBody):
        raise UnsupportedVariant("pool body is not a classic Orca Whirlpool body")
    return body


__all__ = [
    "OrcaWhirlpoolAdapter",
    "_effective_reserves",
    "_tick_index_to_sqrt_price_x64",
    "_sqrt_price_x64_to_tick_index",
]
