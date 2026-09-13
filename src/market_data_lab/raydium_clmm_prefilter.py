"""Read-only Solana CLMM price prefilter for a MEXC ↔ Raydium PUMP cycle.

This is deliberately a *prefilter*, not a swap quoter or trading bot.  It
combines live Raydium CLMM pool state with a live MEXC partial-depth book to
find moments worth an immediate exact-input quote.  Its implied price excludes
CLMM fees, tick traversal, price impact, priority fees, and CEX depth walking.
No wallet, API credential, transaction construction, or order placement is
accepted by this module.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect as websocket_connect

from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import MexcPartialDepthStream
from market_data_lab.cex_dex_cycles import calculate_cycle
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import RaydiumProvider
from market_data_lab.dex_quotes import SOLANA_USDT
from market_data_lab.dex_quotes import SOLANA_USDT_PROVIDER_BASES
from market_data_lab.dex_quotes import SOL_USDT_MINT
from market_data_lab.dex_quotes import SOL_WRAPPED_MINT
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import _timed_fetch
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


SOLANA_HTTP_ENDPOINT = "https://api.mainnet-beta.solana.com"
SOLANA_WS_ENDPOINT = "wss://api.mainnet-beta.solana.com"
RAYDIUM_CLMM_PROGRAM_ID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
PUMP_MINT = "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn"

# These two pools were returned by a contemporaneous Raydium Route API exact
# quote for PUMP/USDT.  The CLI exposes them so the observed routing can be
# replaced without changing code.
PUMP_SOL_POOL_ID = "45ssPkUQs1ssbeDqxD2mZrMdJYAXF7GyQyhS5xDXuWC5"
SOL_USDT_POOL_ID = "3nMFwZXwY1s1M5s8vYAHqd4wGs4iSxXE4LRoUMMYqEgF"

# Offsets include the eight-byte Anchor discriminator.  The Raydium CLMM
# PoolState is repr(C, packed): bump, seven pubkeys, decimals, tick spacing,
# liquidity, then sqrt_price_x64 and tick_current.
CLMM_TOKEN_0_MINT_OFFSET = 73
CLMM_TOKEN_1_MINT_OFFSET = 105
CLMM_TOKEN_0_DECIMALS_OFFSET = 233
CLMM_TOKEN_1_DECIMALS_OFFSET = 234
CLMM_SQRT_PRICE_X64_OFFSET = 253
CLMM_TICK_CURRENT_OFFSET = 269
CLMM_MINIMUM_ACCOUNT_SIZE = CLMM_TICK_CURRENT_OFFSET + 4
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


@dataclass(frozen=True)
class ClmmPoolState:
    """The subset of Raydium CLMM PoolState needed for a mid-price prefilter."""

    pool_id: str
    token_0_mint: str
    token_1_mint: str
    token_0_decimals: int
    token_1_decimals: int
    sqrt_price_x64: int
    tick_current: int
    slot: int | None
    received_realtime_ns: int
    received_monotonic_ns: int
    source: str


def base58_encode(value: bytes) -> str:
    """Encode a 32-byte Solana public key without a third-party dependency."""

    if not value:
        return ""
    leading_zeroes = len(value) - len(value.lstrip(b"\x00"))
    number = int.from_bytes(value, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = BASE58_ALPHABET[remainder] + encoded
    return "1" * leading_zeroes + (encoded or "")


def decode_clmm_pool_state(
    data: bytes,
    *,
    pool_id: str,
    slot: int | None,
    received_realtime_ns: int,
    received_monotonic_ns: int,
    source: str,
) -> ClmmPoolState:
    """Decode fixed leading fields of a Raydium CLMM PoolState account."""

    if len(data) < CLMM_MINIMUM_ACCOUNT_SIZE:
        raise ValueError(
            f"Raydium CLMM pool account is {len(data)} bytes, expected at least "
            f"{CLMM_MINIMUM_ACCOUNT_SIZE}",
        )
    sqrt_price_x64 = int.from_bytes(
        data[CLMM_SQRT_PRICE_X64_OFFSET : CLMM_SQRT_PRICE_X64_OFFSET + 16],
        "little",
    )
    if sqrt_price_x64 <= 0:
        raise ValueError("Raydium CLMM sqrt_price_x64 must be positive")
    return ClmmPoolState(
        pool_id=pool_id,
        token_0_mint=base58_encode(
            data[CLMM_TOKEN_0_MINT_OFFSET : CLMM_TOKEN_0_MINT_OFFSET + 32],
        ),
        token_1_mint=base58_encode(
            data[CLMM_TOKEN_1_MINT_OFFSET : CLMM_TOKEN_1_MINT_OFFSET + 32],
        ),
        token_0_decimals=data[CLMM_TOKEN_0_DECIMALS_OFFSET],
        token_1_decimals=data[CLMM_TOKEN_1_DECIMALS_OFFSET],
        sqrt_price_x64=sqrt_price_x64,
        tick_current=int.from_bytes(
            data[CLMM_TICK_CURRENT_OFFSET : CLMM_TICK_CURRENT_OFFSET + 4],
            "little",
            signed=True,
        ),
        slot=slot,
        received_realtime_ns=received_realtime_ns,
        received_monotonic_ns=received_monotonic_ns,
        source=source,
    )


def parse_solana_account_value(
    value: Any,
    *,
    pool_id: str,
    slot: int | None,
    received_realtime_ns: int,
    received_monotonic_ns: int,
    source: str,
) -> ClmmPoolState:
    """Validate and decode a ``getAccountInfo``/``accountSubscribe`` value."""

    if not isinstance(value, dict):
        raise ValueError("Solana RPC account value is not an object")
    if value.get("owner") != RAYDIUM_CLMM_PROGRAM_ID:
        raise ValueError(f"pool {pool_id} is not owned by Raydium CLMM")
    encoded = value.get("data")
    if (
        not isinstance(encoded, list)
        or len(encoded) < 2
        or not isinstance(encoded[0], str)
        or encoded[1] != "base64"
    ):
        raise ValueError("Solana RPC account data is not base64")
    try:
        data = base64.b64decode(encoded[0], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("Solana RPC account data is invalid base64") from exc
    return decode_clmm_pool_state(
        data,
        pool_id=pool_id,
        slot=slot,
        received_realtime_ns=received_realtime_ns,
        received_monotonic_ns=received_monotonic_ns,
        source=source,
    )


def ui_price(
    pool: ClmmPoolState,
    *,
    base_mint: str,
    quote_mint: str,
) -> Decimal:
    """Return pool mid price in UI quote units per UI base unit.

    This only converts the current CLMM square-root price.  It does *not*
    simulate a trade across ticks and must never be used as an executable
    quote.
    """

    with localcontext() as context:
        context.prec = 60
        token_1_per_token_0 = (
            Decimal(pool.sqrt_price_x64) / Decimal(1 << 64)
        ) ** 2 * (Decimal(10) ** (pool.token_0_decimals - pool.token_1_decimals))
    if token_1_per_token_0 <= 0:
        raise ValueError("Raydium CLMM derived a non-positive price")
    if pool.token_0_mint == base_mint and pool.token_1_mint == quote_mint:
        return token_1_per_token_0
    if pool.token_1_mint == base_mint and pool.token_0_mint == quote_mint:
        return Decimal(1) / token_1_per_token_0
    raise ValueError(
        f"pool {pool.pool_id} does not contain requested {base_mint}/{quote_mint} pair",
    )


async def fetch_clmm_pool_states(
    pool_ids: tuple[str, ...],
    *,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
) -> tuple[dict[str, ClmmPoolState], TimedResponse]:
    """Fetch atomic-ish initial account snapshots in one JSON-RPC request."""

    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [list(pool_ids), {"encoding": "base64", "commitment": "processed"}],
        },
        separators=(",", ":"),
    ).encode()
    response = await _timed_fetch(
        _fetch_json_sync,
        url=endpoint,
        method="POST",
        body=body,
        headers={"Content-Type": "application/json"},
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    if response.error is not None:
        raise RuntimeError(f"Solana initial account request failed: {response.error}")
    payload = response.payload
    result = payload.get("result") if isinstance(payload, dict) else None
    values = result.get("value") if isinstance(result, dict) else None
    context = result.get("context") if isinstance(result, dict) else None
    slot = context.get("slot") if isinstance(context, dict) else None
    if not isinstance(values, list) or len(values) != len(pool_ids):
        raise RuntimeError("Solana getMultipleAccounts returned unexpected account count")
    parsed: dict[str, ClmmPoolState] = {}
    for pool_id, value in zip(pool_ids, values, strict=True):
        parsed[pool_id] = parse_solana_account_value(
            value,
            pool_id=pool_id,
            slot=int(slot) if isinstance(slot, int) else None,
            received_realtime_ns=response.received_realtime_ns,
            received_monotonic_ns=response.received_monotonic_ns,
            source="solana_http_getMultipleAccounts",
        )
    return parsed, response


class SolanaClmmPoolStream:
    """Maintain current CLMM state from documented Solana ``accountSubscribe``."""

    def __init__(
        self,
        pool_ids: tuple[str, ...],
        *,
        endpoint: str,
        proxy_url: str | None,
        timeout_seconds: float,
    ) -> None:
        if not pool_ids or len(set(pool_ids)) != len(pool_ids):
            raise ValueError("Solana CLMM pool IDs must be non-empty and unique")
        self.pool_ids = pool_ids
        self.endpoint = endpoint
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self._websocket: Any = None
        self._receiver: asyncio.Task[None] | None = None
        self._subscription_to_pool: dict[int, str] = {}
        self._latest: dict[str, ClmmPoolState] = {}
        # The original PUMP prefilter consumed ``latest`` on a timer.  Keep a
        # bounded push queue as well so the unified scanner can evaluate an
        # affected route immediately after an on-chain account update.  If a
        # consumer is temporarily slower than the RPC stream, retaining the
        # newest state is more valuable than retaining an unbounded raw log.
        self._updates: asyncio.Queue[ClmmPoolState] = asyncio.Queue(
            maxsize=max(256, len(pool_ids) * 8),
        )
        self._dropped_updates = 0
        self._error: str | None = None

    @property
    def latest(self) -> dict[str, ClmmPoolState]:
        return dict(self._latest)

    @property
    def error(self) -> str | None:
        return self._error

    @property
    def dropped_updates(self) -> int:
        """Number of stale push events discarded under local backpressure."""

        return self._dropped_updates

    async def next_update(self) -> ClmmPoolState:
        """Return the next decoded account update without polling ``latest``."""

        return await self._updates.get()

    def _publish_update(self, state: ClmmPoolState) -> None:
        self._latest[state.pool_id] = state
        if self._updates.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._updates.get_nowait()
                self._dropped_updates += 1
        with contextlib.suppress(asyncio.QueueFull):
            self._updates.put_nowait(state)

    async def start(self) -> None:
        self._websocket = await websocket_connect(
            self.endpoint,
            open_timeout=self.timeout_seconds,
            close_timeout=1,
            ping_interval=20,
            ping_timeout=20,
            proxy=self.proxy_url,
        )
        awaiting_ids: dict[int, str] = {}
        for request_id, pool_id in enumerate(self.pool_ids, start=1):
            awaiting_ids[request_id] = pool_id
            await self._websocket.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "accountSubscribe",
                        "params": [
                            pool_id,
                            {"encoding": "base64", "commitment": "processed"},
                        ],
                    },
                    separators=(",", ":"),
                ),
            )
        deadline = time.monotonic() + self.timeout_seconds
        while awaiting_ids:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Solana accountSubscribe confirmations timed out")
            raw = await asyncio.wait_for(self._websocket.recv(), timeout=remaining)
            self._process_message(raw, awaiting_ids)
        self._receiver = asyncio.create_task(self._receive_loop())

    def _process_message(self, raw: Any, awaiting_ids: dict[int, str] | None = None) -> None:
        if not isinstance(raw, str):
            return
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        response_id = payload.get("id")
        if awaiting_ids is not None and isinstance(response_id, int) and response_id in awaiting_ids:
            subscription = payload.get("result")
            if not isinstance(subscription, int):
                error = payload.get("error")
                raise RuntimeError(f"Solana accountSubscribe rejected: {error!r}")
            self._subscription_to_pool[subscription] = awaiting_ids.pop(response_id)
            return
        params = payload.get("params")
        if not isinstance(params, dict):
            return
        subscription = params.get("subscription")
        pool_id = self._subscription_to_pool.get(subscription)
        result = params.get("result")
        context = result.get("context") if isinstance(result, dict) else None
        value = result.get("value") if isinstance(result, dict) else None
        if pool_id is None or not isinstance(context, dict):
            return
        slot = context.get("slot")
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        try:
            state = parse_solana_account_value(
                value,
                pool_id=pool_id,
                slot=int(slot) if isinstance(slot, int) else None,
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
                source="solana_websocket_accountSubscribe",
            )
            self._publish_update(state)
        except ValueError:
            return

    async def _receive_loop(self) -> None:
        try:
            while True:
                self._process_message(await self._websocket.recv())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"

    async def close(self) -> None:
        if self._receiver is not None:
            self._receiver.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._receiver
            self._receiver = None
        if self._websocket is not None:
            with contextlib.suppress(Exception):
                await self._websocket.close()
            self._websocket = None


def build_pump_prefilter_observation(
    *,
    pools: dict[str, ClmmPoolState],
    pump_sol_pool_id: str,
    sol_usdt_pool_id: str,
    cex_book: Any,
    trigger_bps: Decimal,
) -> dict[str, Any]:
    """Compare CLMM mid prices to best MEXC levels; never treat it as a quote."""

    pump_sol = pools[pump_sol_pool_id]
    sol_usdt = pools[sol_usdt_pool_id]
    pump_per_sol = ui_price(
        pump_sol,
        base_mint=PUMP_MINT,
        quote_mint=SOL_WRAPPED_MINT,
    )
    usdt_per_sol = ui_price(
        sol_usdt,
        base_mint=SOL_WRAPPED_MINT,
        quote_mint=SOL_USDT_MINT,
    )
    implied_usdt_per_pump = pump_per_sol * usdt_per_sol
    cex_best_bid = cex_book.bids[0][0]
    cex_best_ask = cex_book.asks[0][0]
    buy_dex_sell_cex_bps = (cex_best_bid / implied_usdt_per_pump - Decimal(1)) * Decimal(
        10_000,
    )
    buy_cex_sell_dex_bps = (implied_usdt_per_pump / cex_best_ask - Decimal(1)) * Decimal(
        10_000,
    )
    strongest_bps = max(buy_dex_sell_cex_bps, buy_cex_sell_dex_bps)
    observed_realtime_ns = time.time_ns()
    return {
        "schema_version": 1,
        "observed_realtime_ns": observed_realtime_ns,
        "status": "ok",
        "cex_venue": "MEXC",
        "cex_symbol": cex_book.symbol,
        "cex_book_source": cex_book.source,
        "cex_book_received_realtime_ns": cex_book.response.received_realtime_ns,
        "cex_book_age_ms": round(
            (observed_realtime_ns - cex_book.response.received_realtime_ns) / 1_000_000,
            6,
        ),
        "pump_sol_pool_id": pump_sol.pool_id,
        "pump_sol_pool_slot": pump_sol.slot,
        "pump_sol_pool_received_realtime_ns": pump_sol.received_realtime_ns,
        "pump_sol_pool_source": pump_sol.source,
        "pump_sol_sqrt_price_x64": str(pump_sol.sqrt_price_x64),
        "sol_usdt_pool_id": sol_usdt.pool_id,
        "sol_usdt_pool_slot": sol_usdt.slot,
        "sol_usdt_pool_received_realtime_ns": sol_usdt.received_realtime_ns,
        "sol_usdt_pool_source": sol_usdt.source,
        "sol_usdt_sqrt_price_x64": str(sol_usdt.sqrt_price_x64),
        "implied_mid_usdt_per_pump": _decimal_text(implied_usdt_per_pump),
        "cex_best_bid": _decimal_text(cex_best_bid),
        "cex_best_ask": _decimal_text(cex_best_ask),
        "buy_dex_sell_cex_mid_edge_bps": round(float(buy_dex_sell_cex_bps), 6),
        "buy_cex_sell_dex_mid_edge_bps": round(float(buy_cex_sell_dex_bps), 6),
        "trigger_bps": _decimal_text(trigger_bps),
        "triggered": strongest_bps >= trigger_bps,
        "quote_required_before_action": True,
        "model_scope": "mid-price prefilter only; excludes CLMM fee, tick traversal, impact, CEX depth, CEX fee, and network cost",
    }


async def run_triggered_exact_check(
    *,
    check_id: int,
    prefilter_observation: dict[str, Any],
    provider: RaydiumProvider,
    notional_quote: Decimal,
    mexc_stream: MexcPartialDepthStream,
    cex_taker_fee_bps: Decimal,
    minimum_network_cost_quote: Decimal,
    max_response_skew_ms: Decimal,
) -> list[dict[str, Any]]:
    """Request exact Raydium routes only after a prefilter trigger.

    The function remains read-only.  It selects the nearest retained MEXC
    websocket event per returned DEX quote and applies the same depth/fee/floor
    model as the general CEX↔DEX scanner.
    """

    records = await provider.quote_round(check_id, (notional_quote,))
    checks: list[dict[str, Any]] = []
    for record in records:
        check: dict[str, Any] = {
            "schema_version": 1,
            "check_id": check_id,
            "prefilter_observed_realtime_ns": prefilter_observation.get(
                "observed_realtime_ns",
            ),
            "prefilter_buy_dex_sell_cex_mid_edge_bps": prefilter_observation.get(
                "buy_dex_sell_cex_mid_edge_bps",
            ),
            "prefilter_buy_cex_sell_dex_mid_edge_bps": prefilter_observation.get(
                "buy_cex_sell_dex_mid_edge_bps",
            ),
            "requested_notional_quote": _decimal_text(notional_quote),
            "dex_quote": record,
        }
        if record.get("status") != "ok":
            check.update(status="dex_quote_unavailable", cycle=None)
            checks.append(check)
            continue
        try:
            book = mexc_stream.nearest_snapshot(
                "PUMPUSDT",
                int(record["response_received_realtime_ns"]),
            )
            if book is None:
                raise ValueError("MEXC websocket history has no PUMPUSDT book")
            cycle = calculate_cycle(
                market=MARKETS["PUMP_SOLANA_RAYDIUM_USDT"],
                dex_record=record,
                book=book,
                cex_taker_fee_bps=cex_taker_fee_bps,
                network_cost_floor_quote=minimum_network_cost_quote,
                max_response_skew_ms=max_response_skew_ms,
                cex_venue="MEXC",
            )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            check.update(status="cycle_calculation_error", error=f"{type(exc).__name__}: {exc}")
        else:
            check.update(status="ok", cycle=cycle)
        checks.append(check)
    return checks


async def record_pump_prefilter(
    *,
    duration_seconds: float,
    interval_seconds: float,
    trigger_bps: Decimal,
    cex_depth: int,
    cex_stream_max_age_ms: Decimal,
    solana_http_url: str,
    solana_ws_url: str,
    pump_sol_pool_id: str,
    sol_usdt_pool_id: str,
    proxy_url: str | None,
    timeout_seconds: float,
    output_directory: Path,
    exact_quote_notional: Decimal | None = None,
    exact_min_interval_seconds: float = 5.0,
    exact_raydium_min_request_interval_seconds: float = 0.65,
    exact_max_response_skew_ms: Decimal = Decimal("300"),
    cex_taker_fee_bps: Decimal = Decimal("0"),
    minimum_network_cost_quote: Decimal = Decimal("0.01"),
) -> dict[str, Any]:
    """Run a bounded, read-only CLMM midpoint prefilter and persist JSONL."""

    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite prefilter run: {output_directory}")
    if exact_quote_notional is not None and (
        exact_quote_notional <= 0 or not exact_quote_notional.is_finite()
    ):
        raise ValueError("exact quote notional must be finite and positive")
    if exact_min_interval_seconds < 0 or exact_raydium_min_request_interval_seconds < 0:
        raise ValueError("exact quote intervals cannot be negative")
    if exact_max_response_skew_ms < 0:
        raise ValueError("exact quote response skew cannot be negative")
    if cex_taker_fee_bps < 0 or cex_taker_fee_bps >= 10_000:
        raise ValueError("CEX taker fee must be in [0, 10000)")
    if minimum_network_cost_quote < 0 or not minimum_network_cost_quote.is_finite():
        raise ValueError("minimum network cost must be finite and non-negative")
    pool_ids = (pump_sol_pool_id, sol_usdt_pool_id)
    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    observations_path = output_directory / "observations.jsonl"
    exact_checks_path = output_directory / "exact_checks.jsonl"
    started_at = time.time_ns()
    started_monotonic = time.monotonic()
    initial_pools, initial_response = await fetch_clmm_pool_states(
        pool_ids,
        endpoint=solana_http_url,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    solana_stream = SolanaClmmPoolStream(
        pool_ids,
        endpoint=solana_ws_url,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    mexc_stream = MexcPartialDepthStream(
        ("PUMPUSDT",),
        levels=cex_depth,
        timeout_seconds=timeout_seconds,
        proxy_url=proxy_url,
    )
    observations: list[dict[str, Any]] = []
    exact_checks: list[dict[str, Any]] = []
    exact_check_runs = 0
    last_exact_check_monotonic = float("-inf")
    exact_provider = (
        RaydiumProvider(
            name="RAYDIUM_USDT_PUMP",
            base=SOLANA_USDT_PROVIDER_BASES["RAYDIUM_USDT_PUMP"],
            quote=SOLANA_USDT,
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            request_pacer=AsyncRequestPacer(exact_raydium_min_request_interval_seconds),
        )
        if exact_quote_notional is not None
        else None
    )
    try:
        await solana_stream.start()
        await mexc_stream.start()
        with observations_path.open("x", encoding="utf-8", buffering=1) as output:
            with (
                exact_checks_path.open("x", encoding="utf-8", buffering=1)
                if exact_provider is not None
                else contextlib.nullcontext(None)
            ) as exact_output:
                while time.monotonic() - started_monotonic < duration_seconds:
                    books = mexc_stream.snapshot_batch(
                        ("PUMPUSDT",),
                        max_age_ms=cex_stream_max_age_ms,
                    )
                    book = books["PUMPUSDT"]
                    pools = {**initial_pools, **solana_stream.latest}
                    if book.status == "ok" and set(pools) == set(pool_ids):
                        try:
                            observation = build_pump_prefilter_observation(
                                pools=pools,
                                pump_sol_pool_id=pump_sol_pool_id,
                                sol_usdt_pool_id=sol_usdt_pool_id,
                                cex_book=book,
                                trigger_bps=trigger_bps,
                            )
                        except (InvalidOperation, KeyError, ValueError) as exc:
                            observation = {
                                "schema_version": 1,
                                "observed_realtime_ns": time.time_ns(),
                                "status": "calculation_error",
                                "error": f"{type(exc).__name__}: {exc}",
                                "triggered": False,
                            }
                    else:
                        observation = {
                            "schema_version": 1,
                            "observed_realtime_ns": time.time_ns(),
                            "status": "input_unavailable",
                            "error": book.error if book.status != "ok" else "missing CLMM pool state",
                            "triggered": False,
                        }
                    observations.append(observation)
                    output.write(
                        json.dumps(observation, ensure_ascii=False, separators=(",", ":")) + "\n",
                    )
                    if (
                        exact_provider is not None
                        and exact_output is not None
                        and observation.get("triggered") is True
                        and time.monotonic() - last_exact_check_monotonic
                        >= exact_min_interval_seconds
                    ):
                        last_exact_check_monotonic = time.monotonic()
                        checks = await run_triggered_exact_check(
                            check_id=exact_check_runs,
                            prefilter_observation=observation,
                            provider=exact_provider,
                            notional_quote=exact_quote_notional,
                            mexc_stream=mexc_stream,
                            cex_taker_fee_bps=cex_taker_fee_bps,
                            minimum_network_cost_quote=minimum_network_cost_quote,
                            max_response_skew_ms=exact_max_response_skew_ms,
                        )
                        exact_check_runs += 1
                        exact_checks.extend(checks)
                        for check in checks:
                            exact_output.write(
                                json.dumps(check, ensure_ascii=False, separators=(",", ":"))
                                + "\n",
                            )
                    await asyncio.sleep(interval_seconds)
    finally:
        await mexc_stream.close()
        await solana_stream.close()

    valid = [item for item in observations if item.get("status") == "ok"]
    triggered = [item for item in valid if item.get("triggered")]
    max_buy_dex = max(
        (float(item["buy_dex_sell_cex_mid_edge_bps"]) for item in valid),
        default=None,
    )
    max_buy_cex = max(
        (float(item["buy_cex_sell_dex_mid_edge_bps"]) for item in valid),
        default=None,
    )
    exact_cycles = [
        check["cycle"]
        for check in exact_checks
        if isinstance(check.get("cycle"), dict)
    ]
    exact_timing_valid = [cycle for cycle in exact_cycles if cycle.get("timing_valid") is True]
    exact_positive = [
        cycle for cycle in exact_timing_valid if cycle.get("positive_after_minimum_network") is True
    ]
    stopped_at = time.time_ns()
    manifest = {
        "status": "ok",
        "started_realtime_ns": started_at,
        "stopped_realtime_ns": stopped_at,
        "duration_requested_seconds": duration_seconds,
        "duration_wall_seconds": round(time.monotonic() - started_monotonic, 6),
        "interval_seconds": interval_seconds,
        "observations": len(observations),
        "valid_observations": len(valid),
        "triggered_observations": len(triggered),
        "max_buy_dex_sell_cex_mid_edge_bps": max_buy_dex,
        "max_buy_cex_sell_dex_mid_edge_bps": max_buy_cex,
        "trigger_bps": _decimal_text(trigger_bps),
        "exact_quote_layer": {
            "enabled": exact_provider is not None,
            "notional_quote": (
                _decimal_text(exact_quote_notional) if exact_quote_notional is not None else None
            ),
            "minimum_trigger_interval_seconds": exact_min_interval_seconds,
            "raydium_min_request_interval_seconds": exact_raydium_min_request_interval_seconds,
            "max_response_skew_ms": _decimal_text(exact_max_response_skew_ms),
            "cex_taker_fee_bps": _decimal_text(cex_taker_fee_bps),
            "minimum_network_cost_quote": _decimal_text(minimum_network_cost_quote),
            "checks_started": exact_check_runs,
            "quote_records": len(exact_checks),
            "timing_valid_cycles": len(exact_timing_valid),
            "positive_after_minimum_network": len(exact_positive),
            "provider": exact_provider.config() if exact_provider is not None else None,
        },
        "cex": {
            "venue": "MEXC",
            "symbol": "PUMPUSDT",
            "source": "websocket_partial_depth",
            "depth": cex_depth,
            "stream_max_age_ms": _decimal_text(cex_stream_max_age_ms),
        },
        "onchain": {
            "chain": "solana",
            "protocol": "Raydium CLMM",
            "program_id": RAYDIUM_CLMM_PROGRAM_ID,
            "http_endpoint_origin": _redact_url(solana_http_url),
            "websocket_endpoint_origin": _redact_url(solana_ws_url),
            "initial_request_rtt_ms": round(initial_response.rtt_ms, 6),
            "pools": list(pool_ids),
            "websocket_error": solana_stream.error,
        },
        "network_route": network_route,
        "api_credentials_used": False,
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "model_scope": {
            "included": [
                "Raydium CLMM current sqrt price from pool state",
                "MEXC live partial-depth best bid and ask",
            ],
            "excluded": [
                "CLMM swap fee, tick traversal and price impact",
                "CEX depth walk and CEX trading fee",
                "priority fee, tip, transaction inclusion and state change",
                "deposit, withdrawal, rebalance and wrapper-basis costs",
            ],
            "interpretation": "A trigger only authorizes an immediate exact quote check; it is not a trade signal.",
        },
        "files": {
            "observations": str(observations_path.resolve()),
            "exact_checks": str(exact_checks_path.resolve()) if exact_provider is not None else None,
            "manifest": str((output_directory / "manifest.json").resolve()),
        },
    }
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--interval-seconds", type=float, default=0.25)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--trigger-bps", type=Decimal, default=Decimal("30"))
    parser.add_argument("--cex-depth", type=int, choices=(5, 10, 20), default=20)
    parser.add_argument("--cex-stream-max-age-ms", type=Decimal, default=Decimal("500"))
    parser.add_argument("--solana-http-url", default=SOLANA_HTTP_ENDPOINT)
    parser.add_argument("--solana-ws-url", default=SOLANA_WS_ENDPOINT)
    parser.add_argument("--pump-sol-pool", default=PUMP_SOL_POOL_ID)
    parser.add_argument("--sol-usdt-pool", default=SOL_USDT_POOL_ID)
    parser.add_argument(
        "--exact-quote-notional",
        type=Decimal,
        help="Optional USDT size for an exact Raydium quote after a prefilter trigger",
    )
    parser.add_argument(
        "--exact-min-interval-seconds",
        type=float,
        default=5.0,
        help="Minimum delay between trigger-driven exact quote checks",
    )
    parser.add_argument(
        "--exact-raydium-min-request-interval-seconds",
        type=float,
        default=0.65,
        help="Shared spacing between Raydium requests made by the exact quote layer",
    )
    parser.add_argument(
        "--exact-max-response-skew-ms",
        type=Decimal,
        default=Decimal("300"),
    )
    parser.add_argument("--cex-taker-fee-bps", type=Decimal, default=Decimal("0"))
    parser.add_argument("--minimum-network-cost-quote", type=Decimal, default=Decimal("0.01"))
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/clmm-prefilter"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.duration_seconds <= 0 or args.interval_seconds <= 0 or args.timeout_seconds <= 0:
        raise SystemExit("duration, interval, and timeout must be positive")
    if args.trigger_bps < 0 or not args.trigger_bps.is_finite():
        raise SystemExit("--trigger-bps must be finite and non-negative")
    if args.cex_stream_max_age_ms < 0:
        raise SystemExit("--cex-stream-max-age-ms cannot be negative")
    if args.exact_quote_notional is not None and (
        args.exact_quote_notional <= 0 or not args.exact_quote_notional.is_finite()
    ):
        raise SystemExit("--exact-quote-notional must be finite and positive")
    if (
        args.exact_min_interval_seconds < 0
        or args.exact_raydium_min_request_interval_seconds < 0
    ):
        raise SystemExit("exact quote intervals cannot be negative")
    if args.exact_max_response_skew_ms < 0:
        raise SystemExit("--exact-max-response-skew-ms cannot be negative")
    if args.cex_taker_fee_bps < 0 or args.cex_taker_fee_bps >= 10_000:
        raise SystemExit("--cex-taker-fee-bps must be in [0, 10000)")
    if args.minimum_network_cost_quote < 0 or not args.minimum_network_cost_quote.is_finite():
        raise SystemExit("--minimum-network-cost-quote must be finite and non-negative")
    run_id = args.run_id or default_run_id("raydium-clmm-pump-prefilter")
    validate_run_id(run_id)
    manifest = asyncio.run(
        record_pump_prefilter(
            duration_seconds=args.duration_seconds,
            interval_seconds=args.interval_seconds,
            trigger_bps=args.trigger_bps,
            cex_depth=args.cex_depth,
            cex_stream_max_age_ms=args.cex_stream_max_age_ms,
            solana_http_url=args.solana_http_url,
            solana_ws_url=args.solana_ws_url,
            pump_sol_pool_id=args.pump_sol_pool,
            sol_usdt_pool_id=args.sol_usdt_pool,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            output_directory=args.output_root / run_id,
            exact_quote_notional=args.exact_quote_notional,
            exact_min_interval_seconds=args.exact_min_interval_seconds,
            exact_raydium_min_request_interval_seconds=(
                args.exact_raydium_min_request_interval_seconds
            ),
            exact_max_response_skew_ms=args.exact_max_response_skew_ms,
            cex_taker_fee_bps=args.cex_taker_fee_bps,
            minimum_network_cost_quote=args.minimum_network_cost_quote,
        ),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
