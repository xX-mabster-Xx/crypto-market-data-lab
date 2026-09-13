"""Normalized, bounded public perpetual-market feeds.

This module is deliberately a *data layer*, rather than a collection of
arbitrage strategies.  Every venue adapter publishes the same two event
types into :class:`~market_data_lab.realtime_scanner.RealtimeScanner`:

* ``perp_book``: an executable-side public order-book snapshot;
* ``perp_context``: mark/index/funding and contract metadata.

The scanner keeps the typed state and a short event window only in RAM.  Its
status file contains a compact latest-state projection for observability, not
a raw tick log.  Consumers can later compare any compatible pair of markets
(CEX↔DEX, perp↔perp, spot↔perp, or a multi-leg route) without opening another
venue connection.

The current adapters are Hyperliquid, Aevo, Bulk, dYdX, Drift, Extended,
Lighter, Paradex, and Aster.  Drift is read from public on-chain
DLOB/vAMM/oracle subscriptions through the local Solana worker because its
historical hosted DLOB endpoint is no longer live.  The deliberately
venue-neutral state/cache classes remain the extension point for further
venues.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
import ssl
import time
import urllib.parse
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect as websocket_connect

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import ScannerSource


HYPERLIQUID_PUBLIC_WS_ENDPOINT = "wss://api.hyperliquid.xyz/ws"
HYPERLIQUID_INFO_ENDPOINT = "https://api.hyperliquid.xyz/info"
HYPERLIQUID_FUNDING_INTERVAL_MINUTES = 60
AEVO_PUBLIC_WS_ENDPOINT = "wss://ws.aevo.xyz"
AEVO_MARKETS_ENDPOINT = "https://api.aevo.xyz/markets"
AEVO_FUNDING_ENDPOINT = "https://api.aevo.xyz/funding"
BULK_PUBLIC_WS_ENDPOINT = "wss://mainnet-ws1.bulk.trade"
BULK_EXCHANGE_INFO_ENDPOINT = "https://mainnet-api1.bulk.trade/api/v1/exchangeInfo"
BULK_FUNDING_INTERVAL_MINUTES = 60
DYDX_PUBLIC_WS_ENDPOINT = "wss://indexer.dydx.trade/v4/ws"
DYDX_PERPETUAL_MARKETS_ENDPOINT = "https://indexer.dydx.trade/v4/perpetualMarkets"
DYDX_FUNDING_INTERVAL_MINUTES = 60
LIGHTER_PUBLIC_WS_ENDPOINT = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
LIGHTER_PUBLIC_MARKETS_ENDPOINT = "https://explorer.elliot.ai/api/markets"
LIGHTER_FUNDING_INTERVAL_MINUTES = 60
# Lighter's public spot stream examples start at index 2048.  Its public
# catalogue uses lower indices for perpetual markets.
LIGHTER_SPOT_MARKET_INDEX_START = 2048
PARADEX_PUBLIC_WS_ENDPOINT = "wss://ws.api.prod.paradex.trade/v1"
PARADEX_PUBLIC_MARKETS_ENDPOINT = "https://api.prod.paradex.trade/v1/markets"
ASTER_PUBLIC_WS_ROOT = "wss://fstream.asterdex.com"
ASTER_PUBLIC_EXCHANGE_INFO_ENDPOINT = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"
EXTENDED_PUBLIC_MARKETS_ENDPOINT = "https://api.starknet.extended.exchange/api/v1/info/markets"
EXTENDED_PUBLIC_WS_ROOT = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
EXTENDED_FUNDING_INTERVAL_MINUTES = 60
# Extended's public endpoints were DNS-unreachable on the direct route during
# the live probe.  This fallback is deliberately local to that one adapter;
# it is never installed as a process-wide proxy and is used only after a
# direct network-reachability failure.
EXTENDED_LOCAL_FALLBACK_PROXY_URL = "socks5h://127.0.0.1:2060"
DRIFT_ONCHAIN_VENUE = "DRIFT"
SOLANA_QUOTE_WORKER_ROOT = (
    Path(__file__).resolve().parents[2] / "workers" / "solana-quote-worker"
)
_URL_PATTERN = re.compile(r"(?:https?|wss?)://[^\s\"']+")

# This is the current common liquid-ish universe between the already monitored
# CEX/Solana set and major public perp DEXes.  An adapter filters it against
# its own live exchange-info response; an absent symbol never blocks the rest.
DEFAULT_SHARED_PERP_BASES = (
    "BTC",
    "ETH",
    "SOL",
    "FARTCOIN",
    "JUP",
    "MEW",
    "PNUT",
    "POPCAT",
    "PUMP",
    "PYTH",
    "RENDER",
    "TRUMP",
    "CATI",
    "GRAM",
    "HMSTR",
    "NOT",
)
DEFAULT_HYPERLIQUID_COINS = DEFAULT_SHARED_PERP_BASES

Publish = Callable[[MarketEvent], Awaitable[None]]


def _optional_decimal(value: Any) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _display_decimal(value: Decimal | None) -> str | None:
    return format(value, "f") if value is not None else None


def _step_from_decimals(value: Any) -> Decimal | None:
    """Convert a venue's allowed size-decimal count into a quantity step."""

    decimals = _optional_int(value)
    if decimals is None or decimals < 0 or decimals > 18:
        return None
    return Decimal(1).scaleb(-decimals)


@dataclass(frozen=True)
class PerpContract:
    """Venue contract metadata required to normalize a future quote.

    A field remains ``None`` until a venue's public API confirms it.  We do
    not invent fees, quantity steps, or funding intervals just to force two
    venues into a comparison.
    """

    venue: str
    venue_symbol: str
    base: str
    settlement: str
    contract_type: str
    funding_interval_minutes: int | None
    tick_size: Decimal | None = None
    quantity_step: Decimal | None = None
    minimum_order_quantity: Decimal | None = None
    public_taker_fee_bps: Decimal | None = None
    fee_source: str | None = None
    # The book shape alone is not enough to say that a price is directly
    # executable.  RFQ venues may expose an indicative book, for example.
    execution_model: str | None = None


@dataclass(frozen=True)
class PerpContext:
    """Latest public funding/mark/index state for one normalized contract."""

    funding_rate: Decimal | None
    mark_price: Decimal | None
    index_price: Decimal | None
    open_interest: Decimal | None
    received_realtime_ns: int
    received_monotonic_ns: int
    exchange_time_ms: int | None = None
    # A future event timestamp is distinct from the exchange timestamp of the
    # context message.  It is optional because many public venues omit it.
    next_funding_time_ms: int | None = None
    funding_rate_kind: str | None = None
    # Most venues expose one signed rate.  Drift exposes estimated long and
    # short values separately, so preserve both rather than silently choosing
    # one in a future cross-venue evaluator.
    funding_rate_short: Decimal | None = None
    funding_period_seconds: int | None = None


@dataclass(frozen=True)
class PerpMarketState:
    """Latest typed state for one perp market; never serialized wholesale."""

    contract: PerpContract
    book: BookSnapshot | None = None
    context: PerpContext | None = None
    # E.g. a Drift top level can be backed by DLOB, vAMM, or both.  It is
    # execution-relevant and belongs to current bounded state, not a disk log.
    book_liquidity_sources: tuple[str, ...] = ()

    @property
    def key(self) -> str:
        return f"perp:{self.contract.venue}:{self.contract.venue_symbol}"

    def compact_summary(self) -> dict[str, Any]:
        """Small latest-state projection, intentionally excluding book depth."""

        book = self.book
        context = self.context
        return {
            "venue": self.contract.venue,
            "venue_symbol": self.contract.venue_symbol,
            "base": self.contract.base,
            "settlement": self.contract.settlement,
            "contract_type": self.contract.contract_type,
            "execution_model": self.contract.execution_model,
            "funding_interval_minutes": self.contract.funding_interval_minutes,
            "book_status": book.status if book is not None else "unavailable",
            "book_source": book.source if book is not None else None,
            "book_liquidity_sources": list(self.book_liquidity_sources),
            "book_level_count": (
                {"bids": len(book.bids), "asks": len(book.asks)} if book is not None else None
            ),
            "funding_available": context is not None and context.funding_rate is not None,
            "funding_rate_kind": context.funding_rate_kind if context is not None else None,
            "next_funding_time_ms": context.next_funding_time_ms if context is not None else None,
            "funding_short_available": context is not None and context.funding_rate_short is not None,
            "mark_available": context is not None and context.mark_price is not None,
            "index_available": context is not None and context.index_price is not None,
            "open_interest_available": context is not None and context.open_interest is not None,
            "fee_known": self.contract.public_taker_fee_bps is not None,
        }


@dataclass(frozen=True, slots=True)
class PerpQuoteEvent:
    """Small rolling-history record; full book depth stays only in latest state.

    A busy L2 feed can generate thousands of updates per second.  Retaining a
    full 20×20 :class:`BookSnapshot` for every one of those updates would make
    a nominally bounded scanner consume unboundedly impractical RAM.  The
    cache therefore retains one current full-depth book per market, while the
    time window contains only top-of-book/context needed to identify a
    synchronized candidate.  An evaluator asks the source cache for current
    executable depth only after such a candidate appears.
    """

    venue: str
    venue_symbol: str
    base: str
    settlement: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    funding_rate: Decimal | None
    mark_price: Decimal | None
    index_price: Decimal | None
    received_realtime_ns: int
    received_monotonic_ns: int
    funding_rate_short: Decimal | None = None
    funding_period_seconds: int | None = None
    book_liquidity_sources: tuple[str, ...] = ()
    # These fields make a later strategy consumer able to validate a small
    # hedge without retaining or serialising the full L2 snapshot.  They are
    # deliberately optional because some RFQ-style public feeds do not expose
    # executable visible quantity or a complete contract specification.
    best_bid_size: Decimal | None = None
    best_ask_size: Decimal | None = None
    book_received_realtime_ns: int | None = None
    book_received_monotonic_ns: int | None = None
    context_received_realtime_ns: int | None = None
    # Local monotonic receipt time is carried alongside UTC evidence time so
    # consumers can reject stale context without trusting wall-clock jumps.
    context_received_monotonic_ns: int | None = None
    next_funding_time_ms: int | None = None
    funding_rate_kind: str | None = None
    funding_interval_minutes: int | None = None
    quantity_step: Decimal | None = None
    minimum_order_quantity: Decimal | None = None
    public_taker_fee_bps: Decimal | None = None
    fee_source: str | None = None
    # Keep the typed contract classification with the compact quote.  A
    # downstream evaluator must reject non-linear contracts rather than reuse
    # base-quantity arithmetic for an inverse or quanto instrument.
    contract_type: str | None = None
    execution_model: str | None = None

    @classmethod
    def from_state(
        cls,
        state: PerpMarketState,
        *,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> "PerpQuoteEvent":
        return cls(
            venue=state.contract.venue,
            venue_symbol=state.contract.venue_symbol,
            base=state.contract.base,
            settlement=state.contract.settlement,
            best_bid=state.book.bids[0][0] if state.book is not None and state.book.bids else None,
            best_ask=state.book.asks[0][0] if state.book is not None and state.book.asks else None,
            funding_rate=state.context.funding_rate if state.context is not None else None,
            mark_price=state.context.mark_price if state.context is not None else None,
            index_price=state.context.index_price if state.context is not None else None,
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            funding_rate_short=(
                state.context.funding_rate_short if state.context is not None else None
            ),
            funding_period_seconds=(
                state.context.funding_period_seconds if state.context is not None else None
            ),
            book_liquidity_sources=state.book_liquidity_sources,
            best_bid_size=(
                state.book.bids[0][1] if state.book is not None and state.book.bids else None
            ),
            best_ask_size=(
                state.book.asks[0][1] if state.book is not None and state.book.asks else None
            ),
            book_received_realtime_ns=(
                state.book.response.received_realtime_ns if state.book is not None else None
            ),
            book_received_monotonic_ns=(
                state.book.response.received_monotonic_ns if state.book is not None else None
            ),
            context_received_realtime_ns=(
                state.context.received_realtime_ns if state.context is not None else None
            ),
            context_received_monotonic_ns=(
                state.context.received_monotonic_ns if state.context is not None else None
            ),
            next_funding_time_ms=(
                state.context.next_funding_time_ms if state.context is not None else None
            ),
            funding_rate_kind=(
                state.context.funding_rate_kind if state.context is not None else None
            ),
            funding_interval_minutes=state.contract.funding_interval_minutes,
            quantity_step=state.contract.quantity_step,
            minimum_order_quantity=state.contract.minimum_order_quantity,
            public_taker_fee_bps=state.contract.public_taker_fee_bps,
            fee_source=state.contract.fee_source,
            contract_type=state.contract.contract_type,
            execution_model=state.contract.execution_model,
        )


class PerpMarketCache:
    """Small shared normalizer used by every public perp venue adapter.

    This is not persistent storage: it owns only the latest typed state per
    configured market.  The surrounding :class:`RealtimeScanner` retains the
    bounded 1–3 minute event window.
    """

    def __init__(self, contracts: Sequence[PerpContract] = ()) -> None:
        self._states = {
            contract.venue_symbol.upper(): PerpMarketState(contract=contract)
            for contract in contracts
        }
        if len(self._states) != len(contracts):
            raise ValueError("perp venue symbols must be unique")

    def register(self, contract: PerpContract) -> PerpMarketState:
        """Add a verified contract discovered from a venue's public universe."""

        key = contract.venue_symbol.upper()
        state = self._states.get(key)
        if state is None:
            state = PerpMarketState(contract=contract)
            self._states[key] = state
            return state
        updated = replace(state, contract=contract)
        self._states[key] = updated
        return updated

    def state(self, venue_symbol: str) -> PerpMarketState | None:
        return self._states.get(venue_symbol.upper())

    def update_book(
        self,
        venue_symbol: str,
        book: BookSnapshot,
        *,
        liquidity_sources: Sequence[str] = (),
    ) -> PerpMarketState | None:
        state = self.state(venue_symbol)
        if state is None:
            return None
        updated = replace(
            state,
            book=book,
            book_liquidity_sources=tuple(dict.fromkeys(liquidity_sources)),
        )
        self._states[venue_symbol.upper()] = updated
        return updated

    def update_context(self, venue_symbol: str, context: PerpContext) -> PerpMarketState | None:
        state = self.state(venue_symbol)
        if state is None:
            return None
        updated = replace(state, context=context)
        self._states[venue_symbol.upper()] = updated
        return updated

    def update_contract(self, contract: PerpContract) -> PerpMarketState | None:
        state = self.state(contract.venue_symbol)
        if state is None:
            return None
        updated = replace(state, contract=contract)
        self._states[contract.venue_symbol.upper()] = updated
        return updated

    def states(self) -> tuple[PerpMarketState, ...]:
        return tuple(self._states[key] for key in sorted(self._states))


def _timed_push_response(received_realtime_ns: int, received_monotonic_ns: int) -> TimedResponse:
    return TimedResponse(
        payload=None,
        error=None,
        sent_realtime_ns=received_realtime_ns,
        received_realtime_ns=received_realtime_ns,
        sent_monotonic_ns=received_monotonic_ns,
        received_monotonic_ns=received_monotonic_ns,
    )


def _parse_hyperliquid_levels(rows: Any, *, reverse: bool, depth: int) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(rows, list):
        raise ValueError("Hyperliquid book side is not a list")
    parsed: dict[Decimal, Decimal] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("Hyperliquid book level is not an object")
        price = _optional_decimal(row.get("px"))
        size = _optional_decimal(row.get("sz"))
        if price is None or size is None or price <= 0 or size <= 0:
            continue
        parsed[price] = size
    result = tuple(sorted(parsed.items(), key=lambda item: item[0], reverse=reverse)[:depth])
    if not result:
        raise ValueError("Hyperliquid book side is empty")
    return result


def parse_hyperliquid_l2_book(payload: Mapping[str, Any], *, depth: int = 20) -> BookSnapshot | None:
    """Convert a public ``l2Book`` push message into the common book type."""

    if payload.get("channel") != "l2Book":
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    coin = data.get("coin")
    levels = data.get("levels")
    if not isinstance(coin, str) or not isinstance(levels, list) or len(levels) != 2:
        return None
    received_realtime_ns = time.time_ns()
    received_monotonic_ns = time.monotonic_ns()
    try:
        bids = _parse_hyperliquid_levels(levels[0], reverse=True, depth=depth)
        asks = _parse_hyperliquid_levels(levels[1], reverse=False, depth=depth)
        if bids[0][0] >= asks[0][0]:
            return None
    except ValueError:
        return None
    return BookSnapshot(
        symbol=coin.upper(),
        category="linear",
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=_optional_int(data.get("time")),
        matching_engine_time_ms=None,
        update_id=None,
        cross_sequence=None,
        response=_timed_push_response(received_realtime_ns, received_monotonic_ns),
        source="hyperliquid_websocket_l2_book",
    )


def parse_hyperliquid_active_asset_context(payload: Mapping[str, Any]) -> tuple[str, PerpContext] | None:
    """Parse a public ``activeAssetCtx`` push without assuming all fields exist."""

    if payload.get("channel") != "activeAssetCtx":
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping):
        return None
    coin = data.get("coin")
    context = data.get("ctx")
    if not isinstance(coin, str) or not isinstance(context, Mapping):
        return None
    received_realtime_ns = time.time_ns()
    received_monotonic_ns = time.monotonic_ns()
    return (
        coin.upper(),
        PerpContext(
            funding_rate=_optional_decimal(context.get("funding")),
            mark_price=_optional_decimal(context.get("markPx")),
            index_price=_optional_decimal(context.get("oraclePx")),
            open_interest=_optional_decimal(context.get("openInterest")),
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            exchange_time_ms=_optional_int(data.get("time")),
            funding_rate_kind="current_hourly_rate",
        ),
    )


def parse_hyperliquid_meta_and_contexts(payload: Any, *, coins: Sequence[str]) -> dict[str, PerpContract]:
    """Parse the documented public ``metaAndAssetCtxs`` response.

    It verifies that requested coins are active perpetuals before a socket is
    opened.  Quantity/tick metadata is filled only when supplied by the
    public response; no execution assumption is made here.
    """

    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("Hyperliquid metaAndAssetCtxs response must be a pair")
    meta = payload[0]
    if not isinstance(meta, Mapping):
        raise ValueError("Hyperliquid metadata is malformed")
    universe = meta.get("universe")
    if not isinstance(universe, list):
        raise ValueError("Hyperliquid metadata has no universe")
    requested = {coin.upper() for coin in coins}
    contracts: dict[str, PerpContract] = {}
    for row in universe:
        if not isinstance(row, Mapping):
            continue
        name = row.get("name")
        if not isinstance(name, str) or name.upper() not in requested:
            continue
        contracts[name.upper()] = PerpContract(
            venue="HYPERLIQUID",
            venue_symbol=name.upper(),
            base=name.upper(),
            settlement="USDC",
            contract_type="linear_perpetual",
            funding_interval_minutes=HYPERLIQUID_FUNDING_INTERVAL_MINUTES,
            # ``szDecimals`` is a size precision, not a price tick.  Keep
            # price tick unknown until a documented source supplies it.
            tick_size=None,
            quantity_step=_step_from_decimals(row.get("szDecimals")),
            minimum_order_quantity=None,
            public_taker_fee_bps=None,
            fee_source=None,
        )
    return contracts


async def _fetch_json_post(
    *,
    url: str,
    payload: Mapping[str, Any],
    proxy_url: str | None,
    timeout_seconds: float,
) -> Any:
    """Use the project’s existing read-only JSON transport in a thread."""

    from market_data_lab.dex_quotes import _fetch_json_sync

    return await asyncio.to_thread(
        _fetch_json_sync,
        url,
        "POST",
        json.dumps(payload, separators=(",", ":")).encode(),
        {"Content-Type": "application/json"},
        proxy_url,
        timeout_seconds,
    )


async def _fetch_json_get(
    *,
    url: str,
    query: Mapping[str, str] | None,
    proxy_url: str | None,
    timeout_seconds: float,
) -> Any:
    """Read one public JSON endpoint through the project's existing transport."""

    from market_data_lab.dex_quotes import _fetch_json_sync

    full_url = url
    if query:
        full_url = f"{url}?{urllib.parse.urlencode(query)}"
    return await asyncio.to_thread(
        _fetch_json_sync,
        full_url,
        "GET",
        None,
        {},
        proxy_url,
        timeout_seconds,
    )


def _is_network_reachability_error(error: BaseException) -> bool:
    """Whether a direct transport failure is eligible for one SOCKS retry.

    Authentication, malformed requests, and HTTP errors must *not* silently
    change network route.  This narrow predicate mirrors the scanner's
    direct-first network policy.
    """

    text = str(error).lower()
    return any(
        marker in text
        for marker in (
            "temporary failure in name resolution",
            "name or service not known",
            "nodename nor servname provided",
            "connection reset",
            "connection refused",
            "network is unreachable",
            "timed out",
            "errno -3",
        )
    )


async def _fetch_json_get_via_socks(
    *,
    url: str,
    proxy_url: str,
    timeout_seconds: float,
) -> Any:
    """Fetch one public JSON document through an explicit SOCKS proxy.

    The standard-library HTTP client supports HTTP proxies but not SOCKS.  A
    tiny transport here keeps that capability scoped to the one public source
    that needs its direct-route fallback; it discards the response as soon as
    JSON has been normalized by the caller.
    """

    from python_socks.async_.asyncio import Proxy

    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("SOCKS JSON fetch requires an absolute HTTP(S) URL")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    target = urllib.parse.urlunparse(("", "", parsed.path or "/", parsed.params, parsed.query, ""))
    # python-socks uses SOCKS5 remote hostname resolution.  ``socks5h`` is a
    # familiar URI spelling for users, so normalize it before handing it on.
    normalized_proxy_url = (
        f"socks5://{proxy_url.removeprefix('socks5h://')}"
        if proxy_url.startswith("socks5h://")
        else proxy_url
    )
    socket: Any = None
    writer: asyncio.StreamWriter | None = None
    try:
        async with asyncio.timeout(timeout_seconds):
            socket = await Proxy.from_url(normalized_proxy_url).connect(
                dest_host=parsed.hostname,
                dest_port=port,
            )
            reader, writer = await asyncio.open_connection(
                sock=socket,
                ssl=ssl.create_default_context() if parsed.scheme == "https" else None,
                server_hostname=parsed.hostname if parsed.scheme == "https" else None,
            )
            host_header = parsed.hostname
            if parsed.port is not None and parsed.port not in {80, 443}:
                host_header = f"{host_header}:{parsed.port}"
            request = (
                f"GET {target} HTTP/1.1\r\n"
                f"Host: {host_header}\r\n"
                "User-Agent: crypto-market-data-lab/0.1\r\n"
                "Accept: application/json\r\n"
                "Accept-Encoding: identity\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            writer.write(request)
            await writer.drain()
            status_line = await reader.readline()
            status_parts = status_line.decode("iso-8859-1", errors="replace").split()
            if len(status_parts) < 2 or not status_parts[1].isdigit():
                raise RuntimeError("SOCKS JSON fetch received an invalid HTTP status line")
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in {b"\r\n", b"\n", b""}:
                    break
                name, separator, value = line.decode("iso-8859-1", errors="replace").partition(":")
                if separator:
                    headers[name.strip().lower()] = value.strip()
            status_code = int(status_parts[1])
            if headers.get("transfer-encoding", "").lower() == "chunked":
                chunks: list[bytes] = []
                while True:
                    size_line = await reader.readline()
                    size_text = size_line.split(b";", 1)[0].strip()
                    try:
                        chunk_size = int(size_text, 16)
                    except ValueError as exc:
                        raise RuntimeError("SOCKS JSON fetch received an invalid chunk size") from exc
                    if chunk_size == 0:
                        while await reader.readline() not in {b"\r\n", b"\n", b""}:
                            pass
                        body = b"".join(chunks)
                        break
                    chunks.append(await reader.readexactly(chunk_size))
                    if await reader.readexactly(2) != b"\r\n":
                        raise RuntimeError("SOCKS JSON fetch received an invalid chunk terminator")
            elif (content_length := headers.get("content-length")) is not None:
                body = await reader.readexactly(int(content_length))
            else:
                body = await reader.read()
            if status_code < 200 or status_code >= 300:
                raise RuntimeError(f"SOCKS JSON fetch returned HTTP {status_code}")
            return json.loads(body.decode("utf-8"))
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        elif socket is not None:
            with contextlib.suppress(Exception):
                socket.close()


def _exchange_time_ms(value: Any) -> int | None:
    """Normalize documented ms or ns timestamps to the common ms field."""

    parsed = _optional_int(value)
    if parsed is None:
        return None
    return parsed // 1_000_000 if abs(parsed) >= 10**15 else parsed


def _book_from_level_maps(
    *,
    symbol: str,
    bids: Mapping[Decimal, Decimal],
    asks: Mapping[Decimal, Decimal],
    depth: int,
    source: str,
    exchange_time: Any,
) -> BookSnapshot | None:
    parsed_bids = tuple(
        (price, size)
        for price, size in sorted(bids.items(), key=lambda item: item[0], reverse=True)
        if price > 0 and size > 0
    )[:depth]
    parsed_asks = tuple(
        (price, size)
        for price, size in sorted(asks.items(), key=lambda item: item[0])
        if price > 0 and size > 0
    )[:depth]
    if not parsed_bids or not parsed_asks or parsed_bids[0][0] >= parsed_asks[0][0]:
        return None
    received_realtime_ns = time.time_ns()
    received_monotonic_ns = time.monotonic_ns()
    return BookSnapshot(
        symbol=symbol.upper(),
        category="linear",
        status="ok",
        error=None,
        bids=parsed_bids,
        asks=parsed_asks,
        exchange_system_time_ms=_exchange_time_ms(exchange_time),
        matching_engine_time_ms=None,
        update_id=None,
        cross_sequence=None,
        response=_timed_push_response(received_realtime_ns, received_monotonic_ns),
        source=source,
    )


def _redact_urls(value: str) -> str:
    """Keep provider credentials out of child-process diagnostics."""

    def replace_url(match: re.Match[str]) -> str:
        candidate = match.group(0)
        parsed = urllib.parse.urlparse(candidate)
        return f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else "[redacted-url]"

    return _URL_PATTERN.sub(replace_url, value)


class DriftOnchainPerpSource:
    """Read Drift DLOB/vAMM L2 locally through the configured Solana RPC.

    Drift's former hosted DLOB endpoint is gone.  Rather than substitute an
    unrelated venue, a small local TypeScript worker uses the official SDK to
    subscribe to the public Drift market/oracle accounts and public user-order
    program accounts.  It reconstructs the DLOB and adds the protocol vAMM
    fallback before publishing normalized quotes here.

    The child never receives a wallet, private key, or transaction command;
    it receives only managed RPC URLs through stdin.  Full L2 state stays in
    its current-memory cache, then this source forwards a bounded normalized
    event stream to ``RealtimeScanner``.
    """

    name = "perp:drift"

    def __init__(
        self,
        *,
        rpc_http_url: str,
        rpc_ws_url: str,
        bases: Sequence[str] = DEFAULT_SHARED_PERP_BASES,
        depth: int = 20,
        update_frequency_ms: int = 400,
        worker_root: Path = SOLANA_QUOTE_WORKER_ROOT,
    ) -> None:
        normalized = tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
        if not normalized:
            raise ValueError("at least one Drift base is required")
        if depth <= 0 or depth > 50 or not 200 <= update_frequency_ms <= 10_000:
            raise ValueError("Drift depth and update frequency are out of range")
        self.rpc_http_url = rpc_http_url
        self.rpc_ws_url = rpc_ws_url
        self.bases = normalized
        self.depth = depth
        self.update_frequency_ms = update_frequency_ms
        self.worker_root = worker_root
        self.cache = PerpMarketCache()
        self._process: asyncio.subprocess.Process | None = None
        self._events: asyncio.Queue[dict[str, Any] | BaseException] = asyncio.Queue(maxsize=2_048)
        self._ready: asyncio.Future[dict[str, Any]] | None = None
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail: list[str] = []
        self._closed = False
        self._worker_ready: dict[str, Any] | None = None
        self._errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._contracts = 0
        self._unusable_books = 0
        self._queue_drops = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": DRIFT_ONCHAIN_VENUE,
            "transport": "local_official_typescript_sdk_onchain_DLOB_vAMM_oracle_websocket",
            "requested_bases": list(self.bases),
            "depth": self.depth,
            "DLOB_hosted_endpoint_required": False,
            "wallet_or_private_key_used": False,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        ready = self._worker_ready or {}
        return {
            "worker_running": self._process is not None and self._process.returncode is None,
            "worker_ready": self._worker_ready is not None,
            "configured_markets": ready.get("configured_markets"),
            "unavailable_requested_bases": ready.get("unavailable_requested_bases"),
            "initial_open_order_accounts": ready.get("initial_open_order_accounts"),
            "update_frequency_ms": ready.get("update_frequency_ms", self.update_frequency_ms),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "contracts": self._contracts,
            "unusable_books": self._unusable_books,
            "queue_drops": self._queue_drops,
            "recent_errors": list(self._errors[-8:]),
            "markets": {
                state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()
            },
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }

    async def _send(self, payload: Mapping[str, object]) -> None:
        process = self._process
        if process is None or process.stdin is None or process.returncode is not None:
            raise RuntimeError("Drift worker stdin is unavailable")
        process.stdin.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        await process.stdin.drain()

    def _enqueue(self, item: dict[str, Any] | BaseException) -> None:
        if self._events.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._events.get_nowait()
                self._queue_drops += 1
        with contextlib.suppress(asyncio.QueueFull):
            self._events.put_nowait(item)

    def _fail(self, error: BaseException) -> None:
        detail = _redact_urls(str(error))[:1_024]
        self._errors.append(detail)
        if self._ready is not None and not self._ready.done():
            self._ready.set_exception(RuntimeError(detail))
        self._enqueue(RuntimeError(detail))

    async def _read_stdout(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            while line := await process.stdout.readline():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    self._fail(RuntimeError(f"Drift worker emitted invalid JSON: {exc}"))
                    return
                if not isinstance(message, dict):
                    self._fail(RuntimeError("Drift worker emitted a non-object JSON message"))
                    return
                kind = message.get("type")
                if kind == "ready":
                    self._worker_ready = message
                    if self._ready is not None and not self._ready.done():
                        self._ready.set_result(message)
                elif kind == "drift_perp_error":
                    detail = _redact_urls(str(message.get("error", "unknown Drift worker error")))[:2_048]
                    self._errors.append(detail)
                elif kind != "stopped":
                    self._enqueue(message)
            if not self._closed:
                return_code = await process.wait()
                suffix = f"; stderr: {' | '.join(self._stderr_tail[-8:])}" if self._stderr_tail else ""
                self._fail(RuntimeError(f"Drift worker exited with code {return_code}{suffix}"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(RuntimeError(f"Drift worker stdout reader failed: {_redact_urls(str(exc))}"))

    async def _read_stderr(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        try:
            while line := await process.stderr.readline():
                text = _redact_urls(line.decode("utf-8", errors="replace").strip())
                if text:
                    self._stderr_tail.append(text[:512])
                    del self._stderr_tail[:-20]
        except asyncio.CancelledError:
            raise

    async def _start(self) -> None:
        if self._process is not None:
            raise RuntimeError("Drift worker is already started")
        executable = self.worker_root / "node_modules" / ".bin" / "tsx"
        if not executable.is_file():
            raise RuntimeError(f"Drift worker executable is missing at {executable}")
        self._closed = False
        self._events = asyncio.Queue(maxsize=2_048)
        self._stderr_tail = []
        self._worker_ready = None
        self._process = await asyncio.create_subprocess_exec(
            str(executable),
            "src/driftPerp.ts",
            cwd=str(self.worker_root),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self._ready = asyncio.get_running_loop().create_future()
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        await self._send(
            {
                "type": "configure",
                "rpc_http_url": self.rpc_http_url,
                "rpc_ws_url": self.rpc_ws_url,
                "bases": list(self.bases),
                "depth": self.depth,
                "update_frequency_ms": self.update_frequency_ms,
            },
        )
        try:
            await asyncio.wait_for(self._ready, timeout=90.0)
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self._closed:
            return
        process = self._process
        if process is not None and process.returncode is None:
            with contextlib.suppress(Exception):
                await self._send({"type": "shutdown"})
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except TimeoutError:
                process.terminate()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(process.wait(), timeout=3.0)
        self._closed = True
        for task in (self._stdout_task, self._stderr_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._stdout_task, self._stderr_task) if task is not None),
            return_exceptions=True,
        )
        self._process = None
        self._stdout_task = None
        self._stderr_task = None

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
        chain_position: int | None,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
                chain_position=chain_position,
            ),
        )

    @staticmethod
    def _levels(raw: Any, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...] | None:
        if not isinstance(raw, list):
            return None
        parsed: dict[Decimal, Decimal] = {}
        for row in raw:
            if not isinstance(row, Mapping):
                return None
            price = _optional_decimal(row.get("price"))
            size = _optional_decimal(row.get("size"))
            if price is None or size is None or price <= 0 or size <= 0:
                continue
            parsed[price] = size
        result = tuple(sorted(parsed.items(), key=lambda item: item[0], reverse=reverse))
        return result or None

    @staticmethod
    def _liquidity_sources(raw_bids: Any, raw_asks: Any) -> tuple[str, ...]:
        sources: list[str] = []
        for side in (raw_bids, raw_asks):
            if not isinstance(side, list):
                continue
            for row in side:
                values = row.get("liquidity_sources") if isinstance(row, Mapping) else None
                if isinstance(values, list):
                    sources.extend(value for value in values if isinstance(value, str) and value)
        return tuple(sorted(set(sources)))

    async def _handle_contract(self, message: Mapping[str, Any], publish: Publish) -> None:
        symbol = message.get("venue_symbol")
        base = message.get("base")
        settlement = message.get("settlement")
        period_seconds = _optional_int(message.get("funding_period_seconds"))
        if (
            not isinstance(symbol, str)
            or not isinstance(base, str)
            or not isinstance(settlement, str)
            or period_seconds is None
            or period_seconds <= 0
        ):
            self._errors.append("malformed Drift contract message")
            return
        contract = PerpContract(
            venue=DRIFT_ONCHAIN_VENUE,
            venue_symbol=symbol.upper(),
            base=base.upper(),
            settlement=settlement.upper(),
            contract_type="linear_perpetual",
            funding_interval_minutes=max(1, period_seconds // 60),
            tick_size=_optional_decimal(message.get("tick_size")),
            quantity_step=_optional_decimal(message.get("quantity_step")),
            minimum_order_quantity=None,
            public_taker_fee_bps=None,
            fee_source=None,
        )
        state = self.cache.register(contract)
        now_realtime_ns = time.time_ns()
        await self._publish_state(
            publish,
            state=state,
            kind="perp_contract",
            received_realtime_ns=now_realtime_ns,
            received_monotonic_ns=time.monotonic_ns(),
            chain_position=_optional_int(message.get("market_index")),
        )
        self._contracts += 1

    async def _handle_update(self, message: Mapping[str, Any], publish: Publish) -> None:
        symbol = message.get("venue_symbol")
        if not isinstance(symbol, str):
            self._errors.append("malformed Drift quote without venue_symbol")
            return
        current = self.cache.state(symbol)
        if current is None:
            self._errors.append(f"Drift quote arrived before its contract: {symbol}"[:256])
            return
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        raw_bids = message.get("bids")
        raw_asks = message.get("asks")
        state = current
        bids = self._levels(raw_bids, reverse=True)
        asks = self._levels(raw_asks, reverse=False)
        if (
            message.get("book_status") == "ok"
            and bids is not None
            and asks is not None
            and bids[0][0] < asks[0][0]
        ):
            book = BookSnapshot(
                symbol=symbol.upper(),
                category="linear",
                status="ok",
                error=None,
                bids=bids[: self.depth],
                asks=asks[: self.depth],
                exchange_system_time_ms=None,
                matching_engine_time_ms=None,
                update_id=_optional_int(message.get("slot")),
                cross_sequence=_optional_int(message.get("slot")),
                response=_timed_push_response(now_realtime_ns, now_monotonic_ns),
                source="drift_local_onchain_dlob_plus_vamm",
            )
            state = self.cache.update_book(
                symbol,
                book,
                liquidity_sources=self._liquidity_sources(raw_bids, raw_asks),
            ) or state
            self._book_updates += 1
        else:
            self._unusable_books += 1
        period_seconds = _optional_int(message.get("funding_period_seconds"))
        context = PerpContext(
            funding_rate=_optional_decimal(message.get("funding_rate_long")),
            funding_rate_short=_optional_decimal(message.get("funding_rate_short")),
            mark_price=_optional_decimal(message.get("mark_price")),
            index_price=_optional_decimal(message.get("index_price")),
            open_interest=None,
            received_realtime_ns=now_realtime_ns,
            received_monotonic_ns=now_monotonic_ns,
            exchange_time_ms=None,
            funding_rate_kind=(
                message.get("funding_rate_kind")
                if isinstance(message.get("funding_rate_kind"), str)
                else None
            ),
            funding_period_seconds=period_seconds if period_seconds is not None and period_seconds > 0 else None,
        )
        state = self.cache.update_context(symbol, context) or state
        await self._publish_state(
            publish,
            state=state,
            kind="perp_quote",
            received_realtime_ns=now_realtime_ns,
            received_monotonic_ns=now_monotonic_ns,
            chain_position=_optional_int(message.get("slot")),
        )
        self._context_updates += 1

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._start()
        try:
            while not stop_event.is_set():
                try:
                    item = await asyncio.wait_for(self._events.get(), timeout=15.0)
                except TimeoutError:
                    process = self._process
                    if process is None or process.returncode is not None:
                        raise RuntimeError("Drift worker stopped without a final event")
                    continue
                if isinstance(item, BaseException):
                    raise item
                kind = item.get("type")
                if kind == "drift_perp_contract":
                    await self._handle_contract(item, publish)
                elif kind == "drift_perp_update":
                    await self._handle_update(item, publish)
                else:
                    self._errors.append(f"unknown Drift worker message {kind!r}"[:256])
        finally:
            await self.close()


class HyperliquidPerpSource:
    """Public Hyperliquid L2 + funding adapter for the shared data bus."""

    name = "perp:hyperliquid"

    def __init__(
        self,
        coins: Sequence[str] = DEFAULT_HYPERLIQUID_COINS,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        context_refresh_seconds: float = 30.0,
        depth: int = 20,
        websocket_endpoint: str = HYPERLIQUID_PUBLIC_WS_ENDPOINT,
        info_endpoint: str = HYPERLIQUID_INFO_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        normalized = tuple(dict.fromkeys(coin.upper() for coin in coins if coin.strip()))
        if not normalized:
            raise ValueError("at least one Hyperliquid coin is required")
        if timeout_seconds <= 0 or context_refresh_seconds <= 0 or depth <= 0:
            raise ValueError("Hyperliquid source timings and depth must be positive")
        self.coins = normalized
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.context_refresh_seconds = context_refresh_seconds
        self.depth = depth
        self.websocket_endpoint = websocket_endpoint
        self.info_endpoint = info_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache(
            [
                PerpContract(
                    venue="HYPERLIQUID",
                    venue_symbol=coin,
                    base=coin,
                    settlement="USDC",
                    contract_type="linear_perpetual",
                    funding_interval_minutes=HYPERLIQUID_FUNDING_INTERVAL_MINUTES,
                )
                for coin in normalized
            ],
        )
        self._metadata_refreshes = 0
        self._metadata_errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "HYPERLIQUID",
            "transport": "public_websocket_l2Book_and_activeAssetCtx",
            "markets": list(self.coins),
            "depth": self.depth,
            "funding_context": True,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "metadata_refreshes": self._metadata_refreshes,
            "metadata_errors": list(self._metadata_errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _refresh_metadata(self, publish: Publish) -> None:
        try:
            payload = await _fetch_json_post(
                url=self.info_endpoint,
                payload={"type": "metaAndAssetCtxs"},
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            parsed = parse_hyperliquid_meta_and_contexts(payload, coins=self.coins)
            if not parsed:
                raise RuntimeError("none of the configured Hyperliquid markets are active")
            for contract in parsed.values():
                state = self.cache.update_contract(contract)
                if state is not None:
                    now_realtime_ns = time.time_ns()
                    await self._publish_state(
                        publish,
                        state=state,
                        kind="perp_contract",
                        received_realtime_ns=now_realtime_ns,
                        received_monotonic_ns=time.monotonic_ns(),
                    )
            self._metadata_refreshes += 1
        except Exception as exc:
            self._metadata_errors.append(f"{type(exc).__name__}: {exc}"[:512])

    async def _metadata_loop(self, publish: Publish, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self._refresh_metadata(publish)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.context_refresh_seconds)
            except TimeoutError:
                pass

    async def _subscribe(self, websocket: Any) -> None:
        for coin in self.coins:
            await websocket.send(
                json.dumps(
                    {"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}},
                    separators=(",", ":"),
                ),
            )
            await websocket.send(
                json.dumps(
                    {"method": "subscribe", "subscription": {"type": "activeAssetCtx", "coin": coin}},
                    separators=(",", ":"),
                ),
            )

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        metadata_task = asyncio.create_task(self._metadata_loop(publish, stop_event))
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Hyperliquid websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                book = parse_hyperliquid_l2_book(payload, depth=self.depth)
                if book is not None:
                    state = self.cache.update_book(book.symbol, book)
                    if state is not None:
                        self._book_updates += 1
                        await self._publish_state(
                            publish,
                            state=state,
                            kind="perp_book",
                            received_realtime_ns=book.response.received_realtime_ns,
                            received_monotonic_ns=book.response.received_monotonic_ns,
                        )
                    continue
                context = parse_hyperliquid_active_asset_context(payload)
                if context is not None:
                    coin, value = context
                    state = self.cache.update_context(coin, value)
                    if state is not None:
                        self._context_updates += 1
                        await self._publish_state(
                            publish,
                            state=state,
                            kind="perp_context",
                            received_realtime_ns=value.received_realtime_ns,
                            received_monotonic_ns=value.received_monotonic_ns,
                        )
                    continue
                # Subscription acknowledgements and heartbeats are normal.
        finally:
            metadata_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await metadata_task
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_aevo_perp_markets(
    payload: Any,
    *,
    bases: Sequence[str] | None,
) -> dict[str, tuple[PerpContract, PerpContext]]:
    """Normalize active Aevo perpetual contracts from public ``GET /markets``."""

    if not isinstance(payload, list):
        raise ValueError("Aevo markets response must be a list")
    selected_bases = {base.upper() for base in bases} if bases is not None else None
    now_realtime_ns = time.time_ns()
    now_monotonic_ns = time.monotonic_ns()
    result: dict[str, tuple[PerpContract, PerpContext]] = {}
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        name = row.get("instrument_name")
        base = row.get("underlying_asset")
        quote = row.get("quote_asset")
        if (
            not isinstance(name, str)
            or not isinstance(base, str)
            or not isinstance(quote, str)
            or row.get("instrument_type") != "PERPETUAL"
            or row.get("is_active") is not True
            or (selected_bases is not None and base.upper() not in selected_bases)
        ):
            continue
        result[name.upper()] = (
            PerpContract(
                venue="AEVO",
                venue_symbol=name.upper(),
                base=base.upper(),
                settlement=quote.upper(),
                contract_type="linear_perpetual",
                # Aevo's REST response exposes the current rate but not a
                # documented interval in this endpoint, so leave it unknown.
                funding_interval_minutes=None,
                tick_size=_optional_decimal(row.get("price_step")),
                quantity_step=_optional_decimal(row.get("amount_step")),
                minimum_order_quantity=None,
                public_taker_fee_bps=None,
                fee_source=None,
            ),
            PerpContext(
                funding_rate=None,
                mark_price=_optional_decimal(row.get("mark_price")),
                index_price=_optional_decimal(row.get("index_price")),
                open_interest=None,
                received_realtime_ns=now_realtime_ns,
                received_monotonic_ns=now_monotonic_ns,
            ),
        )
    return result


def _parse_aevo_level_changes(rows: Any) -> dict[Decimal, Decimal]:
    if not isinstance(rows, list):
        raise ValueError("Aevo orderbook side is not a list")
    result: dict[Decimal, Decimal] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            raise ValueError("Aevo orderbook level is malformed")
        price = _optional_decimal(row[0])
        size = _optional_decimal(row[1])
        if price is None or size is None or price <= 0 or size < 0:
            raise ValueError("Aevo orderbook level has invalid price or size")
        result[price] = size
    return result


class AevoPerpSource:
    """Public Aevo perpetual L2 (100 ms) plus periodic public funding context."""

    name = "perp:aevo"

    def __init__(
        self,
        bases: Sequence[str] | None = ("BTC", "ETH"),
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        funding_refresh_seconds: float = 30.0,
        depth: int = 20,
        websocket_endpoint: str = AEVO_PUBLIC_WS_ENDPOINT,
        markets_endpoint: str = AEVO_MARKETS_ENDPOINT,
        funding_endpoint: str = AEVO_FUNDING_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        if timeout_seconds <= 0 or funding_refresh_seconds <= 0 or depth <= 0:
            raise ValueError("Aevo source timings and depth must be positive")
        self.bases = (
            tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
            if bases is not None
            else None
        )
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.funding_refresh_seconds = funding_refresh_seconds
        self.depth = depth
        self.websocket_endpoint = websocket_endpoint
        self.markets_endpoint = markets_endpoint
        self.funding_endpoint = funding_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._bids: dict[str, dict[Decimal, Decimal]] = {}
        self._asks: dict[str, dict[Decimal, Decimal]] = {}
        self._metadata_refreshes = 0
        self._context_refreshes = 0
        self._errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "AEVO",
            "transport": "public_websocket_orderbook_100ms_plus_rest_funding",
            "requested_bases": list(self.bases) if self.bases is not None else "all_active",
            "depth": self.depth,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "metadata_refreshes": self._metadata_refreshes,
            "context_refreshes": self._context_refreshes,
            "recent_errors": list(self._errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _refresh_markets(self, publish: Publish) -> None:
        try:
            payload = await _fetch_json_get(
                url=self.markets_endpoint,
                query={"instrument_type": "PERPETUAL"},
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            parsed = parse_aevo_perp_markets(payload, bases=self.bases)
            if not parsed:
                raise RuntimeError("no requested active Aevo perpetuals")
            for contract, context in parsed.values():
                state = self.cache.register(contract)
                state = self.cache.update_context(contract.venue_symbol, context) or state
                self._bids.setdefault(contract.venue_symbol, {})
                self._asks.setdefault(contract.venue_symbol, {})
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=context.received_realtime_ns,
                    received_monotonic_ns=context.received_monotonic_ns,
                )
            self._metadata_refreshes += 1
        except Exception as exc:
            self._errors.append(f"markets {type(exc).__name__}: {exc}"[:512])

    async def _refresh_funding(self, publish: Publish) -> None:
        for state in self.cache.states():
            try:
                payload = await _fetch_json_get(
                    url=self.funding_endpoint,
                    query={"instrument_name": state.contract.venue_symbol},
                    proxy_url=self.proxy_url,
                    timeout_seconds=self.timeout_seconds,
                )
                if not isinstance(payload, Mapping):
                    raise ValueError("Aevo funding response is malformed")
                previous = state.context
                now_realtime_ns = time.time_ns()
                context = PerpContext(
                    funding_rate=_optional_decimal(payload.get("funding_rate")),
                    mark_price=previous.mark_price if previous is not None else None,
                    index_price=previous.index_price if previous is not None else None,
                    open_interest=previous.open_interest if previous is not None else None,
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=time.monotonic_ns(),
                    # ``next_epoch`` is a future funding boundary, not the
                    # timestamp of this HTTP response.
                    exchange_time_ms=None,
                    next_funding_time_ms=_exchange_time_ms(payload.get("next_epoch")),
                    funding_rate_kind="current_rate_interval_unknown",
                )
                updated = self.cache.update_context(state.contract.venue_symbol, context)
                if updated is not None:
                    self._context_updates += 1
                    await self._publish_state(
                        publish,
                        state=updated,
                        kind="perp_context",
                        received_realtime_ns=context.received_realtime_ns,
                        received_monotonic_ns=context.received_monotonic_ns,
                    )
            except Exception as exc:
                self._errors.append(
                    f"funding:{state.contract.venue_symbol} {type(exc).__name__}: {exc}"[:512],
                )
        self._context_refreshes += 1

    async def _funding_loop(self, publish: Publish, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self._refresh_funding(publish)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.funding_refresh_seconds)
            except TimeoutError:
                pass

    async def _subscribe(self, websocket: Any) -> None:
        symbols = [state.contract.venue_symbol for state in self.cache.states()]
        if not symbols:
            raise RuntimeError("Aevo has no active market to subscribe")
        # The public API documents a complete snapshot followed by deltas at
        # 100 ms.  One request keeps all compatible markets on one socket.
        await websocket.send(
            json.dumps(
                {"op": "subscribe", "data": [f"orderbook-100ms:{symbol}" for symbol in symbols]},
                separators=(",", ":"),
            ),
        )

    async def _handle_book(self, payload: Mapping[str, Any], publish: Publish) -> bool:
        channel = payload.get("channel")
        data = payload.get("data")
        if not isinstance(channel, str) or not channel.startswith("orderbook-100ms:"):
            return False
        if not isinstance(data, Mapping):
            self._malformed_messages += 1
            return True
        symbol = data.get("instrument_name")
        update_type = data.get("type")
        if not isinstance(symbol, str) or update_type not in {"snapshot", "update"}:
            self._malformed_messages += 1
            return True
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return True
        try:
            bid_changes = _parse_aevo_level_changes(data.get("bids"))
            ask_changes = _parse_aevo_level_changes(data.get("asks"))
        except ValueError:
            self._malformed_messages += 1
            return True
        bids = self._bids.setdefault(symbol, {})
        asks = self._asks.setdefault(symbol, {})
        if update_type == "snapshot":
            bids.clear()
            asks.clear()
        for target, changes in ((bids, bid_changes), (asks, ask_changes)):
            for price, size in changes.items():
                if size == 0:
                    target.pop(price, None)
                else:
                    target[price] = size
        book = _book_from_level_maps(
            symbol=symbol,
            bids=bids,
            asks=asks,
            depth=self.depth,
            source="aevo_websocket_orderbook_100ms",
            exchange_time=data.get("last_updated"),
        )
        if book is not None:
            state = self.cache.update_book(symbol, book)
            if state is not None:
                self._book_updates += 1
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_book",
                    received_realtime_ns=book.response.received_realtime_ns,
                    received_monotonic_ns=book.response.received_monotonic_ns,
                )
        return True

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._refresh_markets(publish)
        if not self.cache.states():
            detail = self._errors[-1] if self._errors else "no active Aevo perpetuals"
            raise RuntimeError(detail)
        funding_task = asyncio.create_task(self._funding_loop(publish, stop_event))
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Aevo websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                await self._handle_book(payload, publish)
        finally:
            funding_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await funding_task
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_bulk_perp_contracts(
    payload: Any,
    *,
    bases: Sequence[str] | None,
) -> dict[str, PerpContract]:
    """Normalize public Bulk ``exchangeInfo`` contracts into the common form."""

    if not isinstance(payload, list):
        raise ValueError("Bulk exchangeInfo response must be a list")
    selected_bases = {base.upper() for base in bases} if bases is not None else None
    result: dict[str, PerpContract] = {}
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        symbol = row.get("symbol")
        base = row.get("baseAsset")
        quote = row.get("quoteAsset")
        if (
            not isinstance(symbol, str)
            or not isinstance(base, str)
            or not isinstance(quote, str)
            or row.get("status") != "TRADING"
            or (selected_bases is not None and base.upper() not in selected_bases)
        ):
            continue
        result[symbol.upper()] = PerpContract(
            venue="BULK",
            venue_symbol=symbol.upper(),
            base=base.upper(),
            settlement=quote.upper(),
            contract_type="linear_perpetual",
            funding_interval_minutes=BULK_FUNDING_INTERVAL_MINUTES,
            tick_size=_optional_decimal(row.get("tickSize")),
            quantity_step=_optional_decimal(row.get("lotSize")),
            minimum_order_quantity=None,
            public_taker_fee_bps=None,
            fee_source=None,
        )
    return result


def _parse_bulk_level_changes(rows: Any) -> dict[Decimal, Decimal]:
    if not isinstance(rows, list):
        raise ValueError("Bulk L2 side is not a list")
    result: dict[Decimal, Decimal] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("Bulk L2 level is malformed")
        price = _optional_decimal(row.get("px"))
        size = _optional_decimal(row.get("sz"))
        if price is None or size is None or price <= 0 or size < 0:
            raise ValueError("Bulk L2 level has invalid price or size")
        result[price] = size
    return result


class BulkPerpSource:
    """Public Bulk L2-delta and ticker adapter for the shared perp data bus."""

    name = "perp:bulk"
    # Bulk's public Cloudflare edge can return a sustained 502 while its
    # origin is unavailable.  The scanner's short default is appropriate for
    # ordinary websocket disconnects, but it would otherwise keep probing a
    # known-unavailable public endpoint.  This source-specific backoff leaves
    # the rest of the shared bus untouched and resumes automatically.
    supervisor_retry_initial_seconds = 30.0
    supervisor_retry_max_seconds = 300.0

    def __init__(
        self,
        bases: Sequence[str] | None = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        depth: int = 20,
        websocket_endpoint: str = BULK_PUBLIC_WS_ENDPOINT,
        exchange_info_endpoint: str = BULK_EXCHANGE_INFO_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        if timeout_seconds <= 0 or depth <= 0:
            raise ValueError("Bulk source timings and depth must be positive")
        self.bases = (
            tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
            if bases is not None
            else None
        )
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.depth = depth
        self.websocket_endpoint = websocket_endpoint
        self.exchange_info_endpoint = exchange_info_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._bids: dict[str, dict[Decimal, Decimal]] = {}
        self._asks: dict[str, dict[Decimal, Decimal]] = {}
        self._metadata_refreshes = 0
        self._errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "BULK",
            "transport": "public_websocket_l2_delta_and_ticker",
            "requested_bases": list(self.bases) if self.bases is not None else "all_active",
            "depth": self.depth,
            "supervisor_retry_backoff_seconds": {
                "initial": self.supervisor_retry_initial_seconds,
                "maximum": self.supervisor_retry_max_seconds,
            },
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "metadata_refreshes": self._metadata_refreshes,
            "recent_errors": list(self._errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _refresh_markets(self, publish: Publish) -> None:
        try:
            payload = await _fetch_json_get(
                url=self.exchange_info_endpoint,
                query=None,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            parsed = parse_bulk_perp_contracts(payload, bases=self.bases)
            if not parsed:
                raise RuntimeError("no requested active Bulk perpetuals")
            for contract in parsed.values():
                state = self.cache.register(contract)
                self._bids.setdefault(contract.venue_symbol, {})
                self._asks.setdefault(contract.venue_symbol, {})
                now_realtime_ns = time.time_ns()
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=time.monotonic_ns(),
                )
            self._metadata_refreshes += 1
        except Exception as exc:
            self._errors.append(f"exchange_info {type(exc).__name__}: {exc}"[:512])

    async def _subscribe(self, websocket: Any) -> None:
        symbols = [state.contract.venue_symbol for state in self.cache.states()]
        if not symbols:
            raise RuntimeError("Bulk has no active market to subscribe")
        subscriptions = [
            subscription
            for symbol in symbols
            for subscription in (
                {"type": "l2Delta", "symbol": symbol},
                {"type": "ticker", "symbol": symbol},
            )
        ]
        await websocket.send(
            json.dumps({"method": "subscribe", "subscription": subscriptions}, separators=(",", ":")),
        )

    async def _handle_book(self, payload: Mapping[str, Any], publish: Publish) -> bool:
        if payload.get("type") != "l2Delta":
            return False
        data = payload.get("data")
        book_payload = data.get("book") if isinstance(data, Mapping) else None
        if not isinstance(book_payload, Mapping):
            self._malformed_messages += 1
            return True
        symbol = book_payload.get("symbol")
        update_type = book_payload.get("updateType")
        if not isinstance(symbol, str) or update_type not in {"snapshot", "delta"}:
            self._malformed_messages += 1
            return True
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return True
        levels = book_payload.get("levels")
        if not isinstance(levels, list) or len(levels) != 2:
            self._malformed_messages += 1
            return True
        try:
            bid_changes = _parse_bulk_level_changes(levels[0])
            ask_changes = _parse_bulk_level_changes(levels[1])
        except ValueError:
            self._malformed_messages += 1
            return True
        bids = self._bids.setdefault(symbol, {})
        asks = self._asks.setdefault(symbol, {})
        if update_type == "snapshot":
            bids.clear()
            asks.clear()
        for target, changes in ((bids, bid_changes), (asks, ask_changes)):
            for price, size in changes.items():
                if size == 0:
                    target.pop(price, None)
                else:
                    target[price] = size
        book = _book_from_level_maps(
            symbol=symbol,
            bids=bids,
            asks=asks,
            depth=self.depth,
            source="bulk_websocket_l2_delta",
            exchange_time=book_payload.get("timestamp"),
        )
        if book is not None:
            state = self.cache.update_book(symbol, book)
            if state is not None:
                self._book_updates += 1
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_book",
                    received_realtime_ns=book.response.received_realtime_ns,
                    received_monotonic_ns=book.response.received_monotonic_ns,
                )
        return True

    async def _handle_ticker(self, payload: Mapping[str, Any], publish: Publish) -> bool:
        if payload.get("type") != "ticker":
            return False
        data = payload.get("data")
        ticker = data.get("ticker") if isinstance(data, Mapping) else None
        if not isinstance(ticker, Mapping):
            self._malformed_messages += 1
            return True
        symbol = ticker.get("symbol")
        if not isinstance(symbol, str):
            self._malformed_messages += 1
            return True
        symbol = symbol.upper()
        current = self.cache.state(symbol)
        if current is None:
            return True
        now_realtime_ns = time.time_ns()
        context = PerpContext(
            funding_rate=_optional_decimal(ticker.get("fundingRate")),
            mark_price=_optional_decimal(ticker.get("markPrice")),
            index_price=_optional_decimal(ticker.get("oraclePrice")),
            open_interest=_optional_decimal(ticker.get("openInterest")),
            received_realtime_ns=now_realtime_ns,
            received_monotonic_ns=time.monotonic_ns(),
            exchange_time_ms=_exchange_time_ms(ticker.get("timestamp")),
            funding_rate_kind="current_hourly_rate",
        )
        state = self.cache.update_context(symbol, context)
        if state is not None:
            self._context_updates += 1
            await self._publish_state(
                publish,
                state=state,
                kind="perp_context",
                received_realtime_ns=context.received_realtime_ns,
                received_monotonic_ns=context.received_monotonic_ns,
            )
        return True

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._refresh_markets(publish)
        if not self.cache.states():
            detail = self._errors[-1] if self._errors else "no active Bulk perpetuals"
            raise RuntimeError(detail)
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Bulk websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                if await self._handle_book(payload, publish):
                    continue
                await self._handle_ticker(payload, publish)
        finally:
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_dydx_perp_markets(
    payload: Any,
    *,
    bases: Sequence[str] | None,
) -> dict[str, tuple[PerpContract, PerpContext]]:
    """Normalize dYdX Indexer's public perpetual-market map.

    dYdX calls this value ``nextFundingRate``.  It is retained with the
    explicit ``next_hourly_rate`` label, so a later evaluator cannot silently
    treat it as a settled historical funding payment.
    """

    markets = payload.get("markets") if isinstance(payload, Mapping) else None
    if not isinstance(markets, Mapping):
        raise ValueError("dYdX perpetualMarkets response has no markets map")
    selected_bases = {base.upper() for base in bases} if bases is not None else None
    now_realtime_ns = time.time_ns()
    now_monotonic_ns = time.monotonic_ns()
    result: dict[str, tuple[PerpContract, PerpContext]] = {}
    for market_key, row in markets.items():
        if not isinstance(market_key, str) or not isinstance(row, Mapping):
            continue
        ticker = row.get("ticker", market_key)
        if not isinstance(ticker, str) or row.get("status") != "ACTIVE":
            continue
        base, separator, settlement = ticker.rpartition("-")
        if not separator or not base or not settlement:
            continue
        if selected_bases is not None and base.upper() not in selected_bases:
            continue
        result[ticker.upper()] = (
            PerpContract(
                venue="DYDX",
                venue_symbol=ticker.upper(),
                base=base.upper(),
                settlement=settlement.upper(),
                contract_type="linear_perpetual",
                funding_interval_minutes=DYDX_FUNDING_INTERVAL_MINUTES,
                tick_size=_optional_decimal(row.get("tickSize")),
                quantity_step=_optional_decimal(row.get("stepSize")),
                minimum_order_quantity=None,
                public_taker_fee_bps=None,
                fee_source=None,
            ),
            PerpContext(
                funding_rate=_optional_decimal(row.get("nextFundingRate")),
                mark_price=None,
                index_price=_optional_decimal(row.get("oraclePrice")),
                open_interest=_optional_decimal(row.get("openInterest")),
                received_realtime_ns=now_realtime_ns,
                received_monotonic_ns=now_monotonic_ns,
                funding_rate_kind="next_hourly_rate",
            ),
        )
    return result


def _parse_dydx_level_changes(rows: Any) -> dict[Decimal, tuple[Decimal, int]]:
    """Accept both snapshot dictionaries and compact incremental arrays."""

    if not isinstance(rows, list):
        raise ValueError("dYdX book side is not a list")
    result: dict[Decimal, tuple[Decimal, int]] = {}
    for row in rows:
        if isinstance(row, Mapping):
            raw_price = row.get("price")
            raw_size = row.get("size")
            raw_offset = row.get("offset", "0")
        elif isinstance(row, list) and len(row) >= 2:
            raw_price = row[0]
            raw_size = row[1]
            raw_offset = row[2] if len(row) >= 3 else "0"
        else:
            raise ValueError("dYdX book level is malformed")
        price = _optional_decimal(raw_price)
        size = _optional_decimal(raw_size)
        offset = _optional_int(raw_offset)
        if price is None or size is None or offset is None or price <= 0 or size < 0:
            raise ValueError("dYdX book level has invalid price, size, or offset")
        result[price] = (size, offset)
    return result


def _uncross_dydx_book(
    bids: dict[Decimal, tuple[Decimal, int]],
    asks: dict[Decimal, tuple[Decimal, int]],
) -> None:
    """Apply dYdX's documented offset-aware local book uncrossing rule."""

    while bids and asks:
        bid_price = max(bids)
        ask_price = min(asks)
        if bid_price < ask_price:
            return
        bid_size, bid_offset = bids[bid_price]
        ask_size, ask_offset = asks[ask_price]
        if bid_offset < ask_offset:
            bids.pop(bid_price, None)
        elif bid_offset > ask_offset:
            asks.pop(ask_price, None)
        elif bid_size > ask_size:
            asks.pop(ask_price, None)
            bids[bid_price] = (bid_size - ask_size, bid_offset)
        elif bid_size < ask_size:
            bids.pop(bid_price, None)
            asks[ask_price] = (ask_size - bid_size, ask_offset)
        else:
            bids.pop(bid_price, None)
            asks.pop(ask_price, None)


class DydxPerpSource:
    """Public dYdX Indexer L2 book and market/funding context adapter."""

    name = "perp:dydx"

    def __init__(
        self,
        bases: Sequence[str] | None = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        context_refresh_seconds: float = 30.0,
        subscription_interval_seconds: float = 0.35,
        depth: int = 20,
        websocket_endpoint: str = DYDX_PUBLIC_WS_ENDPOINT,
        markets_endpoint: str = DYDX_PERPETUAL_MARKETS_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        if (
            timeout_seconds <= 0
            or context_refresh_seconds <= 0
            or subscription_interval_seconds <= 0
            or depth <= 0
        ):
            raise ValueError("dYdX source timings and depth must be positive")
        self.bases = (
            tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
            if bases is not None
            else None
        )
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.context_refresh_seconds = context_refresh_seconds
        self.subscription_interval_seconds = subscription_interval_seconds
        self.depth = depth
        self.websocket_endpoint = websocket_endpoint
        self.markets_endpoint = markets_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._bids: dict[str, dict[Decimal, tuple[Decimal, int]]] = {}
        self._asks: dict[str, dict[Decimal, tuple[Decimal, int]]] = {}
        self._metadata_refreshes = 0
        self._errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "DYDX",
            "transport": "public_indexer_websocket_v4_orderbook_plus_rest_market_context",
            "requested_bases": list(self.bases) if self.bases is not None else "all_active",
            "depth": self.depth,
            "subscription_interval_seconds": self.subscription_interval_seconds,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "metadata_refreshes": self._metadata_refreshes,
            "recent_errors": list(self._errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _refresh_markets(self, publish: Publish) -> None:
        try:
            payload = await _fetch_json_get(
                url=self.markets_endpoint,
                query=None,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            parsed = parse_dydx_perp_markets(payload, bases=self.bases)
            if not parsed:
                raise RuntimeError("no requested active dYdX perpetuals")
            for contract, context in parsed.values():
                state = self.cache.register(contract)
                state = self.cache.update_context(contract.venue_symbol, context) or state
                self._bids.setdefault(contract.venue_symbol, {})
                self._asks.setdefault(contract.venue_symbol, {})
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=context.received_realtime_ns,
                    received_monotonic_ns=context.received_monotonic_ns,
                )
            self._metadata_refreshes += 1
        except Exception as exc:
            self._errors.append(f"markets {type(exc).__name__}: {exc}"[:512])

    async def _context_loop(self, publish: Publish, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self._refresh_markets(publish)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.context_refresh_seconds)
            except TimeoutError:
                pass

    async def _subscribe(self, websocket: Any) -> None:
        for state in self.cache.states():
            await websocket.send(
                json.dumps(
                    {
                        "type": "subscribe",
                        "channel": "v4_orderbook",
                        "id": state.contract.venue_symbol,
                    },
                    separators=(",", ":"),
                ),
            )
            # Stay conservatively inside the documented subscription ceiling.
            await asyncio.sleep(self.subscription_interval_seconds)

    async def _handle_book(self, payload: Mapping[str, Any], publish: Publish) -> bool:
        if payload.get("channel") != "v4_orderbook":
            return False
        symbol = payload.get("id")
        contents = payload.get("contents")
        if not isinstance(symbol, str) or not isinstance(contents, Mapping):
            self._malformed_messages += 1
            return True
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return True
        try:
            bid_changes = _parse_dydx_level_changes(contents.get("bids", []))
            ask_changes = _parse_dydx_level_changes(contents.get("asks", []))
        except ValueError:
            self._malformed_messages += 1
            return True
        bids = self._bids.setdefault(symbol, {})
        asks = self._asks.setdefault(symbol, {})
        if payload.get("type") == "subscribed":
            bids.clear()
            asks.clear()
        for target, changes in ((bids, bid_changes), (asks, ask_changes)):
            for price, value in changes.items():
                size, _offset = value
                if size == 0:
                    target.pop(price, None)
                else:
                    target[price] = value
        _uncross_dydx_book(bids, asks)
        book = _book_from_level_maps(
            symbol=symbol,
            bids={price: value[0] for price, value in bids.items()},
            asks={price: value[0] for price, value in asks.items()},
            depth=self.depth,
            source="dydx_indexer_websocket_v4_orderbook",
            exchange_time=None,
        )
        if book is not None:
            state = self.cache.update_book(symbol, book)
            if state is not None:
                self._book_updates += 1
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_book",
                    received_realtime_ns=book.response.received_realtime_ns,
                    received_monotonic_ns=book.response.received_monotonic_ns,
                )
        return True

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._refresh_markets(publish)
        if not self.cache.states():
            detail = self._errors[-1] if self._errors else "no active dYdX perpetuals"
            raise RuntimeError(detail)
        context_task = asyncio.create_task(self._context_loop(publish, stop_event))
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("dYdX websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                await self._handle_book(payload, publish)
        finally:
            context_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await context_task
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_lighter_perp_catalog(payload: Any, *, bases: Sequence[str]) -> dict[int, str]:
    """Select current Lighter perpetual market indices from its public catalogue."""

    if not isinstance(payload, list):
        raise ValueError("Lighter markets response must be a list")
    requested = {base.upper() for base in bases}
    result: dict[int, str] = {}
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        symbol = row.get("symbol")
        market_index = _optional_int(row.get("market_index"))
        if (
            not isinstance(symbol, str)
            or market_index is None
            or market_index < 0
            or market_index >= LIGHTER_SPOT_MARKET_INDEX_START
            or symbol.upper() not in requested
        ):
            continue
        result[market_index] = symbol.upper()
    return result


class LighterPerpSource:
    """Public Lighter 50 ms order-book and all-market-statistics source.

    The adapter checks Lighter's documented ``begin_nonce`` continuity rule
    on every delta.  A gap triggers a fresh snapshot subscription instead of
    accidentally combining two incompatible local book states.
    """

    name = "perp:lighter"

    def __init__(
        self,
        bases: Sequence[str] = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        depth: int = 20,
        websocket_endpoint: str = LIGHTER_PUBLIC_WS_ENDPOINT,
        markets_endpoint: str = LIGHTER_PUBLIC_MARKETS_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        normalized = tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
        if not normalized:
            raise ValueError("at least one Lighter base is required")
        if timeout_seconds <= 0 or depth <= 0:
            raise ValueError("Lighter source timings and depth must be positive")
        self.bases = normalized
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.depth = depth
        self.websocket_endpoint = websocket_endpoint
        self.markets_endpoint = markets_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._symbols_by_index: dict[int, str] = {}
        self._indices_by_symbol: dict[str, int] = {}
        self._bids: dict[int, dict[Decimal, Decimal]] = {}
        self._asks: dict[int, dict[Decimal, Decimal]] = {}
        self._last_nonce: dict[int, int] = {}
        self._catalog_refreshes = 0
        self._catalog_errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._sequence_resyncs = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "LIGHTER",
            "transport": "public_websocket_order_book_50ms_plus_market_stats_all",
            "requested_bases": list(self.bases),
            "depth": self.depth,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "catalog_refreshes": self._catalog_refreshes,
            "catalog_errors": list(self._catalog_errors[-8:]),
            "selected_market_indices": {
                str(index): symbol for index, symbol in sorted(self._symbols_by_index.items())
            },
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "sequence_resyncs": self._sequence_resyncs,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _load_catalog(self, publish: Publish) -> bool:
        try:
            payload = await _fetch_json_get(
                url=self.markets_endpoint,
                query=None,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            selected = parse_lighter_perp_catalog(payload, bases=self.bases)
            if not selected:
                raise RuntimeError("no requested active Lighter perpetuals")
            self._symbols_by_index = dict(selected)
            self._indices_by_symbol = {symbol: index for index, symbol in selected.items()}
            now_realtime_ns = time.time_ns()
            now_monotonic_ns = time.monotonic_ns()
            for index, symbol in selected.items():
                state = self.cache.register(
                    PerpContract(
                        venue="LIGHTER",
                        venue_symbol=symbol,
                        base=symbol,
                        settlement="USDC",
                        contract_type="linear_perpetual",
                        funding_interval_minutes=LIGHTER_FUNDING_INTERVAL_MINUTES,
                        # The public catalogue intentionally only exposes
                        # symbol/index, so do not fabricate order steps or a
                        # user-specific fee tier here.
                        execution_model="central_limit_order_book",
                    ),
                )
                self._bids.setdefault(index, {})
                self._asks.setdefault(index, {})
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                )
            self._catalog_refreshes += 1
            return True
        except Exception as error:
            self._catalog_errors.append(f"catalog {type(error).__name__}: {_redact_urls(str(error))}"[:512])
            return False

    @staticmethod
    def _apply_levels(target: dict[Decimal, Decimal], rows: Any) -> None:
        if not isinstance(rows, list):
            raise ValueError("Lighter order-book side is not a list")
        for row in rows:
            if not isinstance(row, Mapping):
                raise ValueError("Lighter order-book level is not an object")
            price = _optional_decimal(row.get("price"))
            size = _optional_decimal(row.get("size"))
            if price is None or size is None or price <= 0 or size < 0:
                continue
            if size == 0:
                target.pop(price, None)
            else:
                target[price] = size

    async def _handle_order_book(
        self,
        payload: Mapping[str, Any],
        websocket: Any,
        publish: Publish,
    ) -> bool:
        channel = payload.get("channel")
        book_data = payload.get("order_book")
        if not isinstance(channel, str) or not isinstance(book_data, Mapping):
            return False
        _prefix, separator, index_text = channel.partition(":")
        market_index = _optional_int(index_text) if separator else None
        if market_index is None or market_index not in self._symbols_by_index:
            return True
        message_type = payload.get("type")
        nonce = _optional_int(book_data.get("nonce"))
        begin_nonce = _optional_int(book_data.get("begin_nonce"))
        if nonce is None or begin_nonce is None:
            self._malformed_messages += 1
            return True
        bids = self._bids.setdefault(market_index, {})
        asks = self._asks.setdefault(market_index, {})
        if message_type == "subscribed/order_book":
            bids.clear()
            asks.clear()
        elif message_type == "update/order_book":
            previous_nonce = self._last_nonce.get(market_index)
            if previous_nonce is None or begin_nonce != previous_nonce:
                self._sequence_resyncs += 1
                bids.clear()
                asks.clear()
                self._last_nonce.pop(market_index, None)
                await websocket.send(
                    json.dumps(
                        {"type": "subscribe", "channel": f"order_book/{market_index}"},
                        separators=(",", ":"),
                    ),
                )
                return True
        else:
            return False
        try:
            self._apply_levels(bids, book_data.get("bids"))
            self._apply_levels(asks, book_data.get("asks"))
        except ValueError:
            self._malformed_messages += 1
            return True
        self._last_nonce[market_index] = nonce
        symbol = self._symbols_by_index[market_index]
        book = _book_from_level_maps(
            symbol=symbol,
            bids=bids,
            asks=asks,
            depth=self.depth,
            source="lighter_public_websocket_order_book",
            exchange_time=payload.get("timestamp"),
        )
        if book is None:
            return True
        state = self.cache.update_book(symbol, book, liquidity_sources=("lighter_clob",))
        if state is None:
            return True
        self._book_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_book",
            received_realtime_ns=book.response.received_realtime_ns,
            received_monotonic_ns=book.response.received_monotonic_ns,
        )
        return True

    async def _handle_market_stats(self, payload: Mapping[str, Any], publish: Publish) -> bool:
        if payload.get("channel") != "market_stats:all":
            return False
        rows = payload.get("market_stats")
        if not isinstance(rows, Mapping):
            self._malformed_messages += 1
            return True
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        exchange_time_ms = _exchange_time_ms(payload.get("timestamp"))
        for key, row in rows.items():
            market_index = _optional_int(key)
            if market_index is None or market_index not in self._symbols_by_index or not isinstance(row, Mapping):
                continue
            symbol = self._symbols_by_index[market_index]
            reported_symbol = row.get("symbol")
            if isinstance(reported_symbol, str) and reported_symbol.upper() != symbol:
                self._malformed_messages += 1
                continue
            context = PerpContext(
                # Lighter exposes both the upcoming estimate and the prior
                # paid rate.  For prospective carry, retain the former and
                # label it rather than conflating the two.
                funding_rate=_optional_decimal(row.get("current_funding_rate")),
                mark_price=_optional_decimal(row.get("mark_price")),
                index_price=_optional_decimal(row.get("index_price")),
                open_interest=_optional_decimal(row.get("open_interest")),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
                exchange_time_ms=exchange_time_ms,
                funding_rate_kind="estimated_next_hourly",
                funding_period_seconds=LIGHTER_FUNDING_INTERVAL_MINUTES * 60,
            )
            state = self.cache.update_context(symbol, context)
            if state is None:
                continue
            self._context_updates += 1
            await self._publish_state(
                publish,
                state=state,
                kind="perp_context",
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            )
        return True

    async def _subscribe(self, websocket: Any) -> None:
        await websocket.send(
            json.dumps({"type": "subscribe", "channel": "market_stats/all"}, separators=(",", ":")),
        )
        for market_index in sorted(self._symbols_by_index):
            await websocket.send(
                json.dumps(
                    {"type": "subscribe", "channel": f"order_book/{market_index}"},
                    separators=(",", ":"),
                ),
            )
            # A modest spacing keeps startup well below the documented public
            # WebSocket subscription limits without affecting push cadence.
            await asyncio.sleep(0.05)

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._load_catalog(publish)
        if not self._symbols_by_index:
            detail = self._catalog_errors[-1] if self._catalog_errors else "no active Lighter perpetuals"
            raise RuntimeError(detail)
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Lighter websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                if await self._handle_order_book(payload, websocket, publish):
                    continue
                await self._handle_market_stats(payload, publish)
        finally:
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_aster_perp_catalog(payload: Any, *, bases: Sequence[str]) -> dict[str, PerpContract]:
    """Select active Aster USDT perpetuals from public exchangeInfo."""

    if not isinstance(payload, Mapping) or not isinstance(payload.get("symbols"), list):
        raise ValueError("Aster exchangeInfo response must contain a symbols list")
    requested = {base.upper() for base in bases}
    result: dict[str, PerpContract] = {}
    for row in payload["symbols"]:
        if not isinstance(row, Mapping):
            continue
        symbol = row.get("symbol")
        base = row.get("baseAsset")
        settlement = row.get("marginAsset")
        if (
            row.get("contractType") != "PERPETUAL"
            or row.get("status") != "TRADING"
            or not isinstance(symbol, str)
            or not isinstance(base, str)
            or not isinstance(settlement, str)
            or base.upper() not in requested
        ):
            continue
        filters = row.get("filters")
        filters = filters if isinstance(filters, list) else []
        price_filter = next(
            (item for item in filters if isinstance(item, Mapping) and item.get("filterType") == "PRICE_FILTER"),
            {},
        )
        lot_filter = next(
            (item for item in filters if isinstance(item, Mapping) and item.get("filterType") == "LOT_SIZE"),
            {},
        )
        result[symbol.upper()] = PerpContract(
            venue="ASTER",
            venue_symbol=symbol.upper(),
            base=base.upper(),
            settlement=settlement.upper(),
            contract_type="linear_perpetual",
            # The live mark-price stream reports the next funding timestamp,
            # but not a stable contract interval in exchangeInfo.
            funding_interval_minutes=None,
            tick_size=_optional_decimal(price_filter.get("tickSize")),
            quantity_step=_optional_decimal(lot_filter.get("stepSize")),
            minimum_order_quantity=_optional_decimal(lot_filter.get("minQty")),
            public_taker_fee_bps=None,
            fee_source=None,
            execution_model="central_limit_order_book_partial_l2",
        )
    return result


class AsterPerpSource:
    """Public Aster partial L2 and mark/index/funding combined WebSocket feed."""

    name = "perp:aster"

    def __init__(
        self,
        bases: Sequence[str] = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        depth: int = 20,
        websocket_root: str = ASTER_PUBLIC_WS_ROOT,
        exchange_info_endpoint: str = ASTER_PUBLIC_EXCHANGE_INFO_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        normalized = tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
        if not normalized:
            raise ValueError("at least one Aster base is required")
        if timeout_seconds <= 0 or depth not in {5, 10, 20}:
            raise ValueError("Aster source timeout/depth is invalid")
        self.bases = normalized
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.depth = depth
        self.websocket_root = websocket_root.rstrip("/")
        self.exchange_info_endpoint = exchange_info_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._catalog_refreshes = 0
        self._catalog_errors: list[str] = []
        self._last_depth_update_id: dict[str, int] = {}
        self._book_updates = 0
        self._context_updates = 0
        self._out_of_order_books = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "ASTER",
            "transport": "public_combined_websocket_partial_l2_100ms_plus_mark_index_funding_1s",
            "requested_bases": list(self.bases),
            "book_depth": self.depth,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "catalog_refreshes": self._catalog_refreshes,
            "catalog_errors": list(self._catalog_errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "out_of_order_books": self._out_of_order_books,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _load_catalog(self, publish: Publish) -> bool:
        try:
            payload = await _fetch_json_get(
                url=self.exchange_info_endpoint,
                query=None,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            contracts = parse_aster_perp_catalog(payload, bases=self.bases)
            if not contracts:
                raise RuntimeError("no requested active Aster perpetuals")
            now_realtime_ns = time.time_ns()
            now_monotonic_ns = time.monotonic_ns()
            for contract in contracts.values():
                state = self.cache.register(contract)
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                )
            self._catalog_refreshes += 1
            return True
        except Exception as error:
            self._catalog_errors.append(f"catalog {type(error).__name__}: {_redact_urls(str(error))}"[:512])
            return False

    def _websocket_endpoint(self) -> str:
        streams: list[str] = []
        for state in self.cache.states():
            symbol = state.contract.venue_symbol.lower()
            streams.extend((f"{symbol}@depth{self.depth}@100ms", f"{symbol}@markPrice@1s"))
        return f"{self.websocket_root}/stream?{urllib.parse.urlencode({'streams': '/'.join(streams)})}"

    @staticmethod
    def _parse_aster_levels(rows: Any) -> dict[Decimal, Decimal]:
        if not isinstance(rows, list):
            raise ValueError("Aster partial-book side is not a list")
        result: dict[Decimal, Decimal] = {}
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                raise ValueError("Aster partial-book level is malformed")
            price = _optional_decimal(row[0])
            size = _optional_decimal(row[1])
            if price is not None and size is not None and price > 0 and size > 0:
                result[price] = size
        return result

    async def _handle_depth(self, data: Mapping[str, Any], publish: Publish) -> None:
        if data.get("e") != "depthUpdate":
            return
        symbol = data.get("s")
        if not isinstance(symbol, str):
            self._malformed_messages += 1
            return
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return
        update_id = _optional_int(data.get("u"))
        previous = self._last_depth_update_id.get(symbol)
        if update_id is not None and previous is not None and update_id <= previous:
            self._out_of_order_books += 1
            return
        try:
            bids = self._parse_aster_levels(data.get("b"))
            asks = self._parse_aster_levels(data.get("a"))
        except ValueError:
            self._malformed_messages += 1
            return
        if update_id is not None:
            self._last_depth_update_id[symbol] = update_id
        book = _book_from_level_maps(
            symbol=symbol,
            bids=bids,
            asks=asks,
            depth=self.depth,
            source="aster_public_partial_l2_websocket",
            exchange_time=data.get("E"),
        )
        if book is None:
            self._malformed_messages += 1
            return
        state = self.cache.update_book(symbol, book, liquidity_sources=("aster_partial_l2",))
        if state is None:
            return
        self._book_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_book",
            received_realtime_ns=book.response.received_realtime_ns,
            received_monotonic_ns=book.response.received_monotonic_ns,
        )

    async def _handle_mark_price(self, data: Mapping[str, Any], publish: Publish) -> None:
        if data.get("e") != "markPriceUpdate":
            return
        symbol = data.get("s")
        if not isinstance(symbol, str):
            self._malformed_messages += 1
            return
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        context = PerpContext(
            funding_rate=_optional_decimal(data.get("r")),
            mark_price=_optional_decimal(data.get("p")),
            index_price=_optional_decimal(data.get("i")),
            open_interest=None,
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            exchange_time_ms=_exchange_time_ms(data.get("E")),
            next_funding_time_ms=_optional_int(data.get("T")),
            funding_rate_kind="next_funding_rate",
        )
        state = self.cache.update_context(symbol, context)
        if state is None:
            return
        self._context_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_context",
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
        )

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._load_catalog(publish)
        if not self.cache.states():
            detail = self._catalog_errors[-1] if self._catalog_errors else "no active Aster perpetuals"
            raise RuntimeError(detail)
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self._websocket_endpoint(),
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Aster websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                data = payload.get("data")
                if not isinstance(data, Mapping):
                    self._malformed_messages += 1
                    continue
                await self._handle_depth(data, publish)
                await self._handle_mark_price(data, publish)
        finally:
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_paradex_perp_catalog(payload: Any, *, bases: Sequence[str]) -> dict[str, PerpContract]:
    """Normalize Paradex's public static catalogue into perpetual contracts."""

    if not isinstance(payload, Mapping) or not isinstance(payload.get("results"), list):
        raise ValueError("Paradex markets response must contain a results list")
    requested = {base.upper() for base in bases}
    result: dict[str, PerpContract] = {}
    for row in payload["results"]:
        if not isinstance(row, Mapping) or row.get("asset_kind") != "PERP":
            continue
        symbol = row.get("symbol")
        base = row.get("base_currency")
        settlement = row.get("settlement_currency")
        if (
            not isinstance(symbol, str)
            or not isinstance(base, str)
            or not isinstance(settlement, str)
            or base.upper() not in requested
        ):
            continue
        funding_period_hours = _optional_int(row.get("funding_period_hours"))
        funding_interval_minutes = (
            funding_period_hours * 60
            if funding_period_hours is not None and funding_period_hours > 0
            else None
        )
        fee_config = row.get("fee_config")
        api_fee = fee_config.get("api_fee") if isinstance(fee_config, Mapping) else None
        taker_fee = api_fee.get("taker_fee") if isinstance(api_fee, Mapping) else None
        raw_taker_rate = taker_fee.get("fee") if isinstance(taker_fee, Mapping) else None
        decimal_taker_rate = _optional_decimal(raw_taker_rate)
        result[symbol.upper()] = PerpContract(
            venue="PARADEX",
            venue_symbol=symbol.upper(),
            base=base.upper(),
            settlement=settlement.upper(),
            contract_type="linear_perpetual",
            funding_interval_minutes=funding_interval_minutes,
            tick_size=_optional_decimal(row.get("price_tick_size")),
            quantity_step=_optional_decimal(row.get("order_size_increment")),
            # ``min_notional`` is a quote-value threshold, not a base quantity.
            minimum_order_quantity=None,
            public_taker_fee_bps=(
                decimal_taker_rate * Decimal("10000") if decimal_taker_rate is not None else None
            ),
            fee_source=("public_market_fee_config.api_fee.taker_fee" if decimal_taker_rate is not None else None),
            execution_model="central_limit_order_book",
        )
    return result


class ParadexPerpSource:
    """Public Paradex BBO and market-summary adapter over one JSON-RPC socket."""

    name = "perp:paradex"

    def __init__(
        self,
        bases: Sequence[str] = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        timeout_seconds: float = 10.0,
        subscription_interval_seconds: float = 0.05,
        websocket_endpoint: str = PARADEX_PUBLIC_WS_ENDPOINT,
        markets_endpoint: str = PARADEX_PUBLIC_MARKETS_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        normalized = tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
        if not normalized:
            raise ValueError("at least one Paradex base is required")
        if timeout_seconds <= 0 or subscription_interval_seconds <= 0:
            raise ValueError("Paradex source timings must be positive")
        self.bases = normalized
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.subscription_interval_seconds = subscription_interval_seconds
        self.websocket_endpoint = websocket_endpoint
        self.markets_endpoint = markets_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        self._catalog_refreshes = 0
        self._catalog_errors: list[str] = []
        self._last_bbo_sequence: dict[str, int] = {}
        self._book_updates = 0
        self._context_updates = 0
        self._out_of_order_bbo = 0
        self._malformed_messages = 0

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "PARADEX",
            "transport": "public_jsonrpc_websocket_bbo_plus_market_summary",
            "requested_bases": list(self.bases),
            "book_depth": 1,
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "catalog_refreshes": self._catalog_refreshes,
            "catalog_errors": list(self._catalog_errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "out_of_order_bbo": self._out_of_order_bbo,
            "malformed_messages": self._malformed_messages,
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    async def _load_catalog(self, publish: Publish) -> bool:
        try:
            payload = await _fetch_json_get(
                url=self.markets_endpoint,
                query=None,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            contracts = parse_paradex_perp_catalog(payload, bases=self.bases)
            if not contracts:
                raise RuntimeError("no requested active Paradex perpetuals")
            now_realtime_ns = time.time_ns()
            now_monotonic_ns = time.monotonic_ns()
            for contract in contracts.values():
                state = self.cache.register(contract)
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=now_realtime_ns,
                    received_monotonic_ns=now_monotonic_ns,
                )
            self._catalog_refreshes += 1
            return True
        except Exception as error:
            self._catalog_errors.append(f"catalog {type(error).__name__}: {_redact_urls(str(error))}"[:512])
            return False

    @staticmethod
    def _subscription(payload: Mapping[str, Any]) -> tuple[str, Mapping[str, Any]] | None:
        if payload.get("method") != "subscription":
            return None
        params = payload.get("params")
        if not isinstance(params, Mapping):
            return None
        channel = params.get("channel")
        data = params.get("data")
        return (channel, data) if isinstance(channel, str) and isinstance(data, Mapping) else None

    async def _handle_bbo(self, data: Mapping[str, Any], publish: Publish) -> None:
        symbol = data.get("market")
        if not isinstance(symbol, str):
            self._malformed_messages += 1
            return
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return
        sequence = _optional_int(data.get("seq_no"))
        previous_sequence = self._last_bbo_sequence.get(symbol)
        if sequence is not None and previous_sequence is not None and sequence <= previous_sequence:
            self._out_of_order_bbo += 1
            return
        bid = _optional_decimal(data.get("bid"))
        bid_size = _optional_decimal(data.get("bid_size"))
        ask = _optional_decimal(data.get("ask"))
        ask_size = _optional_decimal(data.get("ask_size"))
        if (
            bid is None
            or bid_size is None
            or ask is None
            or ask_size is None
            or bid <= 0
            or bid_size <= 0
            or ask <= 0
            or ask_size <= 0
        ):
            self._malformed_messages += 1
            return
        if sequence is not None:
            self._last_bbo_sequence[symbol] = sequence
        book = _book_from_level_maps(
            symbol=symbol,
            bids={bid: bid_size},
            asks={ask: ask_size},
            depth=1,
            source="paradex_public_bbo_websocket",
            exchange_time=data.get("last_updated_at"),
        )
        if book is None:
            self._malformed_messages += 1
            return
        state = self.cache.update_book(symbol, book, liquidity_sources=("paradex_clob",))
        if state is None:
            return
        self._book_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_book",
            received_realtime_ns=book.response.received_realtime_ns,
            received_monotonic_ns=book.response.received_monotonic_ns,
        )

    async def _handle_market_summary(self, data: Mapping[str, Any], publish: Publish) -> None:
        symbol = data.get("symbol")
        if not isinstance(symbol, str):
            self._malformed_messages += 1
            return
        symbol = symbol.upper()
        if self.cache.state(symbol) is None:
            return
        future_rate = _optional_decimal(data.get("future_funding_rate"))
        current_rate = _optional_decimal(data.get("funding_rate"))
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        context = PerpContext(
            funding_rate=future_rate if future_rate is not None else current_rate,
            mark_price=_optional_decimal(data.get("mark_price")),
            # Paradex calls this public field underlying_price rather than
            # index_price; it is retained as the venue's underlying reference,
            # not claimed to be a cross-venue executable spot price.
            index_price=_optional_decimal(data.get("underlying_price")),
            open_interest=_optional_decimal(data.get("open_interest")),
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            exchange_time_ms=_exchange_time_ms(data.get("created_at")),
            funding_rate_kind=("future" if future_rate is not None else "current"),
        )
        state = self.cache.update_context(symbol, context)
        if state is None:
            return
        self._context_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_context",
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
        )

    async def _subscribe(self, websocket: Any) -> None:
        request_id = 1
        for state in self.cache.states():
            symbol = state.contract.venue_symbol
            for channel in (f"bbo.{symbol}", f"markets_summary.{symbol}"):
                await websocket.send(
                    json.dumps(
                        {
                            "id": request_id,
                            "jsonrpc": "2.0",
                            "method": "subscribe",
                            "params": {"channel": channel},
                        },
                        separators=(",", ":"),
                    ),
                )
                request_id += 1
                await asyncio.sleep(self.subscription_interval_seconds)

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._load_catalog(publish)
        if not self.cache.states():
            detail = self._catalog_errors[-1] if self._catalog_errors else "no active Paradex perpetuals"
            raise RuntimeError(detail)
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.websocket_endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            await self._subscribe(websocket)
            while not stop_event.is_set():
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("Paradex websocket closed by peer")
                if not isinstance(raw, str):
                    self._malformed_messages += 1
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    self._malformed_messages += 1
                    continue
                if not isinstance(payload, Mapping):
                    self._malformed_messages += 1
                    continue
                subscription = self._subscription(payload)
                if subscription is None:
                    continue
                channel, data = subscription
                if channel.startswith("bbo."):
                    await self._handle_bbo(data, publish)
                elif channel.startswith("markets_summary."):
                    await self._handle_market_summary(data, publish)
        finally:
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()


def parse_extended_perp_markets(
    payload: Any,
    *,
    bases: Sequence[str],
) -> dict[str, tuple[PerpContract, PerpContext, bool]]:
    """Normalize Extended's verified active perpetual-market response.

    Extended also has RFQ perpetuals.  Their public BBO is useful market data,
    but it is explicitly labelled as indicative rather than silently treated
    as a resting CLOB for a later execution evaluator.
    """

    if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
        raise ValueError("Extended markets response must contain a data list")
    selected_bases = {base.upper() for base in bases}
    now_realtime_ns = time.time_ns()
    now_monotonic_ns = time.monotonic_ns()
    result: dict[str, tuple[PerpContract, PerpContext, bool]] = {}
    for row in payload["data"]:
        if not isinstance(row, Mapping):
            continue
        name = row.get("name")
        base = row.get("assetName")
        settlement = row.get("collateralAssetName")
        if (
            row.get("type") != "PERPETUAL"
            or row.get("active") is not True
            or row.get("status") != "ACTIVE"
            or not isinstance(name, str)
            or not isinstance(base, str)
            or not isinstance(settlement, str)
            or base.upper() not in selected_bases
        ):
            continue
        statistics = row.get("marketStats")
        trading = row.get("tradingConfig")
        statistics = statistics if isinstance(statistics, Mapping) else {}
        trading = trading if isinstance(trading, Mapping) else {}
        is_rfq = row.get("isRfq") is True
        contract = PerpContract(
            venue="EXTENDED",
            venue_symbol=name.upper(),
            base=base.upper(),
            settlement=settlement.upper(),
            contract_type="linear_perpetual",
            funding_interval_minutes=EXTENDED_FUNDING_INTERVAL_MINUTES,
            tick_size=_optional_decimal(trading.get("minPriceChange")),
            quantity_step=_optional_decimal(trading.get("minOrderSizeChange")),
            minimum_order_quantity=_optional_decimal(trading.get("minOrderSize")),
            public_taker_fee_bps=None,
            fee_source=None,
            execution_model=("rfq_indicative_book" if is_rfq else "central_limit_order_book"),
        )
        context = PerpContext(
            funding_rate=_optional_decimal(statistics.get("fundingRate")),
            mark_price=_optional_decimal(statistics.get("markPrice")),
            index_price=_optional_decimal(statistics.get("indexPrice")),
            open_interest=_optional_decimal(statistics.get("openInterest")),
            received_realtime_ns=now_realtime_ns,
            received_monotonic_ns=now_monotonic_ns,
            exchange_time_ms=None,
            funding_rate_kind="current_hourly",
            funding_period_seconds=EXTENDED_FUNDING_INTERVAL_MINUTES * 60,
        )
        result[contract.venue_symbol] = (contract, context, is_rfq)
    return result


def parse_extended_bbo(
    payload: Any,
    *,
    depth: int,
) -> tuple[str, dict[Decimal, Decimal], dict[Decimal, Decimal], int | None] | None:
    """Parse one documented Extended BBO snapshot without retaining raw JSON."""

    if not isinstance(payload, Mapping) or payload.get("type") != "SNAPSHOT":
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping) or data.get("d") != "1":
        return None
    symbol = data.get("m")
    if not isinstance(symbol, str):
        raise ValueError("Extended BBO snapshot has no market symbol")

    def levels(rows: Any) -> dict[Decimal, Decimal]:
        if not isinstance(rows, list):
            raise ValueError("Extended BBO side is not a list")
        result: dict[Decimal, Decimal] = {}
        for row in rows[:depth]:
            if not isinstance(row, Mapping):
                raise ValueError("Extended BBO level is not an object")
            price = _optional_decimal(row.get("p"))
            quantity = _optional_decimal(row.get("q"))
            if price is not None and quantity is not None and price > 0 and quantity > 0:
                result[price] = quantity
        return result

    return (
        symbol.upper(),
        levels(data.get("b")),
        levels(data.get("a")),
        _exchange_time_ms(payload.get("ts")),
    )


class ExtendedPerpSource:
    """Public Extended BBO plus context feed with a per-source SOCKS fallback.

    The source begins direct.  Only a DNS/timeout/reset class failure enables
    its own local SOCKS route; it never changes proxy settings for the other
    CEX, DEX, or Solana feeds in the shared process.
    """

    name = "perp:extended"

    def __init__(
        self,
        bases: Sequence[str] = DEFAULT_SHARED_PERP_BASES,
        *,
        proxy_url: str | None = None,
        fallback_socks_proxy_url: str | None = EXTENDED_LOCAL_FALLBACK_PROXY_URL,
        timeout_seconds: float = 10.0,
        context_refresh_seconds: float = 60.0,
        websocket_root: str = EXTENDED_PUBLIC_WS_ROOT,
        markets_endpoint: str = EXTENDED_PUBLIC_MARKETS_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        normalized = tuple(dict.fromkeys(base.upper() for base in bases if base.strip()))
        if not normalized:
            raise ValueError("at least one Extended base is required")
        if timeout_seconds <= 0 or context_refresh_seconds <= 0:
            raise ValueError("Extended source timings must be positive")
        self.bases = normalized
        self.proxy_url = proxy_url
        self.fallback_socks_proxy_url = fallback_socks_proxy_url
        self.timeout_seconds = timeout_seconds
        self.context_refresh_seconds = context_refresh_seconds
        self.websocket_root = websocket_root.rstrip("/")
        self.markets_endpoint = markets_endpoint
        self.connect_websocket = connect_websocket
        self.cache = PerpMarketCache()
        # Extended rejects a multi-market request when even one requested name
        # is absent.  Start from the public catalogue once, then ask only for
        # previously verified live names on future context refreshes.
        self._known_markets: tuple[str, ...] = ()
        self._rfq_symbols: set[str] = set()
        self._last_sequence: dict[str, int] = {}
        self._metadata_refreshes = 0
        self._metadata_errors: list[str] = []
        self._book_errors: list[str] = []
        self._book_updates = 0
        self._context_updates = 0
        self._malformed_messages = 0
        self._network_route = "configured_proxy" if proxy_url is not None else "direct"
        self._effective_proxy_url = proxy_url

    def describe(self) -> dict[str, Any]:
        return {
            "source": self.name,
            "venue": "EXTENDED",
            "transport": "public_per_market_bbo_websocket_depth_1_plus_public_market_context",
            "requested_bases": list(self.bases),
            "book_depth": 1,
            "rfq_semantics": "public_indicative_bbo_labeled_not_resting_clob",
            "orders_or_transactions": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "network_route": self._network_route,
            "metadata_refreshes": self._metadata_refreshes,
            "metadata_errors": list(self._metadata_errors[-8:]),
            "book_errors": list(self._book_errors[-8:]),
            "book_updates": self._book_updates,
            "context_updates": self._context_updates,
            "malformed_messages": self._malformed_messages,
            "rfq_indicative_markets": sorted(self._rfq_symbols),
            "markets": {state.contract.venue_symbol: state.compact_summary() for state in self.cache.states()},
        }

    async def _publish_state(
        self,
        publish: Publish,
        *,
        state: PerpMarketState,
        kind: str,
        received_realtime_ns: int,
        received_monotonic_ns: int,
    ) -> None:
        await publish(
            MarketEvent(
                source=self.name,
                key=state.key,
                kind=kind,
                value=PerpQuoteEvent.from_state(
                    state,
                    received_realtime_ns=received_realtime_ns,
                    received_monotonic_ns=received_monotonic_ns,
                ),
                summary=state.compact_summary(),
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
            ),
        )

    def _markets_url(self) -> str:
        if not self._known_markets:
            return self.markets_endpoint
        query = urllib.parse.urlencode([("market", market) for market in self._known_markets])
        return f"{self.markets_endpoint}?{query}"

    async def _fetch_markets(self) -> Any:
        url = self._markets_url()
        try:
            return await self._fetch_markets_url(url)
        except RuntimeError as error:
            # A once-valid market can later be delisted.  A 400 on the compact
            # known-market query is then a cue to rediscover, not a reason to
            # retry a bad query forever or take other sources down.
            if self._known_markets and "HTTP 400" in str(error):
                self._known_markets = ()
                return await self._fetch_markets_url(self._markets_url())
            raise

    async def _fetch_markets_url(self, url: str) -> Any:
        if self._effective_proxy_url is not None:
            if self._effective_proxy_url.startswith(("socks4://", "socks4a://", "socks5://", "socks5h://")):
                return await _fetch_json_get_via_socks(
                    url=url,
                    proxy_url=self._effective_proxy_url,
                    timeout_seconds=self.timeout_seconds,
                )
            return await _fetch_json_get(
                url=url,
                query=None,
                proxy_url=self._effective_proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
        try:
            return await _fetch_json_get(
                url=url,
                query=None,
                proxy_url=None,
                # A known unreachable host should not delay source startup by
                # the full per-request budget before its one permitted retry.
                timeout_seconds=min(self.timeout_seconds, 3.0),
            )
        except Exception as error:
            if not self.fallback_socks_proxy_url or not _is_network_reachability_error(error):
                raise
            self._effective_proxy_url = self.fallback_socks_proxy_url
            self._network_route = "socks_fallback_after_direct_network_error"
            return await _fetch_json_get_via_socks(
                url=url,
                proxy_url=self._effective_proxy_url,
                timeout_seconds=self.timeout_seconds,
            )

    async def _refresh_markets(self, publish: Publish) -> bool:
        try:
            parsed = parse_extended_perp_markets(await self._fetch_markets(), bases=self.bases)
            if not parsed:
                raise RuntimeError("no requested active Extended perpetuals")
            self._known_markets = tuple(sorted(parsed))
            self._rfq_symbols = {symbol for symbol, (_contract, _context, is_rfq) in parsed.items() if is_rfq}
            for contract, context, _is_rfq in parsed.values():
                state = self.cache.register(contract)
                state = self.cache.update_context(contract.venue_symbol, context) or state
                self._context_updates += 1
                await self._publish_state(
                    publish,
                    state=state,
                    kind="perp_contract",
                    received_realtime_ns=context.received_realtime_ns,
                    received_monotonic_ns=context.received_monotonic_ns,
                )
            self._metadata_refreshes += 1
            return True
        except Exception as error:
            self._metadata_errors.append(f"markets {type(error).__name__}: {_redact_urls(str(error))}"[:512])
            return False

    async def _context_loop(self, publish: Publish, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await self._refresh_markets(publish)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.context_refresh_seconds)
            except TimeoutError:
                pass

    async def _handle_book_message(self, payload: Mapping[str, Any], publish: Publish) -> None:
        try:
            parsed = parse_extended_bbo(payload, depth=1)
        except ValueError:
            self._malformed_messages += 1
            return
        if parsed is None:
            return
        symbol, bids, asks, exchange_time_ms = parsed
        state = self.cache.state(symbol)
        if state is None:
            return
        sequence = _optional_int(payload.get("seq"))
        previous = self._last_sequence.get(symbol)
        if sequence is not None and previous is not None and sequence <= previous:
            self._malformed_messages += 1
            return
        if sequence is not None:
            self._last_sequence[symbol] = sequence
        source = (
            "extended_public_rfq_indicative_bbo_websocket"
            if symbol in self._rfq_symbols
            else "extended_public_clob_bbo_websocket"
        )
        book = _book_from_level_maps(
            symbol=symbol,
            bids=bids,
            asks=asks,
            depth=1,
            source=source,
            exchange_time=exchange_time_ms,
        )
        if book is None:
            self._malformed_messages += 1
            return
        state = self.cache.update_book(
            symbol,
            book,
            liquidity_sources=("extended_rfq_indicative",)
            if symbol in self._rfq_symbols
            else ("extended_clob",),
        )
        if state is None:
            return
        self._book_updates += 1
        await self._publish_state(
            publish,
            state=state,
            kind="perp_book",
            received_realtime_ns=book.response.received_realtime_ns,
            received_monotonic_ns=book.response.received_monotonic_ns,
        )

    async def _book_loop(
        self,
        *,
        symbol: str,
        publish: Publish,
        stop_event: asyncio.Event,
    ) -> None:
        endpoint = f"{self.websocket_root}/orderbooks/{urllib.parse.quote(symbol, safe='-')}?depth=1"
        while not stop_event.is_set():
            websocket: Any = None
            try:
                websocket = await self.connect_websocket(
                    endpoint,
                    open_timeout=self.timeout_seconds,
                    close_timeout=1,
                    ping_interval=20,
                    ping_timeout=20,
                    proxy=self._effective_proxy_url,
                )
                while not stop_event.is_set():
                    raw = await websocket.recv()
                    if raw is None:
                        raise ConnectionError(f"Extended websocket closed for {symbol}")
                    if not isinstance(raw, str):
                        self._malformed_messages += 1
                        continue
                    try:
                        payload = json.loads(raw)
                    except json.JSONDecodeError:
                        self._malformed_messages += 1
                        continue
                    if not isinstance(payload, Mapping):
                        self._malformed_messages += 1
                        continue
                    await self._handle_book_message(payload, publish)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self._effective_proxy_url is None and _is_network_reachability_error(error) and self.fallback_socks_proxy_url:
                    self._effective_proxy_url = self.fallback_socks_proxy_url
                    self._network_route = "socks_fallback_after_direct_network_error"
                self._book_errors.append(f"{symbol} {type(error).__name__}: {_redact_urls(str(error))}"[:512])
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=1.0)
                except TimeoutError:
                    pass
            finally:
                if websocket is not None:
                    with contextlib.suppress(Exception):
                        await websocket.close()

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        await self._refresh_markets(publish)
        states = self.cache.states()
        if not states:
            detail = self._metadata_errors[-1] if self._metadata_errors else "no active Extended perpetuals"
            raise RuntimeError(detail)
        context_task = asyncio.create_task(self._context_loop(publish, stop_event))
        book_tasks = [
            asyncio.create_task(
                self._book_loop(
                    symbol=state.contract.venue_symbol,
                    publish=publish,
                    stop_event=stop_event,
                ),
            )
            for state in states
        ]
        try:
            await stop_event.wait()
        finally:
            context_task.cancel()
            for task in book_tasks:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await context_task
            for task in book_tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task


def build_perp_venue_sources(
    *,
    hyperliquid_coins: Sequence[str] = DEFAULT_HYPERLIQUID_COINS,
    solana_rpc_http_url: str | None = None,
    solana_rpc_ws_url: str | None = None,
    proxy_url: str | None = None,
    timeout_seconds: float = 10.0,
) -> tuple[ScannerSource, ...]:
    """Create source-neutral perp adapters without starting a scanner.

    The caller may attach these sources to the standalone perp recorder or to
    the global market-data bus together with spot and CEX sources.  No source
    here knows which later comparison, hedge, or route may consume its quote.
    """

    hyperliquid = HyperliquidPerpSource(
        hyperliquid_coins,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    aevo = AevoPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    bulk = BulkPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    dydx = DydxPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    lighter = LighterPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    aster = AsterPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    paradex = ParadexPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    extended = ExtendedPerpSource(proxy_url=proxy_url, timeout_seconds=timeout_seconds)
    if (solana_rpc_http_url is None) != (solana_rpc_ws_url is None):
        raise ValueError("both Solana HTTP and WebSocket RPC endpoints are required for Drift")
    drift = (
        DriftOnchainPerpSource(
            rpc_http_url=solana_rpc_http_url,
            rpc_ws_url=solana_rpc_ws_url,
        )
        if solana_rpc_http_url is not None and solana_rpc_ws_url is not None
        else None
    )
    return (
        hyperliquid,
        aevo,
        bulk,
        dydx,
        lighter,
        aster,
        paradex,
        extended,
        *(source for source in (drift,) if source is not None),
    )


async def record_perp_venue_feeds(
    *,
    output_directory: Path,
    duration_seconds: float | None,
    hyperliquid_coins: Sequence[str] = DEFAULT_HYPERLIQUID_COINS,
    solana_rpc_http_url: str | None = None,
    solana_rpc_ws_url: str | None = None,
    proxy_url: str | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    """Run the general perp data layer, without evaluating a trade strategy."""

    sources = build_perp_venue_sources(
        hyperliquid_coins=hyperliquid_coins,
        solana_rpc_http_url=solana_rpc_http_url,
        solana_rpc_ws_url=solana_rpc_ws_url,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    scanner = RealtimeScanner(
        sources=sources,
        output_directory=output_directory,
        retention_seconds=180.0,
        # The live bus is unthrottled.  The retrospective timeline is sampled
        # at most at 50 Hz, so 9k points per market covers the full three
        # minutes without retaining every full-depth L2 update.
        max_events_per_key=9_000,
        history_minimum_interval_ms=20.0,
        event_bus_capacity=16_384,
        status_flush_seconds=2.0,
        status_providers={source.name: source.status for source in sources},
    )
    return await scanner.run(duration_seconds=duration_seconds)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", help="safe output directory name")
    parser.add_argument("--duration-seconds", type=float, help="optional smoke-test duration")
    parser.add_argument(
        "--coins",
        default=",".join(DEFAULT_HYPERLIQUID_COINS),
        help="comma-separated Hyperliquid perpetual coins",
    )
    parser.add_argument("--proxy-url", help="optional per-process HTTP/SOCKS proxy URL")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--output-root", type=Path, default=Path("data/live/perp-market-data"))
    return parser


def main() -> None:
    args = _parser().parse_args()
    coins = tuple(coin.strip().upper() for coin in args.coins.split(",") if coin.strip())
    if not coins:
        raise SystemExit("at least one coin is required")
    if args.duration_seconds is not None and args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds must be positive")
    try:
        configure_process_network_route(args.proxy_url)
        run_id = args.run_id or default_run_id()
        validate_run_id(run_id)
        output = args.output_root / run_id
        manifest = asyncio.run(
            record_perp_venue_feeds(
                output_directory=output,
                duration_seconds=args.duration_seconds,
                hyperliquid_coins=coins,
                proxy_url=args.proxy_url,
                timeout_seconds=args.timeout_seconds,
            ),
        )
    except (OSError, ValueError, RuntimeError) as exc:
        raise SystemExit(f"perp market-data feed failed: {type(exc).__name__}: {exc}") from exc
    print(json.dumps({"output_directory": str(output), "status": manifest["status"]}, sort_keys=True))


if __name__ == "__main__":
    main()
