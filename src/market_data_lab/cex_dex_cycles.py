"""Record and evaluate executable CEX spot <-> DEX inventory cycles.

The scanner is deliberately read-only.  It requests exact-input DEX quotes and
public CEX order-book snapshots, then walks CEX depth for the exact base
quantity returned by the DEX.  DEX pool fees and price impact are already in
the quotes; the configured CEX taker fee and a conservative minimum network
cost are applied separately.

The result is an inventory-cycle screen, not proof of atomic arbitrage.  Funds
must already exist on both venues and later rebalance/transfer costs are not
included.  Wrapped BTC instruments also retain wrapper and redemption basis.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import statistics
import time
import urllib.parse
from collections import defaultdict, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from websockets.asyncio.client import connect as websocket_connect

from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import JUPITER_PROVIDER_BASES
from market_data_lab.dex_quotes import JUPITER_USDT_PROVIDER_BASES
from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import JupiterProvider
from market_data_lab.dex_quotes import OMNISTON_PROVIDER_BASES
from market_data_lab.dex_quotes import OMNISTON_WS_ENDPOINT
from market_data_lab.dex_quotes import OmnistonProvider
from market_data_lab.dex_quotes import RaydiumProvider
from market_data_lab.dex_quotes import SOLANA_PROVIDER_BASES
from market_data_lab.dex_quotes import SOLANA_USDC
from market_data_lab.dex_quotes import SOLANA_USDT
from market_data_lab.dex_quotes import SOLANA_USDT_PROVIDER_BASES
from market_data_lab.dex_quotes import StonFiProvider
from market_data_lab.dex_quotes import TON_PROVIDER_BASES
from market_data_lab.dex_quotes import TON_USDT
from market_data_lab.dex_quotes import TimedResponse
from market_data_lab.dex_quotes import UniswapV3Provider
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import _timed_fetch
from market_data_lab.dex_quotes import evm_markets
from market_data_lab.execution_cost import FeeCurrency
from market_data_lab.execution_cost import cost_to_acquire
from market_data_lab.execution_cost import proceeds_from_sell
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


BYBIT_ORDERBOOK_ENDPOINT = "https://api.bybit.com/v5/market/orderbook"
MEXC_ORDERBOOK_ENDPOINT = "https://api.mexc.com/api/v3/depth"
BINANCE_ORDERBOOK_ENDPOINT = "https://api.binance.com/api/v3/depth"
OKX_ORDERBOOK_ENDPOINT = "https://www.okx.com/api/v5/market/books"
MEXC_PARTIAL_DEPTH_WS_ENDPOINT = "wss://wbs-api.mexc.com/ws"
CEX_BOOK_ENDPOINTS = {
    "BYBIT": BYBIT_ORDERBOOK_ENDPOINT,
    "MEXC": MEXC_ORDERBOOK_ENDPOINT,
    "BINANCE": BINANCE_ORDERBOOK_ENDPOINT,
    "OKX": OKX_ORDERBOOK_ENDPOINT,
}


@dataclass(frozen=True)
class CycleMarket:
    name: str
    provider: str
    chain: str
    dex_pair: str
    cex_symbol: str
    cex_base_symbol: str
    quote_symbol: str
    asset_equivalence: str


def _solana_cex_base_symbol(symbol: str) -> str:
    """Return the CEX ticker for a Solana-wrapped base asset."""

    return "BTC" if symbol == "cbBTC" else symbol


def _solana_asset_equivalence(symbol: str) -> str:
    if symbol == "cbBTC":
        return "Coinbase-custodied Solana cbBTC versus CEX BTC; redemption basis excluded"
    return (
        "same canonical SPL mint on Solana; selected CEX deposit/withdraw network status and "
        "rebalancing path are not verified by this public-data scan"
    )


MARKETS: dict[str, CycleMarket] = {
    "GRAM_TON_STONFI": CycleMarket(
        name="GRAM_TON_STONFI",
        provider="STONFI",
        chain="ton",
        dex_pair="GRAM/USDT",
        cex_symbol="GRAMUSDT",
        cex_base_symbol="GRAM",
        quote_symbol="USDT",
        asset_equivalence="same_native_asset; TON display ticker renamed to GRAM",
    ),
    "SOL_SOLANA_RAYDIUM": CycleMarket(
        name="SOL_SOLANA_RAYDIUM",
        provider="RAYDIUM",
        chain="solana",
        dex_pair="SOL/USDC",
        cex_symbol="SOLUSDC",
        cex_base_symbol="SOL",
        quote_symbol="USDC",
        asset_equivalence="same_native_asset; DEX uses wrapped SOL account representation",
    ),
    "ETH_BASE_UNISWAP": CycleMarket(
        name="ETH_BASE_UNISWAP",
        provider="UNISWAP_BASE",
        chain="base",
        dex_pair="WETH/USDC",
        cex_symbol="ETHUSDC",
        cex_base_symbol="ETH",
        quote_symbol="USDC",
        asset_equivalence="WETH is unwrap-compatible with native ETH on Base",
    ),
    "ETH_POLYGON_UNISWAP": CycleMarket(
        name="ETH_POLYGON_UNISWAP",
        provider="UNISWAP_POLYGON_USDC",
        chain="polygon",
        dex_pair="WETH/USDC",
        cex_symbol="ETHUSDC",
        cex_base_symbol="ETH",
        quote_symbol="USDC",
        asset_equivalence="Polygon PoS bridged WETH versus CEX ETH",
    ),
    "BTC_BASE_UNISWAP": CycleMarket(
        name="BTC_BASE_UNISWAP",
        provider="UNISWAP_BASE_CBBTC",
        chain="base",
        dex_pair="cbBTC/USDC",
        cex_symbol="BTCUSDC",
        cex_base_symbol="BTC",
        quote_symbol="USDC",
        asset_equivalence="Coinbase-custodied cbBTC versus CEX BTC; redemption basis excluded",
    ),
    "BTC_POLYGON_UNISWAP": CycleMarket(
        name="BTC_POLYGON_UNISWAP",
        provider="UNISWAP_POLYGON_WBTC",
        chain="polygon",
        dex_pair="WBTC/USDC",
        cex_symbol="BTCUSDC",
        cex_base_symbol="BTC",
        quote_symbol="USDC",
        asset_equivalence="Polygon PoS bridged WBTC versus CEX BTC; bridge basis excluded",
    ),
    "BTC_SOLANA_RAYDIUM": CycleMarket(
        name="BTC_SOLANA_RAYDIUM",
        provider="RAYDIUM_CBBTC",
        chain="solana",
        dex_pair="cbBTC/USDC",
        cex_symbol="BTCUSDC",
        cex_base_symbol="BTC",
        quote_symbol="USDC",
        asset_equivalence="Coinbase-custodied Solana cbBTC versus CEX BTC; redemption basis excluded",
    ),
}


for provider_name, base in SOLANA_PROVIDER_BASES.items():
    if provider_name in {"RAYDIUM", "RAYDIUM_CBBTC"}:
        continue
    market_name = f"{base.symbol}_SOLANA_RAYDIUM"
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="solana",
        dex_pair=f"{base.symbol}/{SOLANA_USDC.symbol}",
        cex_symbol=f"{base.symbol}USDC",
        cex_base_symbol=_solana_cex_base_symbol(base.symbol),
        quote_symbol=SOLANA_USDC.symbol,
        asset_equivalence=_solana_asset_equivalence(base.symbol),
    )

for provider_name, base in JUPITER_PROVIDER_BASES.items():
    if provider_name == "JUPITER":
        market_name = "SOL_SOLANA_JUPITER"
        cex_symbol = "SOLUSDC"
        cex_base_symbol = "SOL"
        equivalence = "same native asset; DEX uses wrapped SOL account representation"
    elif provider_name == "JUPITER_CBBTC":
        market_name = "BTC_SOLANA_JUPITER"
        cex_symbol = "BTCUSDC"
        cex_base_symbol = "BTC"
        equivalence = _solana_asset_equivalence(base.symbol)
    else:
        market_name = f"{base.symbol}_SOLANA_JUPITER"
        cex_symbol = f"{base.symbol}USDC"
        cex_base_symbol = _solana_cex_base_symbol(base.symbol)
        equivalence = _solana_asset_equivalence(base.symbol)
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="solana",
        dex_pair=f"{base.symbol}/{SOLANA_USDC.symbol}",
        cex_symbol=cex_symbol,
        cex_base_symbol=cex_base_symbol,
        quote_symbol=SOLANA_USDC.symbol,
        asset_equivalence=equivalence,
    )


for provider_name, base in SOLANA_USDT_PROVIDER_BASES.items():
    cex_base_symbol = _solana_cex_base_symbol(base.symbol)
    market_name = f"{cex_base_symbol}_SOLANA_RAYDIUM_USDT"
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="solana",
        dex_pair=f"{base.symbol}/{SOLANA_USDT.symbol}",
        cex_symbol=f"{cex_base_symbol}USDT",
        cex_base_symbol=cex_base_symbol,
        quote_symbol=SOLANA_USDT.symbol,
        asset_equivalence=_solana_asset_equivalence(base.symbol),
    )


for provider_name, base in JUPITER_USDT_PROVIDER_BASES.items():
    cex_base_symbol = _solana_cex_base_symbol(base.symbol)
    market_name = f"{cex_base_symbol}_SOLANA_JUPITER_USDT"
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="solana",
        dex_pair=f"{base.symbol}/{SOLANA_USDT.symbol}",
        cex_symbol=f"{cex_base_symbol}USDT",
        cex_base_symbol=cex_base_symbol,
        quote_symbol=SOLANA_USDT.symbol,
        asset_equivalence=_solana_asset_equivalence(base.symbol),
    )

for provider_name, base in TON_PROVIDER_BASES.items():
    if provider_name == "STONFI":
        continue
    market_name = f"{base.symbol}_TON_STONFI"
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="ton",
        dex_pair=f"{base.symbol}/{TON_USDT.symbol}",
        cex_symbol=f"{base.symbol}USDT",
        cex_base_symbol=base.symbol,
        quote_symbol=TON_USDT.symbol,
        asset_equivalence=(
            "same canonical TON jetton master; Bybit deposit/withdraw status and rebalancing "
            "path are not verified by this public-data scan"
        ),
    )

for provider_name, base in OMNISTON_PROVIDER_BASES.items():
    if provider_name == "OMNISTON":
        market_name = "GRAM_TON_OMNISTON"
        equivalence = "same native asset; TON display ticker renamed to GRAM"
    else:
        market_name = f"{base.symbol}_TON_OMNISTON"
        equivalence = (
            "same canonical TON jetton master; Bybit deposit/withdraw status and rebalancing "
            "path are not verified by this public-data scan"
        )
    MARKETS[market_name] = CycleMarket(
        name=market_name,
        provider=provider_name,
        chain="ton",
        dex_pair=f"{base.symbol}/{TON_USDT.symbol}",
        cex_symbol=f"{base.symbol}USDT",
        cex_base_symbol=base.symbol,
        quote_symbol=TON_USDT.symbol,
        asset_equivalence=equivalence,
    )


# Keep the default scan bounded.  The additional long-tail markets are opt-in
# because each one adds independent public API quote requests every round.
DEFAULT_MARKETS = (
    "GRAM_TON_STONFI",
    "SOL_SOLANA_RAYDIUM",
    "ETH_BASE_UNISWAP",
    "ETH_POLYGON_UNISWAP",
    "BTC_BASE_UNISWAP",
    "BTC_POLYGON_UNISWAP",
    "BTC_SOLANA_RAYDIUM",
)
DEFAULT_NETWORK_COST_FLOORS = {
    "base": Decimal("0.03"),
    "polygon": Decimal("0.02"),
    "solana": Decimal("0.01"),
    "ton": Decimal("0.10"),
}


def market_for_cex(market: CycleMarket, venue: str) -> CycleMarket:
    if venue == "OKX":
        return replace(
            market,
            cex_symbol=f"{market.cex_base_symbol}-{market.quote_symbol}",
        )
    return market


@dataclass(frozen=True)
class BookSnapshot:
    symbol: str
    category: str
    status: str
    error: str | None
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    exchange_system_time_ms: int | None
    matching_engine_time_ms: int | None
    update_id: int | None
    cross_sequence: int | None
    response: TimedResponse
    source: str = "rest"

    def persistence_record(
        self,
        round_id: int,
        phase: str,
        venue: str = "BYBIT",
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "round_id": round_id,
            "phase": phase,
            "venue": venue,
            "source": self.source,
            "category": self.category,
            "symbol": self.symbol,
            "status": self.status,
            "error": self.error,
            "bids": [[_decimal_text(price), _decimal_text(size)] for price, size in self.bids],
            "asks": [[_decimal_text(price), _decimal_text(size)] for price, size in self.asks],
            "exchange_system_time_ms": self.exchange_system_time_ms,
            "matching_engine_time_ms": self.matching_engine_time_ms,
            "update_id": self.update_id,
            "cross_sequence": self.cross_sequence,
            "request_sent_realtime_ns": self.response.sent_realtime_ns,
            "response_received_realtime_ns": self.response.received_realtime_ns,
            "request_sent_monotonic_ns": self.response.sent_monotonic_ns,
            "response_received_monotonic_ns": self.response.received_monotonic_ns,
            "request_rtt_ms": round(self.response.rtt_ms, 6),
        }


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_bybit_book(response: TimedResponse, symbol: str, category: str) -> BookSnapshot:
    if response.error is not None:
        return BookSnapshot(
            symbol=symbol,
            category=category,
            status="request_error",
            error=response.error,
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    payload = response.payload
    if not isinstance(payload, dict) or payload.get("retCode") != 0:
        error = payload.get("retMsg") if isinstance(payload, dict) else repr(payload)
        return BookSnapshot(
            symbol=symbol,
            category=category,
            status="quote_unavailable",
            error=f"Bybit error response: {error}"[:512],
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    result = payload.get("result")
    if not isinstance(result, dict):
        return BookSnapshot(
            symbol=symbol,
            category=category,
            status="quote_unavailable",
            error="Bybit result is not an object",
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )

    def levels(key: str, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        raw = result.get(key)
        if not isinstance(raw, list):
            raise ValueError(f"missing {key} levels")
        parsed: list[tuple[Decimal, Decimal]] = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 2:
                raise ValueError(f"malformed {key} level")
            price = Decimal(str(item[0]))
            size = Decimal(str(item[1]))
            if price > 0 and size > 0:
                parsed.append((price, size))
        if not parsed:
            raise ValueError(f"empty {key} levels")
        parsed.sort(key=lambda level: level[0], reverse=reverse)
        return tuple(parsed)

    try:
        bids = levels("b", reverse=True)
        asks = levels("a", reverse=False)
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed Bybit book")
    except (InvalidOperation, TypeError, ValueError) as exc:
        return BookSnapshot(
            symbol=symbol,
            category=category,
            status="quote_unavailable",
            error=f"invalid Bybit book: {exc}",
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    return BookSnapshot(
        symbol=symbol,
        category=category,
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=_optional_int(result.get("ts")),
        matching_engine_time_ms=_optional_int(result.get("cts")),
        update_id=_optional_int(result.get("u")),
        cross_sequence=_optional_int(result.get("seq")),
        response=response,
    )


async def fetch_bybit_book(
    symbol: str,
    *,
    category: str,
    depth: int,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> BookSnapshot:
    query = urllib.parse.urlencode({"category": category, "symbol": symbol, "limit": depth})
    response = await _timed_fetch(
        fetch_json,
        url=f"{endpoint}?{query}",
        method="GET",
        body=None,
        headers={},
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    return parse_bybit_book(response, symbol, category)


def parse_mexc_book(response: TimedResponse, symbol: str) -> BookSnapshot:
    if response.error is not None:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="request_error",
            error=response.error,
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    payload = response.payload
    code = payload.get("code") if isinstance(payload, dict) else None
    if not isinstance(payload, dict) or (code is not None and str(code) not in {"0", "200"}):
        error = payload.get("msg") if isinstance(payload, dict) else repr(payload)
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"MEXC error response: {error}"[:512],
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )

    def levels(key: str, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        raw = payload.get(key)
        if not isinstance(raw, list):
            raise ValueError(f"missing {key} levels")
        parsed: list[tuple[Decimal, Decimal]] = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 2:
                raise ValueError(f"malformed {key} level")
            price = Decimal(str(item[0]))
            size = Decimal(str(item[1]))
            if price > 0 and size > 0:
                parsed.append((price, size))
        if not parsed:
            raise ValueError(f"empty {key} levels")
        parsed.sort(key=lambda level: level[0], reverse=reverse)
        return tuple(parsed)

    try:
        bids = levels("bids", reverse=True)
        asks = levels("asks", reverse=False)
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed MEXC book")
    except (InvalidOperation, TypeError, ValueError) as exc:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"invalid MEXC book: {exc}",
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=_optional_int(payload.get("timestamp")),
        matching_engine_time_ms=None,
        update_id=_optional_int(payload.get("lastUpdateId")),
        cross_sequence=_optional_int(payload.get("version")),
        response=response,
    )


async def fetch_mexc_book(
    symbol: str,
    *,
    depth: int,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> BookSnapshot:
    query = urllib.parse.urlencode({"symbol": symbol, "limit": depth})
    response = await _timed_fetch(
        fetch_json,
        url=f"{endpoint}?{query}",
        method="GET",
        body=None,
        headers={},
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    return parse_mexc_book(response, symbol)


def _decode_protobuf_varint(data: bytes, offset: int) -> tuple[int, int]:
    """Decode one bounded protobuf varint without a generated protobuf dependency."""

    value = 0
    for index in range(10):
        if offset >= len(data):
            raise ValueError("truncated protobuf varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << (index * 7)
        if not byte & 0x80:
            return value, offset
    raise ValueError("protobuf varint exceeds 10 bytes")


def _decode_protobuf_fields(data: bytes) -> dict[int, list[int | bytes]]:
    """Decode the protobuf wire forms used by MEXC public-depth messages."""

    fields: dict[int, list[int | bytes]] = defaultdict(list)
    offset = 0
    while offset < len(data):
        tag, offset = _decode_protobuf_varint(data, offset)
        field_number = tag >> 3
        wire_type = tag & 0x07
        if field_number == 0:
            raise ValueError("protobuf field number cannot be zero")
        if wire_type == 0:
            value, offset = _decode_protobuf_varint(data, offset)
        elif wire_type == 1:
            if offset + 8 > len(data):
                raise ValueError("truncated fixed64 protobuf field")
            value = data[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            size, offset = _decode_protobuf_varint(data, offset)
            if size > len(data) - offset:
                raise ValueError("truncated length-delimited protobuf field")
            value = data[offset : offset + size]
            offset += size
        elif wire_type == 5:
            if offset + 4 > len(data):
                raise ValueError("truncated fixed32 protobuf field")
            value = data[offset : offset + 4]
            offset += 4
        else:
            raise ValueError(f"unsupported protobuf wire type: {wire_type}")
        fields[field_number].append(value)
    return fields


def _protobuf_text(fields: dict[int, list[int | bytes]], field_number: int) -> str | None:
    values = fields.get(field_number, ())
    if not values or not isinstance(values[-1], bytes):
        return None
    try:
        return values[-1].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid UTF-8 protobuf text for field {field_number}") from exc


def _protobuf_nested_values(
    fields: dict[int, list[int | bytes]],
    field_number: int,
) -> list[bytes]:
    return [value for value in fields.get(field_number, ()) if isinstance(value, bytes)]


def parse_mexc_partial_depth_message(
    message: bytes,
    *,
    received_realtime_ns: int,
    received_monotonic_ns: int,
) -> BookSnapshot:
    """Normalize one MEXC V3 partial-depth protobuf push into a book snapshot.

    MEXC publishes the wrapper's ``publicLimitDepths`` as protobuf field 303.
    Keeping this small decoder local avoids a generated-code dependency while
    intentionally accepting only the wire forms used by the documented feed.
    """

    outer = _decode_protobuf_fields(message)
    channel = _protobuf_text(outer, 1)
    symbol = _protobuf_text(outer, 3)
    depth_payloads = _protobuf_nested_values(outer, 303)
    if channel is None or not channel.startswith("spot@public.limit.depth.v3.api.pb@"):
        raise ValueError("unexpected MEXC websocket channel")
    if symbol is None:
        raise ValueError("MEXC websocket depth message has no symbol")
    if len(depth_payloads) != 1:
        raise ValueError("MEXC websocket depth message has no unique partial-depth payload")
    depth = _decode_protobuf_fields(depth_payloads[0])

    def levels(field_number: int, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        parsed: list[tuple[Decimal, Decimal]] = []
        for raw in _protobuf_nested_values(depth, field_number):
            item = _decode_protobuf_fields(raw)
            price_text = _protobuf_text(item, 1)
            quantity_text = _protobuf_text(item, 2)
            if price_text is None or quantity_text is None:
                raise ValueError("MEXC websocket level is missing price or quantity")
            price = Decimal(price_text)
            quantity = Decimal(quantity_text)
            if price > 0 and quantity > 0:
                parsed.append((price, quantity))
        if not parsed:
            raise ValueError("MEXC websocket depth side is empty")
        parsed.sort(key=lambda level: level[0], reverse=reverse)
        return tuple(parsed)

    try:
        asks = levels(1, reverse=False)
        bids = levels(2, reverse=True)
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed MEXC websocket book")
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid MEXC websocket book: {exc}") from exc

    version = _protobuf_text(depth, 4)
    last_order_times = depth.get(5, ())
    send_times = outer.get(6, ())
    response = TimedResponse(
        payload={"channel": channel, "transport": "websocket_push"},
        error=None,
        sent_realtime_ns=received_realtime_ns,
        received_realtime_ns=received_realtime_ns,
        sent_monotonic_ns=received_monotonic_ns,
        received_monotonic_ns=received_monotonic_ns,
    )
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=(
            int(send_times[-1]) if send_times and isinstance(send_times[-1], int) else None
        ),
        matching_engine_time_ms=(
            int(last_order_times[-1])
            if last_order_times and isinstance(last_order_times[-1], int)
            else None
        ),
        update_id=_optional_int(version),
        cross_sequence=None,
        response=response,
        source="websocket_partial_depth",
    )


class MexcPartialDepthStream:
    """Maintain a bounded history of public MEXC partial-depth snapshots.

    The history is intentionally local receive-time history, rather than an
    exchange sequence reconstruction.  It lets a slow DEX quote be aligned to
    the closest CEX book event, instead of only to a pair of snapshots that
    bracket an entire provider round.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        levels: int,
        timeout_seconds: float,
        proxy_url: str | None,
        endpoint: str = MEXC_PARTIAL_DEPTH_WS_ENDPOINT,
        connect_websocket: Callable[..., Any] = websocket_connect,
        history_capacity_per_symbol: int = 2_048,
        require_all_initial_books: bool = True,
    ) -> None:
        if levels not in {5, 10, 20}:
            raise ValueError("MEXC websocket partial depth supports only 5, 10, or 20 levels")
        unique_symbols = tuple(dict.fromkeys(symbols))
        if not unique_symbols:
            raise ValueError("MEXC websocket partial depth needs at least one symbol")
        if history_capacity_per_symbol <= 0:
            raise ValueError("MEXC websocket history capacity must be positive")
        self.symbols = unique_symbols
        self.levels = levels
        self.timeout_seconds = timeout_seconds
        self.proxy_url = proxy_url
        self.endpoint = endpoint
        self.connect_websocket = connect_websocket
        self.history_capacity_per_symbol = history_capacity_per_symbol
        self.require_all_initial_books = require_all_initial_books
        self._latest: dict[str, BookSnapshot] = {}
        self._history: dict[str, deque[BookSnapshot]] = {
            symbol: deque(maxlen=history_capacity_per_symbol) for symbol in self.symbols
        }
        self._updated = asyncio.Event()
        # The rolling scanner only needs ``nearest_snapshot``.  The continuous
        # monitor also consumes each fresh CEX event, so keep a bounded queue
        # separate from the timing history.  It deliberately drops the oldest
        # item when the consumer falls behind: using an old book for a new DEX
        # quote would be worse than skipping that intermediate update.
        self._updates: asyncio.Queue[BookSnapshot] = asyncio.Queue(maxsize=4_096)
        self._dropped_updates = 0
        self._receiver: asyncio.Task[None] | None = None
        self._websocket: Any = None
        self._error: str | None = None
        self._reconnects = 0
        self._stopping = False

    async def start(self) -> None:
        if self._receiver is not None:
            raise RuntimeError("MEXC websocket stream is already started")
        self._stopping = False
        self._error = None
        self._latest.clear()
        for history in self._history.values():
            history.clear()
        while not self._updates.empty():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._updates.get_nowait()
        self._updated.clear()
        self._receiver = asyncio.create_task(self._receive_loop())
        deadline = time.monotonic() + self.timeout_seconds
        target = set(self.symbols)
        try:
            while set(self._latest) != target:
                self._raise_receiver_failure()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if not self.require_all_initial_books and self._latest:
                        return
                    missing = ", ".join(sorted(target - set(self._latest)))
                    detail = f"; last connection error: {self._error}" if self._error else ""
                    raise TimeoutError(f"MEXC websocket initial books timed out: {missing}{detail}")
                updated = asyncio.create_task(self._updated.wait())
                done, _ = await asyncio.wait(
                    (updated, self._receiver),
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                updated.cancel()
                await asyncio.gather(updated, return_exceptions=True)
                self._updated.clear()
                if self._receiver in done:
                    self._raise_receiver_failure()
        except Exception:
            await self.close()
            raise

    def _raise_receiver_failure(self) -> None:
        receiver = self._receiver
        if receiver is None:
            raise RuntimeError("MEXC websocket stream is not running")
        if not receiver.done():
            return
        try:
            exc = receiver.exception()
        except asyncio.CancelledError:
            raise
        if exc is not None:
            raise RuntimeError(f"MEXC websocket transport failed: {exc}") from exc
        raise RuntimeError("MEXC websocket receiver exited unexpectedly")

    async def _receive_loop(self) -> None:
        channels = [
            f"spot@public.limit.depth.v3.api.pb@{symbol}@{self.levels}"
            for symbol in self.symbols
        ]
        websocket: Any = None
        try:
            websocket = await self.connect_websocket(
                self.endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            self._websocket = websocket
            await websocket.send(
                json.dumps({"method": "SUBSCRIPTION", "params": channels}, separators=(",", ":")),
            )
            while not self._stopping:
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("websocket closed by peer")
                if not isinstance(raw, bytes):
                    continue
                received_realtime_ns = time.time_ns()
                received_monotonic_ns = time.monotonic_ns()
                try:
                    snapshot = parse_mexc_partial_depth_message(
                        raw,
                        received_realtime_ns=received_realtime_ns,
                        received_monotonic_ns=received_monotonic_ns,
                    )
                except (InvalidOperation, ValueError):
                    continue
                if snapshot.symbol not in self.symbols:
                    continue
                self._latest[snapshot.symbol] = snapshot
                self._history[snapshot.symbol].append(snapshot)
                if self._updates.full():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        self._updates.get_nowait()
                    self._dropped_updates += 1
                self._updates.put_nowait(snapshot)
                self._error = None
                self._updated.set()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"
            self._updated.set()
            raise
        finally:
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()
            if self._websocket is websocket:
                self._websocket = None

    def snapshot_batch(
        self,
        symbols: Sequence[str],
        *,
        max_age_ms: Decimal,
    ) -> dict[str, BookSnapshot]:
        now_realtime_ns = time.time_ns()
        max_age_ns = int(max_age_ms * Decimal(1_000_000))
        batch: dict[str, BookSnapshot] = {}
        for symbol in symbols:
            snapshot = self._latest.get(symbol)
            if snapshot is None:
                error = self._error or "MEXC websocket has not produced a book for this symbol"
                response = TimedResponse(
                    payload=None,
                    error=error,
                    sent_realtime_ns=now_realtime_ns,
                    received_realtime_ns=now_realtime_ns,
                    sent_monotonic_ns=time.monotonic_ns(),
                    received_monotonic_ns=time.monotonic_ns(),
                )
                batch[symbol] = BookSnapshot(
                    symbol=symbol,
                    category="spot",
                    status="quote_unavailable",
                    error=error,
                    bids=(),
                    asks=(),
                    exchange_system_time_ms=None,
                    matching_engine_time_ms=None,
                    update_id=None,
                    cross_sequence=None,
                    response=response,
                    source="websocket_partial_depth",
                )
                continue
            age_ns = now_realtime_ns - snapshot.response.received_realtime_ns
            if age_ns <= max_age_ns:
                batch[symbol] = snapshot
                continue
            batch[symbol] = replace(
                snapshot,
                status="stale",
                error=f"MEXC websocket book age {age_ns / 1_000_000:.3f}ms exceeds {max_age_ms}ms",
            )
        return batch

    def nearest_snapshot(
        self,
        symbol: str,
        target_realtime_ns: int,
    ) -> BookSnapshot | None:
        """Return the locally received depth event nearest a DEX response.

        ``calculate_cycle`` remains responsible for rejecting a match whose
        absolute timing skew breaches the caller's configured threshold.  That
        preserves one timing rule for REST and websocket CEX sources while
        avoiding a misleading pair of whole-round brackets for paced quote
        APIs such as Jupiter.
        """

        history = self._history.get(symbol)
        if not history:
            return None
        return min(
            history,
            key=lambda snapshot: abs(
                snapshot.response.received_realtime_ns - target_realtime_ns,
            ),
        )

    async def next_update(self) -> BookSnapshot:
        """Wait for the next locally received partial-depth snapshot."""
        if not self._updates.empty():
            return self._updates.get_nowait()
        self._raise_receiver_failure()
        assert self._receiver is not None
        update = asyncio.create_task(self._updates.get())
        done, _ = await asyncio.wait(
            (update, self._receiver),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if update in done:
            return update.result()
        update.cancel()
        await asyncio.gather(update, return_exceptions=True)
        self._raise_receiver_failure()
        raise RuntimeError("MEXC websocket receiver exited unexpectedly")

    @property
    def dropped_updates(self) -> int:
        """Number of obsolete queued updates discarded under local load."""

        return self._dropped_updates

    @property
    def available_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._latest))

    @property
    def reconnects(self) -> int:
        return self._reconnects

    @property
    def malformed_messages(self) -> int:
        # Invalid protobuf frames are deliberately ignored.  The stream has no
        # separate parser counter yet, so do not fabricate one in diagnostics.
        return 0

    @property
    def error(self) -> str | None:
        return self._error

    async def close(self) -> None:
        self._stopping = True
        if self._receiver is not None:
            self._receiver.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._receiver
            self._receiver = None
        if self._websocket is not None:
            with contextlib.suppress(Exception):
                await self._websocket.close()
            self._websocket = None


def parse_binance_book(response: TimedResponse, symbol: str) -> BookSnapshot:
    if response.error is not None:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="request_error",
            error=response.error,
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    payload = response.payload
    if not isinstance(payload, dict) or "code" in payload:
        error = payload.get("msg") if isinstance(payload, dict) else repr(payload)
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"Binance error response: {error}"[:512],
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )

    def levels(key: str, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        raw = payload.get(key)
        if not isinstance(raw, list):
            raise ValueError(f"missing {key} levels")
        parsed: list[tuple[Decimal, Decimal]] = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 2:
                raise ValueError(f"malformed {key} level")
            price = Decimal(str(item[0]))
            size = Decimal(str(item[1]))
            if price > 0 and size > 0:
                parsed.append((price, size))
        if not parsed:
            raise ValueError(f"empty {key} levels")
        parsed.sort(key=lambda level: level[0], reverse=reverse)
        return tuple(parsed)

    try:
        bids = levels("bids", reverse=True)
        asks = levels("asks", reverse=False)
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed Binance book")
    except (InvalidOperation, TypeError, ValueError) as exc:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"invalid Binance book: {exc}",
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=None,
        matching_engine_time_ms=None,
        update_id=_optional_int(payload.get("lastUpdateId")),
        cross_sequence=None,
        response=response,
    )


async def fetch_binance_book(
    symbol: str,
    *,
    depth: int,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> BookSnapshot:
    query = urllib.parse.urlencode({"symbol": symbol, "limit": depth})
    response = await _timed_fetch(
        fetch_json,
        url=f"{endpoint}?{query}",
        method="GET",
        body=None,
        headers={},
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    return parse_binance_book(response, symbol)


def parse_okx_book(response: TimedResponse, symbol: str) -> BookSnapshot:
    if response.error is not None:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="request_error",
            error=response.error,
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    payload = response.payload
    data = payload.get("data") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("code") != "0"
        or not isinstance(data, list)
        or not data
        or not isinstance(data[0], dict)
    ):
        error = payload.get("msg") if isinstance(payload, dict) else repr(payload)
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"OKX error response: {error}"[:512],
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    book = data[0]

    def levels(key: str, *, reverse: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        raw = book.get(key)
        if not isinstance(raw, list):
            raise ValueError(f"missing {key} levels")
        parsed: list[tuple[Decimal, Decimal]] = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 2:
                raise ValueError(f"malformed {key} level")
            price = Decimal(str(item[0]))
            size = Decimal(str(item[1]))
            if price > 0 and size > 0:
                parsed.append((price, size))
        if not parsed:
            raise ValueError(f"empty {key} levels")
        parsed.sort(key=lambda level: level[0], reverse=reverse)
        return tuple(parsed)

    try:
        bids = levels("bids", reverse=True)
        asks = levels("asks", reverse=False)
        if bids[0][0] >= asks[0][0]:
            raise ValueError("crossed OKX book")
    except (InvalidOperation, TypeError, ValueError) as exc:
        return BookSnapshot(
            symbol=symbol,
            category="spot",
            status="quote_unavailable",
            error=f"invalid OKX book: {exc}",
            bids=(),
            asks=(),
            exchange_system_time_ms=None,
            matching_engine_time_ms=None,
            update_id=None,
            cross_sequence=None,
            response=response,
        )
    return BookSnapshot(
        symbol=symbol,
        category="spot",
        status="ok",
        error=None,
        bids=bids,
        asks=asks,
        exchange_system_time_ms=_optional_int(book.get("ts")),
        matching_engine_time_ms=None,
        update_id=_optional_int(book.get("checksum")),
        cross_sequence=_optional_int(book.get("seqId")),
        response=response,
    )


async def fetch_okx_book(
    symbol: str,
    *,
    depth: int,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> BookSnapshot:
    query = urllib.parse.urlencode({"instId": symbol, "sz": depth})
    response = await _timed_fetch(
        fetch_json,
        url=f"{endpoint}?{query}",
        method="GET",
        body=None,
        headers={},
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    return parse_okx_book(response, symbol)


def _sell_base(levels: Sequence[tuple[Decimal, Decimal]], target_base: Decimal) -> Decimal | None:
    estimate = proceeds_from_sell(
        target_base,
        levels,
        fee_bps=Decimal(0),
        fee_currency="quote",
        base_currency="BASE",
        quote_currency="QUOTE",
        fee_source="no_fee_depth_compatibility_wrapper",
        fee_quality="exact_zero",
    )
    return estimate.gross_book_quote_amount if estimate.complete else None


def _buy_base(levels: Sequence[tuple[Decimal, Decimal]], target_base: Decimal) -> Decimal | None:
    estimate = cost_to_acquire(
        target_base,
        levels,
        fee_bps=Decimal(0),
        fee_currency="quote",
        base_currency="BASE",
        quote_currency="QUOTE",
        fee_source="no_fee_depth_compatibility_wrapper",
        fee_quality="exact_zero",
    )
    return estimate.gross_book_quote_amount if estimate.complete else None


def choose_nearest_book(
    dex_received_ns: int,
    snapshots: Sequence[BookSnapshot],
) -> BookSnapshot | None:
    valid = [snapshot for snapshot in snapshots if snapshot.status == "ok"]
    if not valid:
        return None
    return min(
        valid,
        key=lambda snapshot: abs(snapshot.response.received_realtime_ns - dex_received_ns),
    )


def _best_dex_records(records: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record.get("status") != "ok":
            continue
        notional = record.get("requested_notional_quote")
        direction = record.get("direction")
        if isinstance(notional, str) and isinstance(direction, str):
            grouped[(notional, direction)].append(record)

    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for key, candidates in grouped.items():
        direction = key[1]
        if direction == "buy_base":
            selected[key] = max(candidates, key=lambda item: Decimal(str(item["base_amount"])))
        elif direction == "sell_base":
            selected[key] = max(candidates, key=lambda item: Decimal(str(item["quote_amount"])))
    return selected


def calculate_cycle(
    *,
    market: CycleMarket,
    dex_record: dict[str, Any],
    book: BookSnapshot,
    cex_taker_fee_bps: Decimal,
    network_cost_floor_quote: Decimal,
    max_response_skew_ms: Decimal,
    cex_venue: str = "BYBIT",
    cex_buy_taker_fee_bps: Decimal | None = None,
    cex_sell_taker_fee_bps: Decimal | None = None,
    cex_fee_source: str | None = None,
    cex_fee_account_verified: bool | None = None,
    cex_fee_assumptions: Sequence[str] = (),
    cex_buy_fee_currency: FeeCurrency = "base",
    cex_sell_fee_currency: FeeCurrency = "quote",
) -> dict[str, Any]:
    direction = str(dex_record["direction"])
    base_amount = Decimal(str(dex_record["base_amount"]))
    dex_quote_amount = Decimal(str(dex_record["quote_amount"]))
    buy_fee_bps = cex_buy_taker_fee_bps if cex_buy_taker_fee_bps is not None else cex_taker_fee_bps
    sell_fee_bps = cex_sell_taker_fee_bps if cex_sell_taker_fee_bps is not None else cex_taker_fee_bps
    for label, fee_bps in (("buy", buy_fee_bps), ("sell", sell_fee_bps)):
        if not fee_bps.is_finite() or fee_bps < 0 or fee_bps >= Decimal(10_000):
            raise ValueError(
                f"CEX {label} taker fee must be finite, non-negative, and below 10000 bps"
            )
    fee_source = cex_fee_source or "configured_cex_taker_fee"
    fee_quality = (
        "account_verified"
        if cex_fee_account_verified is True
        else ("public_unverified" if cex_fee_account_verified is False else "unknown")
    )
    state_version = (
        f"{book.source}:update={book.update_id}:sequence={book.cross_sequence}:"
        f"received={book.response.received_realtime_ns}"
    )

    if direction == "buy_base":
        cycle_direction = "buy_dex_sell_cex"
        fee_side_used = "SELL"
        applied_cex_fee_bps = sell_fee_bps
        cex_execution = proceeds_from_sell(
            base_amount,
            book.bids,
            fee_bps=sell_fee_bps,
            fee_currency=cex_sell_fee_currency,
            base_currency=market.cex_base_symbol,
            quote_currency=market.quote_symbol,
            fee_source=fee_source,
            fee_quality=fee_quality,
            state_version=state_version,
        )
        if not cex_execution.complete:
            if cex_execution.status == "insufficient_known_depth":
                raise ValueError("insufficient CEX bid depth")
            raise ValueError(f"invalid CEX bid book: {cex_execution.reason}")
        cex_gross_quote = cex_execution.gross_book_quote_amount
        gross_cost = dex_quote_amount
        gross_proceeds = cex_gross_quote
        net_cost = gross_cost
        net_proceeds = cex_execution.net_quote_movement
        cex_vwap = cex_execution.average_price
    elif direction == "sell_base":
        cycle_direction = "buy_cex_sell_dex"
        fee_side_used = "BUY"
        applied_cex_fee_bps = buy_fee_bps
        cex_execution = cost_to_acquire(
            base_amount,
            book.asks,
            fee_bps=buy_fee_bps,
            fee_currency=cex_buy_fee_currency,
            base_currency=market.cex_base_symbol,
            quote_currency=market.quote_symbol,
            fee_source=fee_source,
            fee_quality=fee_quality,
            state_version=state_version,
        )
        if not cex_execution.complete:
            if cex_execution.status == "insufficient_known_depth":
                raise ValueError("insufficient CEX ask depth")
            raise ValueError(f"invalid CEX ask book: {cex_execution.reason}")
        cex_gross_quote = cex_execution.gross_book_quote_amount
        gross_cost = cex_gross_quote
        gross_proceeds = dex_quote_amount
        # If the fee is charged in base, the Cost Engine walked the larger
        # gross quantity through the actual book.  This differs from simply
        # scaling the cost at the original quantity when another level is
        # crossed.
        net_cost = -cex_execution.net_quote_movement
        net_proceeds = gross_proceeds
        cex_vwap = cex_execution.average_price
    else:
        raise ValueError(f"unsupported DEX direction: {direction}")
    if cex_vwap is None:
        raise ValueError("CEX execution has no average price")

    gross_pnl = gross_proceeds - gross_cost
    net_pnl_before_network = net_proceeds - net_cost
    net_pnl_after_floor = net_pnl_before_network - network_cost_floor_quote
    gross_edge_bps = gross_pnl / gross_cost * Decimal(10_000)
    net_edge_before_network_bps = net_pnl_before_network / net_cost * Decimal(10_000)
    net_edge_after_floor_bps = net_pnl_after_floor / net_cost * Decimal(10_000)
    if cycle_direction == "buy_dex_sell_cex":
        fee_capacity = (gross_proceeds - gross_cost - network_cost_floor_quote) / gross_proceeds
    elif gross_proceeds > network_cost_floor_quote:
        fee_capacity = Decimal(1) - gross_cost / (gross_proceeds - network_cost_floor_quote)
    else:
        fee_capacity = Decimal(-1)
    max_cex_fee_bps_after_floor = max(Decimal(0), fee_capacity * Decimal(10_000))
    dex_received_ns = int(dex_record["response_received_realtime_ns"])
    response_skew_ms = Decimal(
        abs(book.response.received_realtime_ns - dex_received_ns),
    ) / Decimal(1_000_000)
    timing_valid = response_skew_ms <= max_response_skew_ms

    return {
        "schema_version": 1,
        "round_id": dex_record["round_id"],
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "cex_venue": cex_venue,
        "cex_category": book.category,
        "cex_symbol": book.symbol,
        "cycle_direction": cycle_direction,
        "requested_notional_quote": str(dex_record["requested_notional_quote"]),
        "quote_symbol": market.quote_symbol,
        "base_amount": _decimal_text(base_amount),
        "asset_equivalence": market.asset_equivalence,
        "status": "ok" if timing_valid else "timing_skew_exceeded",
        "timing_valid": timing_valid,
        "response_skew_ms": round(float(response_skew_ms), 6),
        "max_response_skew_ms": _decimal_text(max_response_skew_ms),
        "dex_average_price": str(dex_record["average_price_quote_per_base"]),
        "cex_vwap": _decimal_text(cex_vwap),
        "gross_cost_quote": _decimal_text(gross_cost),
        "gross_proceeds_quote": _decimal_text(gross_proceeds),
        "gross_pnl_quote": _decimal_text(gross_pnl),
        "gross_edge_bps": round(float(gross_edge_bps), 6),
        "gross_positive": timing_valid and gross_pnl > 0,
        # ``cex_taker_fee_bps`` remains the direction-specific effective
        # value for backward compatibility.  The two explicit values below
        # preserve exchanges such as Binance where buyer and seller rate
        # components can differ.
        "cex_taker_fee_bps": _decimal_text(applied_cex_fee_bps),
        "cex_taker_buy_fee_bps": _decimal_text(buy_fee_bps),
        "cex_taker_sell_fee_bps": _decimal_text(sell_fee_bps),
        "cex_fee_side_used": fee_side_used,
        "cex_fee_currency_used": cex_execution.fee_currency,
        "cex_fee_source": cex_fee_source,
        "cex_fee_account_verified": cex_fee_account_verified,
        "cex_fee_assumptions": list(cex_fee_assumptions),
        "cex_execution_estimate": cex_execution.as_dict(),
        "cex_book_base_quantity": _decimal_text(
            cex_execution.filled_book_base_quantity
        ),
        "cex_effective_quote_per_requested_base": _decimal_text(
            abs(cex_execution.net_quote_movement) / base_amount
        ),
        # Legacy callers omit this field and retain their prior candidate
        # behavior.  The continuous monitor always supplies a boolean: a
        # configured public baseline may be observed, but cannot be persisted
        # as an account-fee-confirmed candidate.
        "candidate_eligible_with_account_verified_fee": cex_fee_account_verified is not False,
        "net_pnl_before_network_quote": _decimal_text(net_pnl_before_network),
        "net_edge_before_network_bps": round(float(net_edge_before_network_bps), 6),
        "minimum_network_cost_quote": _decimal_text(network_cost_floor_quote),
        "net_pnl_after_minimum_network_quote": _decimal_text(net_pnl_after_floor),
        "net_edge_after_minimum_network_bps": round(float(net_edge_after_floor_bps), 6),
        "positive_before_network": timing_valid and net_pnl_before_network > 0,
        "positive_after_minimum_network": timing_valid and net_pnl_after_floor > 0,
        "max_cex_fee_bps_after_minimum_network": round(
            float(max_cex_fee_bps_after_floor),
            6,
        ),
        "break_even_additional_cost_quote": _decimal_text(max(Decimal(0), net_pnl_before_network)),
        "dex_fee_and_price_impact_included": True,
        "cex_depth_walked": True,
        "network_cost_is_floor_not_priority_auction": True,
        "transfer_rebalance_cost_included": False,
        "funding_or_borrow_cost_included": False,
        "dex_response_received_realtime_ns": dex_received_ns,
        "dex_request_rtt_ms": dex_record.get("request_rtt_ms"),
        "cex_response_received_realtime_ns": book.response.received_realtime_ns,
        "cex_request_rtt_ms": round(book.response.rtt_ms, 6),
        "cex_exchange_system_time_ms": book.exchange_system_time_ms,
        "cex_matching_engine_time_ms": book.matching_engine_time_ms,
        "cex_update_id": book.update_id,
        "cex_cross_sequence": book.cross_sequence,
        "dex_fee_tier": dex_record.get("fee_tier"),
        "dex_fee_bps": dex_record.get("fee_bps"),
        "dex_chain_context": dex_record.get("chain_context"),
        "dex_quote_service_metadata": dex_record.get("quote_service_metadata"),
    }


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[round((len(ordered) - 1) * fraction)]


def summarize_cycles(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[
            (
                str(row.get("market")),
                str(row.get("cycle_direction")),
                str(row.get("requested_notional_quote")),
            )
        ].append(row)
    summary: dict[str, Any] = {}
    for (market, direction, notional), observations in sorted(grouped.items()):
        valid = [row for row in observations if row.get("timing_valid")]
        gross = [float(row["gross_edge_bps"]) for row in valid]
        before = [float(row["net_edge_before_network_bps"]) for row in valid]
        after = [float(row["net_edge_after_minimum_network_bps"]) for row in valid]
        fee_capacity = [float(row["max_cex_fee_bps_after_minimum_network"]) for row in valid]
        key = f"{market}|{direction}|{notional}"
        best = max(valid, key=lambda row: float(row["net_edge_after_minimum_network_bps"])) if valid else None
        summary[key] = {
            "market": market,
            "direction": direction,
            "notional_quote": notional,
            "observations": len(observations),
            "timing_valid_observations": len(valid),
            "gross_positive": sum(value > 0 for value in gross),
            "positive_before_network": sum(value > 0 for value in before),
            "positive_after_minimum_network": sum(value > 0 for value in after),
            "gross_edge_bps": {
                "min": min(gross) if gross else None,
                "median": statistics.median(gross) if gross else None,
                "p95": _percentile(gross, 0.95),
                "max": max(gross) if gross else None,
            },
            "net_edge_before_network_bps": {
                "min": min(before) if before else None,
                "median": statistics.median(before) if before else None,
                "p95": _percentile(before, 0.95),
                "max": max(before) if before else None,
            },
            "net_edge_after_minimum_network_bps": {
                "min": min(after) if after else None,
                "median": statistics.median(after) if after else None,
                "p95": _percentile(after, 0.95),
                "max": max(after) if after else None,
            },
            "max_cex_fee_bps_after_minimum_network": {
                "median": statistics.median(fee_capacity) if fee_capacity else None,
                "p95": _percentile(fee_capacity, 0.95),
                "max": max(fee_capacity) if fee_capacity else None,
            },
            "best_observation": best,
        }
    return summary


def build_cycle_providers(
    market_names: Sequence[str],
    *,
    base_rpc_url: str,
    polygon_rpc_url: str,
    fee_tiers: Sequence[int],
    proxy_url: str | None,
    timeout_seconds: float,
    raydium_slippage_bps: int,
    stonfi_slippage_tolerance: Decimal,
    raydium_min_request_interval_seconds: float = 0.6,
    raydium_request_pacer: AsyncRequestPacer | None = None,
    evm_request_pacer: AsyncRequestPacer | Mapping[str, AsyncRequestPacer] | None = None,
    stonfi_request_pacer: AsyncRequestPacer | None = None,
    omniston_request_pacer: AsyncRequestPacer | None = None,
    evm_min_request_interval_seconds: float = 0.6,
    stonfi_min_request_interval_seconds: float = 0.6,
    omniston_min_request_interval_seconds: float = 0.6,
    jupiter_api_key: str | None = None,
    jupiter_min_request_interval_seconds: float | None = None,
    jupiter_request_pacer: AsyncRequestPacer | None = None,
    omniston_ws_url: str = OMNISTON_WS_ENDPOINT,
    omniston_quote_selection_window_seconds: float = 0.5,
    omniston_max_price_slippage_bps: int = 50,
    omniston_max_routes: int = 4,
    omniston_allow_risky_routes: bool = False,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, DexQuoteProvider]:
    requested = {MARKETS[name].provider for name in market_names}
    configured = evm_markets(
        base_rpc_url=base_rpc_url,
        polygon_rpc_url=polygon_rpc_url,
        fee_tiers=fee_tiers,
    )
    providers: dict[str, DexQuoteProvider] = {}
    jupiter_interval = (
        jupiter_min_request_interval_seconds
        if jupiter_min_request_interval_seconds is not None
        else (1.05 if jupiter_api_key else 2.05)
    )
    jupiter_pacer = jupiter_request_pacer or AsyncRequestPacer(jupiter_interval)
    raydium_pacer = raydium_request_pacer or AsyncRequestPacer(
        raydium_min_request_interval_seconds,
    )

    if evm_request_pacer is None:
        evm_request_pacer = {
            "base": AsyncRequestPacer(evm_min_request_interval_seconds),
            "polygon": AsyncRequestPacer(evm_min_request_interval_seconds),
        }
    stonfi_request_pacer = stonfi_request_pacer or AsyncRequestPacer(
        stonfi_min_request_interval_seconds,
    )
    omniston_request_pacer = omniston_request_pacer or AsyncRequestPacer(
        omniston_min_request_interval_seconds,
    )

    def evm_pacer_for(market_name: str) -> AsyncRequestPacer | None:
        if isinstance(evm_request_pacer, Mapping):
            return evm_request_pacer.get(configured[market_name].chain)
        return evm_request_pacer

    for name in sorted(requested):
        if name in configured:
            providers[name] = UniswapV3Provider(
                configured[name],
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                request_pacer=evm_pacer_for(name),
                fetch_json=fetch_json,
            )
        elif name in SOLANA_PROVIDER_BASES:
            providers[name] = RaydiumProvider(
                name=name,
                base=SOLANA_PROVIDER_BASES[name],
                quote=SOLANA_USDC,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                slippage_bps=raydium_slippage_bps,
                request_pacer=raydium_pacer,
                fetch_json=fetch_json,
            )
        elif name in JUPITER_PROVIDER_BASES:
            providers[name] = JupiterProvider(
                name=name,
                base=JUPITER_PROVIDER_BASES[name],
                quote=SOLANA_USDC,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                request_pacer=jupiter_pacer,
                api_key=jupiter_api_key,
                fetch_json=fetch_json,
            )
        elif name in SOLANA_USDT_PROVIDER_BASES:
            providers[name] = RaydiumProvider(
                name=name,
                base=SOLANA_USDT_PROVIDER_BASES[name],
                quote=SOLANA_USDT,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                slippage_bps=raydium_slippage_bps,
                request_pacer=raydium_pacer,
                fetch_json=fetch_json,
            )
        elif name in JUPITER_USDT_PROVIDER_BASES:
            providers[name] = JupiterProvider(
                name=name,
                base=JUPITER_USDT_PROVIDER_BASES[name],
                quote=SOLANA_USDT,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                request_pacer=jupiter_pacer,
                api_key=jupiter_api_key,
                fetch_json=fetch_json,
            )
        elif name in TON_PROVIDER_BASES:
            providers[name] = StonFiProvider(
                name=name,
                base=TON_PROVIDER_BASES[name],
                quote=TON_USDT,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                slippage_tolerance=stonfi_slippage_tolerance,
                request_pacer=stonfi_request_pacer,
                fetch_json=fetch_json,
            )
        elif name in OMNISTON_PROVIDER_BASES:
            providers[name] = OmnistonProvider(
                name=name,
                base=OMNISTON_PROVIDER_BASES[name],
                quote=TON_USDT,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                quote_selection_window_seconds=omniston_quote_selection_window_seconds,
                max_price_slippage_bps=omniston_max_price_slippage_bps,
                max_routes=omniston_max_routes,
                allow_risky_routes=omniston_allow_risky_routes,
                endpoint=omniston_ws_url,
                request_pacer=omniston_request_pacer,
            )
        else:
            raise ValueError(f"unsupported provider: {name}")
    return providers


async def _book_batch(
    symbols: Sequence[str],
    *,
    venue: str,
    depth: int,
    endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher,
) -> dict[str, BookSnapshot]:
    async def fetch(symbol: str) -> BookSnapshot:
        if venue == "BYBIT":
            return await fetch_bybit_book(
                symbol,
                category="spot",
                depth=depth,
                endpoint=endpoint,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        if venue == "MEXC":
            return await fetch_mexc_book(
                symbol,
                depth=depth,
                endpoint=endpoint,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        if venue == "BINANCE":
            return await fetch_binance_book(
                symbol,
                depth=depth,
                endpoint=endpoint,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        if venue == "OKX":
            return await fetch_okx_book(
                symbol,
                depth=depth,
                endpoint=endpoint,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        raise ValueError(f"unsupported CEX venue: {venue}")

    snapshots = await asyncio.gather(
        *(fetch(symbol) for symbol in symbols),
    )
    return {snapshot.symbol: snapshot for snapshot in snapshots}


async def record_cycle_scan(
    markets: Sequence[CycleMarket],
    providers: dict[str, DexQuoteProvider],
    *,
    notionals: Sequence[Decimal],
    duration_seconds: float,
    interval_seconds: float,
    cex_taker_fee_bps: Decimal,
    network_cost_floors: dict[str, Decimal],
    max_response_skew_ms: Decimal,
    bybit_depth: int,
    bybit_endpoint: str,
    proxy_url: str | None,
    timeout_seconds: float,
    output_directory: Path,
    cex_venue: str = "BYBIT",
    sequential_providers: bool = False,
    mexc_book_source: str = "rest",
    cex_stream_max_age_ms: Decimal = Decimal("500"),
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, Any]:
    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite existing CEX-DEX run: {output_directory}")
    if cex_venue not in CEX_BOOK_ENDPOINTS:
        raise ValueError(f"unsupported CEX venue: {cex_venue}")
    if mexc_book_source not in {"rest", "ws"}:
        raise ValueError("MEXC book source must be 'rest' or 'ws'")
    if mexc_book_source == "ws" and cex_venue != "MEXC":
        raise ValueError("MEXC websocket book source requires --cex-venue MEXC")
    if cex_stream_max_age_ms < 0:
        raise ValueError("CEX stream max age cannot be negative")
    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    books_path = output_directory / f"{cex_venue.lower()}_books.jsonl"
    dex_path = output_directory / "dex_quotes.jsonl"
    cycles_path = output_directory / "cycles.jsonl"
    symbols = sorted({market.cex_symbol for market in markets})
    markets_by_provider: dict[str, list[CycleMarket]] = defaultdict(list)
    for market in markets:
        markets_by_provider[market.provider].append(market)

    mexc_stream: MexcPartialDepthStream | None = None
    if mexc_book_source == "ws":
        mexc_stream = MexcPartialDepthStream(
            symbols,
            levels=bybit_depth,
            timeout_seconds=timeout_seconds,
            proxy_url=proxy_url,
        )
        await mexc_stream.start()

    async def get_book_batch(provider_symbols: Sequence[str]) -> dict[str, BookSnapshot]:
        if mexc_stream is not None:
            return mexc_stream.snapshot_batch(
                provider_symbols,
                max_age_ms=cex_stream_max_age_ms,
            )
        return await _book_batch(
            provider_symbols,
            venue=cex_venue,
            depth=bybit_depth,
            endpoint=bybit_endpoint,
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )

    started_at = datetime.now(UTC)
    started_monotonic_ns = time.monotonic_ns()
    rounds = 0
    all_cycle_rows: list[dict[str, Any]] = []
    provider_errors: dict[str, str] = {}

    def persist_books(
        output: Any,
        round_id: int,
        phase: str,
        batch: dict[str, BookSnapshot],
    ) -> None:
        for snapshot in batch.values():
            output.write(
                json.dumps(
                    snapshot.persistence_record(round_id, phase, cex_venue),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                + "\n",
            )

    with (
        books_path.open("x", encoding="utf-8", buffering=1) as books_output,
        dex_path.open("x", encoding="utf-8", buffering=1) as dex_output,
        cycles_path.open("x", encoding="utf-8", buffering=1) as cycles_output,
    ):
        while True:
            round_started_ns = time.monotonic_ns()
            provider_names = sorted(providers)
            records_by_provider: dict[str, list[dict[str, Any]]] = {}
            book_pairs_by_provider: dict[
                str,
                tuple[dict[str, BookSnapshot], dict[str, BookSnapshot]],
            ] = {}

            if sequential_providers:
                for provider_name in provider_names:
                    request_pacer = getattr(providers[provider_name], "request_pacer", None)
                    if isinstance(request_pacer, AsyncRequestPacer):
                        ready_delay = request_pacer.seconds_until_ready()
                        if ready_delay > 0:
                            await asyncio.sleep(ready_delay)
                    provider_symbols = sorted(
                        {market.cex_symbol for market in markets_by_provider[provider_name]},
                    )
                    pre_books = await get_book_batch(provider_symbols)
                    try:
                        result: list[dict[str, Any]] | BaseException = await providers[
                            provider_name
                        ].quote_round(rounds, notionals)
                    except BaseException as exc:  # preserve a partial research run
                        result = exc
                    post_books = await get_book_batch(provider_symbols)
                    persist_books(books_output, rounds, f"before_dex:{provider_name}", pre_books)
                    persist_books(books_output, rounds, f"after_dex:{provider_name}", post_books)
                    book_pairs_by_provider[provider_name] = (pre_books, post_books)
                    if isinstance(result, BaseException):
                        provider_errors[provider_name] = f"{type(result).__name__}: {result}"
                        records_by_provider[provider_name] = []
                    else:
                        records_by_provider[provider_name] = result
            else:
                pre_books = await get_book_batch(symbols)
                provider_results = await asyncio.gather(
                    *(providers[name].quote_round(rounds, notionals) for name in provider_names),
                    return_exceptions=True,
                )
                post_books = await get_book_batch(symbols)
                persist_books(books_output, rounds, "before_dex", pre_books)
                persist_books(books_output, rounds, "after_dex", post_books)
                for provider_name, result in zip(provider_names, provider_results, strict=True):
                    book_pairs_by_provider[provider_name] = (pre_books, post_books)
                    if isinstance(result, BaseException):
                        provider_errors[provider_name] = f"{type(result).__name__}: {result}"
                        records_by_provider[provider_name] = []
                    else:
                        records_by_provider[provider_name] = result

            for provider_name in provider_names:
                records = records_by_provider[provider_name]
                for record in records:
                    dex_output.write(
                        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n",
                    )

            for provider_name, provider_markets in markets_by_provider.items():
                selected = _best_dex_records(records_by_provider.get(provider_name, []))
                pre_books, post_books = book_pairs_by_provider[provider_name]
                for market in provider_markets:
                    for notional in notionals:
                        notional_text = _decimal_text(notional)
                        for dex_direction in ("buy_base", "sell_base"):
                            dex_record = selected.get((notional_text, dex_direction))
                            if dex_record is None:
                                continue
                            dex_received_ns = int(dex_record["response_received_realtime_ns"])
                            if mexc_stream is not None:
                                book = mexc_stream.nearest_snapshot(
                                    market.cex_symbol,
                                    dex_received_ns,
                                )
                                if book is not None:
                                    persist_books(
                                        books_output,
                                        rounds,
                                        f"matched_dex:{provider_name}",
                                        {market.cex_symbol: book},
                                    )
                            else:
                                book = choose_nearest_book(
                                    dex_received_ns,
                                    (
                                        pre_books[market.cex_symbol],
                                        post_books[market.cex_symbol],
                                    ),
                                )
                            if book is None:
                                continue
                            try:
                                cycle = calculate_cycle(
                                    market=market,
                                    dex_record=dex_record,
                                    book=book,
                                    cex_taker_fee_bps=cex_taker_fee_bps,
                                    network_cost_floor_quote=network_cost_floors[market.chain],
                                    max_response_skew_ms=max_response_skew_ms,
                                    cex_venue=cex_venue,
                                )
                            except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
                                cycle = {
                                    "schema_version": 1,
                                    "round_id": rounds,
                                    "market": market.name,
                                    "cycle_direction": (
                                        "buy_dex_sell_cex"
                                        if dex_direction == "buy_base"
                                        else "buy_cex_sell_dex"
                                    ),
                                    "requested_notional_quote": notional_text,
                                    "quote_symbol": market.quote_symbol,
                                    "status": "calculation_error",
                                    "error": f"{type(exc).__name__}: {exc}",
                                    "timing_valid": False,
                                }
                            cycles_output.write(
                                json.dumps(cycle, ensure_ascii=False, separators=(",", ":")) + "\n",
                            )
                            all_cycle_rows.append(cycle)
            rounds += 1
            elapsed = (time.monotonic_ns() - started_monotonic_ns) / 1_000_000_000
            if elapsed >= duration_seconds:
                break
            round_elapsed = (time.monotonic_ns() - round_started_ns) / 1_000_000_000
            await asyncio.sleep(min(max(0.0, interval_seconds - round_elapsed), duration_seconds - elapsed))

    if mexc_stream is not None:
        await mexc_stream.close()
    stopped_at = datetime.now(UTC)
    finished_monotonic_ns = time.monotonic_ns()
    summary = summarize_cycles(all_cycle_rows)
    atomic_json(output_directory / "summary.json", summary)
    usable = sum(row.get("status") == "ok" for row in all_cycle_rows)
    manifest: dict[str, Any] = {
        "status": "ok" if usable else "error",
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_requested_seconds": duration_seconds,
        "duration_wall_seconds": round(
            (finished_monotonic_ns - started_monotonic_ns) / 1_000_000_000,
            6,
        ),
        "rounds": rounds,
        "interval_seconds": interval_seconds,
        "provider_execution_mode": "sequential" if sequential_providers else "parallel",
        "notionals_quote": [_decimal_text(value) for value in notionals],
        "markets": [market.__dict__ for market in markets],
        "providers": [providers[name].config() for name in sorted(providers)],
        "cex": {
            "venue": cex_venue,
            "category": "spot",
            "endpoint_origin": _redact_url(
                MEXC_PARTIAL_DEPTH_WS_ENDPOINT if mexc_stream is not None else bybit_endpoint,
            ),
            "book_depth": bybit_depth,
            "book_source": "websocket_partial_depth" if mexc_stream is not None else "rest",
            "stream_max_age_ms": (
                _decimal_text(cex_stream_max_age_ms) if mexc_stream is not None else None
            ),
            "stream_history_capacity_per_symbol": (
                mexc_stream.history_capacity_per_symbol if mexc_stream is not None else None
            ),
            "taker_fee_bps": _decimal_text(cex_taker_fee_bps),
        },
        "minimum_network_cost_quote_by_chain": {
            chain: _decimal_text(value) for chain, value in network_cost_floors.items()
        },
        "max_response_skew_ms": _decimal_text(max_response_skew_ms),
        "network_route": network_route,
        "provider_errors": provider_errors,
        "cycle_observations": len(all_cycle_rows),
        "usable_cycle_observations": usable,
        "api_credentials_used": any(
            provider.config().get("api_credentials_used") is True for provider in providers.values()
        ),
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "files": {
            "cex_books": str(books_path.resolve()),
            "dex_quotes": str(dex_path.resolve()),
            "cycles": str(cycles_path.resolve()),
            "summary": str((output_directory / "summary.json").resolve()),
        },
        "model_scope": {
            "included": [
                "DEX exact-input pool fee and price impact",
                f"{cex_venue} spot order-book depth walk for the same base amount",
                f"configured {cex_venue} taker fee",
                "configured minimum per-transaction network-cost floor",
                *(
                    [
                        "MEXC websocket partial-depth events limited by configured maximum age",
                    ]
                    if mexc_stream is not None
                    else []
                ),
            ],
            "excluded": [
                "priority auction or Jito tip above the minimum floor",
                "deposit, withdrawal, bridge, wrapper redemption and rebalancing costs",
                "fill probability, adverse selection and state change before inclusion",
                "capital, borrow and inventory carrying costs",
            ],
            "interpretation": (
                "A positive row is only a candidate for a pre-funded inventory cycle; it is not "
                "proof of atomic or withdraw-and-transfer arbitrage."
            ),
        },
    }
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parse_names(value: str) -> list[str]:
    names = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    invalid = [name for name in names if name not in MARKETS]
    if not names or invalid:
        raise argparse.ArgumentTypeError(
            f"markets must be comma-separated values from {', '.join(MARKETS)}",
        )
    return names


def _parse_decimals(value: str, label: str) -> list[Decimal]:
    try:
        values = list(dict.fromkeys(Decimal(item.strip()) for item in value.split(",")))
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated decimals") from exc
    if not values or any(item <= 0 or not item.is_finite() for item in values):
        raise argparse.ArgumentTypeError(f"{label} must contain positive finite values")
    return values


def _parse_ints(value: str, label: str) -> list[int]:
    try:
        values = list(dict.fromkeys(int(item.strip()) for item in value.split(",")))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated integers") from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{label} must contain positive values")
    return values


def _parse_costs(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_NETWORK_COST_FLOORS)
    try:
        for item in value.split(","):
            chain, amount = item.strip().split("=", 1)
            if chain not in parsed:
                raise ValueError(f"unknown chain {chain}")
            parsed[chain] = Decimal(amount)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "network costs must look like base=0.03,polygon=0.02,solana=0.01,ton=0.10",
        ) from exc
    if any(value < 0 or not value.is_finite() for value in parsed.values()):
        raise argparse.ArgumentTypeError("network costs must be finite and non-negative")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=_parse_names, default=list(DEFAULT_MARKETS))
    parser.add_argument(
        "--notionals",
        type=lambda value: _parse_decimals(value, "notionals"),
        default=_parse_decimals("100,1000", "notionals"),
    )
    parser.add_argument(
        "--uniswap-fee-tiers",
        type=lambda value: _parse_ints(value, "uniswap-fee-tiers"),
        default=_parse_ints("100,500,3000", "uniswap-fee-tiers"),
    )
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--cex-venue", choices=tuple(CEX_BOOK_ENDPOINTS), default="BYBIT")
    parser.add_argument(
        "--mexc-book-source",
        choices=("rest", "ws"),
        default="rest",
        help="MEXC only: REST snapshots or persistent documented partial-depth websocket",
    )
    parser.add_argument(
        "--cex-stream-max-age-ms",
        type=Decimal,
        default=Decimal("500"),
        help="Reject a websocket CEX book older than this many milliseconds",
    )
    parser.add_argument("--cex-depth", "--bybit-depth", dest="cex_depth", type=int)
    parser.add_argument(
        "--cex-taker-fee-bps",
        "--bybit-taker-fee-bps",
        dest="cex_taker_fee_bps",
        type=Decimal,
        help="Configured fee; defaults to 10 bps for Bybit and 5 bps MEXC standard spot (not account-specific)",
    )
    parser.add_argument("--max-response-skew-ms", type=Decimal, default=Decimal("1000"))
    parser.add_argument(
        "--minimum-network-costs",
        type=_parse_costs,
        default=dict(DEFAULT_NETWORK_COST_FLOORS),
    )
    parser.add_argument("--raydium-slippage-bps", type=int, default=50)
    parser.add_argument(
        "--raydium-min-request-interval-seconds",
        type=float,
        default=0.6,
        help="Shared Raydium request-start spacing; 0.6 stays below its public 120 requests/minute IP limit",
    )
    parser.add_argument(
        "--jupiter-api-key-env",
        default="JUPITER_API_KEY",
        help="Environment variable containing an optional Jupiter API key; never persisted",
    )
    parser.add_argument(
        "--jupiter-min-request-interval-seconds",
        type=float,
        help="Shared Jupiter request-start spacing; defaults to 2.05 keyless or 1.05 with a key",
    )
    parser.add_argument(
        "--sequential-providers",
        action="store_true",
        help="Bracket each provider with its own CEX snapshots (recommended for rate-limited APIs)",
    )
    parser.add_argument("--stonfi-slippage-tolerance", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--omniston-ws-url", default=OMNISTON_WS_ENDPOINT)
    parser.add_argument("--omniston-quote-selection-window-seconds", type=float, default=0.5)
    parser.add_argument("--omniston-max-price-slippage-bps", type=int, default=50)
    parser.add_argument("--omniston-max-routes", type=int, default=4)
    parser.add_argument("--omniston-allow-risky-routes", action="store_true")
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--cex-endpoint", "--bybit-endpoint", dest="cex_endpoint")
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/cex-dex"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    cex_depth = (
        args.cex_depth
        if args.cex_depth is not None
        else (100 if args.cex_venue in {"MEXC", "BINANCE"} else 200)
    )
    cex_taker_fee_bps = (
        args.cex_taker_fee_bps
        if args.cex_taker_fee_bps is not None
        else (Decimal("5") if args.cex_venue == "MEXC" else Decimal("10"))
    )
    cex_endpoint = args.cex_endpoint or CEX_BOOK_ENDPOINTS[args.cex_venue]
    if args.duration_seconds <= 0 or args.interval_seconds <= 0 or args.timeout_seconds <= 0:
        raise SystemExit("duration, interval, and timeout must be positive")
    if not 1 <= cex_depth <= 1000:
        raise SystemExit("--cex-depth must be in [1, 1000]")
    if args.cex_venue == "MEXC" and cex_depth not in {5, 10, 20, 50, 100, 500, 1000}:
        raise SystemExit("MEXC --cex-depth must be one of 5,10,20,50,100,500,1000")
    if args.cex_venue == "BINANCE" and cex_depth not in {5, 10, 20, 50, 100, 500, 1000}:
        raise SystemExit("Binance --cex-depth must be one of 5,10,20,50,100,500,1000")
    if args.cex_venue == "OKX" and cex_depth > 400:
        raise SystemExit("OKX --cex-depth must be at most 400")
    if cex_taker_fee_bps < 0 or cex_taker_fee_bps >= 10_000:
        raise SystemExit("--cex-taker-fee-bps must be in [0, 10000)")
    if args.max_response_skew_ms < 0:
        raise SystemExit("--max-response-skew-ms cannot be negative")
    if args.cex_stream_max_age_ms < 0:
        raise SystemExit("--cex-stream-max-age-ms cannot be negative")
    if args.mexc_book_source == "ws" and args.cex_venue != "MEXC":
        raise SystemExit("--mexc-book-source ws requires --cex-venue MEXC")
    if args.mexc_book_source == "ws" and cex_depth not in {5, 10, 20}:
        raise SystemExit("MEXC websocket partial depth supports --cex-depth 5,10,20 only")
    if args.raydium_slippage_bps < 0:
        raise SystemExit("--raydium-slippage-bps cannot be negative")
    if args.raydium_min_request_interval_seconds < 0:
        raise SystemExit("--raydium-min-request-interval-seconds cannot be negative")
    if (
        args.jupiter_min_request_interval_seconds is not None
        and args.jupiter_min_request_interval_seconds < 0
    ):
        raise SystemExit("--jupiter-min-request-interval-seconds cannot be negative")
    if not Decimal(0) <= args.stonfi_slippage_tolerance < Decimal(1):
        raise SystemExit("--stonfi-slippage-tolerance must be in [0, 1)")
    if args.omniston_quote_selection_window_seconds < 0:
        raise SystemExit("--omniston-quote-selection-window-seconds cannot be negative")
    if args.omniston_max_price_slippage_bps < 0:
        raise SystemExit("--omniston-max-price-slippage-bps cannot be negative")
    if args.omniston_max_routes <= 0:
        raise SystemExit("--omniston-max-routes must be positive")
    run_id = args.run_id or default_run_id()
    validate_run_id(run_id)
    providers = build_cycle_providers(
        args.markets,
        base_rpc_url=args.base_rpc_url,
        polygon_rpc_url=args.polygon_rpc_url,
        fee_tiers=args.uniswap_fee_tiers,
        proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            raydium_slippage_bps=args.raydium_slippage_bps,
            raydium_min_request_interval_seconds=args.raydium_min_request_interval_seconds,
            stonfi_slippage_tolerance=args.stonfi_slippage_tolerance,
        jupiter_api_key=(
            os.getenv(args.jupiter_api_key_env) if args.jupiter_api_key_env else None
        ),
        jupiter_min_request_interval_seconds=args.jupiter_min_request_interval_seconds,
        omniston_ws_url=args.omniston_ws_url,
        omniston_quote_selection_window_seconds=(
            args.omniston_quote_selection_window_seconds
        ),
        omniston_max_price_slippage_bps=args.omniston_max_price_slippage_bps,
        omniston_max_routes=args.omniston_max_routes,
        omniston_allow_risky_routes=args.omniston_allow_risky_routes,
    )
    selected_markets = [market_for_cex(MARKETS[name], args.cex_venue) for name in args.markets]
    manifest = asyncio.run(
        record_cycle_scan(
            selected_markets,
            providers,
            notionals=args.notionals,
            duration_seconds=args.duration_seconds,
            interval_seconds=args.interval_seconds,
            cex_taker_fee_bps=cex_taker_fee_bps,
            network_cost_floors=args.minimum_network_costs,
            max_response_skew_ms=args.max_response_skew_ms,
            bybit_depth=cex_depth,
            bybit_endpoint=cex_endpoint,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            output_directory=args.output_root / run_id,
            cex_venue=args.cex_venue,
            sequential_providers=args.sequential_providers,
            mexc_book_source=args.mexc_book_source,
            cex_stream_max_age_ms=args.cex_stream_max_age_ms,
        ),
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    if manifest["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
