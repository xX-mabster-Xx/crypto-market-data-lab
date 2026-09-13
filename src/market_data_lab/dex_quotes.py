"""Record read-only executable DEX quotes on EVM, Solana, and TON.

The recorder deliberately does not accept a wallet or a private key and never
submits transactions.  EVM quotes are direct ``eth_call`` simulations against
Uniswap v3 QuoterV2.  Raydium and STON.fi are API-based reconnaissance feeds;
their timestamps measure the quote service as observed by this host, not raw
validator arrival time.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Protocol

from websockets.asyncio.client import connect as websocket_connect

from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


USER_AGENT = "crypto-market-data-lab/0.1"
QUOTER_V2_EXACT_INPUT_SINGLE_SELECTOR = "c6a5026a"
# ``quoteExactOutputSingle((address,address,uint256,uint24,uint160))``.
# The tuple layout is the same as exact-input, but its ``amount`` field is
# the requested output and the first return word is therefore the required
# input.  Keep the selector explicit: this module uses raw ``eth_call`` and
# deliberately has no transaction ABI/client.
QUOTER_V2_EXACT_OUTPUT_SINGLE_SELECTOR = "bd21704a"
TON_NATIVE_ADDRESS = "EQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAM9c"
TON_USDT_ADDRESS = "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"
SOL_WRAPPED_MINT = "So11111111111111111111111111111111111111112"
SOL_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOL_USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
SOL_CBBTC_MINT = "cbbtcf3aa214zXHbiAZQwf4122FBYbraNdFqgw4iMij"


JsonFetcher = Callable[[str, str, bytes | None, dict[str, str], str | None, float], Any]


@dataclass(frozen=True)
class Asset:
    symbol: str
    address: str
    decimals: int


SOLANA_PROVIDER_BASES: dict[str, Asset] = {
    "RAYDIUM": Asset("SOL", SOL_WRAPPED_MINT, 9),
    "RAYDIUM_CBBTC": Asset("cbBTC", SOL_CBBTC_MINT, 8),
    "RAYDIUM_JUP": Asset("JUP", "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN", 6),
    "RAYDIUM_MEW": Asset("MEW", "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5", 5),
    "RAYDIUM_PUMP": Asset("PUMP", "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn", 6),
    "RAYDIUM_PYTH": Asset("PYTH", "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3", 6),
    "RAYDIUM_RENDER": Asset("RENDER", "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof", 8),
    "RAYDIUM_TRUMP": Asset("TRUMP", "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN", 6),
    "RAYDIUM_HNT": Asset("HNT", "hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux", 8),
    "RAYDIUM_BONK": Asset("BONK", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", 5),
    "RAYDIUM_FARTCOIN": Asset(
        "FARTCOIN",
        "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
        6,
    ),
    "RAYDIUM_PNUT": Asset("PNUT", "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump", 6),
    "RAYDIUM_POPCAT": Asset(
        "POPCAT",
        "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        9,
    ),
}
SOLANA_USDC = Asset("USDC", SOL_USDC_MINT, 6)
SOLANA_USDT = Asset("USDT", SOL_USDT_MINT, 6)
JUPITER_PROVIDER_BASES: dict[str, Asset] = {
    name.replace("RAYDIUM", "JUPITER", 1): asset
    for name, asset in SOLANA_PROVIDER_BASES.items()
}
SOLANA_USDT_PROVIDER_BASES: dict[str, Asset] = {
    name.replace("RAYDIUM", "RAYDIUM_USDT", 1): asset
    for name, asset in SOLANA_PROVIDER_BASES.items()
}
JUPITER_USDT_PROVIDER_BASES: dict[str, Asset] = {
    name.replace("JUPITER", "JUPITER_USDT", 1): asset
    for name, asset in JUPITER_PROVIDER_BASES.items()
}

TON_PROVIDER_BASES: dict[str, Asset] = {
    # TON renamed the native asset's display ticker to GRAM in June 2026.
    # The chain and native-asset address did not change.
    "STONFI": Asset("GRAM", TON_NATIVE_ADDRESS, 9),
    "STONFI_MAJOR": Asset(
        "MAJOR",
        "EQCuPm01HldiduQ55xaBF_1kaW_WAUy5DHey8suqzU_MAJOR",
        9,
    ),
    "STONFI_NOT": Asset("NOT", "EQAvlWFDxGF2lXm67y4yzC17wYKD9A0guwPkMs1gOsM__NOT", 9),
    "STONFI_CATI": Asset("CATI", "EQD-cvR0Nz6XAyRBvbhz-abTrRC6sI5tvHvvpeQraV9UAAD7", 9),
    "STONFI_DOGS": Asset("DOGS", "EQCvxJy4eG8hyHBFsZ7eePxrRsUQSFE_jpptRAYBmcG_DOGS", 9),
    "STONFI_HMSTR": Asset("HMSTR", "EQAJ8uWd7EBqsmpSWaRdf_I-8R8-XHwh3gsNKhy-UrdrPcUo", 9),
}
TON_USDT = Asset("USDT", TON_USDT_ADDRESS, 6)
OMNISTON_PROVIDER_BASES: dict[str, Asset] = {
    name.replace("STONFI", "OMNISTON", 1): asset for name, asset in TON_PROVIDER_BASES.items()
}

OMNISTON_WS_ENDPOINT = "wss://omni-ws.ston.fi"
OMNISTON_QUOTE_METHOD = "stonfi.omni.v1beta8.QuoteRpc.Quote"
OMNISTON_QUOTE_UNSUBSCRIBE_METHOD = f"{OMNISTON_QUOTE_METHOD}.unsubscribe"


@dataclass(frozen=True)
class EvmMarket:
    provider: str
    chain: str
    chain_id: int
    protocol: str
    rpc_url: str
    block_tag: str
    quoter_address: str
    base: Asset
    quote: Asset
    fee_tiers: tuple[int, ...]


@dataclass(frozen=True)
class TimedResponse:
    payload: Any | None
    error: str | None
    sent_realtime_ns: int
    received_realtime_ns: int
    sent_monotonic_ns: int
    received_monotonic_ns: int

    @property
    def rtt_ms(self) -> float:
        return (self.received_monotonic_ns - self.sent_monotonic_ns) / 1_000_000


class AsyncRequestPacer:
    """Space request starts across providers sharing one public API quota."""

    def __init__(self, minimum_interval_seconds: float) -> None:
        if minimum_interval_seconds < 0:
            raise ValueError("minimum request interval cannot be negative")
        self.minimum_interval_seconds = minimum_interval_seconds
        self._lock = asyncio.Lock()
        self._next_start_monotonic = 0.0

    async def wait(self) -> None:
        async with self._lock:
            delay = self._next_start_monotonic - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next_start_monotonic = time.monotonic() + self.minimum_interval_seconds

    def seconds_until_ready(self) -> float:
        """Return a best-effort delay hint for nearby timestamp bracketing."""
        return max(0.0, self._next_start_monotonic - time.monotonic())

    async def defer(
        self,
        *,
        cooldown_seconds: float,
        minimum_interval_seconds: float | None = None,
    ) -> None:
        """Atomically slow and pause a shared public-request budget.

        This is used after an explicit server-side rate-limit response.  Calls
        already queued in :meth:`wait` share the same lock, so at most the
        request currently being started can escape the newly installed pause.
        """

        if cooldown_seconds < 0:
            raise ValueError("cooldown cannot be negative")
        if minimum_interval_seconds is not None and minimum_interval_seconds < 0:
            raise ValueError("minimum request interval cannot be negative")
        async with self._lock:
            if minimum_interval_seconds is not None:
                self.minimum_interval_seconds = max(
                    self.minimum_interval_seconds,
                    minimum_interval_seconds,
                )
            self._next_start_monotonic = max(
                self._next_start_monotonic,
                time.monotonic() + cooldown_seconds,
            )


class DexQuoteProvider(Protocol):
    name: str

    async def quote_round(
        self,
        round_id: int,
        notionals: Sequence[Decimal],
    ) -> list[dict[str, Any]]: ...


def quote_route_labels(record: Mapping[str, Any]) -> tuple[str, ...]:
    """Return bounded DEX labels from an aggregator quote record.

    Jupiter may split one quote across several venues.  Only short venue
    labels are retained; account keys, serialized transactions and the raw
    route plan remain ephemeral.
    """

    metadata = record.get("quote_service_metadata")
    if not isinstance(metadata, Mapping):
        return ()
    raw_plan = metadata.get("route_plan")
    if not isinstance(raw_plan, list):
        return ()
    labels: list[str] = []
    for leg in raw_plan[:16]:
        if not isinstance(leg, Mapping):
            continue
        swap_info = leg.get("swapInfo")
        source = swap_info if isinstance(swap_info, Mapping) else leg
        label = source.get("label")
        if not isinstance(label, str):
            continue
        compact = label.strip()[:64]
        if compact:
            labels.append(compact)
    return tuple(dict.fromkeys(labels))

    def config(self) -> dict[str, Any]: ...


def _redact_url(url: str) -> str:
    """Keep only the endpoint origin so path/query API keys are never persisted."""
    parsed = urllib.parse.urlsplit(url)
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    return f"{parsed.scheme}://{hostname}{port}"


def _fetch_json_sync(
    url: str,
    method: str,
    body: bytes | None,
    headers: dict[str, str],
    proxy_url: str | None,
    timeout_seconds: float,
) -> Any:
    if proxy_url and not proxy_url.startswith(("http://", "https://")):
        raise ValueError("DEX recorder supports direct, HTTP, or HTTPS proxy URLs")
    handler = (
        urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        if proxy_url
        else urllib.request.ProxyHandler({})
    )
    opener = urllib.request.build_opener(handler)
    request_headers = {"Accept": "application/json", "User-Agent": USER_AGENT, **headers}
    request = urllib.request.Request(
        url,
        data=body,
        headers=request_headers,
        method=method,
    )
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read(512).decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


async def _timed_fetch(
    fetch_json: JsonFetcher,
    *,
    url: str,
    method: str,
    body: bytes | None,
    headers: dict[str, str],
    proxy_url: str | None,
    timeout_seconds: float,
) -> TimedResponse:
    sent_realtime_ns = time.time_ns()
    sent_monotonic_ns = time.monotonic_ns()
    try:
        loop = asyncio.get_running_loop()
        result: asyncio.Future[Any] = loop.create_future()

        def settle(value: Any = None, error: BaseException | None = None) -> None:
            if result.done():
                return
            if error is None:
                result.set_result(value)
            else:
                result.set_exception(error)

        def worker() -> None:
            try:
                value = fetch_json(
                    url,
                    method,
                    body,
                    headers,
                    proxy_url,
                    timeout_seconds,
                )
            except BaseException as exc:
                loop.call_soon_threadsafe(settle, None, exc)
            else:
                loop.call_soon_threadsafe(settle, value, None)

        # Python 3.14's default asyncio executor hangs in some minimal
        # environments.  A bounded HTTP timeout plus daemon worker preserves
        # concurrency without making interpreter shutdown depend on it.
        threading.Thread(target=worker, name="dex-http", daemon=True).start()
        payload = await result
        error = None
    except Exception as exc:
        payload = None
        error = f"{type(exc).__name__}: {exc}"
    received_monotonic_ns = time.monotonic_ns()
    received_realtime_ns = time.time_ns()
    return TimedResponse(
        payload=payload,
        error=error,
        sent_realtime_ns=sent_realtime_ns,
        received_realtime_ns=received_realtime_ns,
        sent_monotonic_ns=sent_monotonic_ns,
        received_monotonic_ns=received_monotonic_ns,
    )


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _to_raw(value: Decimal, decimals: int) -> int:
    raw = (value * (Decimal(10) ** decimals)).to_integral_value(rounding=ROUND_DOWN)
    if raw <= 0:
        raise ValueError("amount becomes zero in blockchain units")
    return int(raw)


def _from_raw(value: int, decimals: int) -> Decimal:
    return Decimal(value) / (Decimal(10) ** decimals)


def _timing_fields(response: TimedResponse) -> dict[str, int | float]:
    return {
        "request_sent_realtime_ns": response.sent_realtime_ns,
        "response_received_realtime_ns": response.received_realtime_ns,
        "request_sent_monotonic_ns": response.sent_monotonic_ns,
        "response_received_monotonic_ns": response.received_monotonic_ns,
        "request_rtt_ms": round(response.rtt_ms, 6),
    }


def _price_fields(
    *,
    direction: str,
    input_raw: int,
    output_raw: int,
    base: Asset,
    quote: Asset,
) -> dict[str, str]:
    if direction == "buy_base":
        quote_amount = _from_raw(input_raw, quote.decimals)
        base_amount = _from_raw(output_raw, base.decimals)
    elif direction == "sell_base":
        base_amount = _from_raw(input_raw, base.decimals)
        quote_amount = _from_raw(output_raw, quote.decimals)
    else:
        raise ValueError(f"unsupported direction: {direction}")
    price = quote_amount / base_amount
    return {
        "base_amount": _decimal_text(base_amount),
        "quote_amount": _decimal_text(quote_amount),
        "average_price_quote_per_base": _decimal_text(price),
    }


def _encode_quoter_v2_single(
    selector: str,
    token_in: str,
    token_out: str,
    amount: int,
    fee: int,
    sqrt_price_limit_x96: int = 0,
) -> str:
    """ABI-encode one all-static QuoterV2 single-pool tuple."""

    if len(selector) != 8:
        raise ValueError("QuoterV2 selector must be four bytes encoded as eight hex characters")
    try:
        int(selector, 16)
    except ValueError as exc:
        raise ValueError("QuoterV2 selector is not hexadecimal") from exc
    if amount <= 0:
        raise ValueError("amount must be positive")
    if not 0 <= fee < 2**24:
        raise ValueError("fee must fit uint24")
    if not 0 <= sqrt_price_limit_x96 < 2**160:
        raise ValueError("sqrt_price_limit_x96 must fit uint160")

    def address_word(value: str) -> str:
        normalized = value.lower().removeprefix("0x")
        if len(normalized) != 40:
            raise ValueError(f"invalid EVM address: {value}")
        try:
            int(normalized, 16)
        except ValueError as exc:
            raise ValueError(f"invalid EVM address: {value}") from exc
        return normalized.rjust(64, "0")

    def uint_word(value: int) -> str:
        return f"{value:064x}"

    return "0x" + selector + "".join(
        (
            address_word(token_in),
            address_word(token_out),
            uint_word(amount),
            uint_word(fee),
            uint_word(sqrt_price_limit_x96),
        ),
    )


def encode_quoter_v2_exact_input_single(
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
    sqrt_price_limit_x96: int = 0,
) -> str:
    """ABI-encode QuoterV2.quoteExactInputSingle's all-static tuple."""

    return _encode_quoter_v2_single(
        QUOTER_V2_EXACT_INPUT_SINGLE_SELECTOR,
        token_in,
        token_out,
        amount_in,
        fee,
        sqrt_price_limit_x96,
    )


def encode_quoter_v2_exact_output_single(
    token_in: str,
    token_out: str,
    amount_out: int,
    fee: int,
    sqrt_price_limit_x96: int = 0,
) -> str:
    """ABI-encode QuoterV2.quoteExactOutputSingle's all-static tuple.

    The result's first word is the amount of ``token_in`` required to receive
    exactly ``amount_out`` of ``token_out``.  It is quote-only ``eth_call``
    data, not an executable router transaction.
    """

    return _encode_quoter_v2_single(
        QUOTER_V2_EXACT_OUTPUT_SINGLE_SELECTOR,
        token_in,
        token_out,
        amount_out,
        fee,
        sqrt_price_limit_x96,
    )


def parse_quoter_v2_result(value: str) -> dict[str, int]:
    normalized = value.removeprefix("0x")
    if len(normalized) < 64 * 4 or len(normalized) % 64 != 0:
        raise ValueError("QuoterV2 result must contain at least four ABI words")
    try:
        words = [int(normalized[index : index + 64], 16) for index in range(0, 64 * 4, 64)]
    except ValueError as exc:
        raise ValueError("QuoterV2 result is not hexadecimal") from exc
    return {
        "amount_out": words[0],
        "sqrt_price_x96_after": words[1],
        "initialized_ticks_crossed": words[2],
        "gas_estimate": words[3],
    }


def _rpc_error(item: Any) -> str | None:
    if not isinstance(item, dict):
        return "malformed JSON-RPC response item"
    error = item.get("error")
    if error is None:
        return None
    if isinstance(error, dict):
        code = error.get("code")
        message = error.get("message")
        return f"JSON-RPC {code}: {message}"
    return f"JSON-RPC error: {error}"


def _block_context(item: Any) -> dict[str, Any]:
    if not isinstance(item, dict) or not isinstance(item.get("result"), dict):
        return {}
    block = item["result"]
    number = block.get("number")
    timestamp = block.get("timestamp")
    return {
        "block_number": int(number, 16) if isinstance(number, str) else None,
        "block_timestamp": int(timestamp, 16) if isinstance(timestamp, str) else None,
        "block_hash": block.get("hash"),
    }


class UniswapV3Provider:
    def __init__(
        self,
        market: EvmMarket,
        *,
        proxy_url: str | None,
        timeout_seconds: float,
        fetch_json: JsonFetcher = _fetch_json_sync,
    ) -> None:
        self.market = market
        self.name = market.provider
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.fetch_json = fetch_json

    def config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "chain": self.market.chain,
            "chain_id": self.market.chain_id,
            "protocol": self.market.protocol,
            "pair": f"{self.market.base.symbol}/{self.market.quote.symbol}",
            "source_kind": "onchain_eth_call",
            "endpoint_origin": _redact_url(self.market.rpc_url),
            "block_tag": self.market.block_tag,
            "quoter_address": self.market.quoter_address,
            "fee_tiers": list(self.market.fee_tiers),
            "api_credentials_required": False,
        }

    async def _batch(self, calls: list[dict[str, Any]]) -> TimedResponse:
        body = json.dumps(calls, separators=(",", ":")).encode()
        return await _timed_fetch(
            self.fetch_json,
            url=self.market.rpc_url,
            method="POST",
            body=body,
            headers={"Content-Type": "application/json"},
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )

    def _base_record(
        self,
        *,
        round_id: int,
        direction: str,
        notional: Decimal,
        fee: int,
        input_raw: int,
        response: TimedResponse,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "round_id": round_id,
            "provider": self.name,
            "chain": self.market.chain,
            "protocol": self.market.protocol,
            "source_kind": "onchain_eth_call",
            "pair": f"{self.market.base.symbol}/{self.market.quote.symbol}",
            "direction": direction,
            "requested_notional_quote": _decimal_text(notional),
            "input_symbol": (
                self.market.quote.symbol if direction == "buy_base" else self.market.base.symbol
            ),
            "output_symbol": (
                self.market.base.symbol if direction == "buy_base" else self.market.quote.symbol
            ),
            "input_amount_raw": str(input_raw),
            "fee_tier": fee,
            "fee_bps": fee / 100,
            "block_tag": self.market.block_tag,
            "chain_context": context,
            **_timing_fields(response),
        }

    async def quote_round(
        self,
        round_id: int,
        notionals: Sequence[Decimal],
    ) -> list[dict[str, Any]]:
        calls: list[dict[str, Any]] = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [self.market.block_tag, False],
            },
        ]
        metadata: dict[int, tuple[Decimal, int, int]] = {}
        next_id = 10
        for notional in notionals:
            input_raw = _to_raw(notional, self.market.quote.decimals)
            for fee in self.market.fee_tiers:
                metadata[next_id] = (notional, fee, input_raw)
                calls.append(
                    {
                        "jsonrpc": "2.0",
                        "id": next_id,
                        "method": "eth_call",
                        "params": [
                            {
                                "to": self.market.quoter_address,
                                "data": encode_quoter_v2_exact_input_single(
                                    self.market.quote.address,
                                    self.market.base.address,
                                    input_raw,
                                    fee,
                                ),
                            },
                            self.market.block_tag,
                        ],
                    },
                )
                next_id += 1

        buy_response = await self._batch(calls)
        if buy_response.error is not None:
            return [
                {
                    "schema_version": 1,
                    "round_id": round_id,
                    "provider": self.name,
                    "chain": self.market.chain,
                    "protocol": self.market.protocol,
                    "source_kind": "onchain_eth_call",
                    "pair": f"{self.market.base.symbol}/{self.market.quote.symbol}",
                    "status": "request_error",
                    "error": buy_response.error,
                    **_timing_fields(buy_response),
                },
            ]
        if not isinstance(buy_response.payload, list):
            return [
                {
                    "schema_version": 1,
                    "round_id": round_id,
                    "provider": self.name,
                    "chain": self.market.chain,
                    "protocol": self.market.protocol,
                    "source_kind": "onchain_eth_call",
                    "pair": f"{self.market.base.symbol}/{self.market.quote.symbol}",
                    "status": "request_error",
                    "error": "batch JSON-RPC response is not a list",
                    **_timing_fields(buy_response),
                },
            ]

        by_id = {
            item.get("id"): item
            for item in buy_response.payload
            if isinstance(item, dict) and isinstance(item.get("id"), int)
        }
        context = _block_context(by_id.get(1))
        records: list[dict[str, Any]] = []
        best_base_raw: dict[Decimal, int] = {}
        for request_id, (notional, fee, input_raw) in metadata.items():
            item = by_id.get(request_id)
            record = self._base_record(
                round_id=round_id,
                direction="buy_base",
                notional=notional,
                fee=fee,
                input_raw=input_raw,
                response=buy_response,
                context=context,
            )
            error = _rpc_error(item)
            if error is not None:
                record.update(status="quote_unavailable", error=error)
                records.append(record)
                continue
            try:
                parsed = parse_quoter_v2_result(str(item["result"]))
                output_raw = parsed["amount_out"]
                if output_raw <= 0:
                    raise ValueError("zero output")
            except (KeyError, TypeError, ValueError) as exc:
                record.update(status="quote_unavailable", error=f"invalid quote result: {exc}")
                records.append(record)
                continue
            best_base_raw[notional] = max(best_base_raw.get(notional, 0), output_raw)
            record.update(
                status="ok",
                output_amount_raw=str(output_raw),
                quote_result={key: str(value) for key, value in parsed.items()},
                quote_includes_pool_fee_and_price_impact=True,
                **_price_fields(
                    direction="buy_base",
                    input_raw=input_raw,
                    output_raw=output_raw,
                    base=self.market.base,
                    quote=self.market.quote,
                ),
            )
            records.append(record)

        reverse_calls: list[dict[str, Any]] = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [self.market.block_tag, False],
            },
        ]
        reverse_metadata: dict[int, tuple[Decimal, int, int]] = {}
        next_id = 10
        for notional, base_input_raw in best_base_raw.items():
            for fee in self.market.fee_tiers:
                reverse_metadata[next_id] = (notional, fee, base_input_raw)
                reverse_calls.append(
                    {
                        "jsonrpc": "2.0",
                        "id": next_id,
                        "method": "eth_call",
                        "params": [
                            {
                                "to": self.market.quoter_address,
                                "data": encode_quoter_v2_exact_input_single(
                                    self.market.base.address,
                                    self.market.quote.address,
                                    base_input_raw,
                                    fee,
                                ),
                            },
                            self.market.block_tag,
                        ],
                    },
                )
                next_id += 1
        if not reverse_metadata:
            return records

        sell_response = await self._batch(reverse_calls)
        if sell_response.error is not None or not isinstance(sell_response.payload, list):
            records.append(
                {
                    "schema_version": 1,
                    "round_id": round_id,
                    "provider": self.name,
                    "chain": self.market.chain,
                    "protocol": self.market.protocol,
                    "source_kind": "onchain_eth_call",
                    "pair": f"{self.market.base.symbol}/{self.market.quote.symbol}",
                    "direction": "sell_base",
                    "status": "request_error",
                    "error": sell_response.error or "batch JSON-RPC response is not a list",
                    **_timing_fields(sell_response),
                },
            )
            return records

        sell_by_id = {
            item.get("id"): item
            for item in sell_response.payload
            if isinstance(item, dict) and isinstance(item.get("id"), int)
        }
        sell_context = _block_context(sell_by_id.get(1))
        for request_id, (notional, fee, input_raw) in reverse_metadata.items():
            item = sell_by_id.get(request_id)
            record = self._base_record(
                round_id=round_id,
                direction="sell_base",
                notional=notional,
                fee=fee,
                input_raw=input_raw,
                response=sell_response,
                context=sell_context,
            )
            error = _rpc_error(item)
            if error is not None:
                record.update(status="quote_unavailable", error=error)
                records.append(record)
                continue
            try:
                parsed = parse_quoter_v2_result(str(item["result"]))
                output_raw = parsed["amount_out"]
                if output_raw <= 0:
                    raise ValueError("zero output")
            except (KeyError, TypeError, ValueError) as exc:
                record.update(status="quote_unavailable", error=f"invalid quote result: {exc}")
                records.append(record)
                continue
            record.update(
                status="ok",
                output_amount_raw=str(output_raw),
                quote_result={key: str(value) for key, value in parsed.items()},
                quote_includes_pool_fee_and_price_impact=True,
                **_price_fields(
                    direction="sell_base",
                    input_raw=input_raw,
                    output_raw=output_raw,
                    base=self.market.base,
                    quote=self.market.quote,
                ),
            )
            records.append(record)
        return records

    async def quote_exact_base_round(
        self,
        round_id: int,
        targets: Sequence[tuple[Decimal, Decimal]],
    ) -> list[dict[str, Any]]:
        """Quote both DEX directions at an exact CEX-perp quantity.

        ``targets`` holds ``(requested_usdt_exposure, base_quantity)`` pairs,
        where ``base_quantity`` was already aligned to a perpetual's quantity
        step.  For a DEX buy this uses QuoterV2's exact-*output* function, so
        the resulting base balance can be hedged exactly.  For a DEX sell the
        same quantity is an exact input.  This is only an ``eth_call`` quote;
        an eventual router transaction would still need a slippage bound and
        can fill against a newer pool state.

        Keeping this separate from :meth:`quote_round` preserves the original
        public scanner's exact-*input* notional semantics for its other users.
        """

        normalized: list[tuple[int, Decimal, Decimal, int]] = []
        for index, (notional, base_amount) in enumerate(targets):
            if notional <= 0 or base_amount <= 0:
                continue
            try:
                target_raw = _to_raw(base_amount, self.market.base.decimals)
            except ValueError:
                continue
            normalized.append((index, notional, base_amount, target_raw))
        if not normalized:
            return []

        def unavailable_record(
            *,
            direction: str,
            notional: Decimal,
            base_amount: Decimal,
            target_raw: int,
            response: TimedResponse,
            error: str,
            context: dict[str, Any],
        ) -> dict[str, Any]:
            # For an exact-output buy the input is unknown when unavailable;
            # represent that as zero only in this rejected diagnostic record.
            input_raw = target_raw if direction == "sell_base" else 0
            record = self._base_record(
                round_id=round_id,
                direction=direction,
                notional=notional,
                fee=0,
                input_raw=input_raw,
                response=response,
                context=context,
            )
            record.update(
                status="quote_unavailable",
                error=error[:512],
                target_base_amount=_decimal_text(base_amount),
                hedge_quantity_exact=False,
            )
            return record

        buy_calls: list[dict[str, Any]] = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [self.market.block_tag, False],
            },
        ]
        buy_metadata: dict[int, tuple[int, Decimal, Decimal, int, int]] = {}
        next_id = 10
        for index, notional, base_amount, target_raw in normalized:
            for fee in self.market.fee_tiers:
                buy_metadata[next_id] = (index, notional, base_amount, target_raw, fee)
                buy_calls.append(
                    {
                        "jsonrpc": "2.0",
                        "id": next_id,
                        "method": "eth_call",
                        "params": [
                            {
                                "to": self.market.quoter_address,
                                "data": encode_quoter_v2_exact_output_single(
                                    self.market.quote.address,
                                    self.market.base.address,
                                    target_raw,
                                    fee,
                                ),
                            },
                            self.market.block_tag,
                        ],
                    },
                )
                next_id += 1

        buy_response = await self._batch(buy_calls)
        if buy_response.error is not None or not isinstance(buy_response.payload, list):
            reason = buy_response.error or "batch JSON-RPC response is not a list"
            return [
                unavailable_record(
                    direction=direction,
                    notional=notional,
                    base_amount=base_amount,
                    target_raw=target_raw,
                    response=buy_response,
                    error=reason,
                    context={},
                )
                for _, notional, base_amount, target_raw in normalized
                for direction in ("buy_base", "sell_base")
            ]

        buy_by_id = {
            item.get("id"): item
            for item in buy_response.payload
            if isinstance(item, dict) and isinstance(item.get("id"), int)
        }
        buy_context = _block_context(buy_by_id.get(1))
        buy_candidates: dict[int, list[dict[str, Any]]] = {}
        buy_errors: dict[int, list[str]] = {}
        for request_id, (index, notional, base_amount, target_raw, fee) in buy_metadata.items():
            item = buy_by_id.get(request_id)
            error = _rpc_error(item)
            if error is not None:
                buy_errors.setdefault(index, []).append(error)
                continue
            try:
                parsed = parse_quoter_v2_result(str(item["result"]))
                quote_input_raw = parsed["amount_out"]
                if quote_input_raw <= 0:
                    raise ValueError("zero required input")
            except (KeyError, TypeError, ValueError) as exc:
                buy_errors.setdefault(index, []).append(f"invalid exact-output quote: {exc}")
                continue
            record = self._base_record(
                round_id=round_id,
                direction="buy_base",
                notional=notional,
                fee=fee,
                input_raw=quote_input_raw,
                response=buy_response,
                context=buy_context,
            )
            record.update(
                status="ok",
                output_amount_raw=str(target_raw),
                quote_result={
                    "amount_in": str(quote_input_raw),
                    "sqrt_price_x96_after": str(parsed["sqrt_price_x96_after"]),
                    "initialized_ticks_crossed": str(parsed["initialized_ticks_crossed"]),
                    "gas_estimate": str(parsed["gas_estimate"]),
                },
                quote_includes_pool_fee_and_price_impact=True,
                quote_semantics="exact_output_base_aligned_to_perp_step",
                hedge_quantity_exact=True,
                target_base_amount=_decimal_text(base_amount),
                **_price_fields(
                    direction="buy_base",
                    input_raw=quote_input_raw,
                    output_raw=target_raw,
                    base=self.market.base,
                    quote=self.market.quote,
                ),
            )
            buy_candidates.setdefault(index, []).append(record)

        records: list[dict[str, Any]] = []
        for index, notional, base_amount, target_raw in normalized:
            candidates = buy_candidates.get(index, [])
            if candidates:
                # Same exact output across tiers: the smallest stablecoin input
                # is the executable best quote.
                records.append(min(candidates, key=lambda item: int(item["input_amount_raw"])))
            else:
                records.append(
                    unavailable_record(
                        direction="buy_base",
                        notional=notional,
                        base_amount=base_amount,
                        target_raw=target_raw,
                        response=buy_response,
                        error="; ".join(buy_errors.get(index, ["no exact-output pool quote"])),
                        context=buy_context,
                    ),
                )

        sell_calls: list[dict[str, Any]] = [
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": [self.market.block_tag, False],
            },
        ]
        sell_metadata: dict[int, tuple[int, Decimal, Decimal, int, int]] = {}
        next_id = 10
        for index, notional, base_amount, target_raw in normalized:
            for fee in self.market.fee_tiers:
                sell_metadata[next_id] = (index, notional, base_amount, target_raw, fee)
                sell_calls.append(
                    {
                        "jsonrpc": "2.0",
                        "id": next_id,
                        "method": "eth_call",
                        "params": [
                            {
                                "to": self.market.quoter_address,
                                "data": encode_quoter_v2_exact_input_single(
                                    self.market.base.address,
                                    self.market.quote.address,
                                    target_raw,
                                    fee,
                                ),
                            },
                            self.market.block_tag,
                        ],
                    },
                )
                next_id += 1

        sell_response = await self._batch(sell_calls)
        if sell_response.error is not None or not isinstance(sell_response.payload, list):
            reason = sell_response.error or "batch JSON-RPC response is not a list"
            records.extend(
                unavailable_record(
                    direction="sell_base",
                    notional=notional,
                    base_amount=base_amount,
                    target_raw=target_raw,
                    response=sell_response,
                    error=reason,
                    context={},
                )
                for _, notional, base_amount, target_raw in normalized
            )
            return records

        sell_by_id = {
            item.get("id"): item
            for item in sell_response.payload
            if isinstance(item, dict) and isinstance(item.get("id"), int)
        }
        sell_context = _block_context(sell_by_id.get(1))
        sell_candidates: dict[int, list[dict[str, Any]]] = {}
        sell_errors: dict[int, list[str]] = {}
        for request_id, (index, notional, base_amount, target_raw, fee) in sell_metadata.items():
            item = sell_by_id.get(request_id)
            error = _rpc_error(item)
            if error is not None:
                sell_errors.setdefault(index, []).append(error)
                continue
            try:
                parsed = parse_quoter_v2_result(str(item["result"]))
                quote_output_raw = parsed["amount_out"]
                if quote_output_raw <= 0:
                    raise ValueError("zero output")
            except (KeyError, TypeError, ValueError) as exc:
                sell_errors.setdefault(index, []).append(f"invalid exact-input quote: {exc}")
                continue
            record = self._base_record(
                round_id=round_id,
                direction="sell_base",
                notional=notional,
                fee=fee,
                input_raw=target_raw,
                response=sell_response,
                context=sell_context,
            )
            record.update(
                status="ok",
                output_amount_raw=str(quote_output_raw),
                quote_result={key: str(value) for key, value in parsed.items()},
                quote_includes_pool_fee_and_price_impact=True,
                quote_semantics="exact_input_base_aligned_to_perp_step",
                hedge_quantity_exact=True,
                target_base_amount=_decimal_text(base_amount),
                **_price_fields(
                    direction="sell_base",
                    input_raw=target_raw,
                    output_raw=quote_output_raw,
                    base=self.market.base,
                    quote=self.market.quote,
                ),
            )
            sell_candidates.setdefault(index, []).append(record)

        for index, notional, base_amount, target_raw in normalized:
            candidates = sell_candidates.get(index, [])
            if candidates:
                # Same exact base input across tiers: maximize stablecoin output.
                records.append(max(candidates, key=lambda item: int(item["output_amount_raw"])))
            else:
                records.append(
                    unavailable_record(
                        direction="sell_base",
                        notional=notional,
                        base_amount=base_amount,
                        target_raw=target_raw,
                        response=sell_response,
                        error="; ".join(sell_errors.get(index, ["no exact-input pool quote"])),
                        context=sell_context,
                    ),
                )
        return records


class RaydiumProvider:
    name = "RAYDIUM"
    endpoint = "https://transaction-v1.raydium.io/compute/swap-base-in"
    base = SOLANA_PROVIDER_BASES["RAYDIUM"]
    quote = SOLANA_USDC

    def __init__(
        self,
        *,
        proxy_url: str | None,
        timeout_seconds: float,
        slippage_bps: int = 50,
        request_pacer: AsyncRequestPacer | None = None,
        name: str | None = None,
        base: Asset | None = None,
        quote: Asset | None = None,
        fetch_json: JsonFetcher = _fetch_json_sync,
        round_lock: asyncio.Lock | None = None,
    ) -> None:
        self.name = name or type(self).name
        self.base = base or type(self).base
        self.quote = quote or type(self).quote
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.slippage_bps = slippage_bps
        self.request_pacer = request_pacer
        self.fetch_json = fetch_json
        self.round_lock = round_lock

    def config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "chain": "solana",
            "protocol": "Raydium Route API v2",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "source_kind": "vendor_route_api",
            "endpoint_origin": _redact_url(self.endpoint),
            "slippage_bps": self.slippage_bps,
            "minimum_request_interval_seconds": (
                self.request_pacer.minimum_interval_seconds
                if self.request_pacer is not None
                else None
            ),
            "api_credentials_required": False,
            "limitation": "response has no Solana context slot; not a raw onchain latency feed",
        }

    async def _quote(self, input_asset: Asset, output_asset: Asset, amount_raw: int) -> TimedResponse:
        if self.request_pacer is not None:
            await self.request_pacer.wait()
        query = urllib.parse.urlencode(
            {
                "inputMint": input_asset.address,
                "outputMint": output_asset.address,
                "amount": str(amount_raw),
                "slippageBps": str(self.slippage_bps),
                "txVersion": "V0",
            },
        )
        return await _timed_fetch(
            self.fetch_json,
            url=f"{self.endpoint}?{query}",
            method="GET",
            body=None,
            headers={},
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )

    def _record(
        self,
        *,
        round_id: int,
        direction: str,
        notional: Decimal,
        input_raw: int,
        response: TimedResponse,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": 1,
            "round_id": round_id,
            "provider": self.name,
            "chain": "solana",
            "protocol": "Raydium Route API v2",
            "source_kind": "vendor_route_api",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "direction": direction,
            "requested_notional_quote": _decimal_text(notional),
            "input_symbol": self.quote.symbol if direction == "buy_base" else self.base.symbol,
            "output_symbol": self.base.symbol if direction == "buy_base" else self.quote.symbol,
            "input_amount_raw": str(input_raw),
            **_timing_fields(response),
        }
        if response.error is not None:
            record.update(status="request_error", error=response.error)
            return record
        payload = response.payload
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(data, dict):
            record.update(status="quote_unavailable", error=f"Raydium error response: {payload!r}"[:512])
            return record
        try:
            output_raw = int(data["outputAmount"])
            if output_raw <= 0:
                raise ValueError("zero output")
        except (KeyError, TypeError, ValueError) as exc:
            record.update(status="quote_unavailable", error=f"invalid Raydium quote: {exc}")
            return record
        record.update(
            status="ok",
            output_amount_raw=str(output_raw),
            quote_includes_pool_fee_and_price_impact=True,
            quote_service_metadata={
                "quote_id": payload.get("id"),
                "api_version": payload.get("version"),
                "other_amount_threshold": data.get("otherAmountThreshold"),
                "price_impact_pct": data.get("priceImpactPct"),
                "route_plan": data.get("routePlan"),
            },
            **_price_fields(
                direction=direction,
                input_raw=input_raw,
                output_raw=output_raw,
                base=self.base,
                quote=self.quote,
            ),
        )
        return record

    async def quote_round(
        self,
        round_id: int,
        notionals: Sequence[Decimal],
    ) -> list[dict[str, Any]]:
        round_lock = getattr(self, "round_lock", None)
        if round_lock is not None:
            async with round_lock:
                return await self._quote_round_unlocked(round_id, notionals)
        return await self._quote_round_unlocked(round_id, notionals)

    async def _quote_round_unlocked(
        self,
        round_id: int,
        notionals: Sequence[Decimal],
    ) -> list[dict[str, Any]]:
        inputs = [_to_raw(notional, self.quote.decimals) for notional in notionals]
        buy_responses = await asyncio.gather(
            *(self._quote(self.quote, self.base, amount) for amount in inputs),
        )
        records = [
            self._record(
                round_id=round_id,
                direction="buy_base",
                notional=notional,
                input_raw=input_raw,
                response=response,
            )
            for notional, input_raw, response in zip(notionals, inputs, buy_responses, strict=True)
        ]
        reverse_inputs: list[tuple[Decimal, int]] = []
        for notional, record in zip(notionals, records, strict=True):
            if record.get("status") == "ok":
                reverse_inputs.append((notional, int(record["output_amount_raw"])))
        sell_responses = await asyncio.gather(
            *(self._quote(self.base, self.quote, amount) for _, amount in reverse_inputs),
        )
        records.extend(
            self._record(
                round_id=round_id,
                direction="sell_base",
                notional=notional,
                input_raw=input_raw,
                response=response,
            )
            for (notional, input_raw), response in zip(
                reverse_inputs,
                sell_responses,
                strict=True,
            )
        )
        return records


class JupiterProvider(RaydiumProvider):
    """Quote Jupiter Swap V2 without a taker, wallet, or transaction build."""

    name = "JUPITER"
    endpoint = "https://api.jup.ag/swap/v2/order"

    def __init__(
        self,
        *,
        proxy_url: str | None,
        timeout_seconds: float,
        request_pacer: AsyncRequestPacer,
        api_key: str | None = None,
        name: str | None = None,
        base: Asset | None = None,
        quote: Asset | None = None,
        fetch_json: JsonFetcher = _fetch_json_sync,
    ) -> None:
        super().__init__(
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            request_pacer=request_pacer,
            name=name,
            base=base,
            quote=quote,
            fetch_json=fetch_json,
        )
        self.request_pacer = request_pacer
        self.api_key = api_key

    def config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "chain": "solana",
            "protocol": "Jupiter Swap V2 Meta-Aggregator",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "source_kind": "vendor_meta_aggregator_quote_api",
            "endpoint_origin": _redact_url(self.endpoint),
            "api_credentials_required": False,
            "api_credentials_used": self.api_key is not None,
            "minimum_request_interval_seconds": self.request_pacer.minimum_interval_seconds,
            "wallet_or_taker_supplied": False,
            "transaction_requested": False,
            "limitation": (
                "quote-only response has no Solana context slot; execution priority fee, account "
                "creation/rent, and state change before inclusion remain outside the quote"
            ),
        }

    async def _quote(self, input_asset: Asset, output_asset: Asset, amount_raw: int) -> TimedResponse:
        await self.request_pacer.wait()
        query = urllib.parse.urlencode(
            {
                "inputMint": input_asset.address,
                "outputMint": output_asset.address,
                "amount": str(amount_raw),
            },
        )
        headers = {"x-api-key": self.api_key} if self.api_key is not None else {}
        return await _timed_fetch(
            self.fetch_json,
            url=f"{self.endpoint}?{query}",
            method="GET",
            body=None,
            headers=headers,
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )

    def _record(
        self,
        *,
        round_id: int,
        direction: str,
        notional: Decimal,
        input_raw: int,
        response: TimedResponse,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": 1,
            "round_id": round_id,
            "provider": self.name,
            "chain": "solana",
            "protocol": "Jupiter Swap V2 Meta-Aggregator",
            "source_kind": "vendor_meta_aggregator_quote_api",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "direction": direction,
            "requested_notional_quote": _decimal_text(notional),
            "input_symbol": self.quote.symbol if direction == "buy_base" else self.base.symbol,
            "output_symbol": self.base.symbol if direction == "buy_base" else self.quote.symbol,
            "input_amount_raw": str(input_raw),
            **_timing_fields(response),
        }
        if response.error is not None:
            record.update(status="request_error", error=response.error)
            return record
        payload = response.payload
        if not isinstance(payload, dict):
            record.update(status="quote_unavailable", error="Jupiter response is not an object")
            return record
        try:
            output_raw = int(payload["outAmount"])
            response_input_raw = int(payload["inAmount"])
            if output_raw <= 0 or response_input_raw != input_raw:
                raise ValueError("zero output or mismatched input amount")
        except (KeyError, TypeError, ValueError) as exc:
            error = payload.get("errorMessage") or payload.get("error") or exc
            record.update(status="quote_unavailable", error=f"invalid Jupiter quote: {error}"[:512])
            return record
        record.update(
            status="ok",
            output_amount_raw=str(output_raw),
            quote_includes_pool_fee_and_price_impact=True,
            quote_includes_aggregator_platform_fee=True,
            quote_service_metadata={
                "request_id": payload.get("requestId"),
                "swap_type": payload.get("swapType"),
                "router": payload.get("router"),
                "mode": payload.get("mode"),
                "fee_bps": payload.get("feeBps"),
                "fee_mint": payload.get("feeMint"),
                "platform_fee": payload.get("platformFee"),
                "price_impact_pct": payload.get("priceImpactPct"),
                "price_impact": payload.get("priceImpact"),
                "route_plan": payload.get("routePlan"),
                "total_time_ms": payload.get("totalTime"),
                "in_usd_value": payload.get("inUsdValue"),
                "out_usd_value": payload.get("outUsdValue"),
                "transaction": payload.get("transaction"),
                "taker": payload.get("taker"),
            },
            **_price_fields(
                direction=direction,
                input_raw=input_raw,
                output_raw=output_raw,
                base=self.base,
                quote=self.quote,
            ),
        )
        return record


class OmnistonProvider(RaydiumProvider):
    """Request quote-only TON routes from the current Omniston v1beta8 stream."""

    name = "OMNISTON"
    endpoint = OMNISTON_WS_ENDPOINT
    base = OMNISTON_PROVIDER_BASES["OMNISTON"]
    quote = TON_USDT

    def __init__(
        self,
        *,
        proxy_url: str | None,
        timeout_seconds: float,
        quote_selection_window_seconds: float = 0.5,
        max_price_slippage_bps: int = 50,
        max_routes: int = 4,
        allow_risky_routes: bool = False,
        endpoint: str = OMNISTON_WS_ENDPOINT,
        name: str | None = None,
        base: Asset | None = None,
        quote: Asset | None = None,
        connect_websocket: Callable[..., Any] = websocket_connect,
    ) -> None:
        if quote_selection_window_seconds < 0:
            raise ValueError("quote selection window cannot be negative")
        if max_price_slippage_bps < 0:
            raise ValueError("max price slippage cannot be negative")
        if max_routes <= 0:
            raise ValueError("max routes must be positive")
        self.name = name or type(self).name
        self.base = base or type(self).base
        self.quote = quote or type(self).quote
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.quote_selection_window_seconds = quote_selection_window_seconds
        self.max_price_slippage_bps = max_price_slippage_bps
        self.max_routes = max_routes
        self.allow_risky_routes = allow_risky_routes
        self.endpoint = endpoint
        self.connect_websocket = connect_websocket

    def config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "chain": "ton",
            "protocol": "Omniston v1beta8 Meta-Aggregator",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "source_kind": "vendor_meta_aggregator_stream",
            "endpoint_origin": _redact_url(self.endpoint),
            "quote_selection_window_seconds": self.quote_selection_window_seconds,
            "max_price_slippage_bps": self.max_price_slippage_bps,
            "max_routes": self.max_routes,
            "allow_risky_routes": self.allow_risky_routes,
            "api_credentials_required": False,
            "api_credentials_used": False,
            "wallet_or_taker_supplied": False,
            "transaction_requested": False,
            "limitation": (
                "bounded quote stream; gas budget is metadata while the configured network-cost "
                "floor remains the cycle model's conservative execution-cost input"
            ),
        }

    @staticmethod
    def _asset_id(asset: Asset) -> dict[str, Any]:
        if asset.address == TON_NATIVE_ADDRESS:
            return {"ton": {"native": {}}}
        return {"ton": {"jetton": asset.address}}

    def _request_payload(
        self,
        request_id: str,
        input_asset: Asset,
        output_asset: Asset,
        amount_raw: int,
    ) -> dict[str, Any]:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": OMNISTON_QUOTE_METHOD,
            "params": {
                "input_asset": self._asset_id(input_asset),
                "output_asset": self._asset_id(output_asset),
                "input_units": str(amount_raw),
                "settlement_params": [
                    {
                        "swap": {
                            "max_price_slippage_pips": self.max_price_slippage_bps * 100,
                            "max_routes": self.max_routes,
                            "flexible_integrator_fee": False,
                            "allow_risky_routes": self.allow_risky_routes,
                        },
                    },
                ],
            },
        }

    async def _quote(self, input_asset: Asset, output_asset: Asset, amount_raw: int) -> TimedResponse:
        connection_started_monotonic_ns = time.monotonic_ns()
        sent_realtime_ns = time.time_ns()
        sent_monotonic_ns = time.monotonic_ns()
        received_realtime_ns = sent_realtime_ns
        received_monotonic_ns = sent_monotonic_ns
        request_id = str(uuid.uuid4())
        subscription_id: int | None = None
        rfq_id: str | None = None
        quote_updates = 0
        latest_quote: dict[str, Any] | None = None
        latest_event_realtime_ns = sent_realtime_ns
        latest_event_monotonic_ns = sent_monotonic_ns
        first_quote_deadline: float | None = None
        try:
            async with self.connect_websocket(
                self.endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=20,
                ping_timeout=20,
                proxy=self.proxy_url,
                user_agent_header=USER_AGENT,
            ) as websocket:
                connected_monotonic_ns = time.monotonic_ns()
                sent_realtime_ns = time.time_ns()
                sent_monotonic_ns = time.monotonic_ns()
                await websocket.send(
                    json.dumps(
                        self._request_payload(request_id, input_asset, output_asset, amount_raw),
                        separators=(",", ":"),
                    ),
                )
                deadline = time.monotonic() + self.timeout_seconds
                last_no_quote = False
                while True:
                    active_deadline = min(deadline, first_quote_deadline or deadline)
                    remaining = active_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
                    except TimeoutError:
                        break
                    now_realtime_ns = time.time_ns()
                    now_monotonic_ns = time.monotonic_ns()
                    message = json.loads(raw)
                    if not isinstance(message, dict):
                        continue
                    if message.get("id") == request_id:
                        if "error" in message:
                            raise RuntimeError(f"Omniston JSON-RPC error: {message['error']!r}")
                        result = message.get("result")
                        if isinstance(result, int):
                            subscription_id = result
                        continue
                    if message.get("method") != OMNISTON_QUOTE_METHOD:
                        continue
                    params = message.get("params")
                    if not isinstance(params, dict):
                        continue
                    if "error" in params:
                        raise RuntimeError(f"Omniston stream error: {params['error']!r}")
                    event = params.get("result")
                    if not isinstance(event, dict):
                        continue
                    ack = event.get("ack")
                    if isinstance(ack, dict) and ack.get("rfq_id") is not None:
                        rfq_id = str(ack["rfq_id"])
                    quote = event.get("quote_updated")
                    if isinstance(quote, dict) and quote.get("quote_id"):
                        latest_quote = quote
                        quote_updates += 1
                        last_no_quote = False
                        latest_event_realtime_ns = now_realtime_ns
                        latest_event_monotonic_ns = now_monotonic_ns
                        if first_quote_deadline is None:
                            first_quote_deadline = (
                                time.monotonic() + self.quote_selection_window_seconds
                            )
                    elif "no_quote" in event:
                        latest_quote = None
                        last_no_quote = True
                        latest_event_realtime_ns = now_realtime_ns
                        latest_event_monotonic_ns = now_monotonic_ns
                if subscription_id is not None:
                    unsubscribe_id = str(uuid.uuid4())
                    await websocket.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": unsubscribe_id,
                                "method": OMNISTON_QUOTE_UNSUBSCRIBE_METHOD,
                                "params": [subscription_id],
                            },
                            separators=(",", ":"),
                        ),
                    )
                if latest_quote is None and not last_no_quote:
                    latest_event_realtime_ns = time.time_ns()
                    latest_event_monotonic_ns = time.monotonic_ns()
                received_realtime_ns = latest_event_realtime_ns
                received_monotonic_ns = latest_event_monotonic_ns
                payload = {
                    "quote": latest_quote,
                    "no_quote": last_no_quote or latest_quote is None,
                    "subscription_id": subscription_id,
                    "rfq_id": rfq_id,
                    "quote_updates": quote_updates,
                    "connection_handshake_ms": round(
                        (connected_monotonic_ns - connection_started_monotonic_ns) / 1_000_000,
                        6,
                    ),
                    "quote_selection_window_seconds": self.quote_selection_window_seconds,
                }
                return TimedResponse(
                    payload=payload,
                    error=None,
                    sent_realtime_ns=sent_realtime_ns,
                    received_realtime_ns=received_realtime_ns,
                    sent_monotonic_ns=sent_monotonic_ns,
                    received_monotonic_ns=received_monotonic_ns,
                )
        except Exception as exc:
            received_realtime_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            return TimedResponse(
                payload=None,
                error=f"{type(exc).__name__}: {exc}",
                sent_realtime_ns=sent_realtime_ns,
                received_realtime_ns=received_realtime_ns,
                sent_monotonic_ns=sent_monotonic_ns,
                received_monotonic_ns=received_monotonic_ns,
            )

    def _record(
        self,
        *,
        round_id: int,
        direction: str,
        notional: Decimal,
        input_raw: int,
        response: TimedResponse,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": 1,
            "round_id": round_id,
            "provider": self.name,
            "chain": "ton",
            "protocol": "Omniston v1beta8 Meta-Aggregator",
            "source_kind": "vendor_meta_aggregator_stream",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "direction": direction,
            "requested_notional_quote": _decimal_text(notional),
            "input_symbol": self.quote.symbol if direction == "buy_base" else self.base.symbol,
            "output_symbol": self.base.symbol if direction == "buy_base" else self.quote.symbol,
            "input_amount_raw": str(input_raw),
            **_timing_fields(response),
        }
        if response.error is not None:
            record.update(status="request_error", error=response.error)
            return record
        payload = response.payload
        quote = payload.get("quote") if isinstance(payload, dict) else None
        if not isinstance(payload, dict) or not isinstance(quote, dict):
            record.update(status="quote_unavailable", error="Omniston returned no active quote")
            return record
        try:
            response_input_raw = int(quote["input_units"])
            output_raw = int(quote["output_units"])
            if response_input_raw != input_raw or output_raw <= 0:
                raise ValueError("zero output or mismatched input amount")
        except (KeyError, TypeError, ValueError) as exc:
            record.update(status="quote_unavailable", error=f"invalid Omniston quote: {exc}")
            return record
        swap = quote.get("swap") if isinstance(quote.get("swap"), dict) else None
        record.update(
            status="ok",
            output_amount_raw=str(output_raw),
            quote_includes_pool_fee_and_price_impact=True,
            quote_includes_aggregator_protocol_fee=True,
            quote_service_metadata={
                "api_version": "v1beta8",
                "rfq_id": quote.get("rfq_id") or payload.get("rfq_id"),
                "quote_id": quote.get("quote_id"),
                "resolver_id": quote.get("resolver_id"),
                "resolver_name": quote.get("resolver_name"),
                "integrator_fee_units": quote.get("integrator_fee_units"),
                "protocol_fee_units": quote.get("protocol_fee_units"),
                "quote_timestamp": quote.get("quote_timestamp"),
                "estimated_settlement_duration": quote.get("estimated_settlement_duration"),
                "gas_budget": quote.get("gas_budget"),
                "estimated_gas_consumption": quote.get("estimated_gas_consumption"),
                "swap": swap,
                "quote_updates": payload.get("quote_updates"),
                "connection_handshake_ms": payload.get("connection_handshake_ms"),
                "quote_selection_window_seconds": payload.get(
                    "quote_selection_window_seconds",
                ),
            },
            **_price_fields(
                direction=direction,
                input_raw=input_raw,
                output_raw=output_raw,
                base=self.base,
                quote=self.quote,
            ),
        )
        return record


class StonFiProvider:
    name = "STONFI"
    endpoint = "https://api.ston.fi/v1/swap/simulate"
    base = TON_PROVIDER_BASES["STONFI"]
    quote = TON_USDT

    def __init__(
        self,
        *,
        proxy_url: str | None,
        timeout_seconds: float,
        slippage_tolerance: Decimal = Decimal("0.005"),
        name: str | None = None,
        base: Asset | None = None,
        quote: Asset | None = None,
        fetch_json: JsonFetcher = _fetch_json_sync,
    ) -> None:
        self.name = name or type(self).name
        self.base = base or type(self).base
        self.quote = quote or type(self).quote
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds
        self.slippage_tolerance = slippage_tolerance
        self.fetch_json = fetch_json

    def config(self) -> dict[str, Any]:
        return {
            "provider": self.name,
            "chain": "ton",
            "protocol": "STON.fi DEX API",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "source_kind": "vendor_swap_simulation_api",
            "endpoint_origin": _redact_url(self.endpoint),
            "slippage_tolerance": _decimal_text(self.slippage_tolerance),
            "api_credentials_required": False,
            "limitation": (
                "API quote has no raw shard/account arrival timestamp; TON execution is an "
                "asynchronous multi-contract trace"
            ),
        }

    async def _quote(self, input_asset: Asset, output_asset: Asset, amount_raw: int) -> TimedResponse:
        query = urllib.parse.urlencode(
            {
                "offer_address": input_asset.address,
                "ask_address": output_asset.address,
                "units": str(amount_raw),
                "slippage_tolerance": _decimal_text(self.slippage_tolerance),
                "dex_v2": "true",
            },
        )
        return await _timed_fetch(
            self.fetch_json,
            url=f"{self.endpoint}?{query}",
            method="POST",
            body=b"",
            headers={},
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )

    def _record(
        self,
        *,
        round_id: int,
        direction: str,
        notional: Decimal,
        input_raw: int,
        response: TimedResponse,
    ) -> dict[str, Any]:
        record: dict[str, Any] = {
            "schema_version": 1,
            "round_id": round_id,
            "provider": self.name,
            "chain": "ton",
            "protocol": "STON.fi DEX API",
            "source_kind": "vendor_swap_simulation_api",
            "pair": f"{self.base.symbol}/{self.quote.symbol}",
            "direction": direction,
            "requested_notional_quote": _decimal_text(notional),
            "input_symbol": self.quote.symbol if direction == "buy_base" else self.base.symbol,
            "output_symbol": self.base.symbol if direction == "buy_base" else self.quote.symbol,
            "input_amount_raw": str(input_raw),
            **_timing_fields(response),
        }
        if response.error is not None:
            record.update(status="request_error", error=response.error)
            return record
        payload = response.payload
        if not isinstance(payload, dict):
            record.update(status="quote_unavailable", error="STON.fi response is not an object")
            return record
        try:
            output_raw = int(payload["ask_units"])
            if output_raw <= 0:
                raise ValueError("zero output")
        except (KeyError, TypeError, ValueError) as exc:
            record.update(status="quote_unavailable", error=f"invalid STON.fi quote: {exc}")
            return record
        router = payload.get("router")
        record.update(
            status="ok",
            output_amount_raw=str(output_raw),
            quote_includes_pool_fee_and_price_impact=True,
            quote_service_metadata={
                "pool_address": payload.get("pool_address"),
                "router_address": payload.get("router_address"),
                "router_major_version": router.get("major_version") if isinstance(router, dict) else None,
                "router_minor_version": router.get("minor_version") if isinstance(router, dict) else None,
                "min_ask_units": payload.get("min_ask_units"),
                "recommended_min_ask_units": payload.get("recommended_min_ask_units"),
                "swap_rate": payload.get("swap_rate"),
                "price_impact": payload.get("price_impact"),
                "fee_units": payload.get("fee_units"),
                "fee_percent": payload.get("fee_percent"),
                "gas_params": payload.get("gas_params"),
            },
            **_price_fields(
                direction=direction,
                input_raw=input_raw,
                output_raw=output_raw,
                base=self.base,
                quote=self.quote,
            ),
        )
        return record

    async def quote_round(
        self,
        round_id: int,
        notionals: Sequence[Decimal],
    ) -> list[dict[str, Any]]:
        inputs = [_to_raw(notional, self.quote.decimals) for notional in notionals]
        buy_responses = await asyncio.gather(
            *(self._quote(self.quote, self.base, amount) for amount in inputs),
        )
        records = [
            self._record(
                round_id=round_id,
                direction="buy_base",
                notional=notional,
                input_raw=input_raw,
                response=response,
            )
            for notional, input_raw, response in zip(notionals, inputs, buy_responses, strict=True)
        ]
        reverse_inputs: list[tuple[Decimal, int]] = []
        for notional, record in zip(notionals, records, strict=True):
            if record.get("status") == "ok":
                reverse_inputs.append((notional, int(record["output_amount_raw"])))
        sell_responses = await asyncio.gather(
            *(self._quote(self.base, self.quote, amount) for _, amount in reverse_inputs),
        )
        records.extend(
            self._record(
                round_id=round_id,
                direction="sell_base",
                notional=notional,
                input_raw=input_raw,
                response=response,
            )
            for (notional, input_raw), response in zip(
                reverse_inputs,
                sell_responses,
                strict=True,
            )
        )
        return records


PROVIDER_NAMES = (
    "UNISWAP_BASE",
    "UNISWAP_BASE_CBBTC",
    "UNISWAP_POLYGON_USDC",
    "UNISWAP_POLYGON_USDCE",
    "UNISWAP_POLYGON_WBTC",
    *SOLANA_PROVIDER_BASES,
    *JUPITER_PROVIDER_BASES,
    *SOLANA_USDT_PROVIDER_BASES,
    *JUPITER_USDT_PROVIDER_BASES,
    *TON_PROVIDER_BASES,
    *OMNISTON_PROVIDER_BASES,
)


def evm_markets(
    *,
    base_rpc_url: str,
    polygon_rpc_url: str,
    fee_tiers: Sequence[int],
) -> dict[str, EvmMarket]:
    weth_base = Asset("WETH", "0x4200000000000000000000000000000000000006", 18)
    usdc_base = Asset("USDC", "0x833589fCD6eDb6E08f4C7C32D4f71b54bdA02913", 6)
    cbbtc_base = Asset("cbBTC", "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf", 8)
    weth_polygon = Asset("WETH", "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619", 18)
    usdc_polygon = Asset("USDC", "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359", 6)
    usdce_polygon = Asset("USDC.e", "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", 6)
    wbtc_polygon = Asset("WBTC", "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6", 8)
    return {
        "UNISWAP_BASE": EvmMarket(
            provider="UNISWAP_BASE",
            chain="base",
            chain_id=8453,
            protocol="Uniswap v3",
            rpc_url=base_rpc_url,
            block_tag="pending",
            quoter_address="0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
            base=weth_base,
            quote=usdc_base,
            fee_tiers=tuple(fee_tiers),
        ),
        "UNISWAP_BASE_CBBTC": EvmMarket(
            provider="UNISWAP_BASE_CBBTC",
            chain="base",
            chain_id=8453,
            protocol="Uniswap v3",
            rpc_url=base_rpc_url,
            block_tag="pending",
            quoter_address="0x3d4e44Eb1374240CE5F1B871ab261CD16335B76a",
            base=cbbtc_base,
            quote=usdc_base,
            fee_tiers=tuple(fee_tiers),
        ),
        "UNISWAP_POLYGON_USDC": EvmMarket(
            provider="UNISWAP_POLYGON_USDC",
            chain="polygon",
            chain_id=137,
            protocol="Uniswap v3",
            rpc_url=polygon_rpc_url,
            block_tag="latest",
            quoter_address="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
            base=weth_polygon,
            quote=usdc_polygon,
            fee_tiers=tuple(fee_tiers),
        ),
        "UNISWAP_POLYGON_USDCE": EvmMarket(
            provider="UNISWAP_POLYGON_USDCE",
            chain="polygon",
            chain_id=137,
            protocol="Uniswap v3",
            rpc_url=polygon_rpc_url,
            block_tag="latest",
            quoter_address="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
            base=weth_polygon,
            quote=usdce_polygon,
            fee_tiers=tuple(fee_tiers),
        ),
        "UNISWAP_POLYGON_WBTC": EvmMarket(
            provider="UNISWAP_POLYGON_WBTC",
            chain="polygon",
            chain_id=137,
            protocol="Uniswap v3",
            rpc_url=polygon_rpc_url,
            block_tag="latest",
            quoter_address="0x61fFE014bA17989E743c5F6cB21bF9697530B21e",
            base=wbtc_polygon,
            quote=usdc_polygon,
            fee_tiers=tuple(fee_tiers),
        ),
    }


def _evm_markets(args: argparse.Namespace) -> dict[str, EvmMarket]:
    return evm_markets(
        base_rpc_url=args.base_rpc_url,
        polygon_rpc_url=args.polygon_rpc_url,
        fee_tiers=args.uniswap_fee_tiers,
    )


def build_providers(
    args: argparse.Namespace,
    *,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> list[DexQuoteProvider]:
    evm = _evm_markets(args)
    providers: list[DexQuoteProvider] = []
    jupiter_api_key = os.getenv(args.jupiter_api_key_env) if args.jupiter_api_key_env else None
    jupiter_interval = (
        args.jupiter_min_request_interval_seconds
        if args.jupiter_min_request_interval_seconds is not None
        else (1.05 if jupiter_api_key else 2.05)
    )
    jupiter_pacer = AsyncRequestPacer(jupiter_interval)
    raydium_pacer = AsyncRequestPacer(args.raydium_min_request_interval_seconds)
    for name in args.providers:
        if name in evm:
            providers.append(
                UniswapV3Provider(
                    evm[name],
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    fetch_json=fetch_json,
                ),
            )
        elif name in SOLANA_PROVIDER_BASES:
            providers.append(
                RaydiumProvider(
                    name=name,
                    base=SOLANA_PROVIDER_BASES[name],
                    quote=SOLANA_USDC,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    slippage_bps=args.slippage_bps,
                    request_pacer=raydium_pacer,
                    fetch_json=fetch_json,
                ),
            )
        elif name in JUPITER_PROVIDER_BASES:
            providers.append(
                JupiterProvider(
                    name=name,
                    base=JUPITER_PROVIDER_BASES[name],
                    quote=SOLANA_USDC,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    request_pacer=jupiter_pacer,
                    api_key=jupiter_api_key,
                    fetch_json=fetch_json,
                ),
            )
        elif name in SOLANA_USDT_PROVIDER_BASES:
            providers.append(
                RaydiumProvider(
                    name=name,
                    base=SOLANA_USDT_PROVIDER_BASES[name],
                    quote=SOLANA_USDT,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    slippage_bps=args.slippage_bps,
                    request_pacer=raydium_pacer,
                    fetch_json=fetch_json,
                ),
            )
        elif name in JUPITER_USDT_PROVIDER_BASES:
            providers.append(
                JupiterProvider(
                    name=name,
                    base=JUPITER_USDT_PROVIDER_BASES[name],
                    quote=SOLANA_USDT,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    request_pacer=jupiter_pacer,
                    api_key=jupiter_api_key,
                    fetch_json=fetch_json,
                ),
            )
        elif name in TON_PROVIDER_BASES:
            providers.append(
                StonFiProvider(
                    name=name,
                    base=TON_PROVIDER_BASES[name],
                    quote=TON_USDT,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    slippage_tolerance=args.stonfi_slippage_tolerance,
                    fetch_json=fetch_json,
                ),
            )
        elif name in OMNISTON_PROVIDER_BASES:
            providers.append(
                OmnistonProvider(
                    name=name,
                    base=OMNISTON_PROVIDER_BASES[name],
                    quote=TON_USDT,
                    proxy_url=args.proxy_url,
                    timeout_seconds=args.timeout_seconds,
                    quote_selection_window_seconds=(
                        args.omniston_quote_selection_window_seconds
                    ),
                    max_price_slippage_bps=args.omniston_max_price_slippage_bps,
                    max_routes=args.omniston_max_routes,
                    allow_risky_routes=args.omniston_allow_risky_routes,
                    endpoint=args.omniston_ws_url,
                ),
            )
        else:
            raise ValueError(f"unsupported provider: {name}")
    return providers


def _summary(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "min": None, "mean": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        return ordered[round((len(ordered) - 1) * fraction)]

    return {
        "count": len(values),
        "min": round(ordered[0], 6),
        "mean": round(sum(values) / len(values), 6),
        "p50": round(percentile(0.50), 6),
        "p95": round(percentile(0.95), 6),
        "max": round(ordered[-1], 6),
    }


async def record_dex_quotes(
    providers: Sequence[DexQuoteProvider],
    *,
    notionals: Sequence[Decimal],
    duration_seconds: float,
    interval_seconds: float,
    output_directory: Path,
    proxy_url: str | None,
) -> dict[str, Any]:
    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite existing DEX run: {output_directory}")
    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    quotes_path = output_directory / "quotes.jsonl"
    started_at = datetime.now(UTC)
    start_monotonic_ns = time.monotonic_ns()
    provider_stats: dict[str, dict[str, Any]] = {
        provider.name: {
            "rounds": 0,
            "observations": 0,
            "ok": 0,
            "quote_unavailable": 0,
            "request_error": 0,
            "last_error": None,
            "rtt_ms": [],
            "first_received_realtime_ns": None,
            "last_received_realtime_ns": None,
        }
        for provider in providers
    }
    rounds = 0
    with quotes_path.open("x", encoding="utf-8", buffering=1) as output:
        while True:
            round_started_ns = time.monotonic_ns()
            results = await asyncio.gather(
                *(provider.quote_round(rounds, notionals) for provider in providers),
                return_exceptions=True,
            )
            for provider, result in zip(providers, results, strict=True):
                stats = provider_stats[provider.name]
                stats["rounds"] += 1
                if isinstance(result, BaseException):
                    records = [
                        {
                            "schema_version": 1,
                            "round_id": rounds,
                            "provider": provider.name,
                            "status": "request_error",
                            "error": f"{type(result).__name__}: {result}",
                            "response_received_realtime_ns": time.time_ns(),
                            "response_received_monotonic_ns": time.monotonic_ns(),
                        },
                    ]
                else:
                    records = result
                for record in records:
                    output.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                    stats["observations"] += 1
                    status = str(record.get("status", "request_error"))
                    if status in stats:
                        stats[status] += 1
                    if status != "ok":
                        stats["last_error"] = record.get("error")
                    rtt = record.get("request_rtt_ms")
                    if isinstance(rtt, (int, float)):
                        stats["rtt_ms"].append(float(rtt))
                    received = record.get("response_received_realtime_ns")
                    if isinstance(received, int):
                        if stats["first_received_realtime_ns"] is None:
                            stats["first_received_realtime_ns"] = received
                        stats["last_received_realtime_ns"] = received
            rounds += 1
            elapsed = (time.monotonic_ns() - start_monotonic_ns) / 1_000_000_000
            if elapsed >= duration_seconds:
                break
            round_elapsed = (time.monotonic_ns() - round_started_ns) / 1_000_000_000
            await asyncio.sleep(min(max(0.0, interval_seconds - round_elapsed), duration_seconds - elapsed))

    stopped_at = datetime.now(UTC)
    finish_monotonic_ns = time.monotonic_ns()
    summarized_stats: dict[str, Any] = {}
    providers_with_data = 0
    for name, stats in provider_stats.items():
        rtt_values = stats.pop("rtt_ms")
        stats["rtt_ms"] = _summary(rtt_values)
        if stats["ok"] > 0:
            providers_with_data += 1
        summarized_stats[name] = stats
    if providers_with_data == len(providers):
        status = "ok"
    elif providers_with_data:
        status = "partial"
    else:
        status = "error"
    manifest: dict[str, Any] = {
        "status": status,
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_requested_seconds": duration_seconds,
        "duration_wall_seconds": round(
            (finish_monotonic_ns - start_monotonic_ns) / 1_000_000_000,
            6,
        ),
        "rounds": rounds,
        "interval_seconds": interval_seconds,
        "notionals_quote": [_decimal_text(value) for value in notionals],
        "network_route": network_route,
        "api_credentials_used": any(
            provider.config().get("api_credentials_used") is True for provider in providers
        ),
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "quotes_path": str(quotes_path.resolve()),
        "providers": [provider.config() for provider in providers],
        "provider_stats": summarized_stats,
        "timestamp_policy": {
            "primary": "local response_received_realtime_ns for alignment with same-host CEX data",
            "latency": "CLOCK_MONOTONIC around each HTTP request",
            "warning": (
                "vendor quote APIs can cache or aggregate state; only EVM eth_call records are direct "
                "chain-state simulations"
            ),
        },
    }
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parse_names(value: str) -> list[str]:
    names = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    invalid = [name for name in names if name not in PROVIDER_NAMES]
    if not names or invalid:
        raise argparse.ArgumentTypeError(
            f"providers must be comma-separated values from {', '.join(PROVIDER_NAMES)}",
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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--providers",
        type=_parse_names,
        default=_parse_names(
            "UNISWAP_BASE,UNISWAP_POLYGON_USDC,UNISWAP_POLYGON_USDCE,RAYDIUM,STONFI",
        ),
    )
    parser.add_argument(
        "--notionals",
        type=lambda value: _parse_decimals(value, "notionals"),
        default=_parse_decimals("100,1000,5000", "notionals"),
        help="Quote-currency notionals used to size each round",
    )
    parser.add_argument(
        "--uniswap-fee-tiers",
        type=lambda value: _parse_ints(value, "uniswap-fee-tiers"),
        default=_parse_ints("100,500,3000", "uniswap-fee-tiers"),
    )
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--slippage-bps", type=int, default=50)
    parser.add_argument(
        "--raydium-min-request-interval-seconds",
        type=float,
        default=0.6,
        help="Shared Raydium request-start spacing; 0.6 stays below its public 120 requests/minute IP limit",
    )
    parser.add_argument(
        "--jupiter-api-key-env",
        default="JUPITER_API_KEY",
        help="Environment variable containing an optional Jupiter API key; the value is never persisted",
    )
    parser.add_argument(
        "--jupiter-min-request-interval-seconds",
        type=float,
        help="Shared Jupiter request-start spacing; defaults to 2.05 keyless or 1.05 with a key",
    )
    parser.add_argument(
        "--stonfi-slippage-tolerance",
        type=Decimal,
        default=Decimal("0.005"),
    )
    parser.add_argument("--omniston-ws-url", default=OMNISTON_WS_ENDPOINT)
    parser.add_argument("--omniston-quote-selection-window-seconds", type=float, default=0.5)
    parser.add_argument("--omniston-max-price-slippage-bps", type=int, default=50)
    parser.add_argument("--omniston-max-routes", type=int, default=4)
    parser.add_argument("--omniston-allow-risky-routes", action="store_true")
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/dex"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds must be positive")
    if args.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be positive")
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    if args.slippage_bps < 0:
        raise SystemExit("--slippage-bps cannot be negative")
    if args.raydium_min_request_interval_seconds < 0:
        raise SystemExit("--raydium-min-request-interval-seconds cannot be negative")
    if (
        args.jupiter_min_request_interval_seconds is not None
        and args.jupiter_min_request_interval_seconds < 0
    ):
        raise SystemExit("--jupiter-min-request-interval-seconds cannot be negative")
    if not Decimal("0") <= args.stonfi_slippage_tolerance < Decimal("1"):
        raise SystemExit("--stonfi-slippage-tolerance must be in [0, 1)")
    if args.omniston_quote_selection_window_seconds < 0:
        raise SystemExit("--omniston-quote-selection-window-seconds cannot be negative")
    if args.omniston_max_price_slippage_bps < 0:
        raise SystemExit("--omniston-max-price-slippage-bps cannot be negative")
    if args.omniston_max_routes <= 0:
        raise SystemExit("--omniston-max-routes must be positive")
    run_id = args.run_id or default_run_id()
    validate_run_id(run_id)
    providers = build_providers(args)
    manifest = asyncio.run(
        record_dex_quotes(
            providers,
            notionals=args.notionals,
            duration_seconds=args.duration_seconds,
            interval_seconds=args.interval_seconds,
            output_directory=args.output_root / run_id,
            proxy_url=args.proxy_url,
        ),
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    if manifest["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
