"""Read account-specific spot fee schedules without trading or balance access.

Public fee tables are useful only as conservative defaults.  Each exchange can
apply account tier, region, symbol, promotion, or fee-token rules, so this
module reads the documented *read-only* fee endpoints for Bybit, OKX, Binance
and MEXC.  For current OKX fee groups it also reads that account's instrument
metadata, but deliberately has no order, balance, position, withdrawal, or
wallet endpoint.

The resulting JSON report contains fee rates, their exact source and explicit
limitations, but never an API key, secret, passphrase, signed query string, or
raw private API response.  It can later be supplied to a monitor as a
symbol-specific fee schedule.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import json
import os
import re
import time
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import _timed_fetch
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


BYBIT_FEE_ENDPOINT = "https://api.bybit.com/v5/account/fee-rate"
BINANCE_SPOT_COMMISSION_ENDPOINT = "https://api.binance.com/api/v3/account/commission"
OKX_TRADE_FEE_ENDPOINT = "https://www.okx.com/api/v5/account/trade-fee"
OKX_ACCOUNT_INSTRUMENTS_ENDPOINT = "https://www.okx.com/api/v5/account/instruments"
MEXC_TRADE_FEE_ENDPOINT = "https://api.mexc.com/api/v3/tradeFee"
SUPPORTED_VENUES = ("BYBIT", "OKX", "BINANCE", "MEXC")
MARKET_UNIVERSES = (
    "rolling-maximum",
    "continuous-maximum",
    "triangle-default",
    "all-current",
)
DEFAULT_SYMBOLS_BY_VENUE_TEXT = (
    "BYBIT=BTCUSDT|ETHUSDT|SOLUSDT;"
    "OKX=BTC-USDT|ETH-USDT|SOL-USDT;"
    "BINANCE=BTCUSDT|ETHUSDT|SOLUSDT;"
    "MEXC=BTCUSDT|ETHUSDT|SOLUSDT"
)
_SENSITIVE_ERROR_QUERY_VALUE = re.compile(
    r"(?i)\b(signature|api[_-]?key|secret|passphrase)=([^&\s]+)",
)


@dataclass(frozen=True)
class SpotFeeRate:
    """One account's current fee schedule for one spot symbol.

    All fields use a common **cost** convention: positive bps is a fee paid,
    negative bps is a maker rebate.  Binance can have buyer/seller components,
    hence the four side-specific fields.  Other venues normally return the
    same value for buy and sell.
    """

    venue: str
    symbol: str
    maker_buy_bps: Decimal
    maker_sell_bps: Decimal
    taker_buy_bps: Decimal
    taker_sell_bps: Decimal
    account_verified: bool
    source: str
    assumptions: tuple[str, ...] = ()

    def taker_bps(self, side: str) -> Decimal:
        normalized = side.upper()
        if normalized == "BUY":
            return self.taker_buy_bps
        if normalized == "SELL":
            return self.taker_sell_bps
        raise ValueError(f"unsupported spot side: {side}")

    def record(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "symbol": self.symbol,
            "maker_buy_bps": _decimal_text(self.maker_buy_bps),
            "maker_sell_bps": _decimal_text(self.maker_sell_bps),
            "taker_buy_bps": _decimal_text(self.taker_buy_bps),
            "taker_sell_bps": _decimal_text(self.taker_sell_bps),
            "account_verified": self.account_verified,
            "source": self.source,
            "assumptions": list(self.assumptions),
        }


@dataclass(frozen=True)
class SpotFeeAuditOutcome:
    """Successful rates and safely recorded per-symbol audit failures.

    A broad market universe can contain a symbol that an individual venue does
    not list.  That should not discard already verified rates for the other
    symbols, nor should it turn an unavailable pair into a made-up fee.
    """

    rates: dict[str, dict[str, SpotFeeRate]]
    symbol_errors: dict[str, dict[str, str]]


def resolve_spot_fee_rate(
    *,
    venue: str,
    symbol: str,
    fallback_taker_bps: Decimal,
    account_fee_rates: Mapping[tuple[str, str], SpotFeeRate] | None,
) -> SpotFeeRate:
    """Return an audited symbol rate, or an explicitly non-verified fallback.

    This is intentionally centralized so every CEX-spot monitor uses the same
    policy: a public default is allowed for diagnostics, never mislabelled as
    an account-specific trading condition.
    """

    normalized_venue = venue.upper()
    normalized_symbol = symbol.upper()
    if normalized_venue not in SUPPORTED_VENUES:
        raise ValueError(f"unsupported fee venue: {normalized_venue}")
    if not fallback_taker_bps.is_finite() or fallback_taker_bps >= Decimal(10_000):
        raise ValueError("fallback taker fee must be finite and below 10000 bps")
    audited = (account_fee_rates or {}).get((normalized_venue, normalized_symbol))
    if audited is not None:
        return audited
    return SpotFeeRate(
        venue=normalized_venue,
        symbol=normalized_symbol,
        maker_buy_bps=fallback_taker_bps,
        maker_sell_bps=fallback_taker_bps,
        taker_buy_bps=fallback_taker_bps,
        taker_sell_bps=fallback_taker_bps,
        account_verified=False,
        source="configured_public_baseline_not_account_verified",
        assumptions=("missing_symbol_specific_read_only_account_fee_audit",),
    )


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal {field}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite decimal {field}")
    return result


def _bps(rate: Any, *, field: str, inverted_cost_sign: bool = False) -> Decimal:
    """Normalize an exchange decimal rate to our positive-cost bps convention."""

    value = _decimal(rate, field=field) * Decimal(10_000)
    return -value if inverted_cost_sign else value


def _hmac_query(secret: str, query: str) -> str:
    if not secret:
        raise ValueError("API secret must be non-empty")
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


def _signed_query(
    values: Sequence[tuple[str, str]],
    *,
    secret: str,
) -> str:
    query = urllib.parse.urlencode(values)
    return f"{query}&signature={_hmac_query(secret, query)}"


def bybit_readonly_headers(
    *,
    api_key: str,
    api_secret: str,
    query: str,
    timestamp_ms: int | None = None,
    recv_window_ms: int = 5_000,
) -> dict[str, str]:
    """Sign Bybit's documented V5 private GET preimage."""

    if not api_key or not api_secret:
        raise ValueError("Bybit API key and secret must both be non-empty")
    timestamp = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1_000))
    recv_window = str(recv_window_ms)
    preimage = f"{timestamp}{api_key}{recv_window}{query}".encode()
    signature = hmac.new(api_secret.encode(), preimage, hashlib.sha256).hexdigest()
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }


def binance_readonly_headers(*, api_key: str) -> dict[str, str]:
    if not api_key:
        raise ValueError("Binance API key must be non-empty")
    return {"X-MBX-APIKEY": api_key}


def mexc_readonly_headers(*, api_key: str) -> dict[str, str]:
    if not api_key:
        raise ValueError("MEXC API key must be non-empty")
    return {"X-MEXC-APIKEY": api_key}


def okx_readonly_headers(
    *,
    api_key: str,
    api_secret: str,
    passphrase: str,
    request_path: str,
    timestamp: str | None = None,
) -> dict[str, str]:
    """Sign one documented OKX private GET request without retaining secrets."""

    if not api_key or not api_secret or not passphrase:
        raise ValueError("OKX API key, secret and passphrase must all be non-empty")
    moment = timestamp or datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    preimage = f"{moment}GET{request_path}".encode()
    signature = base64.b64encode(hmac.new(api_secret.encode(), preimage, hashlib.sha256).digest()).decode()
    return {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": moment,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }


def parse_bybit_spot_fee(payload: Any, *, symbol: str) -> SpotFeeRate:
    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise ValueError(f"Bybit fee-rate error response: {payload!r}"[:512])
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, dict) else None
    if not isinstance(rows, list) or not rows:
        raise ValueError("Bybit fee-rate result is empty")
    row = next(
        (
            item
            for item in rows
            if isinstance(item, dict) and str(item.get("symbol", "")).upper() == symbol.upper()
        ),
        rows[0],
    )
    if not isinstance(row, Mapping):
        raise ValueError("Bybit fee-rate row is malformed")
    maker = _bps(row["makerFeeRate"], field="makerFeeRate")
    taker = _bps(row["takerFeeRate"], field="takerFeeRate")
    return SpotFeeRate(
        venue="BYBIT",
        symbol=symbol.upper(),
        maker_buy_bps=maker,
        maker_sell_bps=maker,
        taker_buy_bps=taker,
        taker_sell_bps=taker,
        account_verified=True,
        source="bybit_v5_account_fee_rate_spot",
    )


def parse_mexc_spot_fee(payload: Any, *, symbol: str) -> SpotFeeRate:
    if not isinstance(payload, dict) or str(payload.get("code")) not in {"0", "200"}:
        raise ValueError(f"MEXC trade-fee error response: {payload!r}"[:512])
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ValueError("MEXC trade-fee result has no data")
    maker = _bps(data["makerCommission"], field="makerCommission")
    taker = _bps(data["takerCommission"], field="takerCommission")
    return SpotFeeRate(
        venue="MEXC",
        symbol=symbol.upper(),
        maker_buy_bps=maker,
        maker_sell_bps=maker,
        taker_buy_bps=taker,
        taker_sell_bps=taker,
        account_verified=True,
        source="mexc_v3_trade_fee",
    )


def _binance_component(
    payload: Mapping[str, Any],
    name: str,
    role: str,
    side_component: str,
) -> Decimal:
    component = payload.get(name)
    if not isinstance(component, Mapping):
        raise ValueError(f"Binance commission response has no {name}")
    return _bps(component[role], field=f"{name}.{role}") + _bps(
        component[side_component],
        field=f"{name}.{side_component}",
    )


def parse_binance_spot_fee(payload: Any, *, symbol: str) -> SpotFeeRate:
    if not isinstance(payload, Mapping):
        raise ValueError("Binance commission response is not an object")
    # The symbol-specific endpoint gives standard, special and tax components.
    # BNB discount applies only to the standard component and only if BNB is
    # actually available at fill time, so this intentionally records the
    # conservative non-discounted account rate.
    components = ("standardCommission", "specialCommission", "taxCommission")

    def fee(role: str, side_component: str) -> Decimal:
        return sum(
            (_binance_component(payload, component, role, side_component) for component in components),
            start=Decimal(0),
        )

    discount = payload.get("discount")
    assumptions: list[str] = ["conservative_no_optional_bnb_fee_discount"]
    if isinstance(discount, Mapping) and (
        discount.get("enabledForAccount") is True and discount.get("enabledForSymbol") is True
    ):
        assumptions.append("BNB_discount_is_available_in_schedule_but_not_assumed_at_fill_time")
    return SpotFeeRate(
        venue="BINANCE",
        symbol=symbol.upper(),
        maker_buy_bps=fee("maker", "buyer"),
        maker_sell_bps=fee("maker", "seller"),
        taker_buy_bps=fee("taker", "buyer"),
        taker_sell_bps=fee("taker", "seller"),
        account_verified=True,
        source="binance_spot_account_commission_conservative_no_bnb_discount",
        assumptions=tuple(assumptions),
    )


def parse_okx_spot_instrument_group(payload: Any, *, symbol: str) -> str:
    """Return the current account-visible OKX fee group for one spot symbol."""

    if not isinstance(payload, Mapping) or str(payload.get("code")) != "0":
        raise ValueError(f"OKX account instruments error response: {payload!r}"[:512])
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise ValueError("OKX account instruments result is empty")
    row = next(
        (
            item
            for item in rows
            if isinstance(item, Mapping) and str(item.get("instId", "")).upper() == symbol.upper()
        ),
        None,
    )
    if row is None:
        raise ValueError(f"OKX account instruments has no exact symbol {symbol}")
    group_id = str(row.get("groupId", "")).strip()
    if not group_id:
        raise ValueError(f"OKX account instruments has no fee group for {symbol}")
    return group_id


def parse_okx_spot_fee(
    payload: Any,
    *,
    symbol: str,
    group_id: str | None = None,
) -> SpotFeeRate:
    if not isinstance(payload, Mapping) or str(payload.get("code")) != "0":
        raise ValueError(f"OKX trade-fee error response: {payload!r}"[:512])
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], Mapping):
        raise ValueError("OKX trade-fee result is empty")
    row = rows[0]
    fee_row: Mapping[str, Any]
    source = "okx_v5_account_trade_fee_spot_legacy_direct_fields"
    if group_id is not None:
        fee_groups = row.get("feeGroup")
        if not isinstance(fee_groups, list):
            raise ValueError("OKX trade-fee result has no feeGroup array")
        fee_row = next(
            (
                item
                for item in fee_groups
                if isinstance(item, Mapping) and str(item.get("groupId", "")) == group_id
            ),
            None,
        )
        if fee_row is None:
            raise ValueError(f"OKX trade-fee result has no fee group {group_id} for {symbol}")
        source = "okx_v5_account_trade_fee_spot_fee_group"
    else:
        # Kept only for backward-compatible parser use.  Live audit requests
        # a groupId and rejects a response without its matching feeGroup.
        fee_row = row
    # OKX documents negative maker/taker as commission paid and positive as
    # rebate; invert to the shared positive-cost convention.
    maker = _bps(fee_row["maker"], field="maker", inverted_cost_sign=True)
    taker = _bps(fee_row["taker"], field="taker", inverted_cost_sign=True)
    return SpotFeeRate(
        venue="OKX",
        symbol=symbol.upper(),
        maker_buy_bps=maker,
        maker_sell_bps=maker,
        taker_buy_bps=taker,
        taker_sell_bps=taker,
        account_verified=True,
        source=source,
        assumptions=("OKX_zero_fee_promotions_may_not_be_reflected_by_open_api",),
    )


async def _get_json(
    *,
    endpoint: str,
    query: str,
    headers: dict[str, str],
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher,
) -> Any:
    response = await _timed_fetch(
        fetch_json,
        url=f"{endpoint}?{query}" if query else endpoint,
        method="GET",
        body=None,
        headers=headers,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    if response.error is not None:
        raise RuntimeError(f"private fee request failed: {response.error}")
    return response.payload


async def fetch_bybit_spot_fees(
    symbols: Sequence[str],
    *,
    api_key: str,
    api_secret: str,
    endpoint: str = BYBIT_FEE_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, SpotFeeRate]:
    result: dict[str, SpotFeeRate] = {}
    for symbol in sorted(set(item.upper() for item in symbols)):
        query = urllib.parse.urlencode((("category", "spot"), ("symbol", symbol)))
        payload = await _get_json(
            endpoint=endpoint,
            query=query,
            headers=bybit_readonly_headers(
                api_key=api_key,
                api_secret=api_secret,
                query=query,
            ),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        result[symbol] = parse_bybit_spot_fee(payload, symbol=symbol)
    return result


async def fetch_binance_spot_fees(
    symbols: Sequence[str],
    *,
    api_key: str,
    api_secret: str,
    endpoint: str = BINANCE_SPOT_COMMISSION_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    recv_window_ms: int = 5_000,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, SpotFeeRate]:
    result: dict[str, SpotFeeRate] = {}
    for symbol in sorted(set(item.upper() for item in symbols)):
        query = _signed_query(
            (
                ("symbol", symbol),
                ("recvWindow", str(recv_window_ms)),
                ("timestamp", str(int(time.time() * 1_000))),
            ),
            secret=api_secret,
        )
        payload = await _get_json(
            endpoint=endpoint,
            query=query,
            headers=binance_readonly_headers(api_key=api_key),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        result[symbol] = parse_binance_spot_fee(payload, symbol=symbol)
    return result


async def fetch_mexc_spot_fees(
    symbols: Sequence[str],
    *,
    api_key: str,
    api_secret: str,
    endpoint: str = MEXC_TRADE_FEE_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    recv_window_ms: int = 5_000,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, SpotFeeRate]:
    result: dict[str, SpotFeeRate] = {}
    for symbol in sorted(set(item.upper() for item in symbols)):
        query = _signed_query(
            (
                ("symbol", symbol),
                ("recvWindow", str(recv_window_ms)),
                ("timestamp", str(int(time.time() * 1_000))),
            ),
            secret=api_secret,
        )
        payload = await _get_json(
            endpoint=endpoint,
            query=query,
            headers=mexc_readonly_headers(api_key=api_key),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        result[symbol] = parse_mexc_spot_fee(payload, symbol=symbol)
        # The documented endpoint is weight 20; preserve a very small spacing
        # even for a short manually selected audit.
        await asyncio.sleep(0.06)
    return result


async def fetch_okx_spot_fees(
    symbols: Sequence[str],
    *,
    api_key: str,
    api_secret: str,
    passphrase: str,
    endpoint: str = OKX_TRADE_FEE_ENDPOINT,
    instruments_endpoint: str = OKX_ACCOUNT_INSTRUMENTS_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, SpotFeeRate]:
    """Read exact current OKX spot fees through the symbol's fee group.

    Since the 2025 fee-scheme update, the direct maker/taker fields are
    deprecated.  A current account-instruments response maps each symbol to
    its groupId; account/trade-fee then returns the applicable maker/taker
    numbers within ``feeGroup``.  Rejecting a missing mapping is safer than
    silently using a USDT default for a USDC or special pair.
    """

    result: dict[str, SpotFeeRate] = {}
    endpoint_path = urllib.parse.urlsplit(endpoint).path
    instruments_endpoint_path = urllib.parse.urlsplit(instruments_endpoint).path
    for symbol in sorted(set(item.upper() for item in symbols)):
        instrument_query = urllib.parse.urlencode((("instType", "SPOT"), ("instId", symbol)))
        instrument_request_path = f"{instruments_endpoint_path}?{instrument_query}"
        instrument_payload = await _get_json(
            endpoint=instruments_endpoint,
            query=instrument_query,
            headers=okx_readonly_headers(
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                request_path=instrument_request_path,
            ),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        group_id = parse_okx_spot_instrument_group(instrument_payload, symbol=symbol)
        fee_query = urllib.parse.urlencode((("instType", "SPOT"), ("groupId", group_id)))
        fee_request_path = f"{endpoint_path}?{fee_query}"
        fee_payload = await _get_json(
            endpoint=endpoint,
            query=fee_query,
            headers=okx_readonly_headers(
                api_key=api_key,
                api_secret=api_secret,
                passphrase=passphrase,
                request_path=fee_request_path,
            ),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
            fetch_json=fetch_json,
        )
        result[symbol] = parse_okx_spot_fee(fee_payload, symbol=symbol, group_id=group_id)
    return result


def _parse_venues(value: str) -> list[str]:
    result = [item.strip().upper() for item in value.split(",") if item.strip()]
    unknown = [item for item in result if item not in SUPPORTED_VENUES]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown fee-audit venue(s): {', '.join(unknown)}")
    if not result:
        raise argparse.ArgumentTypeError("at least one venue is required")
    return list(dict.fromkeys(result))


def _parse_symbols_by_venue(value: str) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    try:
        chunks = [item.strip() for item in value.split(";") if item.strip()]
        for chunk in chunks:
            venue, symbols = chunk.split("=", 1)
            normalized_venue = venue.strip().upper()
            if normalized_venue not in SUPPORTED_VENUES:
                raise ValueError(f"unknown venue {normalized_venue}")
            values = [item.strip().upper() for item in symbols.split("|") if item.strip()]
            if not values:
                raise ValueError(f"no symbols for {normalized_venue}")
            result[normalized_venue] = list(dict.fromkeys(values))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "symbols must look like BYBIT=BTCUSDT|ETHUSDT;OKX=BTC-USDT|ETH-USDT",
        ) from exc
    if not result:
        raise argparse.ArgumentTypeError("at least one venue symbol list is required")
    return result


def symbols_for_market_universe(
    market_universe: str,
    venues: Sequence[str],
) -> dict[str, list[str]]:
    """Build exact CEX spot symbols for one monitor universe.

    Imports stay inside this function because the monitors themselves import
    this module to load a finished report.  The CLI calls this only after all
    modules have initialized, so the deferred imports avoid a circular import
    without keeping a duplicate, easily stale symbol table here.

    ``rolling-maximum`` and ``continuous-maximum`` use the same current 44
    single-asset routes.  ``triangle-default`` needs both USDT legs of each
    direct DEX cross pair.  ``all-current`` is their union.
    """

    normalized_universe = market_universe.strip().lower()
    if normalized_universe not in MARKET_UNIVERSES:
        raise ValueError(
            f"unsupported market universe {market_universe!r}; choose one of: "
            + ", ".join(MARKET_UNIVERSES),
        )
    normalized_venues = tuple(dict.fromkeys(str(venue).upper() for venue in venues))
    unknown_venues = set(normalized_venues) - set(SUPPORTED_VENUES)
    if unknown_venues:
        raise ValueError(f"unsupported fee-audit venues: {sorted(unknown_venues)}")
    if not normalized_venues:
        raise ValueError("at least one venue is required")

    symbols: dict[str, set[str]] = {venue: set() for venue in normalized_venues}
    include_single_asset = normalized_universe in {
        "rolling-maximum",
        "continuous-maximum",
        "all-current",
    }
    include_triangle = normalized_universe in {"triangle-default", "all-current"}

    if include_single_asset:
        from market_data_lab.cex_dex_cycles import MARKETS
        from market_data_lab.cex_dex_cycles import market_for_cex
        from market_data_lab.rolling_cycle_monitor import MAXIMUM_COVERAGE_MARKETS

        for market_name in MAXIMUM_COVERAGE_MARKETS:
            market = MARKETS[market_name]
            for venue in normalized_venues:
                symbols[venue].add(market_for_cex(market, venue).cex_symbol.upper())

    if include_triangle:
        from market_data_lab.triangle_cycle_monitor import TRIANGLE_MARKETS
        from market_data_lab.triangle_cycle_monitor import cex_symbol

        for market in TRIANGLE_MARKETS:
            for venue in normalized_venues:
                symbols[venue].add(cex_symbol(market.base, venue).upper())
                symbols[venue].add(cex_symbol(market.quote, venue).upper())

    return {venue: sorted(items) for venue, items in symbols.items()}


def _require_environment(*names: str) -> tuple[str, ...]:
    values = tuple(os.getenv(name) for name in names)
    if any(not value for value in values):
        raise RuntimeError(
            "missing required local read-only credential environment variable(s): " + ", ".join(names),
        )
    return tuple(value for value in values if value is not None)


def _require_venue_credentials(
    venue: str,
    *,
    bybit_key_env: str,
    bybit_secret_env: str,
    binance_key_env: str,
    binance_secret_env: str,
    okx_key_env: str,
    okx_secret_env: str,
    okx_passphrase_env: str,
    mexc_key_env: str,
    mexc_secret_env: str,
) -> tuple[str, ...]:
    """Read only the selected venue's local credential names once."""

    if venue == "BYBIT":
        return _require_environment(bybit_key_env, bybit_secret_env)
    if venue == "BINANCE":
        return _require_environment(binance_key_env, binance_secret_env)
    if venue == "OKX":
        return _require_environment(okx_key_env, okx_secret_env, okx_passphrase_env)
    if venue == "MEXC":
        return _require_environment(mexc_key_env, mexc_secret_env)
    raise ValueError(f"unsupported fee-audit venue: {venue}")


def _safe_symbol_error(error: Exception) -> str:
    """Keep diagnostics useful without allowing a signed URL into the report."""

    message = " ".join(str(error).split())
    message = _SENSITIVE_ERROR_QUERY_VALUE.sub(r"\1=REDACTED", message)
    return f"{type(error).__name__}: {message[:400]}" if message else type(error).__name__


async def audit_spot_fee_rates(
    symbols_by_venue: Mapping[str, Sequence[str]],
    *,
    proxy_url: str | None,
    timeout_seconds: float,
    bybit_key_env: str = "BYBIT_READONLY_API_KEY",
    bybit_secret_env: str = "BYBIT_READONLY_API_SECRET",
    binance_key_env: str = "BINANCE_READONLY_API_KEY",
    binance_secret_env: str = "BINANCE_READONLY_API_SECRET",
    okx_key_env: str = "OKX_READONLY_API_KEY",
    okx_secret_env: str = "OKX_READONLY_API_SECRET",
    okx_passphrase_env: str = "OKX_READONLY_API_PASSPHRASE",
    mexc_key_env: str = "MEXC_READONLY_API_KEY",
    mexc_secret_env: str = "MEXC_READONLY_API_SECRET",
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, dict[str, SpotFeeRate]]:
    """Read selected current schedules.  Keys are consulted only for requested venues."""

    normalized_symbols_by_venue = {
        str(venue).upper(): tuple(str(symbol).upper() for symbol in raw_symbols)
        for venue, raw_symbols in symbols_by_venue.items()
    }
    unknown = set(normalized_symbols_by_venue) - set(SUPPORTED_VENUES)
    if unknown:
        raise ValueError(f"unsupported fee-audit venues: {sorted(unknown)}")
    results: dict[str, dict[str, SpotFeeRate]] = {}
    for venue, symbols in normalized_symbols_by_venue.items():
        if not symbols:
            continue
        if venue == "BYBIT":
            key, secret = _require_venue_credentials(
                venue,
                bybit_key_env=bybit_key_env,
                bybit_secret_env=bybit_secret_env,
                binance_key_env=binance_key_env,
                binance_secret_env=binance_secret_env,
                okx_key_env=okx_key_env,
                okx_secret_env=okx_secret_env,
                okx_passphrase_env=okx_passphrase_env,
                mexc_key_env=mexc_key_env,
                mexc_secret_env=mexc_secret_env,
            )
            results[venue] = await fetch_bybit_spot_fees(
                symbols,
                api_key=key,
                api_secret=secret,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        elif venue == "BINANCE":
            key, secret = _require_venue_credentials(
                venue,
                bybit_key_env=bybit_key_env,
                bybit_secret_env=bybit_secret_env,
                binance_key_env=binance_key_env,
                binance_secret_env=binance_secret_env,
                okx_key_env=okx_key_env,
                okx_secret_env=okx_secret_env,
                okx_passphrase_env=okx_passphrase_env,
                mexc_key_env=mexc_key_env,
                mexc_secret_env=mexc_secret_env,
            )
            results[venue] = await fetch_binance_spot_fees(
                symbols,
                api_key=key,
                api_secret=secret,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        elif venue == "OKX":
            key, secret, passphrase = _require_venue_credentials(
                venue,
                bybit_key_env=bybit_key_env,
                bybit_secret_env=bybit_secret_env,
                binance_key_env=binance_key_env,
                binance_secret_env=binance_secret_env,
                okx_key_env=okx_key_env,
                okx_secret_env=okx_secret_env,
                okx_passphrase_env=okx_passphrase_env,
                mexc_key_env=mexc_key_env,
                mexc_secret_env=mexc_secret_env,
            )
            results[venue] = await fetch_okx_spot_fees(
                symbols,
                api_key=key,
                api_secret=secret,
                passphrase=passphrase,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
        elif venue == "MEXC":
            key, secret = _require_venue_credentials(
                venue,
                bybit_key_env=bybit_key_env,
                bybit_secret_env=bybit_secret_env,
                binance_key_env=binance_key_env,
                binance_secret_env=binance_secret_env,
                okx_key_env=okx_key_env,
                okx_secret_env=okx_secret_env,
                okx_passphrase_env=okx_passphrase_env,
                mexc_key_env=mexc_key_env,
                mexc_secret_env=mexc_secret_env,
            )
            results[venue] = await fetch_mexc_spot_fees(
                symbols,
                api_key=key,
                api_secret=secret,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            )
    return results


async def audit_spot_fee_rates_best_effort(
    symbols_by_venue: Mapping[str, Sequence[str]],
    *,
    proxy_url: str | None,
    timeout_seconds: float,
    minimum_request_interval_seconds: float = 0.55,
    bybit_key_env: str = "BYBIT_READONLY_API_KEY",
    bybit_secret_env: str = "BYBIT_READONLY_API_SECRET",
    binance_key_env: str = "BINANCE_READONLY_API_KEY",
    binance_secret_env: str = "BINANCE_READONLY_API_SECRET",
    okx_key_env: str = "OKX_READONLY_API_KEY",
    okx_secret_env: str = "OKX_READONLY_API_SECRET",
    okx_passphrase_env: str = "OKX_READONLY_API_PASSPHRASE",
    mexc_key_env: str = "MEXC_READONLY_API_KEY",
    mexc_secret_env: str = "MEXC_READONLY_API_SECRET",
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> SpotFeeAuditOutcome:
    """Audit a broad universe one symbol at a time without hiding gaps.

    A single unlisted or temporarily rejected symbol is recorded in
    ``symbol_errors`` and does not stop the rest of the read-only audit.  A
    monitor receiving this report still uses an explicitly unverified fallback
    for every missing symbol, so partial output cannot create a false
    executable candidate.
    """

    if minimum_request_interval_seconds < 0:
        raise ValueError("minimum request interval must be non-negative")
    normalized_symbols_by_venue = {
        str(venue).upper(): tuple(dict.fromkeys(str(symbol).upper() for symbol in raw_symbols))
        for venue, raw_symbols in symbols_by_venue.items()
    }
    unknown = set(normalized_symbols_by_venue) - set(SUPPORTED_VENUES)
    if unknown:
        raise ValueError(f"unsupported fee-audit venues: {sorted(unknown)}")

    rates: dict[str, dict[str, SpotFeeRate]] = {}
    symbol_errors: dict[str, dict[str, str]] = {}
    for venue, symbols in normalized_symbols_by_venue.items():
        rates[venue] = {}
        symbol_errors[venue] = {}
        if not symbols:
            continue
        try:
            _require_venue_credentials(
                venue,
                bybit_key_env=bybit_key_env,
                bybit_secret_env=bybit_secret_env,
                binance_key_env=binance_key_env,
                binance_secret_env=binance_secret_env,
                okx_key_env=okx_key_env,
                okx_secret_env=okx_secret_env,
                okx_passphrase_env=okx_passphrase_env,
                mexc_key_env=mexc_key_env,
                mexc_secret_env=mexc_secret_env,
            )
        except (RuntimeError, ValueError) as error:
            safe_error = _safe_symbol_error(error)
            symbol_errors[venue] = {symbol: safe_error for symbol in symbols}
            continue

        for index, symbol in enumerate(symbols):
            try:
                audited = await audit_spot_fee_rates(
                    {venue: (symbol,)},
                    proxy_url=proxy_url,
                    timeout_seconds=timeout_seconds,
                    bybit_key_env=bybit_key_env,
                    bybit_secret_env=bybit_secret_env,
                    binance_key_env=binance_key_env,
                    binance_secret_env=binance_secret_env,
                    okx_key_env=okx_key_env,
                    okx_secret_env=okx_secret_env,
                    okx_passphrase_env=okx_passphrase_env,
                    mexc_key_env=mexc_key_env,
                    mexc_secret_env=mexc_secret_env,
                    fetch_json=fetch_json,
                )
                rates[venue][symbol] = audited[venue][symbol]
            except Exception as error:  # a remote symbol must not poison other symbols
                symbol_errors[venue][symbol] = _safe_symbol_error(error)
            if index + 1 < len(symbols) and minimum_request_interval_seconds:
                await asyncio.sleep(minimum_request_interval_seconds)
    return SpotFeeAuditOutcome(rates=rates, symbol_errors=symbol_errors)


def load_spot_fee_audit(path: Path) -> dict[tuple[str, str], SpotFeeRate]:
    """Load a completed, locally generated fee-audit report safely.

    The monitor receives only this compact, secret-free file.  It does *not*
    read API credentials or call private endpoints itself.  A malformed or
    incomplete report is rejected rather than silently being treated as an
    account-confirmed fee schedule.
    """

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read fee-audit report {path}: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise ValueError("fee-audit report must be a JSON object")
    if payload.get("schema_version") != 1:
        raise ValueError("unsupported fee-audit report schema")
    if payload.get("status") != "completed":
        raise ValueError("fee-audit report is not completed")
    venues = payload.get("venues")
    if not isinstance(venues, Mapping):
        raise ValueError("fee-audit report has no venues object")

    result: dict[tuple[str, str], SpotFeeRate] = {}
    for raw_venue, section in venues.items():
        venue = str(raw_venue).upper()
        if venue not in SUPPORTED_VENUES:
            raise ValueError(f"unsupported venue in fee-audit report: {venue}")
        if not isinstance(section, Mapping):
            raise ValueError(f"fee-audit report venue {venue} is malformed")
        records = section.get("rates")
        if not isinstance(records, list):
            raise ValueError(f"fee-audit report venue {venue} has no rates list")
        for record in records:
            if not isinstance(record, Mapping):
                raise ValueError(f"fee-audit report contains malformed {venue} rate")
            record_venue = str(record.get("venue", "")).upper()
            symbol = str(record.get("symbol", "")).upper()
            if record_venue != venue or not symbol:
                raise ValueError(f"fee-audit report has malformed {venue} rate identity")
            account_verified = record.get("account_verified")
            if not isinstance(account_verified, bool):
                raise ValueError(f"fee-audit report {venue}:{symbol} has no verification flag")
            source = record.get("source")
            if not isinstance(source, str) or not source:
                raise ValueError(f"fee-audit report {venue}:{symbol} has no source")
            assumptions = record.get("assumptions", [])
            if not isinstance(assumptions, list) or not all(
                isinstance(item, str) for item in assumptions
            ):
                raise ValueError(f"fee-audit report {venue}:{symbol} has malformed assumptions")
            fee = SpotFeeRate(
                venue=venue,
                symbol=symbol,
                maker_buy_bps=_decimal(record.get("maker_buy_bps"), field="maker_buy_bps"),
                maker_sell_bps=_decimal(record.get("maker_sell_bps"), field="maker_sell_bps"),
                taker_buy_bps=_decimal(record.get("taker_buy_bps"), field="taker_buy_bps"),
                taker_sell_bps=_decimal(record.get("taker_sell_bps"), field="taker_sell_bps"),
                account_verified=account_verified,
                source=source,
                assumptions=tuple(assumptions),
            )
            key = (venue, symbol)
            if key in result:
                raise ValueError(f"fee-audit report contains duplicate rate for {venue}:{symbol}")
            result[key] = fee
    if not result:
        raise ValueError("fee-audit report contains no rates")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venues", type=_parse_venues, default=list(SUPPORTED_VENUES))
    symbol_scope = parser.add_mutually_exclusive_group()
    symbol_scope.add_argument(
        "--symbols-by-venue",
        type=_parse_symbols_by_venue,
        help="Manual lists, e.g. BYBIT=BTCUSDT|ETHUSDT;OKX=BTC-USDT. Defaults to BTC/ETH/SOL.",
    )
    symbol_scope.add_argument(
        "--market-universe",
        choices=MARKET_UNIVERSES,
        help=(
            "Build CEX symbols from a monitor universe: rolling-maximum and "
            "continuous-maximum are the current 44 single-asset routes; "
            "triangle-default contains both CEX legs of all 95 cross pairs; "
            "all-current is their union."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--minimum-request-interval-seconds",
        type=float,
        default=0.55,
        help=(
            "Minimum spacing between per-symbol private GET requests (default: 0.55; "
            "keeps the OKX fee endpoint below its documented 5 requests / 2 seconds limit)."
        ),
    )
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/fee-audits"))
    parser.add_argument("--run-id")
    parser.add_argument("--bybit-key-env", default="BYBIT_READONLY_API_KEY")
    parser.add_argument("--bybit-secret-env", default="BYBIT_READONLY_API_SECRET")
    parser.add_argument("--binance-key-env", default="BINANCE_READONLY_API_KEY")
    parser.add_argument("--binance-secret-env", default="BINANCE_READONLY_API_SECRET")
    parser.add_argument("--okx-key-env", default="OKX_READONLY_API_KEY")
    parser.add_argument("--okx-secret-env", default="OKX_READONLY_API_SECRET")
    parser.add_argument("--okx-passphrase-env", default="OKX_READONLY_API_PASSPHRASE")
    parser.add_argument("--mexc-key-env", default="MEXC_READONLY_API_KEY")
    parser.add_argument("--mexc-secret-env", default="MEXC_READONLY_API_SECRET")
    return parser


async def _run_cli(args: argparse.Namespace) -> dict[str, Any]:
    if args.timeout_seconds <= 0:
        raise ValueError("timeout must be positive")
    if args.minimum_request_interval_seconds < 0:
        raise ValueError("minimum request interval must be non-negative")
    if args.market_universe is not None:
        selected = symbols_for_market_universe(args.market_universe, args.venues)
        selection_mode = f"market_universe:{args.market_universe}"
    else:
        manual_symbols = args.symbols_by_venue or _parse_symbols_by_venue(
            DEFAULT_SYMBOLS_BY_VENUE_TEXT,
        )
        selected = {
            venue: symbols
            for venue, symbols in manual_symbols.items()
            if venue in set(args.venues)
        }
        selection_mode = "manual" if args.symbols_by_venue is not None else "default_small_manual"
    missing = [venue for venue in args.venues if venue not in selected]
    if missing:
        raise ValueError(f"--symbols-by-venue has no symbols for: {', '.join(missing)}")
    network_route = configure_process_network_route(args.proxy_url)
    outcome = await audit_spot_fee_rates_best_effort(
        selected,
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
        minimum_request_interval_seconds=args.minimum_request_interval_seconds,
        bybit_key_env=args.bybit_key_env,
        bybit_secret_env=args.bybit_secret_env,
        binance_key_env=args.binance_key_env,
        binance_secret_env=args.binance_secret_env,
        okx_key_env=args.okx_key_env,
        okx_secret_env=args.okx_secret_env,
        okx_passphrase_env=args.okx_passphrase_env,
        mexc_key_env=args.mexc_key_env,
        mexc_secret_env=args.mexc_secret_env,
    )
    if not any(outcome.rates.values()):
        raise RuntimeError("fee audit obtained no account-verified symbol rates; no report was written")
    run_id = args.run_id or default_run_id("spot-fees")
    validate_run_id(run_id)
    output = args.output_root / f"{run_id}.json"
    if output.exists():
        raise FileExistsError(f"refusing to overwrite fee-audit report: {output}")
    report = {
        "schema_version": 1,
        "status": "completed",
        "generated_at": datetime.now(UTC).isoformat(),
        "mode": "read_only_account_specific_spot_fee_audit",
        "symbol_selection": {
            "mode": selection_mode,
            "market_universe": args.market_universe,
        },
        "request_pacing": {
            "minimum_request_interval_seconds": args.minimum_request_interval_seconds,
            "per_symbol_private_get": True,
        },
        "venues": {
            venue: {
                "symbols_requested": list(selected[venue]),
                "rates": [
                    outcome.rates.get(venue, {})[symbol].record()
                    for symbol in sorted(outcome.rates.get(venue, {}))
                ],
                "symbol_errors": [
                    {"symbol": symbol, "error": error}
                    for symbol, error in sorted(outcome.symbol_errors.get(venue, {}).items())
                ],
            }
            for venue in args.venues
        },
        "api_credentials_used": True,
        "credential_storage": "not persisted; only local environment values were used in memory",
        "private_endpoints": {
            "BYBIT": _redact_url(BYBIT_FEE_ENDPOINT),
            "OKX": _redact_url(OKX_TRADE_FEE_ENDPOINT),
            "OKX_ACCOUNT_INSTRUMENTS": _redact_url(OKX_ACCOUNT_INSTRUMENTS_ENDPOINT),
            "BINANCE": _redact_url(BINANCE_SPOT_COMMISSION_ENDPOINT),
            "MEXC": _redact_url(MEXC_TRADE_FEE_ENDPOINT),
        },
        "operations": (
            "GET fee-rate endpoints plus OKX account-instruments metadata only; "
            "no balances, positions, orders, transactions or withdrawals"
        ),
        "limitations": [
            "fee schedule is a current snapshot; it can change with VIP tier, region or promotion",
            "Binance output intentionally assumes no optional BNB discount at fill time",
            "OKX documents that some zero-fee promotions may not be reflected by Open API",
            "a symbol_error is not a rate; monitors retain the non-verified fallback for it",
            "network gas, funding, withdrawal, bridge, wrapper and future exit costs are not account trading fees",
        ],
        "network_route": network_route,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(output, report)
    return {**report, "output": str(output.resolve())}


def main() -> None:
    args = _parser().parse_args()
    try:
        report = asyncio.run(_run_cli(args))
    except (FileExistsError, RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
