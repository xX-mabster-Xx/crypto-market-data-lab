"""Read-only DEX spot <-> Bybit linear-perpetual basis monitor.

This is intentionally a *position* monitor, not a claim of atomic arbitrage.
For example, ``USDC -> WBTC`` on a DEX plus a Bybit ``BTCUSDT`` short opens a
delta-hedged basis position.  The monitor keeps public market data only in
memory, retains compact positive-candidate lifecycles, and never builds an
order, transaction, wallet request, or execution client.

It adds data that the older spot-only monitors do not have:

* Bybit linear L50 order-book depth;
* funding rate, next funding time, mark and index price from the public ticker;
* public linear contract limits (quantity step, minimum notional, etc.);
* a USDC/USDT conversion book whenever a DEX quote is denominated in USDC;
* optional, *read-only* account fee-rate lookup.  Credentials are never read
  unless ``--use-account-fee-rate`` is explicitly passed, never persisted, and
  no endpoint capable of placing orders is present in this module.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import os
import time
import urllib.parse
from collections import Counter, defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any

from market_data_lab.cex_book_streams import BybitLinearOrderBookStream
from market_data_lab.cex_book_streams import BybitLinearTicker
from market_data_lab.cex_book_streams import PublicBookStream
from market_data_lab.cex_book_streams import build_public_book_stream
from market_data_lab.cex_book_streams import stream_health
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import _best_dex_records
from market_data_lab.cex_dex_cycles import _buy_base
from market_data_lab.cex_dex_cycles import _sell_base
from market_data_lab.cex_dex_cycles import build_cycle_providers
from market_data_lab.continuous_cycle_monitor import _provider_gate_key
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import OMNISTON_WS_ENDPOINT
from market_data_lab.dex_quotes import quote_route_labels
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _timed_fetch
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.rolling_cycle_monitor import MAXIMUM_COVERAGE_MARKETS


BYBIT_PUBLIC_INSTRUMENTS_ENDPOINT = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_ACCOUNT_FEE_RATE_ENDPOINT = "https://api.bybit.com/v5/account/fee-rate"
BYBIT_LINEAR_VIP0_STANDARD_TAKER_FEE_BPS = Decimal("5.5")
BYBIT_USDC_USDT_SPOT_SYMBOL = "USDCUSDT"


@dataclass(frozen=True)
class BybitLinearInstrument:
    """The tradability constraints needed to turn a DEX amount into a hedge."""

    symbol: str
    base_coin: str
    quote_coin: str
    settle_coin: str
    contract_type: str
    status: str
    min_order_qty: Decimal
    qty_step: Decimal
    max_market_order_qty: Decimal
    min_notional_value: Decimal
    tick_size: Decimal
    funding_interval_minutes: int
    upper_funding_rate: Decimal | None
    lower_funding_rate: Decimal | None


@dataclass(frozen=True)
class PerpFeeRate:
    symbol: str
    taker_fee_bps: Decimal
    maker_fee_bps: Decimal | None
    source: str
    account_verified: bool


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal {field}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"non-finite decimal {field}")
    return result


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


def parse_bybit_linear_instruments(payload: Any) -> dict[str, BybitLinearInstrument]:
    """Parse only active USDT-settled linear perpetual contracts.

    The public endpoint also returns inverse contracts and dated futures when
    queried by a base coin.  Including either would silently give a wrong PnL
    and funding model, so they are rejected here.
    """

    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise ValueError(f"Bybit instruments error response: {payload!r}"[:512])
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, dict) else None
    if not isinstance(rows, list):
        raise ValueError("Bybit instruments result has no list")
    parsed: dict[str, BybitLinearInstrument] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("contractType") != "LinearPerpetual" or row.get("status") != "Trading":
            continue
        if row.get("quoteCoin") != "USDT" or row.get("settleCoin") != "USDT":
            continue
        lot = row.get("lotSizeFilter")
        price = row.get("priceFilter")
        if not isinstance(lot, dict) or not isinstance(price, dict):
            continue
        try:
            instrument = BybitLinearInstrument(
                symbol=str(row["symbol"]).upper(),
                base_coin=str(row["baseCoin"]).upper(),
                quote_coin=str(row["quoteCoin"]).upper(),
                settle_coin=str(row["settleCoin"]).upper(),
                contract_type=str(row["contractType"]),
                status=str(row["status"]),
                min_order_qty=_decimal(lot["minOrderQty"], field="minOrderQty"),
                qty_step=_decimal(lot["qtyStep"], field="qtyStep"),
                max_market_order_qty=_decimal(lot["maxMktOrderQty"], field="maxMktOrderQty"),
                min_notional_value=_decimal(lot["minNotionalValue"], field="minNotionalValue"),
                tick_size=_decimal(price["tickSize"], field="tickSize"),
                funding_interval_minutes=int(row.get("fundingInterval", 0)),
                upper_funding_rate=_optional_decimal(row.get("upperFundingRate")),
                lower_funding_rate=_optional_decimal(row.get("lowerFundingRate")),
            )
        except (KeyError, TypeError, ValueError):
            continue
        if instrument.qty_step <= 0 or instrument.min_order_qty <= 0:
            continue
        parsed[instrument.symbol] = instrument
    return parsed


async def fetch_bybit_linear_instruments(
    *,
    endpoint: str = BYBIT_PUBLIC_INSTRUMENTS_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, BybitLinearInstrument]:
    """Fetch the paginated public Bybit linear-perpetual universe."""

    cursor: str | None = None
    instruments: dict[str, BybitLinearInstrument] = {}
    while True:
        query: list[tuple[str, str]] = [("category", "linear"), ("limit", "1000")]
        if cursor:
            query.append(("cursor", cursor))
        response = await _timed_fetch(
            fetch_json,
            url=f"{endpoint}?{urllib.parse.urlencode(query)}",
            method="GET",
            body=None,
            headers={},
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
        )
        if response.error is not None:
            raise RuntimeError(f"Bybit instruments request failed: {response.error}")
        instruments.update(parse_bybit_linear_instruments(response.payload))
        result = response.payload.get("result") if isinstance(response.payload, dict) else None
        next_cursor = result.get("nextPageCursor") if isinstance(result, dict) else None
        if not isinstance(next_cursor, str) or not next_cursor:
            return instruments
        cursor = next_cursor


def _bybit_hmac_headers(
    *,
    api_key: str,
    api_secret: str,
    query: str,
    timestamp_ms: int | None = None,
    recv_window_ms: int = 5_000,
) -> dict[str, str]:
    """Create the documented signature for a read-only V5 GET request."""

    if not api_key or not api_secret:
        raise ValueError("Bybit API key and secret must both be non-empty")
    timestamp = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1_000))
    recv_window = str(recv_window_ms)
    payload = f"{timestamp}{api_key}{recv_window}{query}".encode()
    signature = hmac.new(api_secret.encode(), payload, hashlib.sha256).hexdigest()
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature,
    }


def parse_bybit_fee_rate(payload: Any, *, symbol: str) -> PerpFeeRate:
    """Parse one authenticated exact fee-rate response without retaining it."""

    if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
        raise ValueError(f"Bybit fee-rate error response: {payload!r}"[:512])
    result = payload.get("result")
    rows = result.get("list") if isinstance(result, dict) else None
    if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
        raise ValueError("Bybit fee-rate result is empty")
    row = next(
        (
            item
            for item in rows
            if isinstance(item, dict) and str(item.get("symbol", "")).upper() == symbol.upper()
        ),
        rows[0],
    )
    if not isinstance(row, dict):
        raise ValueError("Bybit fee-rate row is malformed")
    return PerpFeeRate(
        symbol=symbol.upper(),
        taker_fee_bps=_decimal(row["takerFeeRate"], field="takerFeeRate") * Decimal(10_000),
        maker_fee_bps=(
            _decimal(row["makerFeeRate"], field="makerFeeRate") * Decimal(10_000)
            if row.get("makerFeeRate") is not None
            else None
        ),
        source="bybit_account_fee_rate_api",
        account_verified=True,
    )


async def fetch_bybit_account_fee_rates(
    symbols: Sequence[str],
    *,
    api_key: str,
    api_secret: str,
    endpoint: str = BYBIT_ACCOUNT_FEE_RATE_ENDPOINT,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, PerpFeeRate]:
    """Read the user's exact linear fee rate once per selected contract.

    This performs only documented ``GET /v5/account/fee-rate`` calls.  It does
    not inspect balances, positions, orders or trade history.
    """

    result: dict[str, PerpFeeRate] = {}
    for symbol in sorted(set(item.upper() for item in symbols)):
        query = urllib.parse.urlencode((("category", "linear"), ("symbol", symbol)))
        response = await _timed_fetch(
            fetch_json,
            url=f"{endpoint}?{query}",
            method="GET",
            body=None,
            headers=_bybit_hmac_headers(
                api_key=api_key,
                api_secret=api_secret,
                query=query,
            ),
            proxy_url=proxy_url,
            timeout_seconds=timeout_seconds,
        )
        if response.error is not None:
            raise RuntimeError(f"Bybit fee-rate request for {symbol} failed: {response.error}")
        result[symbol] = parse_bybit_fee_rate(response.payload, symbol=symbol)
    return result


def public_fallback_fee_rates(
    symbols: Sequence[str],
    *,
    taker_fee_bps: Decimal = BYBIT_LINEAR_VIP0_STANDARD_TAKER_FEE_BPS,
) -> dict[str, PerpFeeRate]:
    """Provide a clearly labelled provisional rate when no account key is used."""

    if taker_fee_bps < 0 or not taker_fee_bps.is_finite():
        raise ValueError("fallback perp taker fee must be finite and non-negative")
    return {
        symbol.upper(): PerpFeeRate(
            symbol=symbol.upper(),
            taker_fee_bps=taker_fee_bps,
            maker_fee_bps=None,
            source="bybit_public_vip0_standard_schedule_not_account_verified",
            account_verified=False,
        )
        for symbol in symbols
    }


def _instrument_record(instrument: BybitLinearInstrument) -> dict[str, Any]:
    """Make contract metadata JSON-safe without losing precision."""

    return {
        "symbol": instrument.symbol,
        "base_coin": instrument.base_coin,
        "quote_coin": instrument.quote_coin,
        "settle_coin": instrument.settle_coin,
        "contract_type": instrument.contract_type,
        "status": instrument.status,
        "min_order_qty": _decimal_text(instrument.min_order_qty),
        "qty_step": _decimal_text(instrument.qty_step),
        "max_market_order_qty": _decimal_text(instrument.max_market_order_qty),
        "min_notional_value": _decimal_text(instrument.min_notional_value),
        "tick_size": _decimal_text(instrument.tick_size),
        "funding_interval_minutes": instrument.funding_interval_minutes,
        "upper_funding_rate": (
            _decimal_text(instrument.upper_funding_rate)
            if instrument.upper_funding_rate is not None
            else None
        ),
        "lower_funding_rate": (
            _decimal_text(instrument.lower_funding_rate)
            if instrument.lower_funding_rate is not None
            else None
        ),
    }


def _fee_rate_record(fee_rate: PerpFeeRate) -> dict[str, Any]:
    return {
        "symbol": fee_rate.symbol,
        "taker_fee_bps": _decimal_text(fee_rate.taker_fee_bps),
        "maker_fee_bps": (
            _decimal_text(fee_rate.maker_fee_bps) if fee_rate.maker_fee_bps is not None else None
        ),
        "source": fee_rate.source,
        "account_verified": fee_rate.account_verified,
    }


def bybit_linear_symbol(market: CycleMarket) -> str:
    """Map the CEX hedge ticker proxy configured for a DEX market to Bybit."""

    return f"{market.cex_base_symbol.upper()}USDT"


def _round_down_quantity(amount: Decimal, qty_step: Decimal) -> Decimal:
    if amount <= 0 or qty_step <= 0:
        return Decimal(0)
    return (amount / qty_step).to_integral_value(rounding=ROUND_DOWN) * qty_step


def _perp_mid_price(book: BookSnapshot | None) -> Decimal | None:
    """Return a conservative sizing reference from the current L50 top level."""

    if book is None or book.status != "ok" or not book.bids or not book.asks:
        return None
    bid = book.bids[0][0]
    ask = book.asks[0][0]
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    return (bid + ask) / Decimal(2)


def exact_perp_step_targets(
    notionals: Sequence[Decimal],
    *,
    book: BookSnapshot | None,
    instrument: BybitLinearInstrument,
) -> tuple[Decimal | None, list[tuple[Decimal, Decimal]]]:
    """Map desired USDT exposure to DEX quantities aligned to a perp step.

    The top-of-book mid is only a *sizing* reference; final PnL is always
    calculated later from the DEX quote and the executable CEX depth.  A
    target is omitted if it cannot form even the minimum allowed perpetual
    order.  This prevents an exact-output quote from creating an inherently
    unhedgeable DEX balance.
    """

    mid = _perp_mid_price(book)
    if mid is None:
        return None, []
    targets: list[tuple[Decimal, Decimal]] = []
    for notional in notionals:
        quantity = _round_down_quantity(notional / mid, instrument.qty_step)
        if quantity < instrument.min_order_qty or quantity > instrument.max_market_order_qty:
            continue
        targets.append((notional, quantity))
    return mid, targets


def _stable_usdt_value(
    *,
    amount: Decimal,
    symbol: str,
    side: str,
    stable_book: BookSnapshot | None,
) -> tuple[Decimal, str]:
    """Value a DEX stable amount in USDT with an executable conversion side.

    ``side=cost`` buys USDC using USDT; ``side=proceeds`` sells USDC into USDT.
    This matters even though the rate is normally close to one: it stops the
    model from silently treating two separate inventory assets as identical.
    """

    normalized = symbol.upper()
    if normalized == "USDT":
        return amount, "same_quote_asset"
    if normalized != "USDC":
        raise ValueError(f"unsupported DEX quote asset for USDT valuation: {symbol}")
    if stable_book is None:
        raise ValueError("USDC/USDT public conversion book is unavailable")
    if side == "cost":
        converted = _buy_base(stable_book.asks, amount)
        mode = "buy_usdc_with_usdt_vwap"
    elif side == "proceeds":
        converted = _sell_base(stable_book.bids, amount)
        mode = "sell_usdc_for_usdt_vwap"
    else:
        raise ValueError(f"unsupported stable conversion side: {side}")
    if converted is None:
        raise ValueError("insufficient USDC/USDT conversion depth")
    return converted, mode


def _gas_hint(dex_record: Mapping[str, Any]) -> str | None:
    quote_result = dex_record.get("quote_result")
    if isinstance(quote_result, Mapping) and quote_result.get("gas_estimate") is not None:
        return str(quote_result["gas_estimate"])
    metadata = dex_record.get("quote_service_metadata")
    if isinstance(metadata, Mapping):
        for key in ("gas_budget", "estimated_gas_consumption", "gas_params"):
            if metadata.get(key) is not None:
                return str(metadata[key])[:256]
    return None


def calculate_perp_dex_basis(
    *,
    market: CycleMarket,
    dex_record: Mapping[str, Any],
    perp_book: BookSnapshot,
    stable_book: BookSnapshot | None,
    instrument: BybitLinearInstrument,
    fee_rate: PerpFeeRate,
    ticker: BybitLinearTicker | None,
    network_cost_floor_usdt: Decimal,
    max_response_skew_ms: Decimal,
    max_ticker_age_ms: Decimal,
    max_hedge_residual_bps: Decimal,
    dex_network_transactions_round_trip: int = 2,
) -> dict[str, Any]:
    """Price one DEX swap plus its delta-opposite linear-perp hedge.

    The returned ``modeled_entry_basis_*`` values are not realised cash PnL.
    They reserve a taker fee for opening and a current-book reserve for closing
    the perpetual, plus configured DEX network-cost floors for *both* entry
    and eventual DEX exit.  Funding is shown separately for one *current-rate*
    interval because it is variable until settlement.
    """

    common: dict[str, Any] = {
        "schema_version": 1,
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "bybit_symbol": instrument.symbol,
        "dex_direction": dex_record.get("direction"),
        "requested_notional_quote": dex_record.get("requested_notional_quote"),
        "dex_quote_symbol": market.quote_symbol,
        "asset_equivalence": market.asset_equivalence,
        "perp_contract": _instrument_record(instrument),
        "perp_taker_fee_bps": _decimal_text(fee_rate.taker_fee_bps),
        "perp_fee_source": fee_rate.source,
        "perp_fee_account_verified": fee_rate.account_verified,
        "network_cost_floor_usdt": _decimal_text(network_cost_floor_usdt),
        "dex_network_transactions_round_trip": dex_network_transactions_round_trip,
        "dex_execution_gas_hint": _gas_hint(dex_record),
        "dex_quote_semantics": dex_record.get("quote_semantics", "exact_input_quote_notional"),
        "dex_hedge_quantity_exact": dex_record.get("hedge_quantity_exact") is True,
        "perp_target_sizing_reference_mid_usdt": dex_record.get(
            "perp_target_sizing_reference_mid_usdt",
        ),
        "dex_request_rtt_ms": dex_record.get("request_rtt_ms"),
        "perp_book_received_realtime_ns": perp_book.response.received_realtime_ns,
        "dex_received_realtime_ns": dex_record.get("response_received_realtime_ns"),
    }
    if dex_network_transactions_round_trip <= 0:
        raise ValueError("DEX network transaction reserve count must be positive")
    if dex_record.get("status") != "ok":
        return {**common, "status": "dex_quote_unavailable", "timing_valid": False}
    if perp_book.status != "ok" or perp_book.category != "linear":
        return {**common, "status": "perp_book_unavailable", "timing_valid": False}
    if ticker is None:
        return {**common, "status": "perp_ticker_unavailable", "timing_valid": False}
    if ticker.mark_price is None or ticker.funding_rate is None:
        return {**common, "status": "perp_ticker_incomplete", "timing_valid": False}
    try:
        dex_received_ns = int(dex_record["response_received_realtime_ns"])
        base_amount = _decimal(dex_record["base_amount"], field="base_amount")
        quote_amount = _decimal(dex_record["quote_amount"], field="quote_amount")
    except (KeyError, TypeError, ValueError) as exc:
        return {**common, "status": "invalid_dex_quote", "timing_valid": False, "error": str(exc)}
    if base_amount <= 0 or quote_amount <= 0:
        return {**common, "status": "invalid_dex_quote", "timing_valid": False}

    response_skews_ns = [abs(perp_book.response.received_realtime_ns - dex_received_ns)]
    if market.quote_symbol.upper() == "USDC":
        if stable_book is None or stable_book.status != "ok":
            return {**common, "status": "stable_fx_unavailable", "timing_valid": False}
        response_skews_ns.append(abs(stable_book.response.received_realtime_ns - dex_received_ns))
    response_skew_ms = Decimal(max(response_skews_ns)) / Decimal(1_000_000)
    ticker_age_ms = Decimal(
        max(0, time.monotonic_ns() - ticker.received_monotonic_ns)
    ) / Decimal(1_000_000)
    timing_valid = response_skew_ms <= max_response_skew_ms
    if ticker_age_ms > max_ticker_age_ms:
        return {
            **common,
            "status": "perp_ticker_stale",
            "timing_valid": False,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
        }

    hedge_quantity = _round_down_quantity(base_amount, instrument.qty_step)
    residual = base_amount - hedge_quantity
    residual_bps = residual / base_amount * Decimal(10_000)
    if hedge_quantity < instrument.min_order_qty:
        return {
            **common,
            "status": "below_perp_minimum_quantity",
            "timing_valid": timing_valid,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
            "dex_base_amount": _decimal_text(base_amount),
            "hedge_quantity": _decimal_text(hedge_quantity),
            "hedge_residual_bps": round(float(residual_bps), 6),
        }
    if hedge_quantity > instrument.max_market_order_qty:
        return {
            **common,
            "status": "above_perp_market_order_limit",
            "timing_valid": timing_valid,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
            "hedge_quantity": _decimal_text(hedge_quantity),
        }

    fee_fraction = fee_rate.taker_fee_bps / Decimal(10_000)
    direction = str(dex_record.get("direction"))
    try:
        if direction == "buy_base":
            # DEX buys the base asset, so sell/short its delta on the perp.
            perp_entry_notional = _sell_base(perp_book.bids, hedge_quantity)
            perp_close_notional = _buy_base(perp_book.asks, hedge_quantity)
            dex_value_usdt, stable_mode = _stable_usdt_value(
                amount=quote_amount,
                symbol=market.quote_symbol,
                side="cost",
                stable_book=stable_book,
            )
            perp_side = "short"
            hedge_action = "sell_linear_perpetual"
            gross_basis = (perp_entry_notional or Decimal(-1)) - dex_value_usdt
            funding_pnl = ticker.mark_price * hedge_quantity * ticker.funding_rate
        elif direction == "sell_base":
            # DEX sells the base asset, so buy/long its delta on the perp.
            perp_entry_notional = _buy_base(perp_book.asks, hedge_quantity)
            perp_close_notional = _sell_base(perp_book.bids, hedge_quantity)
            dex_value_usdt, stable_mode = _stable_usdt_value(
                amount=quote_amount,
                symbol=market.quote_symbol,
                side="proceeds",
                stable_book=stable_book,
            )
            perp_side = "long"
            hedge_action = "buy_linear_perpetual"
            gross_basis = dex_value_usdt - (perp_entry_notional or Decimal(-1))
            funding_pnl = -ticker.mark_price * hedge_quantity * ticker.funding_rate
        else:
            raise ValueError(f"unsupported DEX direction {direction}")
    except ValueError as exc:
        return {
            **common,
            "status": "stable_fx_or_depth_unavailable",
            "timing_valid": timing_valid,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
            "error": str(exc),
        }
    if perp_entry_notional is None or perp_close_notional is None:
        return {
            **common,
            "status": "insufficient_perp_depth",
            "timing_valid": timing_valid,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
        }
    if perp_entry_notional < instrument.min_notional_value:
        return {
            **common,
            "status": "below_perp_minimum_notional",
            "timing_valid": timing_valid,
            "response_skew_ms": round(float(response_skew_ms), 6),
            "ticker_age_ms": round(float(ticker_age_ms), 6),
            "perp_entry_notional_usdt": _decimal_text(perp_entry_notional),
        }

    open_fee = perp_entry_notional * fee_fraction
    close_fee_reserve = perp_close_notional * fee_fraction
    network_cost_reserve = network_cost_floor_usdt * Decimal(dex_network_transactions_round_trip)
    net_before_funding = gross_basis - open_fee - close_fee_reserve - network_cost_reserve
    net_after_one_current_funding = net_before_funding + funding_pnl
    basis_bps = net_before_funding / max(dex_value_usdt, Decimal("0.00000001")) * Decimal(10_000)
    basis_with_funding_bps = (
        net_after_one_current_funding / max(dex_value_usdt, Decimal("0.00000001")) * Decimal(10_000)
    )
    hedge_exact = residual_bps <= max_hedge_residual_bps
    positive = timing_valid and hedge_exact and net_after_one_current_funding > 0
    return {
        **common,
        "status": "ok" if timing_valid and hedge_exact else (
            "timing_skew_exceeded" if not timing_valid else "perp_quantity_residual_too_large"
        ),
        "timing_valid": timing_valid,
        "response_skew_ms": round(float(response_skew_ms), 6),
        "ticker_age_ms": round(float(ticker_age_ms), 6),
        "perp_side": perp_side,
        "hedge_action": hedge_action,
        "dex_base_amount": _decimal_text(base_amount),
        "hedge_quantity": _decimal_text(hedge_quantity),
        "unhedged_base_quantity": _decimal_text(residual),
        "hedge_residual_bps": round(float(residual_bps), 6),
        "max_hedge_residual_bps": _decimal_text(max_hedge_residual_bps),
        "hedge_quantity_exact_enough": hedge_exact,
        "stable_conversion_mode": stable_mode,
        "dex_value_usdt": _decimal_text(dex_value_usdt),
        "perp_entry_vwap_usdt": _decimal_text(perp_entry_notional / hedge_quantity),
        "perp_close_vwap_reserve_usdt": _decimal_text(perp_close_notional / hedge_quantity),
        "perp_entry_notional_usdt": _decimal_text(perp_entry_notional),
        "perp_open_fee_usdt": _decimal_text(open_fee),
        "perp_close_fee_reserve_usdt": _decimal_text(close_fee_reserve),
        "network_cost_reserve_usdt": _decimal_text(network_cost_reserve),
        "gross_entry_basis_usdt": _decimal_text(gross_basis),
        "modeled_entry_basis_after_open_close_fees_and_network_usdt": _decimal_text(net_before_funding),
        "modeled_entry_basis_after_open_close_fees_and_network_bps": round(float(basis_bps), 6),
        "current_funding_rate": _decimal_text(ticker.funding_rate),
        "next_funding_time_ms": ticker.next_funding_time_ms,
        "perp_mark_price": _decimal_text(ticker.mark_price),
        "perp_index_price": _decimal_text(ticker.index_price) if ticker.index_price is not None else None,
        "funding_pnl_if_held_one_current_interval_usdt": _decimal_text(funding_pnl),
        "modeled_entry_basis_after_one_current_funding_interval_usdt": _decimal_text(
            net_after_one_current_funding,
        ),
        "modeled_entry_basis_after_one_current_funding_interval_bps": round(
            float(basis_with_funding_bps),
            6,
        ),
        "positive_after_modeled_costs": positive,
        "candidate_eligible_with_account_verified_fee": positive and fee_rate.account_verified,
        "model_interpretation": (
            "entry basis estimate, not realised PnL; actual exit price, funding and rebalance remain variable"
        ),
    }


def _candidate_key(row: Mapping[str, Any]) -> str:
    return "|".join(
        str(row.get(field, ""))
        for field in ("market", "bybit_symbol", "dex_direction", "requested_notional_quote")
    )


@dataclass
class _ActiveBasisCandidate:
    started_ns: int
    started_at: str
    last_seen_ns: int
    observations: int
    best_basis_usdt: Decimal
    best_row: dict[str, Any]


def _iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).isoformat()


class BasisCandidateTracker:
    """Track positive modeled windows, optionally requiring account fees."""

    def __init__(self, *, require_account_verified_fee: bool = True) -> None:
        self.require_account_verified_fee = require_account_verified_fee
        self.active: dict[str, _ActiveBasisCandidate] = {}
        self.started = 0
        self.improved = 0
        self.closed = 0

    def _candidate(self, row: Mapping[str, Any]) -> bool:
        try:
            return (
                row.get("status") == "ok"
                and row.get("timing_valid") is True
                and row.get("positive_after_modeled_costs") is True
                and (
                    not self.require_account_verified_fee
                    or row.get("perp_fee_account_verified") is True
                )
                and _decimal(row["modeled_entry_basis_after_one_current_funding_interval_usdt"], field="basis")
                > 0
            )
        except (KeyError, TypeError, ValueError):
            return False

    @staticmethod
    def _event(
        event: str,
        key: str,
        state: _ActiveBasisCandidate,
        observed_ns: int,
        *,
        current_row: Mapping[str, Any] | None = None,
        close_reason: str | None = None,
    ) -> dict[str, Any]:
        value: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "candidate_key": key,
            "started_at": state.started_at,
            "last_seen_at": _iso_from_ns(state.last_seen_ns),
            "event_at": _iso_from_ns(observed_ns),
            "duration_seconds": round(max(0, observed_ns - state.started_ns) / 1_000_000_000, 6),
            "positive_observations": state.observations,
            "best_modeled_entry_basis_after_one_current_funding_usdt": _decimal_text(
                state.best_basis_usdt,
            ),
            "best_row": state.best_row,
        }
        if current_row is not None:
            value["current_row"] = dict(current_row)
        if close_reason is not None:
            value["close_reason"] = close_reason
        return value

    def observe(self, row: Mapping[str, Any], *, observed_ns: int) -> list[dict[str, Any]]:
        key = _candidate_key(row)
        state = self.active.get(key)
        if not self._candidate(row):
            if state is None:
                return []
            state.last_seen_ns = observed_ns
            self.active.pop(key)
            self.closed += 1
            return [
                self._event(
                    "candidate_closed",
                    key,
                    state,
                    observed_ns,
                    current_row=row,
                    close_reason="not_positive_or_not_timing_valid",
                ),
            ]
        basis = _decimal(row["modeled_entry_basis_after_one_current_funding_interval_usdt"], field="basis")
        if state is None:
            state = _ActiveBasisCandidate(
                started_ns=observed_ns,
                started_at=_iso_from_ns(observed_ns),
                last_seen_ns=observed_ns,
                observations=1,
                best_basis_usdt=basis,
                best_row=dict(row),
            )
            self.active[key] = state
            self.started += 1
            return [self._event("candidate_started", key, state, observed_ns, current_row=row)]
        state.last_seen_ns = observed_ns
        state.observations += 1
        if basis > state.best_basis_usdt:
            state.best_basis_usdt = basis
            state.best_row = dict(row)
            self.improved += 1
            return [self._event("candidate_improved", key, state, observed_ns, current_row=row)]
        return []

    def close_all(self, *, reason: str, observed_ns: int | None = None) -> list[dict[str, Any]]:
        current_ns = observed_ns if observed_ns is not None else time.time_ns()
        events: list[dict[str, Any]] = []
        for key, state in tuple(self.active.items()):
            state.last_seen_ns = current_ns
            events.append(
                self._event(
                    "candidate_closed",
                    key,
                    state,
                    current_ns,
                    close_reason=reason,
                ),
            )
            self.active.pop(key)
            self.closed += 1
        return events


def _compact_candidate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    result = {
        key: event.get(key)
        for key in (
            "schema_version",
            "event",
            "candidate_key",
            "started_at",
            "last_seen_at",
            "event_at",
            "duration_seconds",
            "positive_observations",
            "best_modeled_entry_basis_after_one_current_funding_usdt",
            "close_reason",
        )
        if key in event
    }
    for source_key, target_key in (("best_row", "best_row"), ("current_row", "current_row")):
        row = event.get(source_key)
        if isinstance(row, Mapping):
            result[target_key] = {
                key: row.get(key)
                for key in (
                    "market",
                    "chain",
                    "dex_provider",
                    "dex_pair",
                    "bybit_symbol",
                    "dex_direction",
                    "requested_notional_quote",
                    "status",
                    "timing_valid",
                    "response_skew_ms",
                    "perp_side",
                    "hedge_quantity",
                    "hedge_residual_bps",
                    "perp_taker_fee_bps",
                    "perp_fee_source",
                    "perp_fee_account_verified",
                    "candidate_eligible_with_account_verified_fee",
                    "network_cost_floor_usdt",
                    "dex_network_transactions_round_trip",
                    "network_cost_reserve_usdt",
                    "gross_entry_basis_usdt",
                    "modeled_entry_basis_after_open_close_fees_and_network_usdt",
                    "current_funding_rate",
                    "funding_pnl_if_held_one_current_interval_usdt",
                    "modeled_entry_basis_after_one_current_funding_interval_usdt",
                )
                if key in row
            }
    return result


class PerpDexStatistics:
    """Small aggregate-only state for a long-running basis reconnaissance run."""

    def __init__(self, *, error_ledger_limit: int = 100) -> None:
        self.dex_rounds: Counter[str] = Counter()
        self.dex_records: Counter[str] = Counter()
        self.aggregator_route_labels: Counter[str] = Counter()
        self.perp_book_updates = 0
        self.stable_fx_book_updates = 0
        self.observations = 0
        self.timing_valid = 0
        self.positive = 0
        self.positive_fee_verified = 0
        self.statuses: Counter[str] = Counter()
        self.provider_errors: Counter[str] = Counter()
        self.cex_errors: Counter[str] = Counter()
        self.disabled_markets: dict[str, str] = {}
        self.routes: dict[str, dict[str, Any]] = {}
        self.errors: deque[dict[str, Any]] = deque(maxlen=error_ledger_limit)
        self.candidate_limit = 0
        self.candidate_persisted = 0
        self.candidate_dropped = 0
        self.last_dex_quote_at: str | None = None
        self.last_perp_update_at: str | None = None

    def error(self, *, kind: str, key: str, detail: str) -> None:
        self.errors.append(
            {"at": datetime.now(UTC).isoformat(), "kind": kind, "key": key, "error": detail[:512]},
        )

    def observe_dex_records(self, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            for label in quote_route_labels(record):
                self.aggregator_route_labels[label] += 1

    def observe(self, row: Mapping[str, Any]) -> None:
        self.observations += 1
        status = str(row.get("status", "unknown"))
        self.statuses[status] += 1
        timing = row.get("timing_valid") is True
        positive = row.get("positive_after_modeled_costs") is True
        if timing:
            self.timing_valid += 1
        if positive:
            self.positive += 1
            if row.get("perp_fee_account_verified") is True:
                self.positive_fee_verified += 1
        route_key = "|".join(
            str(row.get(key, ""))
            for key in ("market", "bybit_symbol", "dex_direction", "requested_notional_quote")
        )
        route = self.routes.setdefault(
            route_key,
            {
                "observations": 0,
                "timing_valid_observations": 0,
                "positive_after_modeled_costs": 0,
                "positive_with_account_verified_fee": 0,
                "best_modeled_entry_basis_after_one_current_funding_usdt": None,
            },
        )
        route["observations"] += 1
        if timing:
            route["timing_valid_observations"] += 1
        if positive:
            route["positive_after_modeled_costs"] += 1
            if row.get("perp_fee_account_verified") is True:
                route["positive_with_account_verified_fee"] += 1
        value = row.get("modeled_entry_basis_after_one_current_funding_interval_usdt")
        if value is not None:
            try:
                decimal_value = _decimal(value, field="basis")
            except ValueError:
                return
            existing = route["best_modeled_entry_basis_after_one_current_funding_usdt"]
            if existing is None or decimal_value > _decimal(existing, field="basis"):
                route["best_modeled_entry_basis_after_one_current_funding_usdt"] = _decimal_text(
                    decimal_value,
                )

    def snapshot(
        self,
        *,
        started_at: str,
        duration_seconds: float,
        tracker: BasisCandidateTracker,
        perp_stream: PublicBookStream,
        stable_stream: PublicBookStream | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "status": "running",
            "started_at": started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "duration_wall_seconds": round(duration_seconds, 6),
            "raw_market_data_persisted": False,
            "dex_rounds": dict(sorted(self.dex_rounds.items())),
            "dex_records": dict(sorted(self.dex_records.items())),
            "aggregator_route_labels": dict(sorted(self.aggregator_route_labels.items())),
            "perp_book_updates": self.perp_book_updates,
            "stable_fx_book_updates": self.stable_fx_book_updates,
            "last_dex_quote_at": self.last_dex_quote_at,
            "last_perp_update_at": self.last_perp_update_at,
            "observations": self.observations,
            "timing_valid_observations": self.timing_valid,
            "positive_after_modeled_costs": self.positive,
            "positive_with_account_verified_fee": self.positive_fee_verified,
            "statuses": dict(sorted(self.statuses.items())),
            "provider_errors": dict(sorted(self.provider_errors.items())),
            "cex_errors": dict(sorted(self.cex_errors.items())),
            "disabled_markets": dict(sorted(self.disabled_markets.items())),
            "candidate_lifecycle": {
                "started": tracker.started,
                "improved": tracker.improved,
                "closed": tracker.closed,
                "active": len(tracker.active),
            },
            "candidate_event_persistence": {
                "limit": self.candidate_limit,
                "persisted": self.candidate_persisted,
                "dropped_after_limit": self.candidate_dropped,
            },
            "stream_health": {
                "bybit_linear": stream_health(perp_stream),
                "bybit_spot_usdc_usdt": stream_health(stable_stream) if stable_stream is not None else None,
            },
            "routes": dict(sorted(self.routes.items())),
            "recent_errors": list(self.errors),
        }


def _market_names(value: str) -> list[str]:
    result = [part.strip() for part in value.split(",") if part.strip()]
    unknown = [name for name in result if name not in MARKETS]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown DEX markets: {', '.join(unknown)}")
    if not result:
        raise argparse.ArgumentTypeError("at least one market is required")
    return list(dict.fromkeys(result))


def _decimal_list(value: str) -> list[Decimal]:
    try:
        values = [Decimal(part.strip()) for part in value.split(",") if part.strip()]
    except InvalidOperation as exc:
        raise argparse.ArgumentTypeError("notionals must be decimal values") from exc
    if not values or any(item <= 0 or not item.is_finite() for item in values):
        raise argparse.ArgumentTypeError("notionals must be finite and positive")
    return list(dict.fromkeys(values))


def _costs(value: str) -> dict[str, Decimal]:
    defaults = {"solana": Decimal("0.01"), "ton": Decimal("0.10"), "base": Decimal("0.03"), "polygon": Decimal("0.02")}
    try:
        for part in value.split(","):
            chain, cost = part.strip().split("=", 1)
            defaults[chain.lower()] = Decimal(cost)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("costs must look like solana=0.01,base=0.03") from exc
    if any(cost < 0 or not cost.is_finite() for cost in defaults.values()):
        raise argparse.ArgumentTypeError("network costs must be finite and non-negative")
    return defaults


async def record_perp_dex_monitor(
    markets: Sequence[CycleMarket],
    providers: Mapping[str, DexQuoteProvider],
    instruments: Mapping[str, BybitLinearInstrument],
    fee_rates: Mapping[str, PerpFeeRate],
    *,
    notionals: Sequence[Decimal],
    duration_seconds: float | None,
    output_directory: Path,
    proxy_url: str | None,
    timeout_seconds: float,
    network_cost_floors: Mapping[str, Decimal],
    dex_network_transactions_round_trip: int = 2,
    max_response_skew_ms: Decimal = Decimal("300"),
    max_dex_cache_age_ms: Decimal = Decimal("300"),
    max_ticker_age_ms: Decimal = Decimal("30_000"),
    max_hedge_residual_bps: Decimal = Decimal("1"),
    history_capacity_per_symbol: int = 256,
    stats_flush_seconds: float = 2.0,
    max_persisted_candidate_events: int = 5_000,
    auxiliary_provider_min_round_intervals: Mapping[str, float] | None = None,
    shared_provider_gates: Mapping[str, AsyncRequestPacer] | None = None,
    stdout_candidates: bool = False,
    perp_stream: PublicBookStream | None = None,
    stable_stream: PublicBookStream | None = None,
    require_account_verified_fees_for_candidates: bool = True,
) -> dict[str, Any]:
    """Run an event-driven public DEX↔perp basis reconnaissance.

    ``duration_seconds=None`` keeps the monitor alive until its surrounding
    supervisor cancels it.  Raw books and quotes remain bounded in memory in
    both modes.
    """

    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite existing perp-DEX run: {output_directory}")
    if not markets or not notionals:
        raise ValueError("markets and notionals must not be empty")
    if (
        (duration_seconds is not None and duration_seconds <= 0)
        or timeout_seconds <= 0
        or stats_flush_seconds <= 0
    ):
        raise ValueError("duration, timeout and stats flush must be positive")
    if max_response_skew_ms < 0 or max_dex_cache_age_ms < 0 or max_ticker_age_ms < 0:
        raise ValueError("timing bounds cannot be negative")
    if max_hedge_residual_bps < 0 or max_persisted_candidate_events <= 0:
        raise ValueError("residual tolerance and event cap must be non-negative/positive")
    if dex_network_transactions_round_trip <= 0:
        raise ValueError("DEX network transaction reserve count must be positive")
    if any(market.provider not in providers for market in markets):
        raise ValueError("each selected market needs a DEX provider")
    if any(market.chain not in network_cost_floors for market in markets):
        raise ValueError("each selected chain needs a network-cost floor")
    symbols = {bybit_linear_symbol(market) for market in markets}
    missing = sorted(symbol for symbol in symbols if symbol not in instruments or symbol not in fee_rates)
    if missing:
        raise ValueError(f"missing Bybit linear metadata or fee rate for {', '.join(missing)}")

    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    started_at = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()
    stats = PerpDexStatistics()
    stats.candidate_limit = max_persisted_candidate_events
    tracker = BasisCandidateTracker(
        require_account_verified_fee=require_account_verified_fees_for_candidates,
    )
    stop_event = asyncio.Event()
    candidate_path = output_directory / "candidate_events.jsonl"
    stats_path = output_directory / "stats.json"
    candidate_path.touch(exist_ok=False)
    needs_usdc_fx = any(market.quote_symbol.upper() == "USDC" for market in markets)
    active_perp_stream = perp_stream or build_public_book_stream(
        "BYBIT",
        sorted(symbols),
        timeout_seconds=timeout_seconds,
        proxy_url=proxy_url,
        history_capacity_per_symbol=history_capacity_per_symbol,
        category="linear",
    )
    active_stable_stream = stable_stream
    if needs_usdc_fx and active_stable_stream is None:
        active_stable_stream = build_public_book_stream(
            "BYBIT",
            [BYBIT_USDC_USDT_SPOT_SYMBOL],
            timeout_seconds=timeout_seconds,
            proxy_url=proxy_url,
            history_capacity_per_symbol=history_capacity_per_symbol,
            category="spot",
        )

    markets_by_symbol: dict[str, list[CycleMarket]] = defaultdict(list)
    for market in markets:
        markets_by_symbol[bybit_linear_symbol(market)].append(market)
    cache_by_market: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    cache_age_ns = int(max_dex_cache_age_ms * Decimal(1_000_000))
    default_gate_intervals = {
        "STONFI": 1.0,
        "OMNISTON": 1.0,
        "UNISWAP_BASE": 0.5,
        "UNISWAP_POLYGON": 0.5,
    }
    quote_gates = dict(shared_provider_gates or {})
    for key, default_interval in default_gate_intervals.items():
        quote_gates.setdefault(
            key,
            AsyncRequestPacer(
                (auxiliary_provider_min_round_intervals or {}).get(key, default_interval),
            ),
        )

    def ticker_for(symbol: str) -> BybitLinearTicker | None:
        getter = getattr(active_perp_stream, "perp_ticker", None)
        return getter(symbol) if callable(getter) else None

    def stable_book_for(target_ns: int) -> BookSnapshot | None:
        if not needs_usdc_fx:
            return None
        if active_stable_stream is None:
            return None
        return active_stable_stream.nearest_snapshot(BYBIT_USDC_USDT_SPOT_SYMBOL, target_ns)

    def persist_event(output: Any, event: Mapping[str, Any]) -> None:
        compact = _compact_candidate_event(event)
        if stdout_candidates:
            print(json.dumps(compact, ensure_ascii=False, separators=(",", ":")), flush=True)
        if stats.candidate_persisted >= max_persisted_candidate_events:
            stats.candidate_dropped += 1
            return
        output.write(json.dumps(compact, ensure_ascii=False, separators=(",", ":")) + "\n")
        stats.candidate_persisted += 1

    def evaluate(
        output: Any,
        *,
        market: CycleMarket,
        dex_record: Mapping[str, Any],
        changed_book: BookSnapshot | None = None,
        observed_ns: int | None = None,
    ) -> None:
        target_ns = int(dex_record.get("response_received_realtime_ns", 0))
        symbol = bybit_linear_symbol(market)
        book = (
            changed_book
            if changed_book is not None and changed_book.symbol == symbol
            else active_perp_stream.nearest_snapshot(symbol, target_ns)
        )
        if book is None:
            stats.cex_errors[f"missing_perp_book:{symbol}"] += 1
            return
        row = calculate_perp_dex_basis(
            market=market,
            dex_record=dex_record,
            perp_book=book,
            stable_book=stable_book_for(target_ns),
            instrument=instruments[symbol],
            fee_rate=fee_rates[symbol],
            ticker=ticker_for(symbol),
            network_cost_floor_usdt=network_cost_floors[market.chain],
            max_response_skew_ms=max_response_skew_ms,
            max_ticker_age_ms=max_ticker_age_ms,
            max_hedge_residual_bps=max_hedge_residual_bps,
            dex_network_transactions_round_trip=dex_network_transactions_round_trip,
        )
        stats.observe(row)
        when = observed_ns if observed_ns is not None else book.response.received_realtime_ns
        for event in tracker.observe(row, observed_ns=when):
            persist_event(output, event)

    async def quote_worker(output: Any, market: CycleMarket) -> None:
        provider = providers[market.provider]
        gate_key = _provider_gate_key(market)
        gate = quote_gates.get(gate_key) if gate_key else None
        exact_base_quote = getattr(provider, "quote_exact_base_round", None)
        request_pacer = getattr(provider, "request_pacer", None)
        pacing = None if isinstance(request_pacer, AsyncRequestPacer) else gate
        round_id = 0
        while not stop_event.is_set():
            if pacing is not None:
                await pacing.wait()
            try:
                if callable(exact_base_quote):
                    symbol = bybit_linear_symbol(market)
                    sizing_book = active_perp_stream.nearest_snapshot(symbol, time.time_ns())
                    sizing_mid, targets = exact_perp_step_targets(
                        notionals,
                        book=sizing_book,
                        instrument=instruments[symbol],
                    )
                    if not targets:
                        # No DEX call is useful until the L50 book is ready or
                        # until at least one requested exposure reaches the
                        # perpetual's minimum quantity.
                        await asyncio.sleep(0.05)
                        continue
                    records = await exact_base_quote(round_id, targets)
                    if sizing_mid is not None:
                        for record in records:
                            record["perp_target_sizing_reference_mid_usdt"] = _decimal_text(
                                sizing_mid,
                            )
                else:
                    records = await provider.quote_round(round_id, notionals)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stats.provider_errors[market.provider] += 1
                stats.error(kind="dex_provider_exception", key=market.provider, detail=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(0.25)
                continue
            stats.dex_rounds[market.provider] += 1
            stats.dex_records[market.provider] += len(records)
            stats.observe_dex_records(records)
            stats.last_dex_quote_at = datetime.now(UTC).isoformat()
            for record in records:
                if record.get("status") == "request_error":
                    stats.provider_errors[market.provider] += 1
                    error = str(record.get("error", "request_error"))
                    stats.error(kind="dex_request_error", key=market.provider, detail=error)
                    if "http 429" in error.lower():
                        pacer = getattr(provider, "request_pacer", None)
                        if isinstance(pacer, AsyncRequestPacer):
                            await pacer.defer(cooldown_seconds=120, minimum_interval_seconds=3)
            for _, record in _best_dex_records(records).items():
                cache_by_market[market.name][
                    (str(record.get("requested_notional_quote")), str(record.get("direction")))
                ] = record
                evaluate(
                    output,
                    market=market,
                    dex_record=record,
                    observed_ns=int(record["response_received_realtime_ns"]),
                )
            round_id += 1
            await asyncio.sleep(0)

    async def perp_update_worker(output: Any) -> None:
        reported_error: str | None = None
        while not stop_event.is_set():
            try:
                book = await asyncio.wait_for(active_perp_stream.next_update(), timeout=5.0)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                error = getattr(active_perp_stream, "error", None)
                if error and error != reported_error:
                    reported_error = str(error)
                    stats.cex_errors["BYBIT:linear_websocket"] += 1
                    stats.error(kind="perp_stream", key="BYBIT", detail=reported_error)
                continue
            stats.perp_book_updates += 1
            stats.last_perp_update_at = datetime.now(UTC).isoformat()
            for market in markets_by_symbol.get(book.symbol, ()):
                for record in tuple(cache_by_market[market.name].values()):
                    if time.monotonic_ns() - int(record.get("response_received_monotonic_ns", 0)) <= cache_age_ns:
                        evaluate(output, market=market, dex_record=record, changed_book=book)

    async def stable_update_worker(output: Any) -> None:
        if active_stable_stream is None:
            return
        reported_error: str | None = None
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(active_stable_stream.next_update(), timeout=5.0)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                error = getattr(active_stable_stream, "error", None)
                if error and error != reported_error:
                    reported_error = str(error)
                    stats.cex_errors["BYBIT:USDCUSDT_websocket"] += 1
                    stats.error(kind="stable_fx_stream", key="BYBIT", detail=reported_error)
                continue
            stats.stable_fx_book_updates += 1
            for market in markets:
                if market.quote_symbol.upper() != "USDC":
                    continue
                for record in tuple(cache_by_market[market.name].values()):
                    if time.monotonic_ns() - int(record.get("response_received_monotonic_ns", 0)) <= cache_age_ns:
                        evaluate(output, market=market, dex_record=record)

    async def stats_writer() -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=stats_flush_seconds)
            except TimeoutError:
                atomic_json(
                    stats_path,
                    stats.snapshot(
                        started_at=started_at,
                        duration_seconds=time.monotonic() - started_monotonic,
                        tracker=tracker,
                        perp_stream=active_perp_stream,
                        stable_stream=active_stable_stream,
                    ),
                )

    selected_metadata = {symbol: _instrument_record(instruments[symbol]) for symbol in sorted(symbols)}
    selected_fees = {symbol: _fee_rate_record(fee_rates[symbol]) for symbol in sorted(symbols)}
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "status": "starting",
        "started_at": started_at,
        "stopped_at": None,
        "duration_requested_seconds": duration_seconds,
        "mode": "event_driven_dex_spot_bybit_linear_basis",
        "universe": {
            "markets": [asdict(market) for market in markets],
            "bybit_linear_symbols": sorted(symbols),
            "formula": "DEX stable/base swap plus one opposite Bybit USDT linear perpetual hedge",
        },
        "bybit_linear_contracts": selected_metadata,
        "perp_fee_rates": selected_fees,
        "dex": {
            "providers": [providers[market.provider].config() for market in markets],
            "notionals": [_decimal_text(value) for value in notionals],
            "max_dex_cache_age_ms": _decimal_text(max_dex_cache_age_ms),
            "max_response_skew_ms": _decimal_text(max_response_skew_ms),
            "max_ticker_age_ms": _decimal_text(max_ticker_age_ms),
            "max_hedge_residual_bps": _decimal_text(max_hedge_residual_bps),
            "network_transactions_reserved_for_round_trip": dex_network_transactions_round_trip,
        },
        "stable_fx": {
            "required_for_usdc_routes": needs_usdc_fx,
            "symbol": BYBIT_USDC_USDT_SPOT_SYMBOL if needs_usdc_fx else None,
            "model": "current Bybit spot-depth VWAP; inventory/rebalance cost remains excluded",
        },
        "minimum_network_cost_floor_per_dex_transaction_usdt_by_chain": {
            chain: _decimal_text(cost) for chain, cost in sorted(network_cost_floors.items())
        },
        "retention": {
            "raw_market_data_persisted": False,
            "max_persisted_candidate_events": max_persisted_candidate_events,
            "policy": "only compact positive-basis lifecycle events, aggregate stats and bounded errors",
            "fee_verification_policy": (
                "positive rows are persisted only when the Bybit account fee-rate API verified "
                "the rate"
                if require_account_verified_fees_for_candidates
                else "positive rows may use the explicitly labelled conservative public Bybit "
                "VIP0 schedule; account verification remains visible on every candidate"
            ),
        },
        "network_route": network_route,
        "api_credentials_used": any(fee.account_verified for fee in fee_rates.values()),
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "execution_client_registered": False,
        "model_scope": {
            "included": [
                "public Bybit linear L50 depth, mark/index/funding ticker and instrument lot constraints",
                "direct DEX quote including provider-reported pool fee and price impact",
                "perp opening taker fee plus current-book reserve for a closing taker fee",
                "two configurable DEX network-cost floors: entry plus eventual DEX exit",
                "one current published funding-rate interval as an explicitly variable scenario",
                "USDC/USDT spot-depth conversion for USDC-denominated DEX routes",
            ],
            "excluded": [
                "actual future funding rate, actual future exit book and realised fill probability",
                "transaction construction, priority fee, gas used, inclusion and DEX state changes after quote",
                "collateral allocation, liquidation threshold, wrapper redemption, bridge and inventory rebalance costs",
            ],
            "interpretation": "A positive row is a modeled hedged-basis entry, not realised PnL or a trade instruction.",
        },
        "files": {
            "candidate_events": str(candidate_path.resolve()),
            "stats": str(stats_path.resolve()),
            "manifest": str((output_directory / "manifest.json").resolve()),
        },
        "error": None,
    }
    atomic_json(output_directory / "manifest.json", manifest)

    tasks: list[asyncio.Task[None]] = []
    final_status = "completed"
    final_error: str | None = None
    try:
        await active_perp_stream.start()
        if active_stable_stream is not None:
            await active_stable_stream.start()
        with candidate_path.open("a", encoding="utf-8", buffering=1) as output:
            tasks = [
                *(asyncio.create_task(quote_worker(output, market)) for market in markets),
                asyncio.create_task(perp_update_worker(output)),
                asyncio.create_task(stats_writer()),
            ]
            if active_stable_stream is not None:
                tasks.append(asyncio.create_task(stable_update_worker(output)))
            try:
                if duration_seconds is None:
                    await stop_event.wait()
                else:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)
                    except TimeoutError:
                        pass
            finally:
                stop_event.set()
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                tasks.clear()
                close_reason = (
                    "requested_duration_elapsed"
                    if duration_seconds is not None
                    else "scanner_stopped"
                )
                for event in tracker.close_all(reason=close_reason):
                    persist_event(output, event)
    except asyncio.CancelledError:
        final_status = "stopped"
    except Exception as exc:
        final_status = "error"
        final_error = f"{type(exc).__name__}: {exc}"
    finally:
        stop_event.set()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        with contextlib.suppress(Exception):
            await active_perp_stream.close()
        if active_stable_stream is not None:
            with contextlib.suppress(Exception):
                await active_stable_stream.close()
        elapsed = time.monotonic() - started_monotonic
        final_stats = stats.snapshot(
            started_at=started_at,
            duration_seconds=elapsed,
            tracker=tracker,
            perp_stream=active_perp_stream,
            stable_stream=active_stable_stream,
        )
        final_stats["status"] = final_status
        atomic_json(stats_path, final_stats)
        manifest.update(
            status=final_status,
            stopped_at=datetime.now(UTC).isoformat(),
            duration_wall_seconds=round(elapsed, 6),
            error=final_error,
        )
        atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=_market_names, default=list(MAXIMUM_COVERAGE_MARKETS))
    parser.add_argument("--notionals", type=_decimal_list, default=[Decimal("100"), Decimal("250"), Decimal("500")])
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-response-skew-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--max-dex-cache-age-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--max-ticker-age-ms", type=Decimal, default=Decimal("30000"))
    parser.add_argument("--max-hedge-residual-bps", type=Decimal, default=Decimal("1"))
    parser.add_argument("--history-capacity-per-symbol", type=int, default=256)
    parser.add_argument("--stats-flush-seconds", type=float, default=2.0)
    parser.add_argument("--max-persisted-candidate-events", type=int, default=5_000)
    parser.add_argument("--minimum-network-costs", type=_costs, default=_costs("solana=0.01,ton=0.1,base=0.03,polygon=0.02"))
    parser.add_argument(
        "--dex-network-transactions-round-trip",
        type=int,
        default=2,
        help="Number of DEX network-fee floors reserved for entry plus eventual exit",
    )
    parser.add_argument("--raydium-slippage-bps", type=int, default=50)
    parser.add_argument("--raydium-min-request-interval-seconds", type=float, default=2.5)
    parser.add_argument("--jupiter-api-key-env", default="JUPITER_API_KEY")
    parser.add_argument("--jupiter-min-request-interval-seconds", type=float)
    parser.add_argument("--stonfi-slippage-tolerance", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--omniston-ws-url", default=OMNISTON_WS_ENDPOINT)
    parser.add_argument("--omniston-quote-selection-window-seconds", type=float, default=0.5)
    parser.add_argument("--omniston-max-price-slippage-bps", type=int, default=50)
    parser.add_argument("--omniston-max-routes", type=int, default=4)
    parser.add_argument("--omniston-allow-risky-routes", action="store_true")
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--stonfi-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--omniston-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--uniswap-base-min-round-interval-seconds", type=float, default=0.5)
    parser.add_argument("--uniswap-polygon-min-round-interval-seconds", type=float, default=0.5)
    parser.add_argument("--bybit-public-instruments-endpoint", default=BYBIT_PUBLIC_INSTRUMENTS_ENDPOINT)
    parser.add_argument("--bybit-public-vip0-taker-fee-bps", type=Decimal, default=BYBIT_LINEAR_VIP0_STANDARD_TAKER_FEE_BPS)
    parser.add_argument("--use-account-fee-rate", action="store_true")
    parser.add_argument("--bybit-fee-api-key-env", default="BYBIT_READONLY_API_KEY")
    parser.add_argument("--bybit-fee-api-secret-env", default="BYBIT_READONLY_API_SECRET")
    parser.add_argument("--bybit-account-fee-rate-endpoint", default=BYBIT_ACCOUNT_FEE_RATE_ENDPOINT)
    parser.add_argument("--stdout-candidates", action="store_true")
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/perp-dex"))
    parser.add_argument("--run-id")
    return parser


async def _run_cli(args: argparse.Namespace) -> dict[str, Any]:
    configure_process_network_route(args.proxy_url)
    all_instruments = await fetch_bybit_linear_instruments(
        endpoint=args.bybit_public_instruments_endpoint,
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
    )
    requested = [MARKETS[name] for name in args.markets]
    active_markets = [market for market in requested if bybit_linear_symbol(market) in all_instruments]
    skipped = {
        market.name: f"no active USDT-settled Bybit linear perpetual {bybit_linear_symbol(market)}"
        for market in requested
        if market not in active_markets
    }
    if not active_markets:
        raise RuntimeError("none of the selected DEX markets has an active matching Bybit USDT perpetual")
    symbols = [bybit_linear_symbol(market) for market in active_markets]
    if args.use_account_fee_rate:
        api_key = os.getenv(args.bybit_fee_api_key_env)
        api_secret = os.getenv(args.bybit_fee_api_secret_env)
        if not api_key or not api_secret:
            raise RuntimeError(
                "--use-account-fee-rate requires non-empty read-only API key and secret environment variables",
            )
        fee_rates = await fetch_bybit_account_fee_rates(
            symbols,
            api_key=api_key,
            api_secret=api_secret,
            endpoint=args.bybit_account_fee_rate_endpoint,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        fee_rates = public_fallback_fee_rates(
            symbols,
            taker_fee_bps=args.bybit_public_vip0_taker_fee_bps,
        )
    providers = build_cycle_providers(
        [market.name for market in active_markets],
        base_rpc_url=args.base_rpc_url,
        polygon_rpc_url=args.polygon_rpc_url,
        fee_tiers=(100, 500, 3000),
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
        raydium_slippage_bps=args.raydium_slippage_bps,
        raydium_min_request_interval_seconds=args.raydium_min_request_interval_seconds,
        stonfi_slippage_tolerance=args.stonfi_slippage_tolerance,
        jupiter_api_key=os.getenv(args.jupiter_api_key_env) if args.jupiter_api_key_env else None,
        jupiter_min_request_interval_seconds=args.jupiter_min_request_interval_seconds,
        omniston_ws_url=args.omniston_ws_url,
        omniston_quote_selection_window_seconds=args.omniston_quote_selection_window_seconds,
        omniston_max_price_slippage_bps=args.omniston_max_price_slippage_bps,
        omniston_max_routes=args.omniston_max_routes,
        omniston_allow_risky_routes=args.omniston_allow_risky_routes,
    )
    run_id = args.run_id or default_run_id("perp-dex")
    manifest = await record_perp_dex_monitor(
        active_markets,
        providers,
        all_instruments,
        fee_rates,
        notionals=args.notionals,
        duration_seconds=args.duration_seconds,
        output_directory=args.output_root / run_id,
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
        network_cost_floors=args.minimum_network_costs,
        dex_network_transactions_round_trip=args.dex_network_transactions_round_trip,
        max_response_skew_ms=args.max_response_skew_ms,
        max_dex_cache_age_ms=args.max_dex_cache_age_ms,
        max_ticker_age_ms=args.max_ticker_age_ms,
        max_hedge_residual_bps=args.max_hedge_residual_bps,
        history_capacity_per_symbol=args.history_capacity_per_symbol,
        stats_flush_seconds=args.stats_flush_seconds,
        max_persisted_candidate_events=args.max_persisted_candidate_events,
        auxiliary_provider_min_round_intervals={
            "STONFI": args.stonfi_min_round_interval_seconds,
            "OMNISTON": args.omniston_min_round_interval_seconds,
            "UNISWAP_BASE": args.uniswap_base_min_round_interval_seconds,
            "UNISWAP_POLYGON": args.uniswap_polygon_min_round_interval_seconds,
        },
        stdout_candidates=args.stdout_candidates,
    )
    manifest["unavailable_markets"] = skipped
    atomic_json(args.output_root / run_id / "manifest.json", manifest)
    return manifest


def main() -> None:
    args = _parser().parse_args()
    numeric = (
        args.duration_seconds,
        args.timeout_seconds,
        args.stats_flush_seconds,
        args.raydium_min_request_interval_seconds,
        args.stonfi_min_round_interval_seconds,
        args.omniston_min_round_interval_seconds,
        args.uniswap_base_min_round_interval_seconds,
        args.uniswap_polygon_min_round_interval_seconds,
    )
    if any(value <= 0 for value in numeric):
        raise SystemExit("all timing intervals must be positive")
    if (
        args.history_capacity_per_symbol <= 0
        or args.max_persisted_candidate_events <= 0
        or args.max_response_skew_ms < 0
        or args.max_dex_cache_age_ms < 0
        or args.max_ticker_age_ms < 0
        or args.max_hedge_residual_bps < 0
        or args.dex_network_transactions_round_trip <= 0
    ):
        raise SystemExit("capacities must be positive and timing/residual values non-negative")
    if args.raydium_slippage_bps < 0 or args.omniston_max_price_slippage_bps < 0:
        raise SystemExit("slippage cannot be negative")
    if args.jupiter_min_request_interval_seconds is not None and args.jupiter_min_request_interval_seconds < 0:
        raise SystemExit("Jupiter interval cannot be negative")
    if not Decimal(0) <= args.stonfi_slippage_tolerance < Decimal(1):
        raise SystemExit("STON.fi slippage tolerance must be in [0, 1)")
    if args.omniston_max_routes <= 0 or args.omniston_quote_selection_window_seconds < 0:
        raise SystemExit("Omniston settings are invalid")
    if args.bybit_public_vip0_taker_fee_bps < 0:
        raise SystemExit("Bybit fallback taker fee cannot be negative")
    if args.run_id is not None:
        validate_run_id(args.run_id)
    manifest = asyncio.run(_run_cli(args))
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    if manifest["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
