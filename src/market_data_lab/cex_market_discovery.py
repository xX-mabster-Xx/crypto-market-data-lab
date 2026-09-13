"""Small, read-only CEX instrument discovery for the common market-data bus.

The public WebSocket adapters should only subscribe to symbols a venue says it
currently lists.  This one-shot metadata step avoids a manually maintained
symbol list and prevents one absent long-tail pair from poisoning a whole
socket subscription.  It does not fetch trades, accounts, balances, or fees.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.solana_realtime_scanner import CexStreamConfig


_INSTRUMENT_URLS = {
    ("BYBIT", "spot"): "https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000",
    ("BYBIT", "linear"): "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000",
    ("BINANCE", "spot"): "https://api.binance.com/api/v3/exchangeInfo",
    ("MEXC", "spot"): "https://api.mexc.com/api/v3/exchangeInfo",
    ("OKX", "spot"): "https://www.okx.com/api/v5/public/instruments?instType=SPOT",
    # Use the same current V3 public product catalogue as the WebSocket
    # transport rather than maintaining a manually copied symbol list.
    ("BITGET", "spot"): "https://api.bitget.com/api/v3/market/instruments?category=SPOT",
}


@dataclass(frozen=True)
class CexDiscoveryResult:
    """Safe startup result: selected symbols and bounded failure diagnostics."""

    streams: tuple[CexStreamConfig, ...]
    requested_bases: tuple[str, ...]
    selected_symbols: Mapping[str, tuple[str, ...]]
    errors: Mapping[str, str]

    def safe_descriptor(self) -> dict[str, Any]:
        return {
            "requested_usdt_bases": list(self.requested_bases),
            "selected_symbols": {
                name: list(symbols) for name, symbols in sorted(self.selected_symbols.items())
            },
            "errors": dict(self.errors),
            "private_api_used": False,
        }


def _expected_symbols(venue: str, bases: Sequence[str]) -> set[str]:
    normalized_bases = {base.strip().upper() for base in bases if base.strip()}
    if venue == "OKX":
        return {f"{base}-USDT" for base in normalized_bases}
    return {f"{base}USDT" for base in normalized_bases}


def _listed_symbols(venue: str, category: str, payload: object) -> set[str]:
    if venue == "BYBIT":
        result = payload.get("result") if isinstance(payload, Mapping) else None
        rows = result.get("list") if isinstance(result, Mapping) else None
        if not isinstance(rows, list):
            raise ValueError("Bybit instruments payload has no result.list")
        return {
            str(row.get("symbol", "")).upper()
            for row in rows
            if isinstance(row, Mapping) and str(row.get("symbol", "")).strip()
        }
    if venue in {"BINANCE", "MEXC"}:
        rows = payload.get("symbols") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ValueError(f"{venue} exchangeInfo payload has no symbols")
        return {
            str(row.get("symbol", "")).upper()
            for row in rows
            if isinstance(row, Mapping) and str(row.get("symbol", "")).strip()
        }
    if venue == "OKX":
        rows = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ValueError("OKX instruments payload has no data")
        return {
            str(row.get("instId", "")).upper()
            for row in rows
            if isinstance(row, Mapping) and str(row.get("instId", "")).strip()
        }
    if venue == "BITGET":
        rows = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise ValueError("Bitget instruments payload has no data")
        return {
            str(row.get("symbol", "")).upper()
            for row in rows
            if (
                isinstance(row, Mapping)
                and str(row.get("status", "")).lower() == "online"
                and str(row.get("symbol", "")).strip()
            )
        }
    raise ValueError(f"unsupported CEX discovery venue/category: {venue}/{category}")


async def _fetch_listing(
    *,
    venue: str,
    category: str,
    proxy_url: str | None,
    timeout_seconds: float,
) -> set[str]:
    payload = await asyncio.to_thread(
        _fetch_json_sync,
        _INSTRUMENT_URLS[(venue, category)],
        "GET",
        None,
        {},
        proxy_url,
        timeout_seconds,
    )
    return _listed_symbols(venue, category, payload)


async def discover_cex_streams(
    *,
    existing_streams: Sequence[CexStreamConfig],
    bases: Sequence[str],
    proxy_url: str | None,
    timeout_seconds: float,
) -> CexDiscoveryResult:
    """Merge public instrument intersections into a validated stream profile."""

    requested_bases = tuple(sorted({base.strip().upper() for base in bases if base.strip()}))
    if not requested_bases:
        raise ValueError("CEX discovery needs at least one base ticker")
    targets = tuple(_INSTRUMENT_URLS)
    results = await asyncio.gather(
        *(
            _fetch_listing(
                venue=venue,
                category=category,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
            )
            for venue, category in targets
        ),
        return_exceptions=True,
    )
    selected: dict[tuple[str, str], tuple[str, ...]] = {}
    errors: dict[str, str] = {}
    for (venue, category), result in zip(targets, results, strict=True):
        key = f"{venue}:{category}"
        if isinstance(result, BaseException):
            errors[key] = f"{type(result).__name__}: {result}"[:512]
            continue
        selected[(venue, category)] = tuple(sorted(result & _expected_symbols(venue, requested_bases)))

    merged: list[CexStreamConfig] = []
    seen: set[tuple[str, str]] = set()
    for stream in existing_streams:
        key = (stream.venue.upper(), stream.category.lower())
        seen.add(key)
        discovered = selected.get(key, ())
        merged.append(
            CexStreamConfig(
                venue=key[0],
                category=key[1],
                symbols=tuple(sorted(set(stream.symbols) | set(discovered))),
            ),
        )
    for key, symbols in sorted(selected.items()):
        if key in seen or not symbols:
            continue
        merged.append(CexStreamConfig(venue=key[0], category=key[1], symbols=symbols))

    return CexDiscoveryResult(
        streams=tuple(merged),
        requested_bases=requested_bases,
        selected_symbols={f"{venue}:{category}": symbols for (venue, category), symbols in selected.items()},
        errors=errors,
    )
