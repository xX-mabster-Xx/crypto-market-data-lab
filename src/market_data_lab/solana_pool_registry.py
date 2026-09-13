"""Low-frequency, read-only Solana pool discovery and selection.

This module deliberately separates *inventory* from the hot market-data path.
Raydium, Meteora and Orca catalogue APIs are useful for finding candidate pools,
but are cached and are not an executable price feed.  A refresh therefore makes at
most one paginated request per protocol by default, selects a small reviewed
universe, and writes only static-ish pool metadata.  The unified scanner then
uses direct Solana RPC/WebSocket subscriptions for hot state.

No wallet, private key, transaction construction, or order submission is
accepted here.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import urllib.parse
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.dex_quotes import SOLANA_PROVIDER_BASES
from market_data_lab.dex_quotes import SOLANA_USDC
from market_data_lab.dex_quotes import SOLANA_USDT
from market_data_lab.dex_quotes import SOL_WRAPPED_MINT
from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import _timed_fetch
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route


RAYDIUM_POOL_LIST_ENDPOINT = "https://api-v3.raydium.io/pools/info/list"
METEORA_DLMM_POOL_LIST_ENDPOINT = "https://dlmm.datapi.meteora.ag/pools"
ORCA_WHIRLPOOL_LIST_ENDPOINT = "https://api.orca.so/v2/solana/pools"
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class RegistryToken:
    """A compact, protocol-neutral token projection from a pool catalogue."""

    address: str
    symbol: str | None
    decimals: int | None
    verified: bool | None

    def as_dict(self) -> dict[str, object]:
        return {
            "address": self.address,
            "symbol": self.symbol,
            "decimals": self.decimals,
            "verified": self.verified,
        }


@dataclass(frozen=True)
class RegistryPool:
    """One normalized discovery record, never a hot quote."""

    protocol: str
    pool_id: str
    pool_kind: str
    token_a: RegistryToken
    token_b: RegistryToken
    tvl_usd: Decimal | None
    volume_24h_usd: Decimal | None
    blacklisted: bool
    fee_hint: Mapping[str, object]
    discovery_origin: str

    def as_dict(self) -> dict[str, object]:
        return {
            "protocol": self.protocol,
            "pool_id": self.pool_id,
            "pool_kind": self.pool_kind,
            "token_a": self.token_a.as_dict(),
            "token_b": self.token_b.as_dict(),
            "tvl_usd": _decimal_text(self.tvl_usd),
            "volume_24h_usd": _decimal_text(self.volume_24h_usd),
            "blacklisted": self.blacklisted,
            "fee_hint": dict(self.fee_hint),
            "discovery_origin": self.discovery_origin,
        }


@dataclass(frozen=True)
class PoolSelectionPolicy:
    """A conservative static-pool eligibility policy.

    ``allowed_mints`` is intentionally explicit.  A pool is selected only
    when *both* sides are known assets we can later map to a CEX/perp hedge or
    a permitted hub.  That avoids silently filling subscriptions with unknown
    launch tokens just because they have transient volume.
    """

    allowed_mints: frozenset[str]
    minimum_tvl_usd: Decimal = Decimal("100000")
    minimum_volume_24h_usd: Decimal = Decimal("25000")
    max_pools_per_protocol: int = 30
    require_verified_tokens: bool = True

    def __post_init__(self) -> None:
        if not self.allowed_mints:
            raise ValueError("allowed_mints must be non-empty")
        if self.minimum_tvl_usd < 0 or self.minimum_volume_24h_usd < 0:
            raise ValueError("minimum TVL and volume must be non-negative")
        if self.max_pools_per_protocol <= 0:
            raise ValueError("max_pools_per_protocol must be positive")

    def as_dict(self) -> dict[str, object]:
        return {
            "allowed_mints": sorted(self.allowed_mints),
            "minimum_tvl_usd": _decimal_text(self.minimum_tvl_usd),
            "minimum_volume_24h_usd": _decimal_text(self.minimum_volume_24h_usd),
            "max_pools_per_protocol": self.max_pools_per_protocol,
            "require_verified_tokens": self.require_verified_tokens,
        }


@dataclass(frozen=True)
class DiscoverySourceResult:
    """One source outcome, including failures without losing other sources."""

    source: str
    endpoint_origin: str
    pools: tuple[RegistryPool, ...]
    pages_requested: int
    error: str | None
    response_rtt_ms: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "source": self.source,
            "endpoint_origin": self.endpoint_origin,
            "pools_observed": len(self.pools),
            "pages_requested": self.pages_requested,
            "error": self.error,
            "response_rtt_ms": self.response_rtt_ms,
        }


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _first_string(payload: Mapping[str, object], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _first_decimal(payload: Mapping[str, object], keys: Sequence[str]) -> Decimal | None:
    for key in keys:
        parsed = _decimal(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _token(value: object) -> RegistryToken | None:
    """Handle both Raydium mint records and Meteora ``token_x/y`` records."""

    if isinstance(value, str) and value.strip():
        return RegistryToken(address=value.strip(), symbol=None, decimals=None, verified=None)
    payload = _mapping(value)
    address = _first_string(payload, ("address", "mint", "mintAddress", "id"))
    if address is None:
        return None
    decimals_value = payload.get("decimals")
    decimals = decimals_value if isinstance(decimals_value, int) and not isinstance(decimals_value, bool) else None
    verified_value = payload.get("is_verified", payload.get("isVerified"))
    verified = verified_value if isinstance(verified_value, bool) else None
    return RegistryToken(
        address=address,
        symbol=_first_string(payload, ("symbol", "name")),
        decimals=decimals,
        verified=verified,
    )


def _is_blacklisted(payload: Mapping[str, object]) -> bool:
    if (
        payload.get("is_blacklisted") is True
        or payload.get("isBlacklisted") is True
        or payload.get("hasWarning") is True
    ):
        return True
    tags = payload.get("tags")
    if isinstance(tags, Mapping):
        words = {str(key).lower() for key, value in tags.items() if value}
    elif isinstance(tags, Sequence) and not isinstance(tags, str):
        words = {str(value).lower() for value in tags}
    else:
        words = set()
    return bool(words.intersection({"scam", "honeypot", "blacklisted"}))


def _rows_from_raydium_payload(payload: object) -> Sequence[object]:
    root = _mapping(payload)
    if root.get("success") is False:
        raise ValueError(f"Raydium reported failure: {root.get('msg') or root.get('error') or 'unknown'}")
    data = root.get("data")
    if isinstance(data, Sequence) and not isinstance(data, str):
        return data
    nested = _mapping(data)
    rows = nested.get("data", nested.get("list", nested.get("rows")))
    if isinstance(rows, Sequence) and not isinstance(rows, str):
        return rows
    raise ValueError("Raydium pool-list response has no pool array")


def parse_raydium_pools(payload: object) -> tuple[RegistryPool, ...]:
    """Normalize a Raydium API-v3 pool-list payload without trusting its price."""

    pools: list[RegistryPool] = []
    for row in _rows_from_raydium_payload(payload):
        item = _mapping(row)
        pool_id = _first_string(item, ("id", "poolId", "address"))
        # The documented legacy page uses mint1/mint2 while some deployed
        # API-v3 payloads expose mintA/mintB.  Accept both without branching
        # the hot scanner on a vendor presentation detail.
        token_a = _token(item.get("mintA", item.get("mint1", item.get("mint_a"))))
        token_b = _token(item.get("mintB", item.get("mint2", item.get("mint_b"))))
        if pool_id is None or token_a is None or token_b is None:
            continue
        kind = _first_string(item, ("type", "poolType")) or "unknown"
        lower_kind = kind.lower()
        if "concentrated" in lower_kind or "clmm" in lower_kind:
            protocol = "raydium_clmm"
        elif "cpmm" in lower_kind:
            protocol = "raydium_cpmm"
        elif "amm" in lower_kind:
            protocol = "raydium_amm_v4"
        elif "standard" in lower_kind:
            # Raydium's inventory groups the legacy AMM v4 and CPMM surface
            # under "Standard".  Do not guess the account layout here; a
            # later on-chain owner/program check chooses the exact decoder.
            protocol = "raydium_standard"
        else:
            protocol = "raydium_unknown"
        day = _mapping(item.get("day"))
        config = _mapping(item.get("config"))
        fee_hint = {
            key: config[key]
            for key in ("tradeFeeRate", "protocolFeeRate", "fundFeeRate", "tickSpacing")
            if key in config and isinstance(config[key], (str, int, float)) and not isinstance(config[key], bool)
        }
        pools.append(
            RegistryPool(
                protocol=protocol,
                pool_id=pool_id,
                pool_kind=kind,
                token_a=token_a,
                token_b=token_b,
                tvl_usd=_first_decimal(item, ("tvl", "tvlUsd")),
                volume_24h_usd=_first_decimal(day, ("volume", "volumeQuote", "volumeUsd")),
                blacklisted=_is_blacklisted(item),
                fee_hint=fee_hint,
                discovery_origin=_redact_url(RAYDIUM_POOL_LIST_ENDPOINT),
            ),
        )
    return tuple(pools)


def _rows_from_meteora_payload(payload: object) -> Sequence[object]:
    root = _mapping(payload)
    rows = root.get("data")
    if isinstance(rows, Sequence) and not isinstance(rows, str):
        return rows
    raise ValueError("Meteora pool-list response has no data array")


def parse_meteora_dlmm_pools(payload: object) -> tuple[RegistryPool, ...]:
    """Normalize the documented Meteora DLMM `/pools` response."""

    pools: list[RegistryPool] = []
    for row in _rows_from_meteora_payload(payload):
        item = _mapping(row)
        pool_id = _first_string(item, ("address", "pool_address", "id"))
        token_a = _token(item.get("token_x", item.get("tokenX")))
        token_b = _token(item.get("token_y", item.get("tokenY")))
        if pool_id is None or token_a is None or token_b is None:
            continue
        volume = _mapping(item.get("volume"))
        pool_config = _mapping(item.get("pool_config", item.get("poolConfig")))
        fee_hint = {
            key: pool_config[key]
            for key in ("base_fee_pct", "max_fee_pct", "protocol_fee_pct", "bin_step")
            if key in pool_config and isinstance(pool_config[key], (str, int, float)) and not isinstance(pool_config[key], bool)
        }
        dynamic_fee = item.get("dynamic_fee_pct", item.get("dynamicFeePct"))
        if isinstance(dynamic_fee, (str, int, float)) and not isinstance(dynamic_fee, bool):
            fee_hint["dynamic_fee_pct"] = dynamic_fee
        pools.append(
            RegistryPool(
                protocol="meteora_dlmm",
                pool_id=pool_id,
                pool_kind="DLMM",
                token_a=token_a,
                token_b=token_b,
                tvl_usd=_first_decimal(item, ("tvl", "tvlUsd")),
                volume_24h_usd=_first_decimal(volume, ("24h", "volume24h", "volume_usd_24h")),
                blacklisted=_is_blacklisted(item),
                fee_hint=fee_hint,
                discovery_origin=_redact_url(METEORA_DLMM_POOL_LIST_ENDPOINT),
            ),
        )
    return tuple(pools)


def parse_orca_whirlpool_pools(payload: object) -> tuple[RegistryPool, ...]:
    """Normalize Orca Public API v2 Whirlpool rows without using its price."""

    root = _mapping(payload)
    rows = root.get("data")
    if not isinstance(rows, Sequence) or isinstance(rows, str):
        raise ValueError("Orca pool-list response has no data array")
    pools: list[RegistryPool] = []
    for row in rows:
        item = _mapping(row)
        pool_id = _first_string(item, ("address", "id"))
        token_a = _token(item.get("tokenA", item.get("tokenMintA")))
        token_b = _token(item.get("tokenB", item.get("tokenMintB")))
        if pool_id is None or token_a is None or token_b is None:
            continue
        stats_24h = _mapping(_mapping(item.get("stats")).get("24h"))
        fee_hint: dict[str, object] = {
            key: item[key]
            for key in ("tickSpacing", "feeRate", "protocolFeeRate", "adaptiveFeeEnabled")
            if key in item
            and isinstance(item[key], (str, int, float, bool))
        }
        adaptive = _mapping(item.get("adaptiveFee"))
        if "currentRate" in adaptive and isinstance(adaptive["currentRate"], (str, int, float)):
            fee_hint["adaptiveFeeCurrentRate"] = adaptive["currentRate"]
        pools.append(
            RegistryPool(
                protocol="orca_whirlpool",
                pool_id=pool_id,
                pool_kind=_first_string(item, ("poolType",)) or "whirlpool",
                token_a=token_a,
                token_b=token_b,
                tvl_usd=_first_decimal(item, ("tvlUsdc", "tvl", "tvlUsd")),
                volume_24h_usd=_first_decimal(stats_24h, ("volume", "volumeUsd")),
                blacklisted=_is_blacklisted(item),
                fee_hint=fee_hint,
                discovery_origin=_redact_url(ORCA_WHIRLPOOL_LIST_ENDPOINT),
            ),
        )
    return tuple(pools)


def select_pools(
    pools: Iterable[RegistryPool],
    *,
    policy: PoolSelectionPolicy,
) -> tuple[RegistryPool, ...]:
    """Return bounded, known-asset pools ordered by liquidity then volume."""

    eligible: list[RegistryPool] = []
    for pool in pools:
        if pool.blacklisted:
            continue
        if pool.token_a.address not in policy.allowed_mints or pool.token_b.address not in policy.allowed_mints:
            continue
        if policy.require_verified_tokens and (
            pool.token_a.verified is False or pool.token_b.verified is False
        ):
            continue
        if pool.tvl_usd is None or pool.tvl_usd < policy.minimum_tvl_usd:
            continue
        if pool.volume_24h_usd is None or pool.volume_24h_usd < policy.minimum_volume_24h_usd:
            continue
        eligible.append(pool)

    eligible.sort(
        key=lambda pool: (
            pool.protocol,
            -(pool.volume_24h_usd or Decimal("-1")),
            -(pool.tvl_usd or Decimal("-1")),
            pool.pool_id,
        ),
    )
    selected: list[RegistryPool] = []
    counts: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for pool in eligible:
        identity = (pool.protocol, pool.pool_id)
        if identity in seen or counts.get(pool.protocol, 0) >= policy.max_pools_per_protocol:
            continue
        seen.add(identity)
        counts[pool.protocol] = counts.get(pool.protocol, 0) + 1
        selected.append(pool)
    return tuple(selected)


def default_allowed_mints() -> frozenset[str]:
    """The currently CEX-mappable Solana universe plus USD/SOL hubs."""

    return frozenset(
        {
            SOL_WRAPPED_MINT,
            SOLANA_USDC.address,
            SOLANA_USDT.address,
            *(asset.address for asset in SOLANA_PROVIDER_BASES.values()),
        },
    )


async def _fetch_page(
    *,
    endpoint: str,
    query: Mapping[str, str],
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher,
    headers: Mapping[str, str] | None = None,
) -> tuple[object | None, str | None, float | None]:
    url = f"{endpoint}?{urllib.parse.urlencode(query)}"
    response = await _timed_fetch(
        fetch_json,
        url=url,
        method="GET",
        body=None,
        headers={} if headers is None else headers,
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
    )
    if response.error is not None:
        return None, response.error[:512], round(response.rtt_ms, 3)
    return response.payload, None, round(response.rtt_ms, 3)


async def discover_raydium_pools(
    *,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> DiscoverySourceResult:
    """Fetch one bulk Raydium page; errors are returned, not retried blindly."""

    payload, error, rtt_ms = await _fetch_page(
        endpoint=RAYDIUM_POOL_LIST_ENDPOINT,
        query={
            "poolType": "all",
            "poolSortField": "default",
            "sortType": "desc",
            "pageSize": "1000",
            "page": "1",
        },
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        fetch_json=fetch_json,
    )
    if error is not None:
        return DiscoverySourceResult(
            source="raydium_api_v3",
            endpoint_origin=_redact_url(RAYDIUM_POOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=error,
            response_rtt_ms=rtt_ms,
        )
    try:
        pools = parse_raydium_pools(payload)
    except (TypeError, ValueError) as exc:
        return DiscoverySourceResult(
            source="raydium_api_v3",
            endpoint_origin=_redact_url(RAYDIUM_POOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=f"invalid response: {exc}"[:512],
            response_rtt_ms=rtt_ms,
        )
    return DiscoverySourceResult(
        source="raydium_api_v3",
        endpoint_origin=_redact_url(RAYDIUM_POOL_LIST_ENDPOINT),
        pools=pools,
        pages_requested=1,
        error=None,
        response_rtt_ms=rtt_ms,
    )


async def discover_meteora_dlmm_pools(
    *,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> DiscoverySourceResult:
    """Fetch one high-volume Meteora DLMM page, not a live price endpoint."""

    payload, error, rtt_ms = await _fetch_page(
        endpoint=METEORA_DLMM_POOL_LIST_ENDPOINT,
        query={
            "page": "1",
            "page_size": "1000",
            "sort_by": "volume_24h:desc",
            "filter_by": "is_blacklisted=false",
        },
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        fetch_json=fetch_json,
    )
    if error is not None:
        return DiscoverySourceResult(
            source="meteora_dlmm_data_api",
            endpoint_origin=_redact_url(METEORA_DLMM_POOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=error,
            response_rtt_ms=rtt_ms,
        )
    try:
        pools = parse_meteora_dlmm_pools(payload)
    except (TypeError, ValueError) as exc:
        return DiscoverySourceResult(
            source="meteora_dlmm_data_api",
            endpoint_origin=_redact_url(METEORA_DLMM_POOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=f"invalid response: {exc}"[:512],
            response_rtt_ms=rtt_ms,
        )
    return DiscoverySourceResult(
        source="meteora_dlmm_data_api",
        endpoint_origin=_redact_url(METEORA_DLMM_POOL_LIST_ENDPOINT),
        pools=pools,
        pages_requested=1,
        error=None,
        response_rtt_ms=rtt_ms,
    )


async def discover_orca_whirlpools(
    *,
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> DiscoverySourceResult:
    """Fetch one Orca volume-sorted page; hot quotes never use this API."""

    payload, error, rtt_ms = await _fetch_page(
        endpoint=ORCA_WHIRLPOOL_LIST_ENDPOINT,
        query={
            "sortBy": "volume24h",
            "sortDirection": "desc",
            "size": "100",
            "stats": "24h",
            "includeBlocked": "false",
        },
        proxy_url=proxy_url,
        timeout_seconds=timeout_seconds,
        fetch_json=fetch_json,
        headers={
            "Accept": "application/json",
            "User-Agent": "market-data-lab/0.1 (+read-only pool discovery)",
        },
    )
    if error is not None:
        return DiscoverySourceResult(
            source="orca_public_api_v2",
            endpoint_origin=_redact_url(ORCA_WHIRLPOOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=error,
            response_rtt_ms=rtt_ms,
        )
    try:
        pools = parse_orca_whirlpool_pools(payload)
    except (TypeError, ValueError) as exc:
        return DiscoverySourceResult(
            source="orca_public_api_v2",
            endpoint_origin=_redact_url(ORCA_WHIRLPOOL_LIST_ENDPOINT),
            pools=(),
            pages_requested=1,
            error=f"invalid response: {exc}"[:512],
            response_rtt_ms=rtt_ms,
        )
    return DiscoverySourceResult(
        source="orca_public_api_v2",
        endpoint_origin=_redact_url(ORCA_WHIRLPOOL_LIST_ENDPOINT),
        pools=pools,
        pages_requested=1,
        error=None,
        response_rtt_ms=rtt_ms,
    )


async def refresh_registry(
    *,
    policy: PoolSelectionPolicy,
    sources: Sequence[str],
    proxy_url: str | None,
    timeout_seconds: float,
    fetch_json: JsonFetcher = _fetch_json_sync,
) -> dict[str, object]:
    """Perform a bounded discovery refresh and retain only selected metadata."""

    selected_sources = tuple(dict.fromkeys(source.lower() for source in sources))
    tasks = []
    if "raydium" in selected_sources:
        tasks.append(
            discover_raydium_pools(
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            ),
        )
    if "meteora" in selected_sources:
        tasks.append(
            discover_meteora_dlmm_pools(
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            ),
        )
    if "orca" in selected_sources:
        tasks.append(
            discover_orca_whirlpools(
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                fetch_json=fetch_json,
            ),
        )
    if not tasks:
        raise ValueError("sources must contain at least raydium, meteora or orca")
    results = await asyncio.gather(*tasks)
    all_pools = tuple(pool for result in results for pool in result.pools)
    selected = select_pools(all_pools, policy=policy)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "kind": "solana_pool_registry",
        "not_a_price_feed": True,
        "hot_price_source": "direct Solana RPC/WebSocket only",
        "selection_policy": policy.as_dict(),
        "sources": [result.as_dict() for result in results],
        "pools": [pool.as_dict() for pool in selected],
        "selected_pool_count": len(selected),
        "raw_catalogue_rows_persisted": False,
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
    }


def _parse_mints(value: str | None) -> frozenset[str]:
    if value is None:
        return default_allowed_mints()
    result = frozenset(item.strip() for item in value.split(",") if item.strip())
    if not result:
        raise ValueError("--allowed-mints must contain at least one mint")
    return result


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/registry/solana-pools.json"))
    parser.add_argument("--sources", default="raydium,meteora,orca")
    parser.add_argument("--allowed-mints")
    parser.add_argument("--minimum-tvl-usd", default="100000")
    parser.add_argument("--minimum-volume-24h-usd", default="25000")
    parser.add_argument("--max-pools-per-protocol", type=int, default=30)
    parser.add_argument("--allow-unverified-tokens", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--proxy-url")
    return parser


def main() -> None:
    args = build_argument_parser().parse_args()
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    try:
        policy = PoolSelectionPolicy(
            allowed_mints=_parse_mints(args.allowed_mints),
            minimum_tvl_usd=Decimal(args.minimum_tvl_usd),
            minimum_volume_24h_usd=Decimal(args.minimum_volume_24h_usd),
            max_pools_per_protocol=args.max_pools_per_protocol,
            require_verified_tokens=not args.allow_unverified_tokens,
        )
        sources = tuple(item.strip().lower() for item in args.sources.split(",") if item.strip())
        network_route = configure_process_network_route(args.proxy_url)
        registry = asyncio.run(
            refresh_registry(
                policy=policy,
                sources=sources,
                proxy_url=args.proxy_url,
                timeout_seconds=args.timeout_seconds,
            ),
        )
    except (InvalidOperation, OSError, ValueError) as exc:
        raise SystemExit(f"pool registry refresh failed: {exc}") from exc
    registry["network_route"] = network_route
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(args.output, registry)
    print(
        json.dumps(
            {
                "output": str(args.output.resolve()),
                "selected_pool_count": registry["selected_pool_count"],
                "sources": registry["sources"],
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
