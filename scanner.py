#!/usr/bin/env python3
"""Run the complete read-only market scanner with one command.

Normal use:

    .venv/bin/python scanner.py

The process starts every currently implemented public-data adapter.  Raw
quotes and books stay in bounded RAM windows; only compact health/statistics
are written to disk.  It has no order, wallet, transfer or transaction
capability.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import signal
import sys
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from market_data_lab.cex_dex_cycles import (  # noqa: E402
    DEFAULT_NETWORK_COST_FLOORS,
    MARKETS,
    build_cycle_providers,
)
from market_data_lab.cex_market_discovery import discover_cex_streams  # noqa: E402
from market_data_lab.continuous_cycle_monitor import record_continuous_cycle_monitor  # noqa: E402
from market_data_lab.dex_quotes import (  # noqa: E402
    AsyncRequestPacer,
    JupiterProvider,
    OMNISTON_WS_ENDPOINT,
    RaydiumProvider,
    SOLANA_PROVIDER_BASES,
    SOLANA_USDC,
    SOLANA_USDT,
)
from market_data_lab.live_common import atomic_json  # noqa: E402
from market_data_lab.perp_dex_monitor import (  # noqa: E402
    bybit_linear_symbol,
    fetch_bybit_linear_instruments,
    public_fallback_fee_rates,
    record_perp_dex_monitor,
)
from market_data_lab.perp_venue_feeds import (  # noqa: E402
    DEFAULT_HYPERLIQUID_COINS,
)
from market_data_lab.rolling_cycle_monitor import DEFAULT_CEX_TAKER_FEES  # noqa: E402
from market_data_lab.solana_quote_worker import (  # noqa: E402
    DiscoveredOrcaPool,
    DiscoveredRaydiumStandardPool,
    OrcaDiscoveryAsset,
    OrcaDiscoveryPair,
    RaydiumStandardDiscoveryCandidate,
    discover_orca_whirlpools,
    discover_raydium_standard_pools,
)
from market_data_lab.solana_realtime_scanner import (  # noqa: E402
    CexStreamConfig,
    MeteoraDlmmPoolConfig,
    OrcaWhirlpoolPoolConfig,
    RaydiumClmmPoolConfig,
    RaydiumStandardPoolConfig,
    load_solana_scanner_config,
)
from market_data_lab.solana_route_evaluator import LocalSpotRoute  # noqa: E402
from market_data_lab.triangle_cycle_monitor import (  # noqa: E402
    TRIANGLE_MARKETS,
    build_triangle_providers,
    record_triangle_cycle_monitor,
)
from market_data_lab.unified_market_data import build_unified_market_data_scanner  # noqa: E402


LOCAL_CONFIG = PROJECT_ROOT / "config" / "solana-rpc.local.toml"
OUTPUT_ROOT = PROJECT_ROOT / "data" / "live" / "scanner"
SOLANA_POOL_REGISTRY = PROJECT_ROOT / "data" / "registry" / "solana-pools.json"
CEX_VENUES = ("MEXC", "BYBIT", "OKX", "BINANCE")

# One global budget per public service.  Direct-stable and triangle workers
# share these objects, so adding a route never silently multiplies the request
# rate.  Raydium's conservative interval reflects the 429s seen in earlier
# wide runs; the direct Solana pool subscription below is independent of this
# HTTP budget and remains high-frequency.
RAYDIUM_REQUEST_INTERVAL_SECONDS = 3.0
STONFI_ROUND_INTERVAL_SECONDS = 2.0
OMNISTON_ROUND_INTERVAL_SECONDS = 1.0
EVM_ROUND_INTERVAL_SECONDS = 1.0
# Status files are rich diagnostics (source health plus bounded analyzer
# summaries), so publishing them every two seconds causes needless atomic
# rewrites during a long-running read-only scan.  Ten seconds keeps health
# checks responsive while bounding background disk churn.
MIN_STATUS_SNAPSHOT_FLUSH_SECONDS = 10.0

DIRECT_NOTIONAL_USDT = Decimal("100")
TRIANGLE_REFERENCE_NOTIONAL_USDT = Decimal("100")
PERP_BASIS_NOTIONAL_USDT = Decimal("100")
MAX_RESPONSE_SKEW_MS = Decimal("1000")
MAX_DEX_CACHE_AGE_MS = Decimal("2000")
MAX_CANDIDATE_EVENTS_PER_COMPONENT = 5_000
MAX_HOT_RAYDIUM_CLMM_POOLS = 11
MAX_HOT_METEORA_POOLS = 19
MAX_HOT_ORCA_POOLS = 6
MAX_HOT_RAYDIUM_STANDARD_POOLS = 9

IMPLEMENTED_DEX_SOURCES = (
    "Raydium CLMM direct Solana account subscription (selected pools)",
    "Raydium CPMM/AMM v4 direct Solana pool/vault subscriptions (selected pools)",
    "Meteora DLMM direct Solana account/bin-array subscriptions (selected pools)",
    "Orca Whirlpool direct Solana account/tick-array subscriptions (selected pools)",
    "Raydium Route API v2",
    "Jupiter Swap V2 quote API",
    "STON.fi quote API",
    "Omniston RFQ/aggregator WebSocket",
    "Uniswap V3 Quoter on Base",
    "Uniswap V3 Quoter on Polygon",
)
NOT_YET_IMPLEMENTED = (
    "DeDust/TON",
    "additional EVM chains and DEX protocols",
)


def _run_id() -> str:
    return "all-markets-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return payload if isinstance(payload, dict) else None


def _counter_total(value: object) -> int:
    if not isinstance(value, Mapping):
        return 0
    return sum(item for item in value.values() if isinstance(item, int))


def _augment_with_registry_raydium_clmm(config: Any) -> Any:
    """Add every reviewed CLMM registry pool and one explicit MEXC route."""

    payload = _read_json(SOLANA_POOL_REGISTRY)
    raw_pools = payload.get("pools") if payload is not None else None
    if not isinstance(raw_pools, list):
        return config
    assets_by_mint: dict[str, tuple[str, str, int]] = {
        asset.address: (
            asset.symbol.upper(),
            "BTC" if asset.symbol.upper() == "CBBTC" else asset.symbol.upper(),
            asset.decimals,
        )
        for asset in SOLANA_PROVIDER_BASES.values()
    }
    assets_by_mint[SOLANA_USDC.address] = ("USDC", "USDC", SOLANA_USDC.decimals)
    assets_by_mint[SOLANA_USDT.address] = ("USDT", "USDT", SOLANA_USDT.decimals)
    pools = list(config.pools)
    routes = list(config.local_route_evaluator.routes)
    existing_pool_ids = {pool.pool_id for pool in pools}
    existing_route_keys = {(route.pool_id, route.cex_venue) for route in routes}
    required_mexc_symbols: set[str] = set()
    hub_priority = {"USDT": 0, "USDC": 1, "SOL": 2}
    fee_bps = DEFAULT_CEX_TAKER_FEES["MEXC"]

    def token(value: object) -> tuple[str, str, int, str] | None:
        if not isinstance(value, Mapping):
            return None
        address = value.get("address")
        if not isinstance(address, str) or address not in assets_by_mint:
            return None
        display, cex_base, decimals = assets_by_mint[address]
        if value.get("decimals") != decimals:
            return None
        return display, cex_base, decimals, address

    selected = 0
    for item in raw_pools:
        if not isinstance(item, Mapping) or item.get("protocol") != "raydium_clmm":
            continue
        pool_id = item.get("pool_id")
        token_a = token(item.get("token_a"))
        token_b = token(item.get("token_b"))
        if not isinstance(pool_id, str) or token_a is None or token_b is None:
            continue
        hubs = [value for value in (token_a, token_b) if value[0] in hub_priority]
        if not hubs:
            continue
        bridge = min(hubs, key=lambda value: hub_priority[value[0]])
        base = token_b if bridge is token_a else token_a
        if base[0] == "USDT":
            base, bridge = bridge, base
        bridge_is_settlement = bridge[0] == "USDT"
        base_cex_symbol = f"{base[1]}USDT"
        bridge_cex_symbol = None if bridge_is_settlement else f"{bridge[1]}USDT"
        label = f"{base[0]}/{bridge[0]} Raydium CLMM"
        if pool_id not in existing_pool_ids:
            pools.append(RaydiumClmmPoolConfig(pool_id=pool_id, label=label))
            existing_pool_ids.add(pool_id)
        required_mexc_symbols.add(base_cex_symbol)
        if bridge_cex_symbol is not None:
            required_mexc_symbols.add(bridge_cex_symbol)
        if (pool_id, "MEXC") not in existing_route_keys:
            routes.append(
                LocalSpotRoute(
                    route_id=(
                        f"raydium-clmm-{pool_id[:8]}-{base[0].lower()}-"
                        f"{bridge[0].lower()}-mexc-usdt"
                    ),
                    pool_id=pool_id,
                    pool_protocol="raydium_clmm",
                    base_mint=base[3],
                    bridge_mint=bridge[3],
                    base_decimals=base[2],
                    bridge_decimals=bridge[2],
                    base_symbol=base[0],
                    bridge_symbol=bridge[0],
                    settlement_symbol="USDT",
                    cex_venue="MEXC",
                    base_cex_symbol=base_cex_symbol,
                    bridge_cex_symbol=bridge_cex_symbol,
                    bridge_is_settlement=bridge_is_settlement,
                    notional_settlement=DIRECT_NOTIONAL_USDT,
                    base_buy_taker_fee_bps=fee_bps,
                    base_sell_taker_fee_bps=fee_bps,
                    bridge_buy_taker_fee_bps=fee_bps,
                    bridge_sell_taker_fee_bps=fee_bps,
                    network_cost_floor_settlement=Decimal("0.01"),
                    asset_equivalence=(
                        f"Raydium CLMM canonical {base[0]}/{bridge[0]} mints mapped to "
                        "MEXC spot tickers; inventory, transfer support and rebalance "
                        "remain unverified"
                    ),
                ),
            )
            existing_route_keys.add((pool_id, "MEXC"))
        selected += 1
        if selected >= MAX_HOT_RAYDIUM_CLMM_POOLS:
            break

    cex_streams: list[CexStreamConfig] = []
    for stream in config.cex_streams:
        if stream.venue == "MEXC" and stream.category == "spot":
            cex_streams.append(
                replace(stream, symbols=tuple(sorted(set(stream.symbols) | required_mexc_symbols))),
            )
        else:
            cex_streams.append(stream)
    return replace(
        config,
        pools=tuple(pools),
        cex_streams=tuple(cex_streams),
        local_route_evaluator=replace(config.local_route_evaluator, routes=tuple(routes)),
    )


def _augment_with_registry_meteora(config: Any) -> Any:
    """Add a bounded, diverse Meteora hot set from the cached safe registry."""

    payload = _read_json(SOLANA_POOL_REGISTRY)
    raw_pools = payload.get("pools") if payload is not None else None
    if not isinstance(raw_pools, list):
        return config

    assets_by_mint: dict[str, tuple[str, str, int]] = {
        asset.address: (
            asset.symbol.upper(),
            "BTC" if asset.symbol.upper() == "CBBTC" else asset.symbol.upper(),
            asset.decimals,
        )
        for asset in SOLANA_PROVIDER_BASES.values()
    }
    assets_by_mint[SOLANA_USDC.address] = ("USDC", "USDC", SOLANA_USDC.decimals)
    assets_by_mint[SOLANA_USDT.address] = ("USDT", "USDT", SOLANA_USDT.decimals)
    existing_pool_ids = {pool.pool_id for pool in config.meteora_pools}
    selected: list[tuple[MeteoraDlmmPoolConfig, LocalSpotRoute]] = []
    hub_priority = {"USDT": 0, "USDC": 1, "SOL": 2}

    def token(value: object) -> tuple[str, str, int, str] | None:
        if not isinstance(value, Mapping):
            return None
        address = value.get("address")
        if not isinstance(address, str) or address not in assets_by_mint:
            return None
        display, cex_base, decimals = assets_by_mint[address]
        if value.get("decimals") != decimals:
            return None
        return display, cex_base, decimals, address

    for item in raw_pools:
        if not isinstance(item, Mapping) or item.get("protocol") != "meteora_dlmm":
            continue
        pool_id = item.get("pool_id")
        token_a = token(item.get("token_a"))
        token_b = token(item.get("token_b"))
        if not isinstance(pool_id, str) or token_a is None or token_b is None:
            continue
        if pool_id in existing_pool_ids:
            continue
        hubs = [value for value in (token_a, token_b) if value[0] in hub_priority]
        if not hubs:
            continue
        bridge = min(hubs, key=lambda value: hub_priority[value[0]])
        base = token_b if bridge is token_a else token_a
        # For USDC/USDT the non-settlement stable is the explicit CEX base.
        if base[0] == "USDT":
            base, bridge = bridge, base
        bridge_is_settlement = bridge[0] == "USDT"
        base_cex_symbol = f"{base[1]}USDT"
        bridge_cex_symbol = None if bridge_is_settlement else f"{bridge[1]}USDT"
        fee_bps = DEFAULT_CEX_TAKER_FEES["MEXC"]
        label = f"{base[0]}/{bridge[0]} Meteora DLMM"
        route = LocalSpotRoute(
            route_id=f"meteora-{pool_id[:8]}-{base[0].lower()}-{bridge[0].lower()}-mexc-usdt",
            pool_id=pool_id,
            pool_protocol="meteora_dlmm",
            base_mint=base[3],
            bridge_mint=bridge[3],
            base_decimals=base[2],
            bridge_decimals=bridge[2],
            base_symbol=base[0],
            bridge_symbol=bridge[0],
            settlement_symbol="USDT",
            cex_venue="MEXC",
            base_cex_symbol=base_cex_symbol,
            bridge_cex_symbol=bridge_cex_symbol,
            bridge_is_settlement=bridge_is_settlement,
            notional_settlement=DIRECT_NOTIONAL_USDT,
            base_buy_taker_fee_bps=fee_bps,
            base_sell_taker_fee_bps=fee_bps,
            bridge_buy_taker_fee_bps=fee_bps,
            bridge_sell_taker_fee_bps=fee_bps,
            network_cost_floor_settlement=Decimal("0.01"),
            asset_equivalence=(
                f"Meteora canonical {base[0]}/{bridge[0]} Solana mints mapped to MEXC "
                "spot tickers; inventory location, deposits/withdrawals and rebalance remain unverified"
            ),
        )
        selected.append((MeteoraDlmmPoolConfig(pool_id=pool_id, label=label), route))
        if len(selected) >= MAX_HOT_METEORA_POOLS:
            break

    if not selected:
        return config
    routes = tuple(
        dict.fromkeys(
            (*config.local_route_evaluator.routes, *(route for _, route in selected)),
        ),
    )
    required_mexc_symbols = {
        symbol
        for _, route in selected
        for symbol in (route.base_cex_symbol, route.bridge_cex_symbol)
        if symbol is not None
    }
    cex_streams: list[CexStreamConfig] = []
    mexc_found = False
    for stream in config.cex_streams:
        if stream.venue == "MEXC" and stream.category == "spot":
            mexc_found = True
            cex_streams.append(
                replace(stream, symbols=tuple(sorted(set(stream.symbols) | required_mexc_symbols))),
            )
        else:
            cex_streams.append(stream)
    if not mexc_found:
        cex_streams.append(
            CexStreamConfig(
                venue="MEXC",
                category="spot",
                symbols=tuple(sorted(required_mexc_symbols)),
            ),
        )
    return replace(
        config,
        meteora_pools=(*config.meteora_pools, *(pool for pool, _ in selected)),
        cex_streams=tuple(cex_streams),
        local_route_evaluator=replace(config.local_route_evaluator, routes=routes),
    )


def _raydium_standard_candidates() -> tuple[RaydiumStandardDiscoveryCandidate, ...]:
    """Take a bounded volume-sorted candidate set from the cached registry."""

    payload = _read_json(SOLANA_POOL_REGISTRY)
    raw_pools = payload.get("pools") if payload is not None else None
    if not isinstance(raw_pools, list):
        return ()
    candidates: list[RaydiumStandardDiscoveryCandidate] = []
    for item in raw_pools:
        if not isinstance(item, Mapping) or item.get("protocol") != "raydium_standard":
            continue
        pool_id = item.get("pool_id")
        token_a = item.get("token_a")
        token_b = item.get("token_b")
        if (
            not isinstance(pool_id, str)
            or not isinstance(token_a, Mapping)
            or not isinstance(token_b, Mapping)
        ):
            continue
        symbol_a = token_a.get("symbol")
        symbol_b = token_b.get("symbol")
        label = (
            f"{symbol_a}/{symbol_b} Raydium Standard"
            if isinstance(symbol_a, str) and isinstance(symbol_b, str)
            else f"{pool_id[:8]} Raydium Standard"
        )
        candidates.append(RaydiumStandardDiscoveryCandidate(pool_id, label))
        if len(candidates) >= MAX_HOT_RAYDIUM_STANDARD_POOLS:
            break
    return tuple(candidates)


def _augment_with_discovered_raydium_standard(
    config: Any,
    discovered: tuple[DiscoveredRaydiumStandardPool, ...],
) -> Any:
    """Add exact local routes only when both mints have an explicit CEX map."""

    if not discovered:
        return config
    assets_by_mint: dict[str, tuple[str, str, int]] = {
        asset.address: (
            asset.symbol.upper(),
            "BTC" if asset.symbol.upper() == "CBBTC" else asset.symbol.upper(),
            asset.decimals,
        )
        for asset in SOLANA_PROVIDER_BASES.values()
    }
    assets_by_mint[SOLANA_USDC.address] = ("USDC", "USDC", SOLANA_USDC.decimals)
    assets_by_mint[SOLANA_USDT.address] = ("USDT", "USDT", SOLANA_USDT.decimals)
    hub_priority = {"USDT": 0, "USDC": 1, "SOL": 2}
    existing_pool_ids = {pool.pool_id for pool in config.raydium_standard_pools}
    existing_route_ids = {route.route_id for route in config.local_route_evaluator.routes}
    pools = list(config.raydium_standard_pools)
    routes = list(config.local_route_evaluator.routes)
    required_mexc_symbols: set[str] = set()
    fee_bps = DEFAULT_CEX_TAKER_FEES["MEXC"]

    for pool in discovered:
        token_a = assets_by_mint.get(pool.token_a_mint)
        token_b = assets_by_mint.get(pool.token_b_mint)
        if (
            token_a is None
            or token_b is None
            or token_a[2] != pool.token_a_decimals
            or token_b[2] != pool.token_b_decimals
        ):
            continue
        values = (
            (*token_a, pool.token_a_mint),
            (*token_b, pool.token_b_mint),
        )
        hubs = [value for value in values if value[0] in hub_priority]
        if not hubs:
            continue
        bridge = min(hubs, key=lambda value: hub_priority[value[0]])
        base = values[1] if bridge is values[0] else values[0]
        if base[0] == "USDT":
            base, bridge = bridge, base
        bridge_is_settlement = bridge[0] == "USDT"
        base_cex_symbol = f"{base[1]}USDT"
        bridge_cex_symbol = None if bridge_is_settlement else f"{bridge[1]}USDT"
        protocol_name = "CPMM" if pool.protocol == "raydium_cpmm" else "AMM v4"
        label = f"{base[0]}/{bridge[0]} Raydium {protocol_name}"
        if pool.pool_id not in existing_pool_ids:
            pools.append(RaydiumStandardPoolConfig(pool.pool_id, label, pool.protocol))
            existing_pool_ids.add(pool.pool_id)
        route_id = (
            f"{pool.protocol.replace('_', '-')}-{pool.pool_id[:8]}-"
            f"{base[0].lower()}-{bridge[0].lower()}-mexc-usdt"
        )
        required_mexc_symbols.add(base_cex_symbol)
        if bridge_cex_symbol is not None:
            required_mexc_symbols.add(bridge_cex_symbol)
        if route_id in existing_route_ids:
            continue
        routes.append(
            LocalSpotRoute(
                route_id=route_id,
                pool_id=pool.pool_id,
                pool_protocol=pool.protocol,
                base_mint=base[3],
                bridge_mint=bridge[3],
                base_decimals=base[2],
                bridge_decimals=bridge[2],
                base_symbol=base[0],
                bridge_symbol=bridge[0],
                settlement_symbol="USDT",
                cex_venue="MEXC",
                base_cex_symbol=base_cex_symbol,
                bridge_cex_symbol=bridge_cex_symbol,
                bridge_is_settlement=bridge_is_settlement,
                notional_settlement=DIRECT_NOTIONAL_USDT,
                base_buy_taker_fee_bps=fee_bps,
                base_sell_taker_fee_bps=fee_bps,
                bridge_buy_taker_fee_bps=fee_bps,
                bridge_sell_taker_fee_bps=fee_bps,
                network_cost_floor_settlement=Decimal("0.01"),
                asset_equivalence=(
                    f"Raydium {protocol_name} canonical {base[0]}/{bridge[0]} mints mapped "
                    "to MEXC spot tickers; inventory, transfer support and rebalance remain unverified"
                ),
            ),
        )
        existing_route_ids.add(route_id)

    if not pools:
        return config
    cex_streams: list[CexStreamConfig] = []
    mexc_found = False
    for stream in config.cex_streams:
        if stream.venue == "MEXC" and stream.category == "spot":
            mexc_found = True
            cex_streams.append(
                replace(stream, symbols=tuple(sorted(set(stream.symbols) | required_mexc_symbols))),
            )
        else:
            cex_streams.append(stream)
    if not mexc_found and required_mexc_symbols:
        cex_streams.append(CexStreamConfig("MEXC", tuple(sorted(required_mexc_symbols))))
    return replace(
        config,
        raydium_standard_pools=tuple(pools),
        cex_streams=tuple(cex_streams),
        local_route_evaluator=replace(config.local_route_evaluator, routes=tuple(routes)),
    )


def _orca_discovery_pairs() -> tuple[OrcaDiscoveryPair, ...]:
    usdc = OrcaDiscoveryAsset("USDC", SOLANA_USDC.address, SOLANA_USDC.decimals, "USDCUSDT")

    def pair(provider: str, cex_base: str) -> OrcaDiscoveryPair:
        asset = SOLANA_PROVIDER_BASES[provider]
        return OrcaDiscoveryPair(
            base=OrcaDiscoveryAsset(
                asset.symbol.upper(),
                asset.address,
                asset.decimals,
                f"{cex_base}USDT",
            ),
            bridge=usdc,
        )

    # One deterministic PDA batch covers every supported Orca fee tier.  The
    # helper selects one highest-liquidity non-adaptive pool per requested pair.
    return (
        pair("RAYDIUM", "SOL"),
        pair("RAYDIUM_CBBTC", "BTC"),
        pair("RAYDIUM_TRUMP", "TRUMP"),
        pair("RAYDIUM_PUMP", "PUMP"),
        pair("RAYDIUM_JUP", "JUP"),
        pair("RAYDIUM_BONK", "BONK"),
    )


def _augment_with_discovered_orca(
    config: Any,
    discovered: tuple[DiscoveredOrcaPool, ...],
) -> Any:
    if not discovered:
        return config
    existing_pool_ids = {pool.pool_id for pool in config.orca_pools}
    existing_route_ids = {route.route_id for route in config.local_route_evaluator.routes}
    pools = list(config.orca_pools)
    routes = list(config.local_route_evaluator.routes)
    required_mexc_symbols: set[str] = set()
    fee_bps = DEFAULT_CEX_TAKER_FEES["MEXC"]
    for pool in discovered:
        required_mexc_symbols.update((pool.base.cex_symbol, pool.bridge.cex_symbol))
        if pool.pool_id not in existing_pool_ids:
            pools.append(OrcaWhirlpoolPoolConfig(pool.pool_id, pool.label))
            existing_pool_ids.add(pool.pool_id)
        route_id = (
            f"orca-{pool.pool_id[:8]}-{pool.base.symbol.lower()}-"
            f"{pool.bridge.symbol.lower()}-mexc-usdt"
        )
        if route_id in existing_route_ids:
            continue
        routes.append(
            LocalSpotRoute(
                route_id=route_id,
                pool_id=pool.pool_id,
                pool_protocol="orca_whirlpool",
                base_mint=pool.base.mint,
                bridge_mint=pool.bridge.mint,
                base_decimals=pool.base.decimals,
                bridge_decimals=pool.bridge.decimals,
                base_symbol=pool.base.symbol,
                bridge_symbol=pool.bridge.symbol,
                settlement_symbol="USDT",
                cex_venue="MEXC",
                base_cex_symbol=pool.base.cex_symbol,
                bridge_cex_symbol=pool.bridge.cex_symbol,
                bridge_is_settlement=False,
                notional_settlement=DIRECT_NOTIONAL_USDT,
                base_buy_taker_fee_bps=fee_bps,
                base_sell_taker_fee_bps=fee_bps,
                bridge_buy_taker_fee_bps=fee_bps,
                bridge_sell_taker_fee_bps=fee_bps,
                network_cost_floor_settlement=Decimal("0.01"),
                asset_equivalence=(
                    f"Orca canonical {pool.base.symbol}/{pool.bridge.symbol} Solana mints "
                    "mapped to MEXC spot tickers; inventory, transfer support and rebalance "
                    "remain unverified"
                ),
            ),
        )
        existing_route_ids.add(route_id)

    cex_streams: list[CexStreamConfig] = []
    mexc_found = False
    for stream in config.cex_streams:
        if stream.venue == "MEXC" and stream.category == "spot":
            mexc_found = True
            cex_streams.append(
                replace(stream, symbols=tuple(sorted(set(stream.symbols) | required_mexc_symbols))),
            )
        else:
            cex_streams.append(stream)
    if not mexc_found:
        cex_streams.append(CexStreamConfig("MEXC", tuple(sorted(required_mexc_symbols))))
    return replace(
        config,
        orca_pools=tuple(pools),
        cex_streams=tuple(cex_streams),
        local_route_evaluator=replace(config.local_route_evaluator, routes=tuple(routes)),
    )


def _compact_component_status(name: str, directory: Path | None) -> dict[str, Any]:
    if directory is None:
        return {"status": "not_started"}
    status_path = directory / (
        "status.json" if name == "market_data" else "stats.json"
    )
    payload = _read_json(status_path)
    if payload is None:
        return {"status": "starting", "directory": str(directory)}

    common: dict[str, Any] = {
        "status": payload.get("status"),
        "updated_at": payload.get("updated_at"),
        "duration_wall_seconds": payload.get("duration_wall_seconds"),
        "raw_market_data_persisted": payload.get("raw_market_data_persisted"),
        "directory": str(directory),
    }
    if name == "market_data":
        sources: dict[str, Any] = {}
        for source, health in (payload.get("sources") or {}).items():
            if not isinstance(health, Mapping):
                continue
            sources[str(source)] = {
                key: health.get(key)
                for key in (
                    "running",
                    "restarts",
                    "updates",
                    "update_rate_per_second",
                    "last_event_age_ms",
                    "last_error",
                )
            }
        evaluator = (payload.get("extensions") or {}).get("local_route_evaluator", {})
        common.update(
            sources=sources,
            retained_state_keys=(payload.get("state") or {}).get("keys"),
            retained_state_total_updates=(payload.get("state") or {}).get("total_updates"),
            retained_state_coalesced_by_interval=(payload.get("state") or {}).get(
                "coalesced_by_interval",
            ),
            candidate_lifecycle=(evaluator or {}).get("candidate_lifecycle"),
            candidate_event_persistence=(evaluator or {}).get("candidate_event_persistence"),
        )
        extensions = payload.get("extensions") or {}
        common.update(
            market_data_sources={
                str(source): {
                    "markets": len(value.get("markets") or {}),
                    "book_updates": value.get("book_updates"),
                    "context_updates": value.get("context_updates"),
                    "recent_errors": value.get("recent_errors", value.get("metadata_errors")),
                }
                for source, value in extensions.items()
                if isinstance(value, Mapping)
            },
        )
        return common

    if name == "perp_basis":
        common.update(
            dex_rounds_total=_counter_total(payload.get("dex_rounds")),
            dex_providers_with_rounds=len(payload.get("dex_rounds") or {}),
            aggregator_route_labels=payload.get("aggregator_route_labels"),
            perp_book_updates=payload.get("perp_book_updates"),
            stable_fx_book_updates=payload.get("stable_fx_book_updates"),
            basis_observations=payload.get("observations"),
            timing_valid_observations=payload.get("timing_valid_observations"),
            positive_after_modeled_costs=payload.get("positive_after_modeled_costs"),
            positive_with_account_verified_fee=payload.get(
                "positive_with_account_verified_fee",
            ),
            provider_errors_total=_counter_total(payload.get("provider_errors")),
            cex_errors_total=_counter_total(payload.get("cex_errors")),
            disabled_markets=len(payload.get("disabled_markets") or {}),
            candidate_lifecycle=payload.get("candidate_lifecycle"),
            candidate_event_persistence=payload.get("candidate_event_persistence"),
            stream_health=payload.get("stream_health"),
        )
        return common

    common.update(
        dex_rounds_total=_counter_total(payload.get("dex_rounds")),
        dex_providers_with_rounds=len(payload.get("dex_rounds") or {}),
        aggregator_route_labels=payload.get("aggregator_route_labels"),
        cex_updates=payload.get("cex_updates"),
        cycle_observations=payload.get("cycle_observations"),
        timing_valid_observations=payload.get("timing_valid_observations"),
        positive_after_minimum_network_observations=payload.get(
            "positive_after_minimum_network_observations",
        ),
        provider_errors_total=_counter_total(payload.get("provider_errors")),
        cex_errors_total=_counter_total(payload.get("cex_errors")),
        unavailable_cex_symbols=len(payload.get("cex_unavailable_symbols") or {}),
        candidate_lifecycle=payload.get("candidate_lifecycle"),
        candidate_event_persistence=payload.get("candidate_event_persistence"),
    )
    if name == "triangle_cycles":
        common["disabled_markets"] = len(payload.get("disabled_markets") or {})
        common["dex_unavailable_total"] = _counter_total(payload.get("dex_unavailable"))
    return common


@dataclass
class ComponentState:
    name: str
    directory: Path | None = None
    running: bool = False
    launches: int = 0
    restarts: int = 0
    last_exit_status: str | None = None
    last_error: str | None = None

    def snapshot(self) -> dict[str, Any]:
        result = _compact_component_status(self.name, self.directory)
        result.update(
            supervisor_running=self.running,
            supervisor_launches=self.launches,
            supervisor_restarts=self.restarts,
            supervisor_last_exit_status=self.last_exit_status,
            supervisor_last_error=self.last_error,
        )
        return result


ComponentFactory = Callable[[Path], Awaitable[dict[str, Any]]]


class _FairRoundGate:
    def __init__(self, coordinator: "FairRoundCoordinator", group: str) -> None:
        self.coordinator = coordinator
        self.group = group
        self.token: object | None = None

    async def __aenter__(self) -> None:
        self.token = await self.coordinator.acquire(self.group)

    async def __aexit__(self, *_exc: object) -> None:
        await self.coordinator.release(self.group, self.token)
        self.token = None


class FairRoundCoordinator:
    """Alternate complete quote rounds between independently useful groups."""

    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._queues: dict[str, deque[object]] = {}
        self._active = False
        self._last_group: str | None = None

    def gate(self, group: str) -> _FairRoundGate:
        return _FairRoundGate(self, group)

    def _selected_group(self) -> str | None:
        ready = sorted(group for group, queue in self._queues.items() if queue)
        if not ready:
            return None
        if len(ready) == 1:
            return ready[0]
        if self._last_group in ready:
            index = ready.index(self._last_group)
            return ready[(index + 1) % len(ready)]
        return ready[0]

    async def acquire(self, group: str) -> object:
        token = object()
        async with self._condition:
            queue = self._queues.setdefault(group, deque())
            queue.append(token)
            try:
                await self._condition.wait_for(
                    lambda: (
                        not self._active
                        and self._selected_group() == group
                        and bool(queue)
                        and queue[0] is token
                    ),
                )
            except asyncio.CancelledError:
                with contextlib.suppress(ValueError):
                    queue.remove(token)
                self._condition.notify_all()
                raise
            queue.popleft()
            self._active = True
            self._last_group = group
            return token

    async def release(self, group: str, token: object | None) -> None:
        if token is None:
            return
        async with self._condition:
            self._active = False
            self._condition.notify_all()


@dataclass
class SharedBudgets:
    raydium: AsyncRequestPacer = field(
        default_factory=lambda: AsyncRequestPacer(RAYDIUM_REQUEST_INTERVAL_SECONDS),
    )
    stonfi: AsyncRequestPacer = field(
        default_factory=lambda: AsyncRequestPacer(STONFI_ROUND_INTERVAL_SECONDS),
    )
    omniston: AsyncRequestPacer = field(
        default_factory=lambda: AsyncRequestPacer(OMNISTON_ROUND_INTERVAL_SECONDS),
    )
    uniswap_base: AsyncRequestPacer = field(
        default_factory=lambda: AsyncRequestPacer(EVM_ROUND_INTERVAL_SECONDS),
    )
    uniswap_polygon: AsyncRequestPacer = field(
        default_factory=lambda: AsyncRequestPacer(EVM_ROUND_INTERVAL_SECONDS),
    )
    raydium_rounds: FairRoundCoordinator = field(default_factory=FairRoundCoordinator)
    jupiter: AsyncRequestPacer = field(default_factory=lambda: AsyncRequestPacer(1.05))
    jupiter_round_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def provider_gates(self) -> dict[str, AsyncRequestPacer]:
        return {
            "STONFI": self.stonfi,
            "OMNISTON": self.omniston,
            "UNISWAP_BASE": self.uniswap_base,
            "UNISWAP_POLYGON": self.uniswap_polygon,
        }


def _attach_shared_raydium_budget(
    providers: Mapping[str, object],
    budgets: SharedBudgets,
    *,
    group: str,
) -> None:
    for provider in providers.values():
        # Jupiter and Omniston inherit implementation from RaydiumProvider but
        # own different service budgets.  The exact type check is deliberate.
        if type(provider) is RaydiumProvider:
            provider.request_pacer = budgets.raydium
            provider.round_lock = budgets.raydium_rounds.gate(group)
        elif isinstance(provider, JupiterProvider):
            provider.request_pacer = budgets.jupiter
            provider.round_lock = budgets.jupiter_round_lock


async def _supervise_component(
    state: ComponentState,
    *,
    run_root: Path,
    factory: ComponentFactory,
    stop_event: asyncio.Event,
) -> None:
    delay = 1.0
    while not stop_event.is_set():
        state.launches += 1
        suffix = "" if state.launches == 1 else f"-r{state.launches}"
        state.directory = run_root / f"{state.name}{suffix}"
        state.running = True
        state.last_error = None
        try:
            result = await factory(state.directory)
            state.last_exit_status = str(result.get("status", "unknown"))
            state.last_error = result.get("error")
        except asyncio.CancelledError:
            state.running = False
            raise
        except Exception as exc:
            state.last_exit_status = "exception"
            state.last_error = f"{type(exc).__name__}: {exc}"[:512]
        finally:
            state.running = False
        if stop_event.is_set():
            return
        state.restarts += 1
        print(
            f"[{state.name}] stopped unexpectedly ({state.last_exit_status}); restarting in {delay:.0f}s",
            flush=True,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except TimeoutError:
            delay = min(delay * 2, 30.0)


async def run_scanner(*, duration_seconds: float | None) -> Path:
    if not LOCAL_CONFIG.exists():
        raise FileNotFoundError(
            f"missing {LOCAL_CONFIG}; copy the example and configure Solana RPC endpoints",
        )
    if duration_seconds is not None and duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")

    local_config = load_solana_scanner_config(LOCAL_CONFIG)
    local_config = _augment_with_registry_raydium_clmm(local_config)
    local_config = _augment_with_registry_meteora(local_config)
    local_config = replace(
        local_config,
        status_flush_seconds=max(
            MIN_STATUS_SNAPSHOT_FLUSH_SECONDS,
            local_config.status_flush_seconds,
        ),
    )
    raydium_standard_discovery_error: str | None = None
    standard_candidates = _raydium_standard_candidates()
    if standard_candidates:
        try:
            discovered_standard = await discover_raydium_standard_pools(
                rpc_http_url=local_config.rpc_http_url,
                candidates=standard_candidates,
                timeout_seconds=max(30.0, local_config.timeout_seconds * 3),
            )
            local_config = _augment_with_discovered_raydium_standard(
                local_config,
                discovered_standard,
            )
        except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
            raydium_standard_discovery_error = f"{type(exc).__name__}: {exc}"[:512]
    orca_discovery_error: str | None = None
    try:
        discovered_orca = await discover_orca_whirlpools(
            rpc_http_url=local_config.rpc_http_url,
            pairs=_orca_discovery_pairs(),
            maximum_pools=MAX_HOT_ORCA_POOLS,
            timeout_seconds=max(30.0, local_config.timeout_seconds * 3),
        )
        local_config = _augment_with_discovered_orca(local_config, discovered_orca)
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        # Orca discovery is additive: a temporary RPC/SDK failure must not
        # take Raydium, Meteora, CEX, TON or EVM monitoring down with it.
        orca_discovery_error = f"{type(exc).__name__}: {exc}"[:512]
    local_config = replace(
        local_config,
        raydium_local_quote_worker=replace(local_config.raydium_local_quote_worker, enabled=True),
        local_route_evaluator=replace(
            local_config.local_route_evaluator,
            enabled=bool(local_config.local_route_evaluator.routes),
        ),
    )
    direct_markets = tuple(MARKETS.values())
    triangle_markets = tuple(TRIANGLE_MARKETS)
    cex_discovery: dict[str, Any] | None = None
    cex_discovery_error: str | None = None
    cex_bases = {
        market.cex_base_symbol
        for market in direct_markets
    }
    cex_bases.update(
        asset.cex_symbol
        for market in triangle_markets
        for asset in (market.base, market.quote)
    )
    try:
        cex_result = await discover_cex_streams(
            existing_streams=local_config.cex_streams,
            bases=tuple(cex_bases),
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
        )
        local_config = replace(local_config, cex_streams=cex_result.streams)
        cex_discovery = cex_result.safe_descriptor()
    except (OSError, RuntimeError, TimeoutError, ValueError) as exc:
        # CEX discovery merely expands coverage.  The configured local stream
        # remains usable if one public metadata endpoint is unavailable.
        cex_discovery_error = f"{type(exc).__name__}: {exc}"[:512]
    budgets = SharedBudgets(
        jupiter=AsyncRequestPacer(local_config.jupiter.minimum_request_interval_seconds),
    )

    run_id = _run_id()
    run_root = OUTPUT_ROOT / run_id
    run_root.mkdir(parents=True, exist_ok=False)
    started_at = datetime.now(UTC).isoformat()
    stop_event = asyncio.Event()
    stop_reason = "requested_duration_elapsed" if duration_seconds is not None else "signal"
    states = {
        name: ComponentState(name)
        for name in ("market_data",)
    }
    preview_market_data_scanner = build_unified_market_data_scanner(
        config=local_config,
        output_directory=run_root / "market_data",
        hyperliquid_coins=DEFAULT_HYPERLIQUID_COINS,
    )
    runtime_components = dict(preview_market_data_scanner.runtime_components)

    coverage = {
        "cex_public_book_streams": [
            {
                "venue": stream.venue,
                "category": stream.category,
                "symbols": len(stream.symbols),
            }
            for stream in local_config.cex_streams
        ],
        "cex_public_instrument_discovery": cex_discovery,
        "cex_public_instrument_discovery_error": cex_discovery_error,
        "dex_sources": list(IMPLEMENTED_DEX_SOURCES),
        "direct_stable_exact_quote_markets": len(direct_markets),
        "direct_stable_exact_quote_market_names": [market.name for market in direct_markets],
        "cross_asset_exact_quote_pairs": len(triangle_markets),
        "cross_asset_exact_quote_market_names": [market.name for market in triangle_markets],
        "market_data_sources": [
            "CEX public spot books",
            "Solana local pool states",
            "HYPERLIQUID",
            "AEVO",
            "BULK",
            "DYDX",
            "LIGHTER",
            "ASTER",
            "PARADEX",
            "EXTENDED",
            "DRIFT",
        ],
        "market_data_requested_common_perp_bases": list(DEFAULT_HYPERLIQUID_COINS),
        "hot_raydium_pools": len(local_config.pools),
        "hot_raydium_pool_labels": [pool.label for pool in local_config.pools],
        "hot_raydium_standard_pools": len(local_config.raydium_standard_pools),
        "hot_raydium_standard_pool_labels": [
            pool.label for pool in local_config.raydium_standard_pools
        ],
        "hot_raydium_standard_discovery_error": raydium_standard_discovery_error,
        "hot_meteora_pools": len(local_config.meteora_pools),
        "hot_meteora_pool_labels": [pool.label for pool in local_config.meteora_pools],
        "hot_orca_pools": len(local_config.orca_pools),
        "hot_orca_pool_labels": [pool.label for pool in local_config.orca_pools],
        "hot_orca_discovery_error": orca_discovery_error,
        "hot_local_routes": runtime_components["local_route_count"],
        **runtime_components,
        "hot_pool_state_snapshot_refresh_interval_ms": (
            local_config.raydium_local_quote_worker.state_snapshot_refresh_interval_ms
        ),
        "hot_pool_core_refresh_after_ms": (
            local_config.raydium_local_quote_worker.core_refresh_after_ms
        ),
        "hot_pool_maintenance_scan_interval_ms": (
            local_config.raydium_local_quote_worker.maintenance_scan_interval_ms
        ),
        "hot_pool_refresh_stagger_window_ms": (
            local_config.raydium_local_quote_worker.refresh_stagger_window_ms
        ),
        "hot_pool_state_emit_min_interval_ms": (
            local_config.raydium_local_quote_worker.pool_state_emit_min_interval_ms
        ),
        "hot_pool_state_maximum_age_ms": (
            local_config.local_route_evaluator.maximum_pool_state_age_ms
        ),
        "hot_solana_rpc_http_min_request_interval_ms": (
            local_config.raydium_local_quote_worker.rpc_http_min_request_interval_ms
        ),
        "hot_tick_bin_full_refresh_max_age_ms": (
            local_config.raydium_local_quote_worker.tick_cache_max_age_ms
        ),
        "hot_local_route_ids": [
            route.route_id
            for route in local_config.local_route_evaluator.routes
            if runtime_components["local_route_evaluator_attached"]
        ],
        "not_yet_implemented": list(NOT_YET_IMPLEMENTED),
    }
    retention = {
        "raw_market_data_persisted": False,
        "in_memory_window_seconds": local_config.retention_seconds,
        "persistent_files": (
            "compact status/manifest plus bounded cycle candidate lifecycles; "
            "no raw-quote or raw-book files"
        ),
    }
    rate_budgets = {
        "cex": "public WebSocket push; no polling interval",
        "bybit_linear_perpetual": "public L50 order-book and ticker WebSocket push",
        "perp_dex_market_data": (
            "public WebSocket push; full L2 latest-only plus 20ms-coalesced "
            "three-minute top-of-book history"
        ),
        "extended_perp": (
            "per-market public BBO WebSocket; context refresh every 60 seconds; "
            "RFQ BBO explicitly marked indicative"
        ),
        "raydium_route_api_min_request_interval_seconds": RAYDIUM_REQUEST_INTERVAL_SECONDS,
        "jupiter_min_request_interval_seconds": local_config.jupiter.minimum_request_interval_seconds,
        "stonfi_shared_round_interval_seconds": STONFI_ROUND_INTERVAL_SECONDS,
        "omniston_shared_round_interval_seconds": OMNISTON_ROUND_INTERVAL_SECONDS,
        "uniswap_shared_round_interval_seconds_per_chain": EVM_ROUND_INTERVAL_SECONDS,
        "raydium_hot_pools": "Solana accountSubscribe push; no HTTP quote polling",
        "raydium_standard_hot_pools": (
            "Solana pool+vault accountSubscribe push; exact CPMM/AMM-v4 quotes computed locally"
        ),
        "meteora_hot_pools": (
            "Solana accountSubscribe push for pool/bin arrays; quotes computed locally on demand"
        ),
        "orca_hot_pools": (
            "Solana accountSubscribe push for pool/tick arrays; quotes computed locally on demand"
        ),
        "solana_pool_state_snapshot_refresh_interval_ms": (
            local_config.raydium_local_quote_worker.state_snapshot_refresh_interval_ms
        ),
        "solana_pool_refresh_policy": "stale_driven_deterministically_staggered",
        "solana_pool_core_refresh_after_ms": (
            local_config.raydium_local_quote_worker.core_refresh_after_ms
        ),
        "solana_pool_maintenance_scan_interval_ms": (
            local_config.raydium_local_quote_worker.maintenance_scan_interval_ms
        ),
        "solana_pool_state_emit_min_interval_ms": (
            local_config.raydium_local_quote_worker.pool_state_emit_min_interval_ms
        ),
        "solana_rpc_http_min_request_interval_ms": (
            local_config.raydium_local_quote_worker.rpc_http_min_request_interval_ms
        ),
        "solana_tick_bin_full_refresh_max_age_ms": (
            local_config.raydium_local_quote_worker.tick_cache_max_age_ms
        ),
        "status_snapshot_flush_seconds": local_config.status_flush_seconds,
    }
    manifest_path = run_root / "manifest.json"
    status_path = run_root / "status.json"
    latest_path = OUTPUT_ROOT / "latest.json"
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "status": "running",
        "started_at": started_at,
        "stopped_at": None,
        "mode": "one_command_all_market_scanner",
        "config_file": str(LOCAL_CONFIG),
        "coverage": coverage,
        "runtime_components": runtime_components,
        "rate_budgets": rate_budgets,
        "retention": retention,
        "api_credentials": {
            "solana_rpc_configured": True,
            "jupiter_key_configured": local_config.jupiter.api_key is not None,
            "secret_values_persisted": False,
        },
        "wallet_or_private_key_used": False,
        "exchange_private_api_used": False,
        "transactions_or_orders_submitted": False,
        "components": {name: str(run_root / name) for name in states},
        "error": None,
    }
    atomic_json(manifest_path, manifest)
    atomic_json(
        latest_path,
        {
            "run_id": run_id,
            "status": "running",
            "run_directory": str(run_root),
            "status_file": str(status_path),
            "updated_at": started_at,
        },
    )

    async def run_market_data(directory: Path) -> dict[str, Any]:
        # One venue-neutral bus for CEX spot, local Solana pool state, and
        # public perp DEX books/context.  Its attached analyzer consumes this
        # state only; it opens no additional market-data connections.
        scanner = (
            preview_market_data_scanner
            if directory == preview_market_data_scanner.output_directory
            else build_unified_market_data_scanner(
                config=local_config,
                output_directory=directory,
                hyperliquid_coins=DEFAULT_HYPERLIQUID_COINS,
            )
        )
        return await scanner.run(duration_seconds=None)

    async def run_direct(directory: Path) -> dict[str, Any]:
        # Avoid opening two identical broad CEX subscription sets in the same
        # scheduler tick.  This also gives the local hot stream time to settle.
        await asyncio.sleep(1.5)
        providers = build_cycle_providers(
            tuple(MARKETS),
            base_rpc_url="https://mainnet-preconf.base.org",
            polygon_rpc_url="https://polygon.drpc.org",
            fee_tiers=(100, 500, 3000),
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            raydium_min_request_interval_seconds=RAYDIUM_REQUEST_INTERVAL_SECONDS,
            jupiter_api_key=local_config.jupiter.api_key,
            jupiter_min_request_interval_seconds=local_config.jupiter.minimum_request_interval_seconds,
            omniston_ws_url=OMNISTON_WS_ENDPOINT,
            omniston_quote_selection_window_seconds=0.5,
            omniston_max_price_slippage_bps=50,
            omniston_max_routes=4,
            omniston_allow_risky_routes=False,
        )
        _attach_shared_raydium_budget(providers, budgets, group="direct")
        return await record_continuous_cycle_monitor(
            direct_markets,
            providers,
            notionals=(DIRECT_NOTIONAL_USDT,),
            duration_seconds=None,
            cex_venues=CEX_VENUES,
            cex_taker_fees=DEFAULT_CEX_TAKER_FEES,
            network_cost_floors=DEFAULT_NETWORK_COST_FLOORS,
            max_response_skew_ms=MAX_RESPONSE_SKEW_MS,
            max_dex_cache_age_ms=MAX_DEX_CACHE_AGE_MS,
            output_directory=directory,
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            history_capacity_per_symbol=512,
            stats_flush_seconds=2.0,
            max_persisted_candidate_events=MAX_CANDIDATE_EVENTS_PER_COMPONENT,
            shared_provider_gates=budgets.provider_gates(),
            stdout_candidates=True,
            require_account_verified_fees_for_candidates=False,
        )

    async def run_triangles(directory: Path) -> dict[str, Any]:
        await asyncio.sleep(3.0)
        providers = build_triangle_providers(
            triangle_markets,
            base_rpc_url="https://mainnet-preconf.base.org",
            polygon_rpc_url="https://polygon.drpc.org",
            fee_tiers=(100, 500, 3000),
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            raydium_slippage_bps=50,
            raydium_min_request_interval_seconds=RAYDIUM_REQUEST_INTERVAL_SECONDS,
            stonfi_slippage_tolerance=Decimal("0.005"),
        )
        _attach_shared_raydium_budget(providers, budgets, group="triangle")
        return await record_triangle_cycle_monitor(
            triangle_markets,
            providers,
            reference_notional_usdt=TRIANGLE_REFERENCE_NOTIONAL_USDT,
            duration_seconds=None,
            cex_venues=CEX_VENUES,
            cex_taker_fees=DEFAULT_CEX_TAKER_FEES,
            network_cost_floors=DEFAULT_NETWORK_COST_FLOORS,
            max_response_skew_ms=MAX_RESPONSE_SKEW_MS,
            max_dex_cache_age_ms=MAX_DEX_CACHE_AGE_MS,
            output_directory=directory,
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            history_capacity_per_symbol=512,
            stats_flush_seconds=2.0,
            max_persisted_candidate_events=MAX_CANDIDATE_EVENTS_PER_COMPONENT,
            raydium_429_cooldown_seconds=120.0,
            raydium_429_min_request_interval_seconds=RAYDIUM_REQUEST_INTERVAL_SECONDS,
            shared_provider_gates=budgets.provider_gates(),
            stdout_candidates=True,
            require_account_verified_fees_for_candidates=False,
        )

    async def run_perp_basis(directory: Path) -> dict[str, Any]:
        # Let the existing spot streams settle before adding the independent
        # public Bybit linear stream and the shared DEX quote consumers.
        await asyncio.sleep(4.5)
        instruments = await fetch_bybit_linear_instruments(
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
        )
        active_markets = tuple(
            market
            for market in direct_markets
            if bybit_linear_symbol(market) in instruments
        )
        if not active_markets:
            raise RuntimeError("no DEX market has an active matching Bybit USDT perpetual")
        symbols = tuple(sorted({bybit_linear_symbol(market) for market in active_markets}))
        fee_rates = public_fallback_fee_rates(symbols)
        providers = build_cycle_providers(
            [market.name for market in active_markets],
            base_rpc_url="https://mainnet-preconf.base.org",
            polygon_rpc_url="https://polygon.drpc.org",
            fee_tiers=(100, 500, 3000),
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            raydium_min_request_interval_seconds=RAYDIUM_REQUEST_INTERVAL_SECONDS,
            jupiter_api_key=local_config.jupiter.api_key,
            jupiter_min_request_interval_seconds=local_config.jupiter.minimum_request_interval_seconds,
            omniston_ws_url=OMNISTON_WS_ENDPOINT,
            omniston_quote_selection_window_seconds=0.5,
            omniston_max_price_slippage_bps=50,
            omniston_max_routes=4,
            omniston_allow_risky_routes=False,
        )
        _attach_shared_raydium_budget(providers, budgets, group="perp")
        return await record_perp_dex_monitor(
            active_markets,
            providers,
            instruments,
            fee_rates,
            notionals=(PERP_BASIS_NOTIONAL_USDT,),
            duration_seconds=None,
            output_directory=directory,
            proxy_url=local_config.proxy_url,
            timeout_seconds=local_config.timeout_seconds,
            network_cost_floors=DEFAULT_NETWORK_COST_FLOORS,
            max_response_skew_ms=MAX_RESPONSE_SKEW_MS,
            max_dex_cache_age_ms=MAX_DEX_CACHE_AGE_MS,
            history_capacity_per_symbol=512,
            stats_flush_seconds=2.0,
            max_persisted_candidate_events=MAX_CANDIDATE_EVENTS_PER_COMPONENT,
            shared_provider_gates=budgets.provider_gates(),
            stdout_candidates=True,
            require_account_verified_fees_for_candidates=False,
        )

    # The historic route monitors below are intentionally not started here.
    # They coupled subscription ownership to one particular strategy and
    # wrote candidate files.  Their underlying public quote providers now run
    # as neutral ``dexquote:*`` sources inside ``market_data`` above.
    factories = {"market_data": run_market_data}

    async def write_status() -> None:
        while not stop_event.is_set():
            now = datetime.now(UTC).isoformat()
            atomic_json(
                status_path,
                {
                    "schema_version": 1,
                    "status": "running",
                    "started_at": started_at,
                    "updated_at": now,
                    "duration_wall_seconds": round(
                        (datetime.now(UTC) - datetime.fromisoformat(started_at)).total_seconds(),
                        3,
                    ),
                    "coverage": coverage,
                    "runtime_components": runtime_components,
                    "rate_budgets": rate_budgets,
                    "retention": retention,
                    "components": {name: state.snapshot() for name, state in states.items()},
                },
            )
            atomic_json(
                latest_path,
                {
                    "run_id": run_id,
                    "status": "running",
                    "run_directory": str(run_root),
                    "status_file": str(status_path),
                    "updated_at": now,
                },
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=local_config.status_flush_seconds,
                )
            except TimeoutError:
                pass

    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop_event.set)

    component_tasks = [
        asyncio.create_task(
            _supervise_component(
                states[name],
                run_root=run_root,
                factory=factory,
                stop_event=stop_event,
            ),
            name=name,
        )
        for name, factory in factories.items()
    ]
    status_task = asyncio.create_task(write_status(), name="status_writer")
    started_monotonic = time.monotonic()
    try:
        if duration_seconds is None:
            await stop_event.wait()
        else:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=duration_seconds)
            except TimeoutError:
                stop_event.set()
    finally:
        stop_event.set()
        for task in component_tasks:
            task.cancel()
        await asyncio.gather(*component_tasks, return_exceptions=True)
        status_task.cancel()
        await asyncio.gather(status_task, return_exceptions=True)

    stopped_at = datetime.now(UTC).isoformat()
    final_status = "completed" if duration_seconds is not None else "stopped"
    final_components = {name: state.snapshot() for name, state in states.items()}
    atomic_json(
        status_path,
        {
            "schema_version": 1,
            "status": final_status,
            "started_at": started_at,
            "updated_at": stopped_at,
            "duration_wall_seconds": round(time.monotonic() - started_monotonic, 3),
            "stop_reason": stop_reason,
            "coverage": coverage,
            "runtime_components": runtime_components,
            "rate_budgets": rate_budgets,
            "retention": retention,
            "components": final_components,
        },
    )
    manifest.update(status=final_status, stopped_at=stopped_at)
    atomic_json(manifest_path, manifest)
    atomic_json(
        latest_path,
        {
            "run_id": run_id,
            "status": final_status,
            "run_directory": str(run_root),
            "status_file": str(status_path),
            "updated_at": stopped_at,
        },
    )
    return run_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--duration-seconds",
        type=float,
        help="optional smoke-test duration; omit it for the normal continuous run",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    try:
        output = asyncio.run(run_scanner(duration_seconds=args.duration_seconds))
    except (OSError, ValueError) as exc:
        raise SystemExit(f"scanner failed to start: {exc}") from exc
    print(f"scanner output: {output}", flush=True)


if __name__ == "__main__":
    main()
