"""One-command, read-only Solana/CEX state collector for the unified scanner.

This is the first migration layer away from separately launched polling
monitors.  It subscribes directly to selected Raydium CLMM pool accounts and
to existing public CEX order-book streams.  All market state remains in RAM;
``status.json`` contains only health and compact latest-state diagnostics.

It is intentionally not a trading client and does not accept wallet keys,
exchange credentials or transaction payloads.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import time
import tomllib
import math
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_data_lab.cex_book_streams import PublicBookStream
from market_data_lab.cex_book_streams import build_public_book_stream
from market_data_lab.cex_book_streams import stream_health
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.jupiter_gated_verifier import JupiterGatedVerifier
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.raydium_clmm_prefilter import ClmmPoolState
from market_data_lab.raydium_clmm_prefilter import SOLANA_HTTP_ENDPOINT
from market_data_lab.raydium_clmm_prefilter import SOLANA_WS_ENDPOINT
from market_data_lab.raydium_clmm_prefilter import SolanaClmmPoolStream
from market_data_lab.raydium_clmm_prefilter import fetch_clmm_pool_states
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import ScannerSource
from market_data_lab.solana_quote_worker import QuoteWorkerPool
from market_data_lab.solana_quote_worker import RaydiumLocalQuoteWorker
from market_data_lab.solana_quote_worker import RaydiumStandardQuoteWorkerPool
from market_data_lab.solana_route_evaluator import LocalRouteEvaluatorConfig
from market_data_lab.solana_route_evaluator import LocalSpotRoute
from market_data_lab.solana_route_evaluator import SolanaRouteEvaluator


Publish = Callable[[MarketEvent], Awaitable[None]]


@dataclass(frozen=True)
class RaydiumClmmPoolConfig:
    """One preselected Raydium CLMM pool to subscribe to."""

    pool_id: str
    label: str


@dataclass(frozen=True)
class RaydiumStandardPoolConfig:
    """One validated Raydium CPMM or AMM-v4 pool."""

    pool_id: str
    label: str
    protocol: str

    def __post_init__(self) -> None:
        if self.protocol not in {"raydium_cpmm", "raydium_amm_v4"}:
            raise ValueError("Raydium standard protocol must be CPMM or AMM v4")


@dataclass(frozen=True)
class MeteoraDlmmPoolConfig:
    """One preselected Meteora DLMM pool for local subscription and quotes."""

    pool_id: str
    label: str


@dataclass(frozen=True)
class OrcaWhirlpoolPoolConfig:
    """One preselected Orca Whirlpool for local subscription and quotes."""

    pool_id: str
    label: str


@dataclass(frozen=True)
class CexStreamConfig:
    """One public CEX stream; a single connection can carry many symbols."""

    venue: str
    symbols: tuple[str, ...]
    category: str = "spot"


@dataclass(frozen=True)
class JupiterConfig:
    """Credentials and pacing for the later *gated* Jupiter verification layer.

    This deliberately is not a hot feed configuration.  Direct pool state is
    the hot path; Jupiter is used later only after an on-chain/CEX prefilter
    has identified a candidate that merits an aggregator cross-check.
    """

    enabled: bool = True
    api_key: str | None = field(default=None, repr=False)
    minimum_request_interval_seconds: float = 2.05

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "api_key_configured": self.api_key is not None,
            "minimum_request_interval_seconds": self.minimum_request_interval_seconds,
            "hot_price_source": False,
            "intended_use": "gated aggregator route verification only",
        }


@dataclass(frozen=True)
class RaydiumLocalQuoteWorkerConfig:
    """Optional local SDK worker; disabled until explicitly enabled in TOML."""

    enabled: bool = False
    tick_cache_max_age_ms: int = 300_000
    state_snapshot_refresh_interval_ms: int = 15_000
    rpc_http_min_request_interval_ms: int = 200

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "tick_cache_max_age_ms": self.tick_cache_max_age_ms,
            "state_snapshot_refresh_interval_ms": self.state_snapshot_refresh_interval_ms,
            "rpc_http_min_request_interval_ms": self.rpc_http_min_request_interval_ms,
            "mode": "local_typescript_sdk_exact_input_quote",
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


@dataclass(frozen=True)
class EvmRpcConfig:
    """Read-only RPC endpoints for exact-quote sources on EVM networks.

    A public Base endpoint is only a fallback.  The unified collector will
    surface and circuit-break it if its shared quota rejects quote calls; a
    dedicated URL can be provided locally without ever being persisted.
    """

    base_rpc_http_url: str
    polygon_rpc_http_url: str

    @staticmethod
    def _origin(url: str) -> str:
        from urllib.parse import urlsplit

        parsed = urlsplit(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "base_rpc_origin": self._origin(self.base_rpc_http_url),
            "polygon_rpc_origin": self._origin(self.polygon_rpc_http_url),
            "secret_values_persisted": False,
        }


@dataclass(frozen=True)
class LocalRouteEvaluatorSettings:
    """Declarative routes for the managed exact-quote/CEX-depth layer.

    It is disabled by default because a route needs an explicit token/CEX
    equivalence mapping.  Enabling it never changes the source protocol from
    read-only market data to execution.
    """

    enabled: bool = False
    routes: tuple[LocalSpotRoute, ...] = ()
    minimum_quote_interval_ms: int = 100
    maximum_book_age_ms: int = 1_000
    maximum_pool_state_age_ms: int = 60_000
    maximum_timing_skew_ms: int = 1_000
    candidate_event_limit: int = 5_000
    minimum_candidate_edge_bps: Decimal = Decimal("0")
    candidate_improvement_bps: Decimal = Decimal("1")
    fee_audit_file: Path | None = None

    def to_evaluator_config(self) -> LocalRouteEvaluatorConfig:
        return LocalRouteEvaluatorConfig(
            routes=self.routes,
            minimum_quote_interval_ms=self.minimum_quote_interval_ms,
            maximum_book_age_ms=self.maximum_book_age_ms,
            maximum_pool_state_age_ms=self.maximum_pool_state_age_ms,
            maximum_timing_skew_ms=self.maximum_timing_skew_ms,
            candidate_event_limit=self.candidate_event_limit,
            minimum_candidate_edge_bps=self.minimum_candidate_edge_bps,
            candidate_improvement_bps=self.candidate_improvement_bps,
        )

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "route_count": len(self.routes),
            "fee_audit_file_configured": self.fee_audit_file is not None,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


@dataclass(frozen=True)
class AmmSimulationConfig:
    """Read-only shadow post-trade AMM simulation profile (disabled by default).

    The scanner never enables this feature by itself; an explicit
    ``[amm_simulation]`` section with ``enabled = true`` is required.  The
    profile only describes simulation limits and the protocol allow-list — it
    adds no collectors, wallets, transactions or execution path.
    """

    enabled: bool = False
    mode: str = "shadow"
    allowed_protocols: tuple[str, ...] = ()
    max_path_legs: int = 4
    max_state_bytes: int = 1_048_576
    max_computation_steps: int = 1_000_000
    max_inflight_requests: int = 32
    max_pending_paths: int = 256
    max_snapshot_dependencies: int = 64
    max_evidence_bundle_bytes: int = 8_388_608
    max_queue_bytes: int = 16_777_216

    def __post_init__(self) -> None:
        if self.mode not in {"shadow", "model_candidates"}:
            raise ValueError("amm_simulation.mode must be 'shadow' or 'model_candidates'")
        supported = {
            "raydium_cpmm",
            "raydium_clmm",
            "raydium_amm_v4",
            "meteora_dlmm",
            "orca_whirlpool",
            "synthetic_cpmm_v1",
        }
        unknown = [protocol for protocol in self.allowed_protocols if protocol not in supported]
        if unknown:
            raise ValueError(f"amm_simulation allowed_protocols contains unsupported values: {unknown}")

    def limits(self) -> dict[str, int]:
        return {
            "max_path_legs": self.max_path_legs,
            "max_state_bytes": self.max_state_bytes,
            "max_computation_steps": self.max_computation_steps,
            "max_inflight_requests": self.max_inflight_requests,
            "max_pending_paths": self.max_pending_paths,
            "max_snapshot_dependencies": self.max_snapshot_dependencies,
            "max_evidence_bundle_bytes": self.max_evidence_bundle_bytes,
            "max_queue_bytes": self.max_queue_bytes,
        }

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "mode": self.mode,
            "allowed_protocols": list(self.allowed_protocols),
            "limits": self.limits(),
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }


@dataclass(frozen=True)
class SolanaScannerConfig:
    """Validated TOML configuration for the first unified scanner profile."""

    rpc_http_url: str
    rpc_ws_url: str
    timeout_seconds: float
    proxy_url: str | None
    pools: tuple[RaydiumClmmPoolConfig, ...]
    raydium_standard_pools: tuple[RaydiumStandardPoolConfig, ...]
    meteora_pools: tuple[MeteoraDlmmPoolConfig, ...]
    orca_pools: tuple[OrcaWhirlpoolPoolConfig, ...]
    cex_streams: tuple[CexStreamConfig, ...]
    jupiter: JupiterConfig
    evm: EvmRpcConfig
    raydium_local_quote_worker: RaydiumLocalQuoteWorkerConfig
    local_route_evaluator: LocalRouteEvaluatorSettings
    amm_simulation: AmmSimulationConfig
    retention_seconds: float
    max_events_per_key: int
    event_bus_capacity: int
    status_flush_seconds: float
    sequential_amm_pool: SequentialAmmPoolConfig | None = None


@dataclass(frozen=True)
class SequentialAmmPoolConfig:
    """CPMM pool and asset mapping for the DEX↔perp sequential vertical slice."""

    pool_id: str
    stable_asset_id: str
    base_asset_id: str
    stable_decimals: int
    perp_symbol: str

    def __post_init__(self) -> None:
        if self.stable_asset_id == self.base_asset_id:
            raise ValueError("sequential AMM stable and base assets must differ")
        stable_decimals = _solana_asset_id_decimals(
            self.stable_asset_id,
            "stable_asset_id",
        )
        _solana_asset_id_decimals(self.base_asset_id, "base_asset_id")
        if stable_decimals != self.stable_decimals:
            raise ValueError(
                "sequential AMM stable_asset_id decimals must match stable_decimals",
            )


def _solana_asset_id_decimals(value: str, field: str) -> int:
    parts = value.split(":")
    if (
        len(parts) != 4
        or parts[0] != "solana"
        or parts[1] != "mainnet"
        or not parts[2]
        or not parts[3].isdigit()
        or (len(parts[3]) > 1 and parts[3].startswith("0"))
    ):
        raise ValueError(
            f"sequential AMM {field} must be solana:mainnet:<mint>:<decimals>",
        )
    decimals = int(parts[3])
    if decimals > 255:
        raise ValueError(f"sequential AMM {field} decimals are out of range")
    return decimals


def _parse_sequential_amm_pool(payload: Mapping[str, Any]) -> SequentialAmmPoolConfig | None:
    """Parse the optional sequential AMM pool configuration."""
    raw = payload.get("sequential_amm_pool", []) if isinstance(payload, Mapping) else []
    if not isinstance(raw, list) or not raw:
        return None
    if len(raw) != 1:
        raise ValueError("[[local_route_evaluator.sequential_amm_pool]] must contain at most one entry")
    item = raw[0]
    if not isinstance(item, dict):
        raise ValueError("sequential_amm_pool must be a TOML table")
    return SequentialAmmPoolConfig(
        pool_id=_required_string(item, "pool_id"),
        stable_asset_id=_required_string(item, "stable_asset_id"),
        base_asset_id=_required_string(item, "base_asset_id"),
        stable_decimals=_positive_int(item, "stable_decimals", 6),
        perp_symbol=_required_string(item, "perp_symbol").upper(),
    )


def _required_string(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"configuration field {key!r} must be a non-empty string")
    return value.strip()


def _positive_float(payload: Mapping[str, Any], key: str, default: float) -> float:
    value = payload.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"configuration field {key!r} must be a positive number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"configuration field {key!r} must be a positive number") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"configuration field {key!r} must be finite (not NaN or Infinity)")
    if parsed <= 0:
        raise ValueError(f"configuration field {key!r} must be positive")
    return parsed


def _positive_int(payload: Mapping[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"configuration field {key!r} must be a positive integer")
    try:
        if isinstance(value, float):
            if not value.is_integer():
                raise ValueError(f"configuration field {key!r} must be an integer, not a fractional float")
            parsed = int(value)
        elif isinstance(value, Decimal):
            if not value.is_finite() or not value == value.to_integral_value():
                raise ValueError(f"configuration field {key!r} must be a finite integer")
            parsed = int(value)
        else:
            parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"configuration field {key!r} must be a positive integer") from exc
    if parsed <= 0:
        raise ValueError(f"configuration field {key!r} must be positive")
    return parsed


def _finite_decimal(payload: Mapping[str, Any], key: str, default: Decimal) -> Decimal:
    value = payload.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"configuration field {key!r} must be a finite decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"configuration field {key!r} must be a finite decimal") from exc
    if not parsed.is_finite():
        raise ValueError(f"configuration field {key!r} must be a finite decimal")
    return parsed


def _non_negative_int(payload: Mapping[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"configuration field {key!r} must be a non-negative integer")
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError(f"configuration field {key!r} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"configuration field {key!r} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"configuration field {key!r} must be non-negative")
    return parsed


def _endpoint_from_config_or_environment(
    payload: Mapping[str, Any],
    *,
    literal_key: str,
    environment_key: str,
    default: str,
) -> str:
    """Resolve an endpoint without ever placing a provider credential in git.

    Managed Solana endpoints commonly embed their API key in the URL.  A
    profile can therefore name an environment variable instead of carrying the
    URL itself.  The resolved endpoint is used in-process only; manifests keep
    only its scheme and host.
    """

    configured_environment = payload.get(environment_key)
    if configured_environment is not None:
        if not isinstance(configured_environment, str) or not configured_environment.strip():
            raise ValueError(f"configuration field {environment_key!r} must name an environment variable")
        resolved = os.environ.get(configured_environment.strip())
        if not resolved:
            raise ValueError(
                f"environment variable {configured_environment.strip()!r} named by {environment_key!r} is empty",
            )
        return resolved
    value = payload.get(literal_key, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"configuration field {literal_key!r} must be a non-empty URL")
    return value.strip()


def _optional_secret_from_config_or_environment(
    payload: Mapping[str, Any],
    *,
    literal_key: str,
    environment_key: str,
) -> str | None:
    """Resolve an optional local secret without returning it in diagnostics."""

    configured_environment = payload.get(environment_key)
    if configured_environment is not None:
        if not isinstance(configured_environment, str) or not configured_environment.strip():
            raise ValueError(f"configuration field {environment_key!r} must name an environment variable")
        resolved = os.environ.get(configured_environment.strip())
        if not resolved:
            raise ValueError(
                f"environment variable {configured_environment.strip()!r} named by {environment_key!r} is empty",
            )
        return resolved
    value = payload.get(literal_key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"configuration field {literal_key!r} must be a non-empty string")
    return value.strip()


def _parse_local_route_evaluator(
    payload: Mapping[str, Any],
    *,
    config_directory: Path,
    pools: Sequence[
        RaydiumClmmPoolConfig
        | RaydiumStandardPoolConfig
        | MeteoraDlmmPoolConfig
        | OrcaWhirlpoolPoolConfig
    ],
    cex_streams: Sequence[CexStreamConfig],
) -> LocalRouteEvaluatorSettings:
    """Parse explicit one-pool routes without inventing CEX equivalence.

    A route has to name both CEX legs (or explicitly mark the bridge as the
    settlement token).  This protects us from the tempting but wrong shortcut
    of treating USDC/USDT/SOL wrappers as interchangeable at a fixed price.
    """

    enabled = payload.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("local_route_evaluator.enabled must be boolean")
    raw_routes = payload.get("route", [])
    if not isinstance(raw_routes, list):
        raise ValueError("[[local_route_evaluator.route]] must be a list of TOML tables")
    configured_pools = {}
    for pool in pools:
        if isinstance(pool, MeteoraDlmmPoolConfig):
            protocol = "meteora_dlmm"
        elif isinstance(pool, OrcaWhirlpoolPoolConfig):
            protocol = "orca_whirlpool"
        elif isinstance(pool, RaydiumStandardPoolConfig):
            protocol = pool.protocol
        else:
            protocol = "raydium_clmm"
        configured_pools[pool.pool_id] = protocol
    configured_symbols = {
        (stream.venue, stream.category, symbol)
        for stream in cex_streams
        for symbol in stream.symbols
    }
    routes: list[LocalSpotRoute] = []
    for item in raw_routes:
        if not isinstance(item, dict):
            raise ValueError("each local route must be a TOML table")
        bridge_is_settlement = item.get("bridge_is_settlement", False)
        if not isinstance(bridge_is_settlement, bool):
            raise ValueError("local route bridge_is_settlement must be boolean")
        bridge_cex_symbol_raw = item.get("bridge_cex_symbol")
        if bridge_cex_symbol_raw is not None and (
            not isinstance(bridge_cex_symbol_raw, str) or not bridge_cex_symbol_raw.strip()
        ):
            raise ValueError("local route bridge_cex_symbol must be a non-empty string when supplied")
        shared_fee = _finite_decimal(item, "cex_taker_fee_bps", Decimal("10"))
        route = LocalSpotRoute(
            route_id=_required_string(item, "route_id"),
            pool_id=_required_string(item, "pool_id"),
            base_mint=_required_string(item, "base_mint"),
            bridge_mint=_required_string(item, "bridge_mint"),
            base_decimals=_non_negative_int(item, "base_decimals", 0),
            bridge_decimals=_non_negative_int(item, "bridge_decimals", 0),
            base_symbol=_required_string(item, "base_symbol").upper(),
            bridge_symbol=_required_string(item, "bridge_symbol").upper(),
            settlement_symbol=_required_string(item, "settlement_symbol").upper(),
            cex_venue=_required_string(item, "cex_venue").upper(),
            base_cex_symbol=_required_string(item, "base_cex_symbol").upper(),
            bridge_cex_symbol=(
                bridge_cex_symbol_raw.strip().upper()
                if isinstance(bridge_cex_symbol_raw, str)
                else None
            ),
            bridge_is_settlement=bridge_is_settlement,
            notional_settlement=_finite_decimal(item, "notional_settlement", Decimal("0")),
            base_buy_taker_fee_bps=_finite_decimal(item, "base_buy_taker_fee_bps", shared_fee),
            base_sell_taker_fee_bps=_finite_decimal(item, "base_sell_taker_fee_bps", shared_fee),
            bridge_buy_taker_fee_bps=_finite_decimal(item, "bridge_buy_taker_fee_bps", shared_fee),
            bridge_sell_taker_fee_bps=_finite_decimal(item, "bridge_sell_taker_fee_bps", shared_fee),
            network_cost_floor_settlement=_finite_decimal(
                item,
                "network_cost_floor_settlement",
                Decimal("0.01"),
            ),
            asset_equivalence=_required_string(item, "asset_equivalence"),
            pool_protocol=str(item.get("pool_protocol", "raydium_clmm")).strip().lower(),
        )
        if route.pool_id not in configured_pools:
            raise ValueError(
                f"local route {route.route_id!r} refers to an unconfigured local quote pool",
            )
        if configured_pools[route.pool_id] != route.pool_protocol:
            raise ValueError(
                f"local route {route.route_id!r} pool_protocol does not match its configured pool",
            )
        base_symbol_key = (route.cex_venue, "spot", route.base_cex_symbol)
        if base_symbol_key not in configured_symbols:
            raise ValueError(
                f"local route {route.route_id!r} base_cex_symbol is not configured in a spot [[cex_stream]]",
            )
        if route.bridge_cex_symbol is not None:
            bridge_symbol_key = (route.cex_venue, "spot", route.bridge_cex_symbol)
            if bridge_symbol_key not in configured_symbols:
                raise ValueError(
                    f"local route {route.route_id!r} bridge_cex_symbol is not configured in a spot [[cex_stream]]",
                )
        routes.append(route)
    if enabled and not routes:
        raise ValueError("local_route_evaluator.enabled requires at least one [[local_route_evaluator.route]]")
    audit_file_raw = payload.get("fee_audit_file")
    if audit_file_raw is not None and (
        not isinstance(audit_file_raw, str) or not audit_file_raw.strip()
    ):
        raise ValueError("local_route_evaluator.fee_audit_file must be a non-empty path when supplied")
    audit_file = (
        (config_directory / audit_file_raw).resolve()
        if isinstance(audit_file_raw, str)
        else None
    )
    return LocalRouteEvaluatorSettings(
        enabled=enabled,
        routes=tuple(routes),
        minimum_quote_interval_ms=_positive_int(payload, "minimum_quote_interval_ms", 100),
        maximum_book_age_ms=_positive_int(payload, "maximum_book_age_ms", 1_000),
        maximum_pool_state_age_ms=_positive_int(payload, "maximum_pool_state_age_ms", 60_000),
        maximum_timing_skew_ms=_positive_int(payload, "maximum_timing_skew_ms", 1_000),
        candidate_event_limit=_positive_int(payload, "candidate_event_limit", 5_000),
        minimum_candidate_edge_bps=_finite_decimal(payload, "minimum_candidate_edge_bps", Decimal("0")),
        candidate_improvement_bps=_finite_decimal(payload, "candidate_improvement_bps", Decimal("1")),
        fee_audit_file=audit_file,
    )


def _parse_amm_simulation(payload: Mapping[str, Any]) -> AmmSimulationConfig:
    """Parse the optional ``[amm_simulation]`` shadow profile.

    The feature is off unless ``enabled = true`` is written explicitly.  The
    section never carries endpoints, keys or acquisition budgets; it only
    describes the simulation domain and resource limits.
    """

    enabled = payload.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ValueError("amm_simulation.enabled must be boolean")
    mode = payload.get("mode", "shadow")
    if not isinstance(mode, str) or mode.strip() != mode:
        raise ValueError("amm_simulation.mode must be a non-empty string")
    mode = mode.strip()
    raw_protocols = payload.get("allowed_protocols", [])
    if not isinstance(raw_protocols, list) or not all(
        isinstance(protocol, str) and protocol.strip() for protocol in raw_protocols
    ):
        raise ValueError("amm_simulation.allowed_protocols must be a list of non-empty strings")
    allowed_protocols = tuple(protocol.strip() for protocol in raw_protocols)
    return AmmSimulationConfig(
        enabled=enabled,
        mode=mode,
        allowed_protocols=allowed_protocols,
        max_path_legs=_positive_int(payload, "max_path_legs", 4),
        max_state_bytes=_positive_int(payload, "max_state_bytes", 1_048_576),
        max_computation_steps=_positive_int(payload, "max_computation_steps", 1_000_000),
        max_inflight_requests=_positive_int(payload, "max_inflight_requests", 32),
        max_pending_paths=_positive_int(payload, "max_pending_paths", 256),
        max_snapshot_dependencies=_positive_int(payload, "max_snapshot_dependencies", 64),
        max_evidence_bundle_bytes=_positive_int(payload, "max_evidence_bundle_bytes", 8_388_608),
        max_queue_bytes=_positive_int(payload, "max_queue_bytes", 16_777_216),
    )


def load_solana_scanner_config(path: Path) -> SolanaScannerConfig:
    """Load a small declarative profile without accepting secrets in TOML."""

    with path.open("rb") as config_file:
        payload = tomllib.load(config_file)
    if not isinstance(payload, dict):
        raise ValueError("scanner TOML root must be a table")
    scanner = payload.get("scanner", {})
    solana = payload.get("solana", {})
    jupiter = payload.get("jupiter", {})
    evm = payload.get("evm", {})
    local_quote_worker = payload.get("raydium_local_quote_worker", {})
    local_route_evaluator = payload.get("local_route_evaluator", {})
    amm_simulation = payload.get("amm_simulation", {})
    cex = payload.get("cex_stream", [])
    if (
        not isinstance(scanner, dict)
        or not isinstance(solana, dict)
        or not isinstance(jupiter, dict)
        or not isinstance(evm, dict)
        or not isinstance(local_quote_worker, dict)
        or not isinstance(local_route_evaluator, dict)
        or not isinstance(amm_simulation, dict)
    ):
        raise ValueError(
            "[scanner], [solana], [jupiter], [evm], [raydium_local_quote_worker], [local_route_evaluator], and [amm_simulation] must be TOML tables",
        )
    raw_pools = solana.get("raydium_clmm_pool", [])
    if not isinstance(raw_pools, list) or not raw_pools:
        raise ValueError("[[solana.raydium_clmm_pool]] must contain at least one pool")
    pools: list[RaydiumClmmPoolConfig] = []
    for item in raw_pools:
        if not isinstance(item, dict):
            raise ValueError("each Raydium pool must be a TOML table")
        pools.append(
            RaydiumClmmPoolConfig(
                pool_id=_required_string(item, "pool_id"),
                label=_required_string(item, "label"),
            ),
        )
    pool_ids = [pool.pool_id for pool in pools]
    if len(set(pool_ids)) != len(pool_ids):
        raise ValueError("Raydium pool_id values must be unique")
    raw_standard_pools = solana.get("raydium_standard_pool", [])
    if not isinstance(raw_standard_pools, list):
        raise ValueError("[[solana.raydium_standard_pool]] must be a list of TOML tables")
    raydium_standard_pools: list[RaydiumStandardPoolConfig] = []
    for item in raw_standard_pools:
        if not isinstance(item, dict):
            raise ValueError("each Raydium standard pool must be a TOML table")
        raydium_standard_pools.append(
            RaydiumStandardPoolConfig(
                pool_id=_required_string(item, "pool_id"),
                label=_required_string(item, "label"),
                protocol=_required_string(item, "protocol").lower(),
            ),
        )
    raw_meteora_pools = solana.get("meteora_dlmm_pool", [])
    if not isinstance(raw_meteora_pools, list):
        raise ValueError("[[solana.meteora_dlmm_pool]] must be a list of TOML tables")
    meteora_pools: list[MeteoraDlmmPoolConfig] = []
    for item in raw_meteora_pools:
        if not isinstance(item, dict):
            raise ValueError("each Meteora pool must be a TOML table")
        meteora_pools.append(
            MeteoraDlmmPoolConfig(
                pool_id=_required_string(item, "pool_id"),
                label=_required_string(item, "label"),
            ),
        )
    raw_orca_pools = solana.get("orca_whirlpool_pool", [])
    if not isinstance(raw_orca_pools, list):
        raise ValueError("[[solana.orca_whirlpool_pool]] must be a list of TOML tables")
    orca_pools: list[OrcaWhirlpoolPoolConfig] = []
    for item in raw_orca_pools:
        if not isinstance(item, dict):
            raise ValueError("each Orca pool must be a TOML table")
        orca_pools.append(
            OrcaWhirlpoolPoolConfig(
                pool_id=_required_string(item, "pool_id"),
                label=_required_string(item, "label"),
            ),
        )
    all_pool_ids = [
        *pool_ids,
        *(pool.pool_id for pool in raydium_standard_pools),
        *(pool.pool_id for pool in meteora_pools),
        *(pool.pool_id for pool in orca_pools),
    ]
    if len(set(all_pool_ids)) != len(all_pool_ids):
        raise ValueError("local quote pool_id values must be unique across protocols")
    raw_cex = cex if isinstance(cex, list) else None
    if raw_cex is None:
        raise ValueError("[[cex_stream]] must be a list of TOML tables")
    cex_streams: list[CexStreamConfig] = []
    for item in raw_cex:
        if not isinstance(item, dict):
            raise ValueError("each CEX stream must be a TOML table")
        symbols_value = item.get("symbols")
        if not isinstance(symbols_value, list) or not symbols_value:
            raise ValueError("CEX stream symbols must be a non-empty list")
        symbols = tuple(
            _required_string({"symbol": symbol}, "symbol").upper()
            for symbol in symbols_value
        )
        if len(set(symbols)) != len(symbols):
            raise ValueError("CEX stream symbols must be unique per venue")
        cex_streams.append(
            CexStreamConfig(
                venue=_required_string(item, "venue").upper(),
                symbols=symbols,
                category=str(item.get("category", "spot")).lower(),
            ),
        )
    source_names = [f"cex:{item.venue}:{item.category}" for item in cex_streams]
    if len(set(source_names)) != len(source_names):
        raise ValueError("each CEX venue/category may appear only once")
    evaluator_settings = _parse_local_route_evaluator(
        local_route_evaluator,
        config_directory=path.resolve().parent,
        pools=(*pools, *raydium_standard_pools, *meteora_pools, *orca_pools),
        cex_streams=cex_streams,
    )
    proxy_url = solana.get("proxy_url")
    if proxy_url is not None and (not isinstance(proxy_url, str) or not proxy_url.strip()):
        raise ValueError("solana.proxy_url must be a non-empty string when supplied")
    root_legacy_jupiter_key = payload.get("jupiter_api_key")
    solana_legacy_jupiter_key = solana.get("jupiter_api_key")
    if root_legacy_jupiter_key is not None and solana_legacy_jupiter_key is not None:
        raise ValueError("jupiter_api_key is present both at root and under [solana]")
    legacy_jupiter_key = root_legacy_jupiter_key or solana_legacy_jupiter_key
    if legacy_jupiter_key is not None and (
        "api_key" in jupiter or "api_key_env" in jupiter
    ):
        raise ValueError("use only one of legacy jupiter_api_key or [jupiter] api_key/api_key_env")
    if legacy_jupiter_key is not None:
        jupiter_api_key = _optional_secret_from_config_or_environment(
            {"api_key": legacy_jupiter_key},
            literal_key="api_key",
            environment_key="api_key_env",
        )
    else:
        jupiter_api_key = _optional_secret_from_config_or_environment(
            jupiter,
            literal_key="api_key",
            environment_key="api_key_env",
        )
    jupiter_enabled = jupiter.get("enabled", True)
    if not isinstance(jupiter_enabled, bool):
        raise ValueError("configuration field 'jupiter.enabled' must be boolean")
    local_quote_worker_enabled = local_quote_worker.get("enabled", False)
    if not isinstance(local_quote_worker_enabled, bool):
        raise ValueError("configuration field 'raydium_local_quote_worker.enabled' must be boolean")
    if local_quote_worker_enabled and proxy_url is not None:
        raise ValueError("local Raydium TypeScript quote worker currently requires direct Solana RPC, not proxy_url")
    jupiter_interval_default = 1.05 if jupiter_api_key is not None else 2.05
    return SolanaScannerConfig(
        rpc_http_url=_endpoint_from_config_or_environment(
            solana,
            literal_key="rpc_http_url",
            environment_key="rpc_http_url_env",
            default=SOLANA_HTTP_ENDPOINT,
        ),
        rpc_ws_url=_endpoint_from_config_or_environment(
            solana,
            literal_key="rpc_ws_url",
            environment_key="rpc_ws_url_env",
            default=SOLANA_WS_ENDPOINT,
        ),
        timeout_seconds=_positive_float(solana, "timeout_seconds", 10.0),
        proxy_url=proxy_url.strip() if isinstance(proxy_url, str) else None,
        pools=tuple(pools),
        raydium_standard_pools=tuple(raydium_standard_pools),
        meteora_pools=tuple(meteora_pools),
        orca_pools=tuple(orca_pools),
        cex_streams=tuple(cex_streams),
        jupiter=JupiterConfig(
            enabled=jupiter_enabled,
            api_key=jupiter_api_key,
            minimum_request_interval_seconds=_positive_float(
                jupiter,
                "minimum_request_interval_seconds",
                jupiter_interval_default,
            ),
        ),
        evm=EvmRpcConfig(
            base_rpc_http_url=_endpoint_from_config_or_environment(
                evm,
                literal_key="base_rpc_http_url",
                environment_key="base_rpc_http_url_env",
                default="https://mainnet.base.org",
            ),
            polygon_rpc_http_url=_endpoint_from_config_or_environment(
                evm,
                literal_key="polygon_rpc_http_url",
                environment_key="polygon_rpc_http_url_env",
                default="https://polygon.drpc.org",
            ),
        ),
        raydium_local_quote_worker=RaydiumLocalQuoteWorkerConfig(
            enabled=local_quote_worker_enabled,
            tick_cache_max_age_ms=_positive_int(
                local_quote_worker,
                "tick_cache_max_age_ms",
                300_000,
            ),
            state_snapshot_refresh_interval_ms=_positive_int(
                local_quote_worker,
                "state_snapshot_refresh_interval_ms",
                15_000,
            ),
            rpc_http_min_request_interval_ms=_positive_int(
                local_quote_worker,
                "rpc_http_min_request_interval_ms",
                200,
            ),
        ),
    local_route_evaluator=evaluator_settings,
    amm_simulation=_parse_amm_simulation(amm_simulation),
    sequential_amm_pool=_parse_sequential_amm_pool(local_route_evaluator),
    retention_seconds=_positive_float(scanner, "retention_seconds", 180.0),
        max_events_per_key=_positive_int(scanner, "max_events_per_key", 4_096),
        event_bus_capacity=_positive_int(scanner, "event_bus_capacity", 8_192),
        status_flush_seconds=_positive_float(scanner, "status_flush_seconds", 2.0),
    )


def _pool_summary(state: ClmmPoolState, *, label: str) -> dict[str, Any]:
    return {
        "label": label,
        "pool_id": state.pool_id,
        "token_0_mint": state.token_0_mint,
        "token_1_mint": state.token_1_mint,
        "token_0_decimals": state.token_0_decimals,
        "token_1_decimals": state.token_1_decimals,
        "sqrt_price_x64": str(state.sqrt_price_x64),
        "tick_current": state.tick_current,
        "slot": state.slot,
        "upstream_source": state.source,
    }


@dataclass
class RaydiumClmmStateSource:
    """Stream selected CLMM pool states and reconnect through the supervisor."""

    pools: Sequence[RaydiumClmmPoolConfig]
    http_url: str
    ws_url: str
    timeout_seconds: float
    proxy_url: str | None
    name: str = "solana:raydium-clmm"

    def describe(self) -> Mapping[str, Any]:
        return {
            "source": self.name,
            "chain": "solana",
            "protocol": "Raydium CLMM",
            "mode": "direct_account_subscribe",
            "pools": [pool.pool_id for pool in self.pools],
            "http_origin": self.http_url.split("/", 3)[:3],
            "ws_origin": self.ws_url.split("/", 3)[:3],
            "credentials_required": False,
            "transactions_submitted": False,
        }

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        pool_ids = tuple(pool.pool_id for pool in self.pools)
        labels = {pool.pool_id: pool.label for pool in self.pools}
        stream = SolanaClmmPoolStream(
            pool_ids,
            endpoint=self.ws_url,
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )
        last_slot: dict[str, int] = {}

        async def emit(state: ClmmPoolState) -> None:
            # A subscription can receive an update while getMultipleAccounts is
            # in flight.  Do not replace a newer slot with that initial read.
            if state.slot is not None:
                previous = last_slot.get(state.pool_id)
                if previous is not None and state.slot < previous:
                    return
                last_slot[state.pool_id] = state.slot
            await publish(
                MarketEvent(
                    source=self.name,
                    key=f"solana:raydium-clmm:{state.pool_id}",
                    kind="pool_state",
                    value=state,
                    summary=_pool_summary(state, label=labels[state.pool_id]),
                    received_realtime_ns=state.received_realtime_ns,
                    received_monotonic_ns=state.received_monotonic_ns,
                    chain_position=state.slot,
                ),
            )

        try:
            # Subscribe first, then take a batched starting snapshot and drain
            # push updates.  This closes the usual HTTP-snapshot-to-WS gap.
            await stream.start()
            initial, _response = await fetch_clmm_pool_states(
                pool_ids,
                endpoint=self.http_url,
                proxy_url=self.proxy_url,
                timeout_seconds=self.timeout_seconds,
            )
            for state in initial.values():
                await emit(state)
            while not stop_event.is_set():
                try:
                    state = await asyncio.wait_for(stream.next_update(), timeout=1.0)
                except TimeoutError:
                    if stream.error is not None:
                        raise RuntimeError(stream.error)
                    continue
                await emit(state)
        finally:
            await stream.close()


@dataclass
class RaydiumLocalQuoteStateSource:
    """Expose the local SDK worker's pool updates through the common bus.

    The worker owns its direct RPC subscription and cached tick arrays.  It is
    a child of this one scanner process, rather than another manually launched
    tmux command.  Exact-input requests are available to the later route
    evaluator through :meth:`quote_exact_input`.
    """

    pools: Sequence[RaydiumClmmPoolConfig]
    raydium_standard_pools: Sequence[RaydiumStandardPoolConfig]
    meteora_pools: Sequence[MeteoraDlmmPoolConfig]
    orca_pools: Sequence[OrcaWhirlpoolPoolConfig]
    http_url: str
    ws_url: str
    timeout_seconds: float
    tick_cache_max_age_ms: int
    state_snapshot_refresh_interval_ms: int
    rpc_http_min_request_interval_ms: int
    amm_simulation_enabled: bool = False
    allowed_protocols: tuple[str, ...] = ()
    name: str = "solana:local-exact-pools"
    _client: RaydiumLocalQuoteWorker | None = field(default=None, init=False, repr=False)

    def _require_simulation_enabled(self) -> None:
        if not self.amm_simulation_enabled:
            raise RuntimeError(
                "post-trade AMM simulation is disabled; set [amm_simulation] enabled=true to enable shadow simulation",
            )

    def describe(self) -> Mapping[str, Any]:
        descriptor = {
            "source": self.name,
            "chain": "solana",
            "amm_simulation_enabled": self.amm_simulation_enabled,
            "amm_simulation_allowed_protocols": list(self.allowed_protocols),
            "protocols": [
                *(("Raydium CLMM",) if self.pools else ()),
                *(("Raydium CPMM/AMM v4",) if self.raydium_standard_pools else ()),
                *(("Meteora DLMM",) if self.meteora_pools else ()),
                *(("Orca Whirlpool",) if self.orca_pools else ()),
            ],
            "mode": "local_typescript_sdk_exact_input_quote",
            "raydium_clmm_pools": [pool.pool_id for pool in self.pools],
            "raydium_standard_pools": [
                {"pool_id": pool.pool_id, "protocol": pool.protocol}
                for pool in self.raydium_standard_pools
            ],
            "meteora_dlmm_pools": [pool.pool_id for pool in self.meteora_pools],
            "orca_whirlpool_pools": [pool.pool_id for pool in self.orca_pools],
            "state_snapshot_refresh_interval_ms": self.state_snapshot_refresh_interval_ms,
            "rpc_http_min_request_interval_ms": self.rpc_http_min_request_interval_ms,
            "credentials_required": False,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }
        if self._client is not None:
            descriptor["worker"] = self._client.safe_descriptor()
        return descriptor

    async def quote_exact_input(
        self,
        *,
        request_id: str,
        pool_id: str,
        input_mint: str,
        output_mint: str,
        input_amount_raw: int,
        minimum_state_slot: int | None = None,
    ) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("local Solana quote worker is not running")
        return await self._client.quote_exact_input(
            request_id=request_id,
            pool_id=pool_id,
            input_mint=input_mint,
            output_mint=output_mint,
            input_amount_raw=input_amount_raw,
            minimum_state_slot=minimum_state_slot,
            timeout_seconds=self.timeout_seconds,
        )

    async def capture_snapshot(
        self,
        *,
        request_id: str,
        pool_ids: Sequence[str],
        required_consistency: str = "validated_multi_account_snapshot",
    ) -> dict[str, Any]:
        """Capture an immutable validated multi-account snapshot in the worker.

        This is a read-only simulation operation: the worker holds the observed
        pool accounts and returns a snapshot token; it never opens a wallet or
        submits a transaction.
        """

        self._require_simulation_enabled()
        if self._client is None:
            raise RuntimeError("local Solana quote worker is not running")
        result = await self._client.capture_snapshot(
            request_id=request_id,
            pool_ids=tuple(pool_ids),
            required_consistency=required_consistency,
            timeout_seconds=self.timeout_seconds,
        )
        # These are receipt clocks in the Python supervisor, not invented
        # chain timestamps.  The analyzer needs both to detect suspend/clock
        # jumps without replacing snapshot age with evaluation time.
        return {
            **result,
            "response_received_realtime_ns": time.time_ns(),
            "response_received_monotonic_ns": time.monotonic_ns(),
        }

    async def simulate_path(
        self,
        *,
        request_id: str,
        snapshot_token: str,
        legs: Sequence[Mapping[str, object]],
        initial_balances: Sequence[Mapping[str, object]],
        deadline_monotonic_ns: int | str | None = None,
    ) -> dict[str, Any]:
        """Run a deterministic sequential path simulation on an immutable snapshot."""

        self._require_simulation_enabled()
        if self._client is None:
            raise RuntimeError("local Solana quote worker is not running")
        return await self._client.simulate_path(
            request_id=request_id,
            snapshot_token=snapshot_token,
            legs=legs,
            initial_balances=initial_balances,
            deadline_monotonic_ns=deadline_monotonic_ns,
            timeout_seconds=self.timeout_seconds,
        )

    async def export_simulation_evidence(
        self,
        *,
        request_id: str,
        snapshot_token: str,
    ) -> dict[str, Any]:
        """Export the captured bundle as replayable simulation evidence."""

        self._require_simulation_enabled()
        if self._client is None:
            raise RuntimeError("local Solana quote worker is not running")
        return await self._client.export_evidence(
            request_id=request_id,
            snapshot_token=snapshot_token,
            timeout_seconds=self.timeout_seconds,
        )

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        client = RaydiumLocalQuoteWorker(
            rpc_http_url=self.http_url,
            rpc_ws_url=self.ws_url,
            pools=tuple(QuoteWorkerPool(pool.pool_id, pool.label) for pool in self.pools),
            raydium_standard_pools=tuple(
                RaydiumStandardQuoteWorkerPool(pool.pool_id, pool.label, pool.protocol)
                for pool in self.raydium_standard_pools
            ),
            meteora_pools=tuple(
                QuoteWorkerPool(pool.pool_id, pool.label) for pool in self.meteora_pools
            ),
            orca_pools=tuple(
                QuoteWorkerPool(pool.pool_id, pool.label) for pool in self.orca_pools
            ),
            tick_cache_max_age_ms=self.tick_cache_max_age_ms,
            state_snapshot_refresh_interval_ms=self.state_snapshot_refresh_interval_ms,
            rpc_http_min_request_interval_ms=self.rpc_http_min_request_interval_ms,
        )
        self._client = client
        try:
            # Raydium's SDK can bootstrap many pools in one request, while the
            # Meteora SDK resolves each DLMM pool and its nearby bin arrays.
            # Keep startup bounded but leave enough room for the combined hot
            # set on a free RPC plan.
            await client.start(timeout_seconds=max(90.0, self.timeout_seconds * 9))
            while not stop_event.is_set():
                try:
                    message = await asyncio.wait_for(client.next_event(), timeout=1.0)
                except TimeoutError:
                    continue
                if message.get("type") == "refresh_health":
                    status = message.get("status")
                    interval_ms = message.get("interval_ms")
                    consecutive_errors = message.get("consecutive_errors")
                    if (
                        status not in {"ok", "error"}
                        or not isinstance(interval_ms, int)
                        or not isinstance(consecutive_errors, int)
                    ):
                        raise RuntimeError("local quote worker refresh-health event is malformed")
                    failed_protocols = message.get("failed_protocols", [])
                    errors = message.get("errors", {})
                    if not isinstance(failed_protocols, list) or not isinstance(errors, dict):
                        raise RuntimeError("local quote worker refresh-health details are malformed")
                    now_realtime = time.time_ns()
                    now_monotonic = time.monotonic_ns()
                    await publish(
                        MarketEvent(
                            source=self.name,
                            key="solana:local-exact-pools:refresh-health",
                            kind="source_health",
                            value=message,
                            summary={
                                "status": status,
                                "interval_ms": interval_ms,
                                "consecutive_errors": consecutive_errors,
                                "failed_protocols": [
                                    str(value)[:64] for value in failed_protocols[:8]
                                ],
                                "errors": {
                                    str(key)[:64]: str(value)[:256]
                                    for key, value in list(errors.items())[:8]
                                },
                            },
                            received_realtime_ns=now_realtime,
                            received_monotonic_ns=now_monotonic,
                        ),
                    )
                    continue
                if message.get("type") != "pool_state":
                    raise RuntimeError(f"unexpected local quote worker event {message.get('type')!r}")
                pool_id = message.get("pool_id")
                label = message.get("label")
                protocol = message.get("protocol")
                slot = message.get("slot")
                token_a_mint = message.get("token_a_mint")
                token_b_mint = message.get("token_b_mint")
                token_a_decimals = message.get("token_a_decimals")
                token_b_decimals = message.get("token_b_decimals")
                if not isinstance(pool_id, str) or not isinstance(label, str):
                    raise RuntimeError("local quote worker pool-state event is malformed")
                if (
                    protocol not in {
                        "raydium_clmm",
                        "raydium_cpmm",
                        "raydium_amm_v4",
                        "meteora_dlmm",
                        "orca_whirlpool",
                    }
                    or not isinstance(slot, int)
                ):
                    raise RuntimeError("local quote worker pool-state protocol/slot is malformed")
                if (
                    not isinstance(token_a_mint, str)
                    or not isinstance(token_b_mint, str)
                    or not isinstance(token_a_decimals, int)
                    or not isinstance(token_b_decimals, int)
                ):
                    raise RuntimeError("local quote worker pool-state mint/decimal payload is malformed")
                protocol_key = str(protocol).replace("_", "-")
                state_summary = {
                    "label": label,
                    "pool_id": pool_id,
                    "protocol": protocol,
                    "token_a_mint": token_a_mint,
                    "token_b_mint": token_b_mint,
                    "token_a_decimals": token_a_decimals,
                    "token_b_decimals": token_b_decimals,
                    "slot": slot,
                    "quote_engine": "local_typescript_sdk",
                }
                if protocol == "raydium_clmm":
                    tick = message.get("tick_current")
                    sqrt_price_x64 = message.get("sqrt_price_x64")
                    if not isinstance(tick, int) or not isinstance(sqrt_price_x64, str):
                        raise RuntimeError("local Raydium pool-state tick/price is malformed")
                    state_summary.update(
                        tick_current=tick,
                        sqrt_price_x64=sqrt_price_x64,
                        tick_cache_age_ms=message.get("tick_cache_age_ms"),
                    )
                elif protocol == "meteora_dlmm":
                    active_id = message.get("active_id")
                    if not isinstance(active_id, int):
                        raise RuntimeError("local Meteora pool-state active_id is malformed")
                    state_summary.update(
                        active_id=active_id,
                        bin_step=message.get("bin_step"),
                        bin_cache_age_ms=message.get("bin_cache_age_ms"),
                    )
                elif protocol == "orca_whirlpool":
                    tick = message.get("tick_current")
                    sqrt_price_x64 = message.get("sqrt_price_x64")
                    if not isinstance(tick, int) or not isinstance(sqrt_price_x64, str):
                        raise RuntimeError("local Orca pool-state tick/price is malformed")
                    state_summary.update(
                        tick_current=tick,
                        sqrt_price_x64=sqrt_price_x64,
                        tick_spacing=message.get("tick_spacing"),
                        fee_rate_millionths=message.get("fee_rate_millionths"),
                        tick_cache_age_ms=message.get("tick_cache_age_ms"),
                    )
                else:
                    reserve_a = message.get("reserve_a_raw")
                    reserve_b = message.get("reserve_b_raw")
                    fee_numerator = message.get("trade_fee_numerator")
                    fee_denominator = message.get("trade_fee_denominator")
                    if not all(
                        isinstance(value, str)
                        for value in (reserve_a, reserve_b, fee_numerator, fee_denominator)
                    ):
                        raise RuntimeError("local Raydium standard reserve/fee state is malformed")
                    state_summary.update(
                        reserve_a_raw=reserve_a,
                        reserve_b_raw=reserve_b,
                        trade_fee_numerator=fee_numerator,
                        trade_fee_denominator=fee_denominator,
                    )
                await publish(
                    MarketEvent(
                        source=self.name,
                        key=f"solana:{protocol_key}:{pool_id}",
                        kind="pool_state",
                        value=message,
                        summary=state_summary,
                        received_realtime_ns=time.time_ns(),
                        received_monotonic_ns=time.monotonic_ns(),
                        chain_position=slot,
                    ),
                )
        finally:
            await client.close()
            self._client = None


def _book_summary(book: BookSnapshot) -> dict[str, Any]:
    return {
        "symbol": book.symbol,
        "category": book.category,
        "source": book.source,
        "best_bid": str(book.bids[0][0]) if book.bids else None,
        "best_ask": str(book.asks[0][0]) if book.asks else None,
        "bid_levels": len(book.bids),
        "ask_levels": len(book.asks),
        "exchange_system_time_ms": book.exchange_system_time_ms,
        "update_id": book.update_id,
    }


@dataclass(frozen=True, slots=True)
class CexTopOfBookEvent:
    """Compact CEX record suitable for the common rolling history.

    A public depth update can contain tens of ``Decimal`` levels.  The stream
    itself keeps its latest full book for later executable-depth validation,
    but copying that full object into every 1--3 minute history entry turns a
    high-frequency data collector into a multi-gigabyte cache.  The shared
    bus therefore retains only the time-aligned BBO and its visible sizes.
    """

    venue: str
    category: str
    symbol: str
    best_bid: Decimal | None
    best_bid_size: Decimal | None
    best_ask: Decimal | None
    best_ask_size: Decimal | None
    source: str
    exchange_system_time_ms: int | None
    matching_engine_time_ms: int | None
    update_id: int | None
    received_realtime_ns: int
    received_monotonic_ns: int

    @classmethod
    def from_book(cls, *, venue: str, book: BookSnapshot) -> "CexTopOfBookEvent":
        bid = book.bids[0] if book.bids else (None, None)
        ask = book.asks[0] if book.asks else (None, None)
        return cls(
            venue=venue.upper(),
            category=book.category,
            symbol=book.symbol,
            best_bid=bid[0],
            best_bid_size=bid[1],
            best_ask=ask[0],
            best_ask_size=ask[1],
            source=book.source,
            exchange_system_time_ms=book.exchange_system_time_ms,
            matching_engine_time_ms=book.matching_engine_time_ms,
            update_id=book.update_id,
            received_realtime_ns=book.response.received_realtime_ns,
            received_monotonic_ns=book.response.received_monotonic_ns,
        )


def _linear_perp_context_summary(ticker: object) -> dict[str, Any]:
    """Compact projection of Bybit's public linear ticker side channel."""

    return {
        "venue": "BYBIT",
        "venue_symbol": getattr(ticker, "symbol", None),
        "contract_type": "linear_perpetual",
        "funding_rate": (
            str(getattr(ticker, "funding_rate"))
            if getattr(ticker, "funding_rate", None) is not None
            else None
        ),
        "next_funding_time_ms": getattr(ticker, "next_funding_time_ms", None),
        "mark_price": (
            str(getattr(ticker, "mark_price"))
            if getattr(ticker, "mark_price", None) is not None
            else None
        ),
        "index_price": (
            str(getattr(ticker, "index_price"))
            if getattr(ticker, "index_price", None) is not None
            else None
        ),
        "source": "bybit_public_linear_ticker_websocket",
    }


@dataclass
class CexBookStateSource:
    """Adapt the existing bounded public CEX order-book streams to the bus."""

    config: CexStreamConfig
    timeout_seconds: float
    proxy_url: str | None
    # The unified data plane has its own three-minute BBO history.  This
    # source needs full depth only for the current execution-validation state,
    # plus a few prior snapshots to tolerate an in-flight consumer.  Retaining
    # 256 full 50-level books *per symbol* duplicated hundreds of megabytes.
    full_depth_history_capacity_per_symbol: int = 8
    # This is the only full-depth state retained by the source layer: one
    # current book per symbol.  The generic scanner history receives the
    # lightweight :class:`CexTopOfBookEvent` above instead.
    _latest_books: dict[str, BookSnapshot] = field(default_factory=dict, init=False, repr=False)
    _stream: PublicBookStream | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.full_depth_history_capacity_per_symbol <= 0:
            raise ValueError("CEX full-depth history capacity must be positive")

    @property
    def name(self) -> str:
        return f"cex:{self.config.venue}:{self.config.category}"

    def describe(self) -> Mapping[str, Any]:
        return {
            "source": self.name,
            "venue": self.config.venue,
            "category": self.config.category,
            "symbols": list(self.config.symbols),
            "mode": "public_websocket",
            "credentials_required": False,
            "transactions_submitted": False,
        }

    def latest_book(self, symbol: str) -> BookSnapshot | None:
        """Return current full depth for a later on-demand executor check.

        This is deliberately not serialized and is not part of the rolling
        generic history.  Consumers must still verify staleness and walk the
        actual depth immediately before an execution decision.
        """

        return self._latest_books.get(symbol.upper())

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        # A supervisor restart starts a new source epoch.  Full-depth books
        # from the disconnected stream must not remain available to later
        # execution checks while the replacement stream is still syncing.
        self._latest_books.clear()
        stream: PublicBookStream = build_public_book_stream(
            self.config.venue,
            self.config.symbols,
            timeout_seconds=self.timeout_seconds,
            proxy_url=self.proxy_url,
            history_capacity_per_symbol=self.full_depth_history_capacity_per_symbol,
            category=self.config.category,
        )
        self._stream = stream
        last_linear_ticker_realtime_ns: dict[str, int] = {}
        try:
            await stream.start()
            while not stop_event.is_set():
                try:
                    book = await asyncio.wait_for(stream.next_update(), timeout=1.0)
                except TimeoutError:
                    error = getattr(stream, "error", None)
                    if error:
                        raise RuntimeError(str(error))
                    continue
                self._latest_books[book.symbol] = book
                await publish(
                    MarketEvent(
                        source=self.name,
                        key=f"{self.name}:{book.symbol}",
                        kind="order_book",
                        value=CexTopOfBookEvent.from_book(
                            venue=self.config.venue,
                            book=book,
                        ),
                        summary=_book_summary(book),
                        received_realtime_ns=book.response.received_realtime_ns,
                        received_monotonic_ns=book.response.received_monotonic_ns,
                        chain_position=book.update_id,
                    ),
                )
                # Bybit linear carries a delta-compressed public ticker on
                # the same connection.  Preserve it as a separate normalized
                # state type rather than silently treating a book update as a
                # funding update.  Other CEX streams simply do not expose
                # this optional method.
                ticker_getter = getattr(stream, "perp_ticker", None)
                if self.config.category == "linear" and callable(ticker_getter):
                    ticker = ticker_getter(book.symbol)
                    ticker_realtime_ns = getattr(ticker, "received_realtime_ns", None)
                    if (
                        ticker is not None
                        and isinstance(ticker_realtime_ns, int)
                        and last_linear_ticker_realtime_ns.get(book.symbol) != ticker_realtime_ns
                    ):
                        last_linear_ticker_realtime_ns[book.symbol] = ticker_realtime_ns
                        ticker_monotonic_ns = getattr(ticker, "received_monotonic_ns", None)
                        await publish(
                            MarketEvent(
                                source=self.name,
                                key=f"{self.name}:{book.symbol}:perp-context",
                                kind="perp_context",
                                value=ticker,
                                summary=_linear_perp_context_summary(ticker),
                                received_realtime_ns=ticker_realtime_ns,
                                received_monotonic_ns=(
                                    ticker_monotonic_ns
                                    if isinstance(ticker_monotonic_ns, int)
                                    else time.monotonic_ns()
                                ),
                            ),
                        )
        finally:
            health = stream_health(stream)
            await stream.close()
            self._stream = None
            # Preserve useful health in the exception path without exposing
            # full book state.  The supervisor will surface the concise error.
            if health.get("error") and not stop_event.is_set():
                raise RuntimeError(str(health["error"]))


def _routes_with_account_fee_audit(
    settings: LocalRouteEvaluatorSettings,
) -> tuple[LocalSpotRoute, ...]:
    """Overlay a separately generated read-only fee audit when supplied.

    The unified scanner itself never sees an exchange API secret.  A user may
    generate a compact read-only audit file beforehand; missing symbols retain
    their explicitly labelled public baseline instead of becoming zero-fee.
    """

    if settings.fee_audit_file is None:
        return settings.routes
    audited = load_spot_fee_audit(settings.fee_audit_file)
    effective: list[LocalSpotRoute] = []
    for route in settings.routes:
        base_fee = audited.get((route.cex_venue, route.base_cex_symbol))
        bridge_fee = (
            audited.get((route.cex_venue, route.bridge_cex_symbol))
            if route.bridge_cex_symbol is not None
            else None
        )
        replacement: dict[str, object] = {}
        if base_fee is not None:
            replacement.update(
                base_buy_taker_fee_bps=base_fee.taker_buy_bps,
                base_sell_taker_fee_bps=base_fee.taker_sell_bps,
                base_fee_source=base_fee.source,
                base_fee_account_verified=base_fee.account_verified,
            )
        if bridge_fee is not None:
            replacement.update(
                bridge_buy_taker_fee_bps=bridge_fee.taker_buy_bps,
                bridge_sell_taker_fee_bps=bridge_fee.taker_sell_bps,
                bridge_fee_source=bridge_fee.source,
                bridge_fee_account_verified=bridge_fee.account_verified,
            )
        effective.append(replace(route, **replacement))
    return tuple(effective)


def build_solana_market_sources(
    config: SolanaScannerConfig,
) -> tuple[
    tuple[ScannerSource, ...],
    RaydiumClmmStateSource | RaydiumLocalQuoteStateSource,
]:
    """Create raw Solana/CEX sources without attaching an evaluator.

    This small seam is important for the top-level collector: it can put
    Solana spot state, CEX books, and other venue families on one common
    :class:`RealtimeScanner` bus.  The legacy route evaluator remains an
    optional consumer below, rather than being coupled to ownership of the
    subscriptions.
    """

    raydium_source: RaydiumClmmStateSource | RaydiumLocalQuoteStateSource
    if config.raydium_local_quote_worker.enabled:
        raydium_source = RaydiumLocalQuoteStateSource(
            pools=config.pools,
            raydium_standard_pools=config.raydium_standard_pools,
            meteora_pools=config.meteora_pools,
            orca_pools=config.orca_pools,
            http_url=config.rpc_http_url,
            ws_url=config.rpc_ws_url,
            timeout_seconds=config.timeout_seconds,
            tick_cache_max_age_ms=config.raydium_local_quote_worker.tick_cache_max_age_ms,
            state_snapshot_refresh_interval_ms=(
                config.raydium_local_quote_worker.state_snapshot_refresh_interval_ms
            ),
            rpc_http_min_request_interval_ms=(
                config.raydium_local_quote_worker.rpc_http_min_request_interval_ms
            ),
            amm_simulation_enabled=config.amm_simulation.enabled,
            allowed_protocols=config.amm_simulation.allowed_protocols,
        )
    else:
        if config.raydium_standard_pools or config.meteora_pools or config.orca_pools:
            raise ValueError(
                "Raydium Standard/Meteora/Orca pools require the managed local TypeScript quote worker",
            )
        raydium_source = RaydiumClmmStateSource(
            pools=config.pools,
            http_url=config.rpc_http_url,
            ws_url=config.rpc_ws_url,
            timeout_seconds=config.timeout_seconds,
            proxy_url=config.proxy_url,
        )
    sources: tuple[ScannerSource, ...] = (
        raydium_source,
        *(
            CexBookStateSource(
                config=stream,
                timeout_seconds=config.timeout_seconds,
                proxy_url=config.proxy_url,
            )
            for stream in config.cex_streams
        ),
    )
    return sources, raydium_source


def build_solana_scanner(config: SolanaScannerConfig, *, output_directory: Path) -> RealtimeScanner:
    """Build the backwards-compatible Solana/CEX supervisor.

    New callers that only need data should use :func:`build_solana_market_sources`.
    This wrapper preserves the optional local route evaluator for the legacy
    standalone Solana entry point.
    """

    sources, raydium_source = build_solana_market_sources(config)
    scanner = RealtimeScanner(
        sources=sources,
        output_directory=output_directory,
        retention_seconds=config.retention_seconds,
        max_events_per_key=config.max_events_per_key,
        event_bus_capacity=config.event_bus_capacity,
        status_flush_seconds=config.status_flush_seconds,
    )
    if config.local_route_evaluator.enabled:
        if not isinstance(raydium_source, RaydiumLocalQuoteStateSource):
            raise ValueError(
                "local_route_evaluator requires the managed local Raydium quote worker",
            )
        effective_settings = replace(
            config.local_route_evaluator,
            routes=_routes_with_account_fee_audit(config.local_route_evaluator),
        )
        evaluator = SolanaRouteEvaluator(
            config=effective_settings.to_evaluator_config(),
            store=scanner.store,
            quote_source=raydium_source,
            output_directory=output_directory,
            gated_verifier=(
                JupiterGatedVerifier(
                    api_key=config.jupiter.api_key,
                    minimum_request_interval_seconds=config.jupiter.minimum_request_interval_seconds,
                    proxy_url=config.proxy_url,
                    timeout_seconds=config.timeout_seconds,
                )
                if config.jupiter.enabled
                else None
            ),
        )
        scanner.event_handler = evaluator.handle_event
        scanner.status_providers = {"local_route_evaluator": evaluator.snapshot}
        scanner.shutdown_handlers = (evaluator.close,)
    return scanner


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--duration-seconds", type=float, default=300.0)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="run until interrupted; status.json continues to refresh while raw market data stays in memory only",
    )
    parser.add_argument("--output-root", type=Path, default=Path("data/live/unified-scanner"))
    parser.add_argument("--run-id")
    parser.add_argument(
        "--enable-local-quote-worker",
        action="store_true",
        help="replace Python CLMM state stream with the managed local TypeScript exact-quote worker",
    )
    parser.add_argument(
        "--enable-local-route-evaluator",
        action="store_true",
        help="enable configured read-only local exact-pool/CEX-depth routes for this run",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    if not args.continuous and args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds must be positive")
    try:
        config = load_solana_scanner_config(args.config)
    except (OSError, ValueError, tomllib.TOMLDecodeError) as exc:
        raise SystemExit(f"invalid scanner config: {exc}") from exc
    run_id = args.run_id or default_run_id()
    try:
        if args.enable_local_quote_worker:
            config = replace(
                config,
                raydium_local_quote_worker=replace(
                    config.raydium_local_quote_worker,
                    enabled=True,
                ),
            )
        if args.enable_local_route_evaluator:
            config = replace(
                config,
                local_route_evaluator=replace(
                    config.local_route_evaluator,
                    enabled=True,
                ),
            )
        validate_run_id(run_id)
        network_route = configure_process_network_route(config.proxy_url)
        scanner = build_solana_scanner(config, output_directory=args.output_root / run_id)
        manifest = asyncio.run(
            scanner.run(duration_seconds=None if args.continuous else args.duration_seconds),
        )
    except (OSError, ValueError) as exc:
        raise SystemExit(f"scanner failed to start: {exc}") from exc
    print(
        {
            **manifest,
            "network_route": network_route,
            "config": str(args.config.resolve()),
        },
        flush=True,
    )


if __name__ == "__main__":
    main()
