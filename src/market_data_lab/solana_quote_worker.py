"""Python supervisor client for the local read-only TypeScript quote worker.

The child process receives managed RPC endpoints through stdin only.  It emits
bounded JSON-lines state/quote events; neither side writes a raw quote log or
persists provider URLs/keys.  The protocol intentionally has no wallet,
transaction, or order operation.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


URL_PATTERN = re.compile(r"(?:https?|wss?)://[^\s\"']+")
CANONICAL_RAW_INTEGER = re.compile(r"^(0|[1-9][0-9]*)$")
MAX_PENDING_SIMULATION_REQUESTS = 32


def _canonical_decimal_string(value: object, *, field: str) -> str:
    """Normalize protocol integers without allowing float/JSON precision loss."""

    if isinstance(value, bool):
        raise ValueError(f"{field} must be a canonical non-negative decimal integer")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{field} must be a canonical non-negative decimal integer")
        return str(value)
    if not isinstance(value, str) or not CANONICAL_RAW_INTEGER.fullmatch(value):
        raise ValueError(f"{field} must be a canonical non-negative decimal integer")
    return value


def _canonical_initial_balances(
    initial_balances: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    if not initial_balances:
        raise ValueError("simulate_path requires non-empty initial_balances")
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, balance in enumerate(initial_balances):
        asset_id = balance.get("asset_id")
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise ValueError(f"initial_balances[{index}].asset_id must be non-empty")
        asset_id = asset_id.strip()
        if asset_id in seen:
            raise ValueError(f"initial_balances contains duplicate asset_id {asset_id!r}")
        seen.add(asset_id)
        result.append({
            "asset_id": asset_id,
            "amount_raw": _canonical_decimal_string(
                balance.get("amount_raw"),
                field=f"initial_balances[{index}].amount_raw",
            ),
        })
    return result


@dataclass(frozen=True)
class QuoteWorkerPool:
    pool_id: str
    label: str


@dataclass(frozen=True)
class RaydiumStandardQuoteWorkerPool:
    pool_id: str
    label: str
    protocol: str

    def __post_init__(self) -> None:
        if self.protocol not in {"raydium_cpmm", "raydium_amm_v4"}:
            raise ValueError("Raydium standard worker protocol must be CPMM or AMM v4")


@dataclass(frozen=True)
class RaydiumStandardDiscoveryCandidate:
    pool_id: str
    label: str


@dataclass(frozen=True)
class DiscoveredRaydiumStandardPool:
    pool_id: str
    label: str
    protocol: str
    token_a_mint: str
    token_b_mint: str
    token_a_decimals: int
    token_b_decimals: int


@dataclass(frozen=True)
class OrcaDiscoveryAsset:
    symbol: str
    mint: str
    decimals: int
    cex_symbol: str


@dataclass(frozen=True)
class OrcaDiscoveryPair:
    base: OrcaDiscoveryAsset
    bridge: OrcaDiscoveryAsset


@dataclass(frozen=True)
class DiscoveredOrcaPool:
    pool_id: str
    label: str
    base: OrcaDiscoveryAsset
    bridge: OrcaDiscoveryAsset
    tick_spacing: int
    fee_rate_millionths: int
    liquidity_raw: str


async def discover_raydium_standard_pools(
    *,
    rpc_http_url: str,
    candidates: Sequence[RaydiumStandardDiscoveryCandidate],
    timeout_seconds: float = 30.0,
) -> tuple[DiscoveredRaydiumStandardPool, ...]:
    """Identify cached Raydium Standard IDs by on-chain owner and layout."""

    if not candidates or len(candidates) > 64 or timeout_seconds <= 0:
        raise ValueError("Raydium standard discovery requires 1..64 candidates and a timeout")
    root = _worker_root()
    executable = root / "node_modules" / ".bin" / "tsx"
    if not executable.is_file():
        raise RuntimeError(f"quote worker dependency executable is missing at {executable}")
    process = await asyncio.create_subprocess_exec(
        str(executable),
        "src/raydiumStandardDiscover.ts",
        cwd=str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    request = {
        "rpc_http_url": rpc_http_url,
        "pools": [
            {"pool_id": candidate.pool_id, "label": candidate.label}
            for candidate in candidates
        ],
    }
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(request, separators=(",", ":")).encode() + b"\n"),
            timeout=timeout_seconds,
        )
    except BaseException:
        if process.returncode is None:
            process.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.wait(), timeout=3.0)
        raise
    stderr_text = _redact_urls(stderr.decode("utf-8", errors="replace").strip())
    try:
        payload = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Raydium standard discovery emitted invalid JSON: {exc}; stderr: {stderr_text[-512:]}",
        ) from exc
    if (
        process.returncode != 0
        or not isinstance(payload, dict)
        or payload.get("type") != "raydium_standard_discovery_result"
    ):
        error = payload.get("error") if isinstance(payload, dict) else "invalid response"
        raise RuntimeError(
            f"Raydium standard discovery failed: {_redact_urls(str(error))}; "
            f"stderr: {stderr_text[-512:]}",
        )
    raw_pools = payload.get("pools")
    if not isinstance(raw_pools, list):
        raise RuntimeError("Raydium standard discovery result has no pool array")
    expected = {candidate.pool_id: candidate for candidate in candidates}
    discovered: list[DiscoveredRaydiumStandardPool] = []
    for item in raw_pools:
        if not isinstance(item, Mapping) or item.get("status") != "ok":
            continue
        pool_id = item.get("pool_id")
        protocol = item.get("protocol")
        token_a_mint = item.get("token_a_mint")
        token_b_mint = item.get("token_b_mint")
        token_a_decimals = item.get("token_a_decimals")
        token_b_decimals = item.get("token_b_decimals")
        if (
            not isinstance(pool_id, str)
            or pool_id not in expected
            or protocol not in {"raydium_cpmm", "raydium_amm_v4"}
            or not isinstance(token_a_mint, str)
            or not isinstance(token_b_mint, str)
            or not isinstance(token_a_decimals, int)
            or not isinstance(token_b_decimals, int)
        ):
            raise RuntimeError("Raydium standard discovery pool is malformed")
        discovered.append(
            DiscoveredRaydiumStandardPool(
                pool_id=pool_id,
                label=expected[pool_id].label,
                protocol=protocol,
                token_a_mint=token_a_mint,
                token_b_mint=token_b_mint,
                token_a_decimals=token_a_decimals,
                token_b_decimals=token_b_decimals,
            ),
        )
    return tuple(discovered)


async def discover_orca_whirlpools(
    *,
    rpc_http_url: str,
    pairs: Sequence[OrcaDiscoveryPair],
    maximum_pools: int,
    timeout_seconds: float = 30.0,
) -> tuple[DiscoveredOrcaPool, ...]:
    """Probe deterministic official Orca PDAs in one bounded RPC batch."""

    if not pairs or not 0 < maximum_pools <= 32 or timeout_seconds <= 0:
        raise ValueError("Orca discovery requires pairs and positive bounded limits")
    root = _worker_root()
    executable = root / "node_modules" / ".bin" / "tsx"
    if not executable.is_file():
        raise RuntimeError(f"quote worker dependency executable is missing at {executable}")
    process = await asyncio.create_subprocess_exec(
        str(executable),
        "src/orcaDiscover.ts",
        cwd=str(root),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    request = {
        "rpc_http_url": rpc_http_url,
        "maximum_pools": maximum_pools,
        "pairs": [
            {
                "base": {
                    "symbol": pair.base.symbol,
                    "mint": pair.base.mint,
                    "decimals": pair.base.decimals,
                    "cex_symbol": pair.base.cex_symbol,
                },
                "bridge": {
                    "symbol": pair.bridge.symbol,
                    "mint": pair.bridge.mint,
                    "decimals": pair.bridge.decimals,
                    "cex_symbol": pair.bridge.cex_symbol,
                },
            }
            for pair in pairs
        ],
    }
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(json.dumps(request, separators=(",", ":")).encode() + b"\n"),
            timeout=timeout_seconds,
        )
    except BaseException:
        if process.returncode is None:
            process.terminate()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(process.wait(), timeout=3.0)
        raise
    stderr_text = _redact_urls(stderr.decode("utf-8", errors="replace").strip())
    try:
        payload = json.loads(stdout.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Orca discovery emitted invalid JSON: {exc}; stderr: {stderr_text[-512:]}",
        ) from exc
    if process.returncode != 0 or not isinstance(payload, dict) or payload.get("type") != "orca_discovery_result":
        error = payload.get("error") if isinstance(payload, dict) else "invalid response"
        raise RuntimeError(
            f"Orca discovery failed: {_redact_urls(str(error))}; stderr: {stderr_text[-512:]}",
        )
    raw_pools = payload.get("pools")
    if not isinstance(raw_pools, list):
        raise RuntimeError("Orca discovery result has no pool array")
    pools: list[DiscoveredOrcaPool] = []
    pair_by_identity = {
        (pair.base.mint, pair.bridge.mint): pair
        for pair in pairs
    }
    pair_by_identity.update({(bridge, base): pair for (base, bridge), pair in pair_by_identity.items()})
    for item in raw_pools:
        if not isinstance(item, Mapping):
            raise RuntimeError("Orca discovery pool is malformed")
        base_payload = item.get("base")
        bridge_payload = item.get("bridge")
        if not isinstance(base_payload, Mapping) or not isinstance(bridge_payload, Mapping):
            raise RuntimeError("Orca discovery token metadata is malformed")
        identity = (str(base_payload.get("mint")), str(bridge_payload.get("mint")))
        pair = pair_by_identity.get(identity)
        if pair is None:
            raise RuntimeError("Orca discovery returned an unrequested mint pair")
        pool_id = item.get("pool_id")
        label = item.get("label")
        tick_spacing = item.get("tick_spacing")
        fee_rate = item.get("fee_rate_millionths")
        liquidity = item.get("liquidity_raw")
        if (
            not isinstance(pool_id, str)
            or not isinstance(label, str)
            or not isinstance(tick_spacing, int)
            or not isinstance(fee_rate, int)
            or not isinstance(liquidity, str)
        ):
            raise RuntimeError("Orca discovery pool fields are malformed")
        pools.append(
            DiscoveredOrcaPool(
                pool_id=pool_id,
                label=label,
                base=pair.base,
                bridge=pair.bridge,
                tick_spacing=tick_spacing,
                fee_rate_millionths=fee_rate,
                liquidity_raw=liquidity,
            ),
        )
    return tuple(pools)


def _worker_root() -> Path:
    return Path(__file__).resolve().parents[2] / "workers" / "solana-quote-worker"


def _redact_urls(value: str) -> str:
    """Strip path/query so a provider API key cannot surface in an error."""

    def replace(match: re.Match[str]) -> str:
        candidate = match.group(0)
        scheme, _, remainder = candidate.partition("://")
        host = remainder.split("/", 1)[0]
        return f"{scheme}://{host}" if host else "[redacted-url]"

    return URL_PATTERN.sub(replace, value)


class _BoundedWorkerEventQueue:
    """Bounded latest-state queue for unsolicited worker messages.

    Replaceable state is coalesced by a stable logical key. A new distinct key
    never evicts an unrelated pending key: the stdout reader waits until the
    scanner consumes capacity. Messages without a safe stable key are lossless
    and use the same bounded backpressure path.

    ``try_put`` is synchronous so ``RaydiumLocalQuoteWorker._dispatch`` keeps
    its existing synchronous contract. The real stdout reader falls back to
    ``put`` and awaits it only when a new distinct key hits capacity.
    """

    def __init__(self, *, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("event queue capacity must be positive")
        self.capacity = capacity
        # dict preserves insertion order. Replacing an existing key updates the
        # payload without moving a cold key behind a hot key.
        self._pending: dict[object, dict[str, Any] | BaseException] = {}
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()

    @staticmethod
    def _coalescing_key(item: dict[str, Any] | BaseException) -> tuple[str, ...] | None:
        if not isinstance(item, dict):
            return None
        kind = item.get("type")
        if kind == "pool_state":
            protocol = item.get("protocol")
            pool_id = item.get("pool_id")
            if (
                isinstance(protocol, str)
                and protocol
                and isinstance(pool_id, str)
                and pool_id
            ):
                return ("pool_state", protocol, pool_id)
            # Do not alias malformed state messages with each other.
            return None
        if kind == "refresh_health":
            return ("refresh_health",)
        return None

    def try_put(self, item: dict[str, Any] | BaseException) -> bool:
        key = self._coalescing_key(item)
        if key is not None and key in self._pending:
            self._pending[key] = item
            self._not_empty.set()
            return True
        if len(self._pending) >= self.capacity:
            return False
        token: object = key if key is not None else object()
        self._pending[token] = item
        self._not_empty.set()
        if len(self._pending) >= self.capacity:
            self._not_full.clear()
        return True

    async def put(self, item: dict[str, Any] | BaseException) -> None:
        while not self.try_put(item):
            await self._not_full.wait()

    async def get(self) -> dict[str, Any] | BaseException:
        while not self._pending:
            await self._not_empty.wait()
        token = next(iter(self._pending))
        item = self._pending.pop(token)
        if not self._pending:
            self._not_empty.clear()
        self._not_full.set()
        return item

    def qsize(self) -> int:
        return len(self._pending)


class RaydiumLocalQuoteWorker:
    """One local JSON-lines worker, supervised by an asyncio parent.

    Quote replies go straight to their awaiting caller rather than an
    unbounded event log.  Pool-state notices are the only messages queued for
    the scanner, and that queue keeps the most recent updates under pressure.
    """

    def __init__(
        self,
        *,
        rpc_http_url: str,
        rpc_ws_url: str,
        pools: Sequence[QuoteWorkerPool],
        raydium_standard_pools: Sequence[RaydiumStandardQuoteWorkerPool] = (),
        meteora_pools: Sequence[QuoteWorkerPool] = (),
        orca_pools: Sequence[QuoteWorkerPool] = (),
        tick_cache_max_age_ms: int = 300_000,
        state_snapshot_refresh_interval_ms: int = 15_000,
        core_refresh_after_ms: int | None = None,
        maintenance_scan_interval_ms: int = 1_000,
        refresh_stagger_window_ms: int = 5_000,
        pool_state_emit_min_interval_ms: int = 100,
        rpc_http_min_request_interval_ms: int = 200,
        event_capacity: int = 2_048,
        worker_stats_refresh_interval_ms: int = 10_000,
    ) -> None:
        all_pools = (*pools, *raydium_standard_pools, *meteora_pools, *orca_pools)
        if not all_pools or len({pool.pool_id for pool in all_pools}) != len(all_pools):
            raise ValueError("quote worker pools must be non-empty and unique across protocols")
        effective_core_refresh_after_ms = (
            state_snapshot_refresh_interval_ms
            if core_refresh_after_ms is None
            else core_refresh_after_ms
        )
        if (
            tick_cache_max_age_ms <= 0
            or state_snapshot_refresh_interval_ms < 1_000
            or effective_core_refresh_after_ms < 1_000
            or maintenance_scan_interval_ms < 100
            or refresh_stagger_window_ms <= 0
            or refresh_stagger_window_ms > effective_core_refresh_after_ms
            or pool_state_emit_min_interval_ms <= 0
            or rpc_http_min_request_interval_ms < 25
            or event_capacity <= 0
        ):
            raise ValueError("quote worker limits must be positive")
        self.rpc_http_url = rpc_http_url
        self.rpc_ws_url = rpc_ws_url
        self.pools = tuple(pools)
        self.raydium_standard_pools = tuple(raydium_standard_pools)
        self.meteora_pools = tuple(meteora_pools)
        self.orca_pools = tuple(orca_pools)
        self._protocol_by_pool_id = {
            **{pool.pool_id: "raydium_clmm" for pool in self.pools},
            **{pool.pool_id: pool.protocol for pool in self.raydium_standard_pools},
            **{pool.pool_id: "meteora_dlmm" for pool in self.meteora_pools},
            **{pool.pool_id: "orca_whirlpool" for pool in self.orca_pools},
        }
        self.tick_cache_max_age_ms = tick_cache_max_age_ms
        self.state_snapshot_refresh_interval_ms = state_snapshot_refresh_interval_ms
        self.core_refresh_after_ms = effective_core_refresh_after_ms
        self.maintenance_scan_interval_ms = maintenance_scan_interval_ms
        self.refresh_stagger_window_ms = refresh_stagger_window_ms
        self.pool_state_emit_min_interval_ms = pool_state_emit_min_interval_ms
        self.rpc_http_min_request_interval_ms = rpc_http_min_request_interval_ms
        self.event_capacity = event_capacity
        self.worker_stats_refresh_interval_ms = worker_stats_refresh_interval_ms
        self._process: asyncio.subprocess.Process | None = None
        self._events = _BoundedWorkerEventQueue(capacity=event_capacity)
        self._ready: asyncio.Future[dict[str, Any]] | None = None
        self._pending_quotes: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_simulations: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._pending_cancellations: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._stdout_task: asyncio.Task[None] | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail: list[str] = []
        self._late_quote_results_dropped = 0
        self._late_quote_errors_dropped = 0
        self._late_simulation_results_dropped = 0
        self._latest_worker_stats: dict[str, Any] | None = None
        self._closed = False

    @property
    def running(self) -> bool:
        return self._process is not None and self._process.returncode is None and not self._closed

    @property
    def latest_worker_stats(self) -> dict[str, Any] | None:
        """Latest validated worker_stats dict, or None if no stats received yet."""
        return self._latest_worker_stats

    def safe_descriptor(self) -> dict[str, object]:
        return {
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
            "tick_cache_max_age_ms": self.tick_cache_max_age_ms,
            "state_snapshot_refresh_interval_ms": self.state_snapshot_refresh_interval_ms,
            "core_refresh_after_ms": self.core_refresh_after_ms,
            "maintenance_scan_interval_ms": self.maintenance_scan_interval_ms,
            "refresh_stagger_window_ms": self.refresh_stagger_window_ms,
            "pool_state_emit_min_interval_ms": self.pool_state_emit_min_interval_ms,
            "rpc_http_min_request_interval_ms": self.rpc_http_min_request_interval_ms,
            "worker_stats_refresh_interval_ms": self.worker_stats_refresh_interval_ms,
            "late_quote_results_dropped": self._late_quote_results_dropped,
            "late_quote_errors_dropped": self._late_quote_errors_dropped,
            "late_simulation_results_dropped": self._late_simulation_results_dropped,
            "provider_credentials_persisted": False,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }

    async def start(self, *, timeout_seconds: float = 30.0) -> dict[str, Any]:
        if self._process is not None:
            raise RuntimeError("quote worker is already started")
        root = _worker_root()
        executable = root / "node_modules" / ".bin" / "tsx"
        if not executable.is_file():
            raise RuntimeError(f"quote worker dependency executable is missing at {executable}")
        self._process = await asyncio.create_subprocess_exec(
            str(executable),
            "src/worker.ts",
            cwd=str(root),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        loop = asyncio.get_running_loop()
        self._ready = loop.create_future()
        self._stdout_task = asyncio.create_task(self._read_stdout())
        self._stderr_task = asyncio.create_task(self._read_stderr())
        await self._send(
            {
                "type": "configure",
                "rpc_http_url": self.rpc_http_url,
                "rpc_ws_url": self.rpc_ws_url,
                "raydium_clmm_pools": [
                    {"pool_id": pool.pool_id, "label": pool.label} for pool in self.pools
                ],
                "raydium_standard_pools": [
                    {
                        "pool_id": pool.pool_id,
                        "label": pool.label,
                        "protocol": pool.protocol,
                    }
                    for pool in self.raydium_standard_pools
                ],
                "meteora_dlmm_pools": [
                    {"pool_id": pool.pool_id, "label": pool.label}
                    for pool in self.meteora_pools
                ],
                "orca_whirlpool_pools": [
                    {"pool_id": pool.pool_id, "label": pool.label}
                    for pool in self.orca_pools
                ],
                "tick_cache_max_age_ms": self.tick_cache_max_age_ms,
                "state_snapshot_refresh_interval_ms": self.state_snapshot_refresh_interval_ms,
                "core_refresh_after_ms": self.core_refresh_after_ms,
                "maintenance_scan_interval_ms": self.maintenance_scan_interval_ms,
                "refresh_stagger_window_ms": self.refresh_stagger_window_ms,
                "pool_state_emit_min_interval_ms": self.pool_state_emit_min_interval_ms,
                "rpc_http_min_request_interval_ms": self.rpc_http_min_request_interval_ms,
                "worker_stats_refresh_interval_ms": self.worker_stats_refresh_interval_ms,
            },
        )
        try:
            return await asyncio.wait_for(self._ready, timeout=timeout_seconds)
        except Exception:
            await self.close()
            raise

    async def next_event(self) -> dict[str, Any]:
        item = await self._events.get()
        if isinstance(item, BaseException):
            raise item
        return item

    async def quote_exact_input(
        self,
        *,
        request_id: str,
        pool_id: str,
        input_mint: str,
        output_mint: str,
        input_amount_raw: int,
        minimum_state_slot: int | None = None,
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("quote worker is not running")
        if not request_id or request_id in self._pending_quotes:
            raise ValueError("request_id must be non-empty and unique while pending")
        if input_amount_raw <= 0:
            raise ValueError("input_amount_raw must be positive")
        protocol = self._protocol_by_pool_id.get(pool_id)
        if protocol is None:
            raise ValueError("pool_id is not configured in the local quote worker")
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_quotes[request_id] = future
        try:
            message: dict[str, object] = {
                "type": "quote_request",
                "request_id": request_id,
                "protocol": protocol,
                "pool_id": pool_id,
                "input_mint": input_mint,
                "output_mint": output_mint,
                "input_amount_raw": str(input_amount_raw),
            }
            if minimum_state_slot is not None:
                if minimum_state_slot < 0:
                    raise ValueError("minimum_state_slot must be non-negative")
                message["minimum_state_slot"] = minimum_state_slot
            await self._send(message)
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_quotes.pop(request_id, None)

    async def capture_snapshot(
        self,
        *,
        request_id: str,
        pool_ids: Sequence[str],
        required_consistency: str = "validated_multi_account_snapshot",
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("quote worker is not running")
        if not request_id or request_id in self._pending_simulations:
            raise ValueError("request_id must be non-empty and unique while pending")
        if len(self._pending_simulations) >= MAX_PENDING_SIMULATION_REQUESTS:
            raise RuntimeError("too many pending simulation requests")
        if not pool_ids or any(not pool_id for pool_id in pool_ids):
            raise ValueError("capture_snapshot requires at least one non-empty pool_id")
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_simulations[request_id] = future
        try:
            await self._send(
                {
                    "type": "snapshot_request",
                    "request_id": request_id,
                    "pool_ids": list(pool_ids),
                    "required_consistency": required_consistency,
                },
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_simulations.pop(request_id, None)

    async def simulate_path(
        self,
        *,
        request_id: str,
        snapshot_token: str,
        legs: Sequence[Mapping[str, object]],
        initial_balances: Sequence[Mapping[str, object]],
        deadline_monotonic_ns: int | str | None = None,
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("quote worker is not running")
        if not request_id or request_id in self._pending_simulations:
            raise ValueError("request_id must be non-empty and unique while pending")
        if len(self._pending_simulations) >= MAX_PENDING_SIMULATION_REQUESTS:
            raise RuntimeError("too many pending simulation requests")
        if not snapshot_token or not legs:
            raise ValueError("simulate_path requires a snapshot_token and non-empty legs")
        balances = _canonical_initial_balances(initial_balances)
        deadline = (
            None
            if deadline_monotonic_ns is None
            else _canonical_decimal_string(
                deadline_monotonic_ns,
                field="deadline_monotonic_ns",
            )
        )
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_simulations[request_id] = future
        try:
            await self._send(
                {
                    "type": "simulate_path_request",
                    "request_id": request_id,
                    "snapshot_token": snapshot_token,
                    "legs": list(legs),
                    "initial_balances": balances,
                    **({} if deadline is None else {"deadline_monotonic_ns": deadline}),
                },
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_simulations.pop(request_id, None)

    async def cancel_simulation(self, *, request_id: str, timeout_seconds: float = 5.0) -> dict[str, Any]:
        if not self.running:
            raise RuntimeError("quote worker is not running")
        if not request_id or request_id in self._pending_cancellations:
            raise ValueError("request_id must be non-empty")
        if len(self._pending_cancellations) >= MAX_PENDING_SIMULATION_REQUESTS:
            raise RuntimeError("too many pending simulation cancellations")
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_cancellations[request_id] = future
        try:
            await self._send(
                {
                    "type": "cancel_simulation",
                    "request_id": request_id,
                },
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_cancellations.pop(request_id, None)

    async def request_worker_stats(
        self, *, request_id: str, timeout_seconds: float = 5.0
    ) -> dict[str, Any]:
        """Send a worker_stats_request and await the result."""
        if not self.running:
            raise RuntimeError("quote worker is not running")
        if not request_id or request_id in self._pending_simulations:
            raise ValueError("request_id must be non-empty and unique while pending")
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_simulations[request_id] = future
        try:
            await self._send(
                {
                    "type": "worker_stats_request",
                    "request_id": request_id,
                },
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_simulations.pop(request_id, None)

    async def export_evidence(
        self,
        *,
        request_id: str,
        snapshot_token: str,
        timeout_seconds: float = 10.0,
    ) -> dict[str, Any]:
        """Export the captured snapshot bundle as immutable replay evidence."""

        if not self.running:
            raise RuntimeError("quote worker is not running")
        if (
            not request_id
            or not snapshot_token
            or request_id in self._pending_simulations
        ):
            raise ValueError("export_evidence requires a request_id and snapshot_token")
        if len(self._pending_simulations) >= MAX_PENDING_SIMULATION_REQUESTS:
            raise RuntimeError("too many pending simulation requests")
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending_simulations[request_id] = future
        try:
            await self._send(
                {
                    "type": "export_simulation_evidence",
                    "request_id": request_id,
                    "snapshot_token": snapshot_token,
                },
            )
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            self._pending_simulations.pop(request_id, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        process = self._process
        if process is not None and process.returncode is None:
            with contextlib.suppress(Exception):
                await self._send({"type": "shutdown"})
            try:
                await asyncio.wait_for(process.wait(), timeout=3)
            except TimeoutError:
                process.terminate()
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(process.wait(), timeout=3)
        for task in (self._stdout_task, self._stderr_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(task for task in (self._stdout_task, self._stderr_task) if task is not None),
            return_exceptions=True,
        )
        failure = RuntimeError("quote worker closed")
        for future in self._pending_quotes.values():
            if not future.done():
                future.set_exception(failure)
        self._pending_quotes.clear()
        for future in self._pending_simulations.values():
            if not future.done():
                future.set_exception(failure)
        self._pending_simulations.clear()
        for future in self._pending_cancellations.values():
            if not future.done():
                future.set_exception(failure)
        self._pending_cancellations.clear()
        self._latest_worker_stats = None

    async def _send(self, payload: Mapping[str, object]) -> None:
        process = self._process
        if process is None or process.stdin is None or process.returncode is not None:
            raise RuntimeError("quote worker stdin is unavailable")
        process.stdin.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        await process.stdin.drain()

    async def _read_stdout(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            return
        try:
            while line := await process.stdout.readline():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError as exc:
                    pending = self._fail(RuntimeError(f"quote worker emitted invalid JSON: {exc}"))
                    if pending is not None:
                        await pending
                    return
                if not isinstance(message, dict):
                    pending = self._fail(RuntimeError("quote worker emitted a non-object JSON message"))
                    if pending is not None:
                        await pending
                    return
                pending = self._dispatch(message)
                if pending is not None:
                    await pending
            if not self._closed:
                return_code = await process.wait()
                suffix = f"; stderr: {' | '.join(self._stderr_tail[-12:])}" if self._stderr_tail else ""
                pending = self._fail(RuntimeError(f"quote worker exited with code {return_code}{suffix}"))
                if pending is not None:
                    await pending
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            pending = self._fail(RuntimeError(f"quote worker stdout reader failed: {_redact_urls(str(exc))}"))
            if pending is not None:
                await pending

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

    def _dispatch(self, message: dict[str, Any]) -> asyncio.Task[None] | None:
        kind = message.get("type")
        if kind == "ready":
            if self._ready is not None and not self._ready.done():
                self._ready.set_result(message)
            return
        if kind == "quote_result":
            request_id = message.get("request_id")
            if not isinstance(request_id, str):
                self._fail(RuntimeError("quote worker result has no request_id"))
                return
            future = self._pending_quotes.get(request_id)
            if future is not None and not future.done():
                future.set_result(message)
            else:
                # asyncio.wait_for cancels a future on timeout.  A result that
                # arrives afterwards is obsolete quote work, not a pool-state
                # event and not a reason to restart every Solana subscription.
                self._late_quote_results_dropped += 1
            return
        if kind == "cancel_simulation_result":
            request_id = message.get("request_id")
            if not isinstance(request_id, str):
                self._fail(RuntimeError("quote worker cancellation result has no request_id"))
                return
            cancellation_future = self._pending_cancellations.get(request_id)
            if cancellation_future is not None and not cancellation_future.done():
                cancellation_future.set_result(message)
                simulation_future = self._pending_simulations.get(request_id)
                if simulation_future is not None and not simulation_future.done():
                    # Cooperative cancellation must also settle the original
                    # simulation waiter; a later complete result is obsolete.
                    simulation_future.set_result({
                        "type": "simulate_path_result",
                        "request_id": request_id,
                        "status": "canceled",
                        "reason": "simulation canceled",
                        "complete": False,
                        "canceled": True,
                    })
            else:
                self._late_simulation_results_dropped += 1
            return
        if kind in {"snapshot_result", "simulate_path_result", "simulation_evidence_result", "worker_stats_result"}:
            request_id = message.get("request_id")
            if not isinstance(request_id, str):
                self._fail(RuntimeError(f"quote worker {kind} has no request_id"))
                return
            future = self._pending_simulations.get(request_id)
            if future is not None and not future.done():
                future.set_result(message)
            else:
                self._late_simulation_results_dropped += 1
            return
        if kind == "worker_error":
            error = _redact_urls(str(message.get("error", "unknown worker error")))
            failure = RuntimeError(f"local quote worker error: {error}")
            request_id = message.get("request_id")
            if isinstance(request_id, str):
                cancellation_future = self._pending_cancellations.get(request_id)
                if cancellation_future is not None and not cancellation_future.done():
                    cancellation_future.set_exception(failure)
                    return
                simulation_future = self._pending_simulations.get(request_id)
                if simulation_future is not None and not simulation_future.done():
                    simulation_future.set_exception(failure)
                    return
                future = self._pending_quotes.get(request_id)
                if future is not None and not future.done():
                    future.set_exception(failure)
                else:
                    if request_id not in self._pending_quotes:
                        self._late_quote_errors_dropped += 1
                if simulation_future is None:
                    self._late_simulation_results_dropped += 1
                return
            if self._ready is not None and not self._ready.done():
                self._ready.set_exception(failure)
                return
            return self._put_event(failure)
        if kind == "worker_stats":
            # The full stats dict is emitted directly via emitState.
            # Store only the latest validated stats (must contain memory key).
            if isinstance(message.get("memory"), dict):
                self._latest_worker_stats = message
            return
        return self._put_event(message)

    def _fail(self, error: BaseException) -> asyncio.Task[None] | None:
        if self._ready is not None and not self._ready.done():
            self._ready.set_exception(error)
        # Set request futures before applying event-queue backpressure so a
        # saturated unsolicited-event path cannot delay request failure.
        for future in self._pending_quotes.values():
            if not future.done():
                future.set_exception(error)
        for future in self._pending_simulations.values():
            if not future.done():
                future.set_exception(error)
        return self._put_event(error)

    def _put_event(
        self, item: dict[str, Any] | BaseException
    ) -> asyncio.Task[None] | None:
        if self._events.try_put(item):
            return None
        # Only saturation of a new distinct key gets here. Scheduling instead
        # of returning a bare coroutine preserves the old synchronous dispatch
        # API for tests/callers while the stdout reader can await this Task to
        # apply real bounded backpressure.
        return asyncio.create_task(self._events.put(item))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only one-shot local Raydium SDK quote probe")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--input-mint", required=True)
    parser.add_argument("--output-mint", required=True)
    parser.add_argument("--input-amount-raw", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    return parser


async def _run_probe(args: argparse.Namespace) -> dict[str, Any]:
    # Delayed import avoids a scanner<->client import cycle.
    from market_data_lab.solana_realtime_scanner import load_solana_scanner_config

    config = load_solana_scanner_config(args.config)
    pool = next((item for item in config.pools if item.pool_id == args.pool_id), None)
    if pool is None:
        raise ValueError("--pool-id must be configured under [[solana.raydium_clmm_pool]]")
    worker = RaydiumLocalQuoteWorker(
        rpc_http_url=config.rpc_http_url,
        rpc_ws_url=config.rpc_ws_url,
        pools=(QuoteWorkerPool(pool.pool_id, pool.label),),
    )
    try:
        ready = await worker.start(timeout_seconds=args.timeout_seconds)
        quote = await worker.quote_exact_input(
            request_id="probe",
            pool_id=args.pool_id,
            input_mint=args.input_mint,
            output_mint=args.output_mint,
            input_amount_raw=args.input_amount_raw,
            timeout_seconds=args.timeout_seconds,
        )
        return {"ready": ready, "quote": quote, "worker": worker.safe_descriptor()}
    finally:
        await worker.close()


def main() -> None:
    args = build_argument_parser().parse_args()
    if args.input_amount_raw <= 0 or args.timeout_seconds <= 0:
        raise SystemExit("--input-amount-raw and --timeout-seconds must be positive")
    try:
        print(json.dumps(asyncio.run(_run_probe(args)), ensure_ascii=False), flush=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise SystemExit(f"local Raydium quote probe failed: {_redact_urls(str(exc))}") from exc


if __name__ == "__main__":
    main()
