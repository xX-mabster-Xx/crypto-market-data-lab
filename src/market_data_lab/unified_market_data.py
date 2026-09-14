"""Source-neutral, read-only market-data collector.

This is the composition root for the scanner's data plane.  A public CEX book,
a Solana pool state update, and a perpetual DEX update all become
``MarketEvent`` objects in one bounded memory store.  The optional cycle
analyzer is an in-process consumer of that state; it does not own or duplicate
venue connections merely to define a route.

Nothing in this module has a wallet, exchange private API, order, transfer, or
transaction capability.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any

from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import build_cycle_providers
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import JupiterProvider
from market_data_lab.dex_quotes import OmnistonProvider
from market_data_lab.dex_quotes import RaydiumProvider
from market_data_lab.dex_quotes import StonFiProvider
from market_data_lab.dex_quotes import UniswapV3Provider
from market_data_lab.perp_venue_feeds import DEFAULT_HYPERLIQUID_COINS
from market_data_lab.perp_venue_feeds import build_perp_venue_sources
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.polling_quote_sources import PollingDexQuoteSource
from market_data_lab.polling_quote_sources import QuoteRoundInput
from market_data_lab.quote_broker import QuoteBroker
from market_data_lab.quote_broker import QuoteBudgetPolicy
from market_data_lab.quote_broker import SharedQuoteBudgetManager
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import RealtimeScanner
from market_data_lab.realtime_scanner import SourceEpochChange
from market_data_lab.solana_realtime_scanner import SolanaScannerConfig
from market_data_lab.solana_realtime_scanner import CexBookStateSource
from market_data_lab.solana_realtime_scanner import RaydiumLocalQuoteStateSource
from market_data_lab.solana_realtime_scanner import build_local_route_evaluator
from market_data_lab.solana_realtime_scanner import build_solana_market_sources
from market_data_lab.triangle_cycle_monitor import TRIANGLE_MARKETS
from market_data_lab.triangle_cycle_monitor import build_triangle_providers
from market_data_lab.triangle_cycle_monitor import cex_symbol
from market_data_lab.unified_cycle_analyzer import UnifiedCycleAnalyzer
from market_data_lab.amm_simulation.backend import LazySnapshotSequentialSimulator
from market_data_lab.unified_perp_analyzer import UnifiedPerpAnalyzer


DEFAULT_EXACT_QUOTE_NOTIONALS = (Decimal("100"),)
_TRIANGLE_REFERENCE_NOTIONAL_USDT = Decimal("100")
_TRIANGLE_REFERENCE_CEX_PRIORITY = ("BYBIT", "BINANCE", "MEXC", "BITGET", "OKX")
_RAYDIUM_REQUEST_INTERVAL_SECONDS = 3.0
_STONFI_REQUEST_INTERVAL_SECONDS = 2.0
_OMNISTON_REQUEST_INTERVAL_SECONDS = 1.0
# The free Base RPC rejected the one-second combined quote cadence in the
# first wide live pass.  Base therefore gets its own conservative budget;
# Polygon remains independent because it did not show that transport limit.
_BASE_EVM_REQUEST_INTERVAL_SECONDS = 5.0
_POLYGON_EVM_REQUEST_INTERVAL_SECONDS = 1.0
# The unauthenticated Base endpoint has no dependable sustained quota.  Once
# it signals a quota breach, leave it alone for the remainder of a typical
# continuous run instead of retrying every default 15-minute cooldown.
_BASE_UNAUTHENTICATED_RATE_LIMIT_CIRCUIT_BREAKER_SECONDS = 24.0 * 60.0 * 60.0


def _quota_domain(provider: object) -> str:
    """Return the shared vendor/endpoint class behind one market adapter."""

    if isinstance(provider, JupiterProvider):
        return "vendor:jupiter:swap-v2"
    if type(provider) is RaydiumProvider:
        return "vendor:raydium:route-api-v2"
    if isinstance(provider, OmnistonProvider):
        return "vendor:stonfi:omniston-v1beta8"
    if isinstance(provider, StonFiProvider):
        return "vendor:stonfi:swap-simulation-v1"
    if isinstance(provider, UniswapV3Provider):
        market = getattr(provider, "market", None)
        return f"rpc:{getattr(market, 'chain', 'unknown')}:eth-call"
    return f"provider:{str(getattr(provider, 'name', 'unknown')).upper()}"


def _triangle_notional_suppliers(
    cex_sources: Sequence[CexBookStateSource],
) -> Mapping[str, Any]:
    """Size every cross-asset DEX input to roughly one common USDT notional.

    A cross pair such as WETH/WBTC cannot safely be quoted with a literal
    ``100`` WBTC input.  The supplier uses only an already-subscribed public
    CEX ask as a *sizing reference*, then the analyzer still walks current
    CEX depth for the actual cycle result.  No REST request or second CEX
    connection is introduced.
    """

    spot_sources = {
        source.config.venue.upper(): source
        for source in cex_sources
        if source.config.category == "spot"
    }
    suppliers: dict[str, Any] = {}
    for market in TRIANGLE_MARKETS:
        def supplier(market: Any = market) -> tuple[QuoteRoundInput, ...]:
            for venue in _TRIANGLE_REFERENCE_CEX_PRIORITY:
                source = spot_sources.get(venue)
                if source is None:
                    continue
                book = source.latest_book(cex_symbol(market.quote, venue))
                if book is None or book.status != "ok" or not book.asks:
                    continue
                ask = book.asks[0][0]
                if ask <= 0:
                    continue
                amount = _TRIANGLE_REFERENCE_NOTIONAL_USDT / ask
                if amount > 0 and amount.is_finite():
                    return (
                        QuoteRoundInput(
                            amount=amount,
                            reference_notional_usdt=_TRIANGLE_REFERENCE_NOTIONAL_USDT,
                        ),
                    )
            return ()

        suppliers[market.provider] = supplier
    return suppliers


def _build_exact_quote_sources(
    config: SolanaScannerConfig,
    *,
    cex_sources: Sequence[CexBookStateSource],
) -> tuple[PollingDexQuoteSource, ...]:
    """Adapt existing public quote providers without their old cycle logic.

    This deliberately uses the full direct-stable and cross-asset universe.
    Public APIs with a shared quota retain exactly one pacer across *both*
    groups, so adding a triangle cannot accidentally double the request rate.
    """

    raydium_pacer = AsyncRequestPacer(_RAYDIUM_REQUEST_INTERVAL_SECONDS)
    jupiter_pacer = AsyncRequestPacer(config.jupiter.minimum_request_interval_seconds)
    # These pacers are passed into provider request boundaries.  The polling
    # source may use the same instances for shared cooldowns, but a round-level
    # wait would not protect providers that issue separate buy/sell requests.
    stonfi_pacer = AsyncRequestPacer(_STONFI_REQUEST_INTERVAL_SECONDS)
    omniston_pacer = AsyncRequestPacer(_OMNISTON_REQUEST_INTERVAL_SECONDS)
    base_evm_pacer = AsyncRequestPacer(_BASE_EVM_REQUEST_INTERVAL_SECONDS)
    polygon_evm_pacer = AsyncRequestPacer(_POLYGON_EVM_REQUEST_INTERVAL_SECONDS)

    def evm_pacer_for_chain(chain: str) -> AsyncRequestPacer:
        return base_evm_pacer if chain == "base" else polygon_evm_pacer

    direct = build_cycle_providers(
        tuple(MARKETS),
        base_rpc_url=config.evm.base_rpc_http_url,
        polygon_rpc_url=config.evm.polygon_rpc_http_url,
        fee_tiers=(100, 500, 3000),
        proxy_url=config.proxy_url,
        timeout_seconds=config.timeout_seconds,
        raydium_slippage_bps=50,
        stonfi_slippage_tolerance=Decimal("0.005"),
        raydium_min_request_interval_seconds=_RAYDIUM_REQUEST_INTERVAL_SECONDS,
        raydium_request_pacer=raydium_pacer,
        evm_request_pacer={"base": base_evm_pacer, "polygon": polygon_evm_pacer},
        stonfi_request_pacer=stonfi_pacer,
        omniston_request_pacer=omniston_pacer,
        jupiter_api_key=config.jupiter.api_key,
        jupiter_min_request_interval_seconds=config.jupiter.minimum_request_interval_seconds,
        jupiter_request_pacer=jupiter_pacer,
        omniston_quote_selection_window_seconds=0.5,
        omniston_max_price_slippage_bps=50,
        omniston_max_routes=4,
        omniston_allow_risky_routes=False,
    )
    triangles = build_triangle_providers(
        TRIANGLE_MARKETS,
        base_rpc_url=config.evm.base_rpc_http_url,
        polygon_rpc_url=config.evm.polygon_rpc_http_url,
        fee_tiers=(100, 500, 3000),
        proxy_url=config.proxy_url,
        timeout_seconds=config.timeout_seconds,
        raydium_slippage_bps=50,
        raydium_min_request_interval_seconds=_RAYDIUM_REQUEST_INTERVAL_SECONDS,
        stonfi_slippage_tolerance=Decimal("0.005"),
        raydium_request_pacer=raydium_pacer,
        evm_request_pacer={"base": base_evm_pacer, "polygon": polygon_evm_pacer},
        stonfi_request_pacer=stonfi_pacer,
    )

    def external_pacer(provider: object) -> AsyncRequestPacer | None:
        if isinstance(provider, StonFiProvider):
            return stonfi_pacer
        if isinstance(provider, OmnistonProvider):
            return omniston_pacer
        if isinstance(provider, UniswapV3Provider):
            market = getattr(provider, "market", None)
            return evm_pacer_for_chain(getattr(market, "chain", "unknown"))
        return None

    def rate_limit_circuit_breaker_events(provider: object) -> int | None:
        if isinstance(provider, UniswapV3Provider):
            market = getattr(provider, "market", None)
            if getattr(market, "chain", None) == "base":
                # Base's unauthenticated public endpoint is explicitly not a
                # production RPC.  Three quota signals are enough evidence to
                # stop probing it until an authenticated URL is configured.
                return 3
        return None

    def rate_limit_circuit_breaker_seconds(provider: object) -> float:
        if isinstance(provider, UniswapV3Provider):
            market = getattr(provider, "market", None)
            if getattr(market, "chain", None) == "base":
                return _BASE_UNAUTHENTICATED_RATE_LIMIT_CIRCUIT_BREAKER_SECONDS
        return 900.0

    def terminal_route_circuit_breaker_events(provider: object) -> int | None:
        # A STON.fi "no pool" / "insufficient liquidity" answer is a route
        # property rather than an endpoint outage.  Keep it visible in status,
        # then stop repeatedly asking for the same unavailable exact quote.
        return 2 if isinstance(provider, StonFiProvider) else None

    providers = {**direct, **triangles}
    triangle_suppliers = _triangle_notional_suppliers(cex_sources)
    sources = tuple(
        PollingDexQuoteSource(
            provider=provider,
            notionals=DEFAULT_EXACT_QUOTE_NOTIONALS,
            notional_supplier=triangle_suppliers.get(provider_name),
           shared_quota_pacer=external_pacer(provider),
            rate_limit_circuit_breaker_events=rate_limit_circuit_breaker_events(provider),
            rate_limit_circuit_breaker_seconds=rate_limit_circuit_breaker_seconds(provider),
            terminal_route_circuit_breaker_events=terminal_route_circuit_breaker_events(provider),
            quota_domain=_quota_domain(provider),
        )
        for provider_name, provider in sorted(providers.items())
    )
    names = [source.name for source in sources]
    if len(set(names)) != len(names):
        raise ValueError("exact-quote source names must be unique")
    return sources


def build_unified_market_data_scanner(
    *,
    config: SolanaScannerConfig,
    output_directory: Path,
    hyperliquid_coins: Sequence[str] = DEFAULT_HYPERLIQUID_COINS,
) -> RealtimeScanner:
    """Build the shared raw quote/state bus for currently implemented feeds."""

    spot_sources, local_quote_source = build_solana_market_sources(config)
    perp_sources = build_perp_venue_sources(
        hyperliquid_coins=hyperliquid_coins,
        solana_rpc_http_url=config.rpc_http_url,
        solana_rpc_ws_url=config.rpc_ws_url,
        proxy_url=config.proxy_url,
        timeout_seconds=config.timeout_seconds,
    )
    cex_sources = tuple(
        source for source in spot_sources if isinstance(source, CexBookStateSource)
    )
    quote_sources = _build_exact_quote_sources(config, cex_sources=cex_sources)
    sources = (*spot_sources, *perp_sources, *quote_sources)
    analyzer = UnifiedCycleAnalyzer(
        output_directory=output_directory,
        cex_sources=cex_sources,
        fee_audit_file=config.local_route_evaluator.fee_audit_file,
    )
    sequential_amm_simulator = None
    if config.amm_simulation.enabled:
        # This delivery deliberately has one allowlisted protocol and one
        # configured pool.  A flag without a validated CPMM composition is a
        # configuration error, never an excuse to fall back to a remote quote
        # as post-trade state.
        if config.sequential_amm_pool is None:
            raise ValueError(
                "amm_simulation.enabled requires one sequential_amm_pool for the CPMM vertical slice",
            )
        if config.amm_simulation.allowed_protocols != ("raydium_cpmm",):
            raise ValueError(
                "CPMM vertical slice requires amm_simulation.allowed_protocols = ['raydium_cpmm']",
            )
        standard_pool = next(
            (
                pool for pool in config.raydium_standard_pools
                if pool.pool_id == config.sequential_amm_pool.pool_id
                and pool.protocol == "raydium_cpmm"
            ),
            None,
        )
        if standard_pool is None:
            raise ValueError(
                "sequential_amm_pool must refer to an allowlisted raydium_cpmm standard pool",
            )
        if not isinstance(local_quote_source, RaydiumLocalQuoteStateSource):
            raise ValueError(
                "amm_simulation.enabled requires the managed local TypeScript worker",
            )
        sequential_amm_simulator = LazySnapshotSequentialSimulator(
            source=local_quote_source,
            pool_id=config.sequential_amm_pool.pool_id,
            stable_asset_id=config.sequential_amm_pool.stable_asset_id,
            base_asset_id=config.sequential_amm_pool.base_asset_id,
            stable_decimals=config.sequential_amm_pool.stable_decimals,
            perp_symbol=config.sequential_amm_pool.perp_symbol,
            initialize_timeout_seconds=config.timeout_seconds,
        )

    perp_analyzer = UnifiedPerpAnalyzer(
        output_directory=output_directory,
        fee_audit_file=config.local_route_evaluator.fee_audit_file,
        sequential_amm_simulator=sequential_amm_simulator,
    )
    quote_source_by_name = {source.name: source for source in quote_sources}
    quote_budget_domains = {
        source.quota_domain
        for source in quote_sources
        if source.quota_domain is not None
    }
    quote_broker = QuoteBroker(
        # Existing adapters remain the only live network callers in this
        # migration slice.  Their normalized observations seed one shared
        # cache; no new endpoint or unverified quota is activated here.
        backends={},
        budgets=SharedQuoteBudgetManager(
            {
                domain: QuoteBudgetPolicy(max_concurrency=1)
                for domain in sorted(quote_budget_domains)
            },
        ),
    )

    sequential_initialized = False
    sequential_task: asyncio.Task[None] | None = None

    async def sequential_lifecycle() -> None:
        """Keep capture/retry/expiry work off the shared event consumer."""

        assert isinstance(sequential_amm_simulator, LazySnapshotSequentialSimulator)
        while True:
            await sequential_amm_simulator.initialize()
            status = sequential_amm_simulator.describe()
            expiry_ns = status.get("state_valid_until_monotonic_ns")
            retry_ns = status.get("next_retry_monotonic_ns")
            monotonic_now_ns = int(asyncio.get_running_loop().time() * 1_000_000_000)
            waits_ns = [
                value - monotonic_now_ns
                for value in (expiry_ns, retry_ns)
                if isinstance(value, int) and value > monotonic_now_ns
            ]
            delay = (min(waits_ns) / 1_000_000_000) if waits_ns else 0.05
            await asyncio.sleep(max(0.01, min(delay, 3_600.0)))

    def initialize_sequential() -> None:
        nonlocal sequential_initialized, sequential_task
        if sequential_amm_simulator is None:
            return
        if isinstance(sequential_amm_simulator, LazySnapshotSequentialSimulator):
            if sequential_task is not None and not sequential_task.done():
                return
            if not sequential_amm_simulator.needs_refresh():
                sequential_initialized = True
                return
            # Capture/refresh runs out of band.  A slow RPC/worker must not
            # stall the shared event bus or delay legacy analyzers.
            sequential_task = asyncio.create_task(sequential_lifecycle())
            sequential_initialized = True

    route_evaluator = None

    async def handle_event(event: MarketEvent) -> None:
        # Initialize the sequential simulator lazily after the worker starts.
        initialize_sequential()

        # Both consumers run from the same collector bus.  They keep only
        # bounded latest state and never create strategy-specific feeds.
        shared_event = event
        if event.kind == "exact_input_quote" and isinstance(event.value, ExactInputQuote):
            quote = replace(event.value, source_epoch=event.source_epoch)
            shared_event = replace(event, value=quote)
            source = quote_source_by_name.get(event.source)
            if source is not None:
                result = source.broker_result(quote)
                if result is not None:
                    quote_broker.observe_result(result)
        await analyzer.handle_event(shared_event)
        await perp_analyzer.handle_event(shared_event)
        if route_evaluator is not None:
            await route_evaluator.handle_event(shared_event)

    status_providers: dict[str, Any] = {}
    for source in (*perp_sources, *quote_sources):
        provider = getattr(source, "status", None)
        if callable(provider):
            status_providers[source.name] = provider
    status_providers["cycle_analysis"] = analyzer.snapshot
    status_providers["perp_analysis"] = perp_analyzer.snapshot
    status_providers["quote_broker"] = quote_broker.snapshot
    if sequential_amm_simulator is not None:
        status_providers["sequential_amm"] = sequential_amm_simulator.describe
    async def shutdown_sequential() -> None:
        if sequential_task is not None and not sequential_task.done():
            sequential_task.cancel()
            await asyncio.gather(sequential_task, return_exceptions=True)

    scanner = RealtimeScanner(
        sources=sources,
        output_directory=output_directory,
        retention_seconds=config.retention_seconds,
        # dYdX can publish faster than a thousand updates per second.  The
        # live bus sees every update; this only bounds the retrospective RAM
        # window to 50 Hz per key, independently of source type.
        max_events_per_key=max(config.max_events_per_key, 9_000),
        history_minimum_interval_ms=20.0,
        event_bus_capacity=max(config.event_bus_capacity, 16_384),
        max_state_keys=getattr(config, "max_state_keys", 65_536),
        status_flush_seconds=config.status_flush_seconds,
        event_handler=handle_event,
    )
    route_evaluator = build_local_route_evaluator(
        config,
        store=scanner.store,
        quote_source=local_quote_source,
        cex_sources=cex_sources,
        output_directory=output_directory,
    )

    async def handle_source_epoch(change: SourceEpochChange) -> None:
        # Full depth must disappear before analyzers/evaluator can observe the
        # first market event in the replacement transport session.
        for source in cex_sources:
            source.handle_source_epoch(change)
        analyzer.handle_source_epoch_change(change)
        perp_analyzer.handle_source_epoch_change(change)
        if route_evaluator is not None:
            await route_evaluator.handle_source_epoch(change)

    if route_evaluator is not None:
        status_providers["local_route_evaluator"] = route_evaluator.snapshot
    scanner.status_providers = status_providers
    scanner.shutdown_handlers = (
        shutdown_sequential,
        analyzer.close,
        perp_analyzer.close,
        quote_broker.close,
        *((route_evaluator.close,) if route_evaluator is not None else ()),
    )
    scanner.epoch_change_handlers = (handle_source_epoch,)
    requested = bool(getattr(config.local_route_evaluator, "enabled", False))
    scanner.runtime_components = {
        "local_route_evaluator_requested": requested,
        "local_route_evaluator_attached": route_evaluator is not None,
        "local_route_count": (
            len(route_evaluator.config.routes) if route_evaluator is not None else 0
        ),
        "local_quote_worker_attached": isinstance(
            local_quote_source,
            RaydiumLocalQuoteStateSource,
        ),
    }
    if requested and getattr(config.local_route_evaluator, "routes", ()) and route_evaluator is None:
        raise RuntimeError("local route evaluator was requested but not attached")
    return scanner


async def record_unified_market_data(
    *,
    config: SolanaScannerConfig,
    output_directory: Path,
    duration_seconds: float | None,
    hyperliquid_coins: Sequence[str] = DEFAULT_HYPERLIQUID_COINS,
) -> Mapping[str, Any]:
    """Run one data-only supervisor for spot, CEX, and perpetual DEX feeds."""

    scanner = build_unified_market_data_scanner(
        config=config,
        output_directory=output_directory,
        hyperliquid_coins=hyperliquid_coins,
    )
    return await scanner.run(duration_seconds=duration_seconds)
