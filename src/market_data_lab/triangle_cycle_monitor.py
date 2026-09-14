"""Continuous public CEX--DEX--CEX triangle reconnaissance.

The monitor prices a direct DEX cross pair ``A/B`` and closes each direction
through two public CEX spot books, ``A/USDT`` and ``B/USDT`` on the *same*
venue.  It is intentionally a research recorder: it never accepts exchange
credentials, a wallet, a private key, or transaction payloads.

The broad default universe has 95 direct pairs: 78 Raydium/Solana, 15
STON.fi/TON and two Uniswap reference pairs.  CEX books are event-driven and
kept only in a short in-memory history.  DEX quote calls are globally paced per
public source, and disk retention is limited to compact candidate lifecycles,
aggregate statistics and a bounded error ledger.

A positive row is not a trade instruction or proof of atomic arbitrage.  In
particular, this screen excludes withdrawals, deposits, bridge/wrapper basis,
rebalance, inclusion, inventory and capital costs.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.account_fee_audit import resolve_spot_fee_rate
from market_data_lab.cex_book_streams import PublicBookStream
from market_data_lab.cex_book_streams import build_public_book_stream
from market_data_lab.cex_book_streams import stream_health
from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CEX_BOOK_ENDPOINTS
from market_data_lab.cex_dex_cycles import DEFAULT_NETWORK_COST_FLOORS
from market_data_lab.cex_dex_cycles import _best_dex_records
from market_data_lab.continuous_cycle_monitor import ContinuousStatistics
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import Asset
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import RaydiumProvider
from market_data_lab.dex_quotes import SOLANA_PROVIDER_BASES
from market_data_lab.dex_quotes import StonFiProvider
from market_data_lab.dex_quotes import TON_PROVIDER_BASES
from market_data_lab.dex_quotes import UniswapV3Provider
from market_data_lab.dex_quotes import _decimal_text
from market_data_lab.dex_quotes import evm_markets
from market_data_lab.execution_cost import FeeCurrency
from market_data_lab.execution_cost import cost_to_acquire
from market_data_lab.execution_cost import proceeds_from_sell
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.rolling_cycle_monitor import CandidateTracker
from market_data_lab.rolling_cycle_monitor import DEFAULT_CEX_TAKER_FEES


@dataclass(frozen=True)
class TriangleAsset:
    """A chain asset and the public CEX ticker used as its hedge proxy."""

    symbol: str
    asset: Asset
    cex_symbol: str
    equivalence: str


@dataclass(frozen=True)
class TriangleMarket:
    """One direct DEX pair that can be closed via same-venue CEX USDT legs."""

    name: str
    provider: str
    provider_kind: str
    chain: str
    dex_pair: str
    base: TriangleAsset
    quote: TriangleAsset
    asset_equivalence: str


def _solana_cex_symbol(symbol: str) -> str:
    return "BTC" if symbol == "cbBTC" else symbol


def _solana_equivalence(symbol: str) -> str:
    if symbol == "cbBTC":
        return "Solana cbBTC is hedged with CEX BTC; wrapper/redemption basis is excluded"
    if symbol == "SOL":
        return "wrapped SOL representation on DEX versus native SOL ticker on CEX"
    return "canonical SPL mint versus matching CEX ticker; deposit/withdraw/rebalance is not verified"


def _ton_equivalence(symbol: str) -> str:
    if symbol == "GRAM":
        return "native TON asset displayed as GRAM versus matching CEX ticker"
    return "canonical TON jetton master versus matching CEX ticker; rebalancing is not verified"


def _triangle_asset(asset: Asset, *, cex_symbol: str, equivalence: str) -> TriangleAsset:
    return TriangleAsset(
        symbol=asset.symbol,
        asset=asset,
        cex_symbol=cex_symbol,
        equivalence=equivalence,
    )


def build_triangle_markets() -> tuple[TriangleMarket, ...]:
    """Build the bounded 95-pair cross-asset universe.

    The ordering deliberately stays stable.  A fixed list makes a run's
    manifest comparable with the next run without persisting raw observations.
    """

    solana_assets = tuple(
        _triangle_asset(
            asset,
            cex_symbol=_solana_cex_symbol(asset.symbol),
            equivalence=_solana_equivalence(asset.symbol),
        )
        for asset in SOLANA_PROVIDER_BASES.values()
    )
    ton_assets = tuple(
        _triangle_asset(
            asset,
            cex_symbol=asset.symbol,
            equivalence=_ton_equivalence(asset.symbol),
        )
        for asset in TON_PROVIDER_BASES.values()
    )

    markets: list[TriangleMarket] = []
    for base, quote in itertools.combinations(solana_assets, 2):
        markets.append(
            TriangleMarket(
                name=f"SOLANA_{base.symbol}_{quote.symbol}_RAYDIUM",
                provider=f"RAYDIUM_CROSS_{base.symbol}_{quote.symbol}",
                provider_kind="raydium",
                chain="solana",
                dex_pair=f"{base.symbol}/{quote.symbol}",
                base=base,
                quote=quote,
                asset_equivalence=f"{base.equivalence}; {quote.equivalence}",
            ),
        )
    for base, quote in itertools.combinations(ton_assets, 2):
        markets.append(
            TriangleMarket(
                name=f"TON_{base.symbol}_{quote.symbol}_STONFI",
                provider=f"STONFI_CROSS_{base.symbol}_{quote.symbol}",
                provider_kind="stonfi",
                chain="ton",
                dex_pair=f"{base.symbol}/{quote.symbol}",
                base=base,
                quote=quote,
                asset_equivalence=f"{base.equivalence}; {quote.equivalence}",
            ),
        )

    # The EVM assets are taken from the same canonical configuration used by
    # the single-asset monitor.  Only their *quote* asset changes from USDC to
    # the direct cross-pair asset for the read-only Quoter call below.
    evm = evm_markets(
        base_rpc_url="https://mainnet-preconf.base.org",
        polygon_rpc_url="https://polygon.drpc.org",
        fee_tiers=(100, 500, 3000),
    )
    base_weth = _triangle_asset(
        evm["UNISWAP_BASE"].base,
        cex_symbol="ETH",
        equivalence="Base WETH versus CEX ETH; bridge and rebalance basis is excluded",
    )
    base_cbbtc = _triangle_asset(
        evm["UNISWAP_BASE_CBBTC"].base,
        cex_symbol="BTC",
        equivalence="Base cbBTC versus CEX BTC; wrapper/redemption basis is excluded",
    )
    polygon_weth = _triangle_asset(
        evm["UNISWAP_POLYGON_USDC"].base,
        cex_symbol="ETH",
        equivalence="Polygon PoS WETH versus CEX ETH; bridge/rebalance basis is excluded",
    )
    polygon_wbtc = _triangle_asset(
        evm["UNISWAP_POLYGON_WBTC"].base,
        cex_symbol="BTC",
        equivalence="Polygon PoS WBTC versus CEX BTC; bridge/redemption basis is excluded",
    )
    markets.extend(
        (
            TriangleMarket(
                name="BASE_WETH_cbBTC_UNISWAP",
                provider="UNISWAP_BASE_CROSS_WETH_cbBTC",
                provider_kind="uniswap_base",
                chain="base",
                dex_pair="WETH/cbBTC",
                base=base_weth,
                quote=base_cbbtc,
                asset_equivalence=f"{base_weth.equivalence}; {base_cbbtc.equivalence}",
            ),
            TriangleMarket(
                name="POLYGON_WETH_WBTC_UNISWAP",
                provider="UNISWAP_POLYGON_CROSS_WETH_WBTC",
                provider_kind="uniswap_polygon",
                chain="polygon",
                dex_pair="WETH/WBTC",
                base=polygon_weth,
                quote=polygon_wbtc,
                asset_equivalence=f"{polygon_weth.equivalence}; {polygon_wbtc.equivalence}",
            ),
        ),
    )
    return tuple(markets)


TRIANGLE_MARKETS = build_triangle_markets()
TRIANGLE_MARKET_BY_NAME = {market.name: market for market in TRIANGLE_MARKETS}
DEFAULT_TRIANGLE_MARKET_NAMES = tuple(market.name for market in TRIANGLE_MARKETS)


def cex_symbol(asset: TriangleAsset, venue: str) -> str:
    """Return the requested venue's USDT spot symbol for a hedge asset."""

    return f"{asset.cex_symbol}-USDT" if venue == "OKX" else f"{asset.cex_symbol}USDT"


def build_triangle_providers(
    markets: Sequence[TriangleMarket],
    *,
    base_rpc_url: str,
    polygon_rpc_url: str,
    fee_tiers: Sequence[int],
    proxy_url: str | None,
    timeout_seconds: float,
    raydium_slippage_bps: int,
    raydium_min_request_interval_seconds: float,
    stonfi_slippage_tolerance: Decimal,
    raydium_request_pacer: AsyncRequestPacer | None = None,
    evm_request_pacer: AsyncRequestPacer | Mapping[str, AsyncRequestPacer] | None = None,
    stonfi_request_pacer: AsyncRequestPacer | None = None,
    evm_min_request_interval_seconds: float = 0.6,
    stonfi_min_request_interval_seconds: float = 0.6,
) -> dict[str, DexQuoteProvider]:
    """Create one DEX quote provider per pair without multiplying a quota.

    Every provider family receives one shared request-start pacer.  Pacing is
    performed at the provider's actual HTTP/RPC/WS request boundary, so a
    logical buy/sell round cannot bypass the advertised quota interval.
    """

    configured_evm = evm_markets(
        base_rpc_url=base_rpc_url,
        polygon_rpc_url=polygon_rpc_url,
        fee_tiers=fee_tiers,
    )
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

    def evm_pacer_for(chain: str) -> AsyncRequestPacer | None:
        if isinstance(evm_request_pacer, Mapping):
            return evm_request_pacer.get(chain)
        return evm_request_pacer

    providers: dict[str, DexQuoteProvider] = {}
    for market in markets:
        if market.provider in providers:
            continue
        if market.provider_kind == "raydium":
            providers[market.provider] = RaydiumProvider(
                name=market.provider,
                base=market.base.asset,
                quote=market.quote.asset,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                slippage_bps=raydium_slippage_bps,
                request_pacer=raydium_pacer,
            )
        elif market.provider_kind == "stonfi":
            providers[market.provider] = StonFiProvider(
                name=market.provider,
                base=market.base.asset,
                quote=market.quote.asset,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                slippage_tolerance=stonfi_slippage_tolerance,
                request_pacer=stonfi_request_pacer,
            )
        elif market.provider_kind in {"uniswap_base", "uniswap_polygon"}:
            source = (
                configured_evm["UNISWAP_BASE"]
                if market.provider_kind == "uniswap_base"
                else configured_evm["UNISWAP_POLYGON_USDC"]
            )
            providers[market.provider] = UniswapV3Provider(
                replace(
                    source,
                    provider=market.provider,
                    base=market.base.asset,
                    quote=market.quote.asset,
                ),
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
                request_pacer=evm_pacer_for(source.chain),
            )
        else:
            raise ValueError(f"unsupported triangle provider kind: {market.provider_kind}")
    return providers


def _parse_market_names(value: str) -> list[str]:
    names = [item.strip() for item in value.split(",") if item.strip()]
    unknown = [item for item in names if item not in TRIANGLE_MARKET_BY_NAME]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown triangle markets: {', '.join(unknown)}")
    if not names:
        raise argparse.ArgumentTypeError("at least one triangle market is required")
    return list(dict.fromkeys(names))


def _parse_venues(value: str) -> list[str]:
    venues = [item.strip().upper() for item in value.split(",") if item.strip()]
    unknown = [item for item in venues if item not in CEX_BOOK_ENDPOINTS]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown CEX venues: {', '.join(unknown)}")
    if not venues:
        raise argparse.ArgumentTypeError("at least one CEX venue is required")
    return list(dict.fromkeys(venues))


def _parse_provider_kinds(value: str) -> list[str]:
    kinds = [item.strip().lower() for item in value.split(",") if item.strip()]
    available = {"raydium", "stonfi", "uniswap_base", "uniswap_polygon"}
    unknown = [item for item in kinds if item not in available]
    if unknown:
        raise argparse.ArgumentTypeError(f"unknown provider kinds: {', '.join(unknown)}")
    if not kinds:
        raise argparse.ArgumentTypeError("at least one provider kind is required")
    return list(dict.fromkeys(kinds))


def _parse_costs(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_NETWORK_COST_FLOORS)
    try:
        for item in value.split(","):
            chain, cost = item.strip().split("=", 1)
            parsed[chain.strip().lower()] = Decimal(cost)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("costs must look like solana=0.01,ton=0.1") from exc
    if any(cost < 0 or not cost.is_finite() for cost in parsed.values()):
        raise argparse.ArgumentTypeError("network costs must be finite and non-negative")
    return parsed


def _parse_fees(value: str) -> dict[str, Decimal]:
    parsed = dict(DEFAULT_CEX_TAKER_FEES)
    try:
        for item in value.split(","):
            venue, fee = item.strip().split("=", 1)
            venue = venue.upper()
            if venue not in parsed:
                raise ValueError(f"unknown venue {venue}")
            parsed[venue] = Decimal(fee)
    except (InvalidOperation, ValueError) as exc:
        raise argparse.ArgumentTypeError("fees must look like MEXC=5,BYBIT=10,OKX=10,BINANCE=10") from exc
    if any(fee < 0 or fee >= 10_000 or not fee.is_finite() for fee in parsed.values()):
        raise argparse.ArgumentTypeError("fees must be finite and in [0, 10000)")
    return parsed


def _provider_gate_key(market: TriangleMarket) -> str | None:
    if market.provider_kind == "stonfi":
        return "STONFI"
    if market.provider_kind == "uniswap_base":
        return "UNISWAP_BASE"
    if market.provider_kind == "uniswap_polygon":
        return "UNISWAP_POLYGON"
    # Raydium has an internal pacer shared by every pair provider.
    return None


def _utc_iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, UTC).isoformat()


class TriangleStatistics(ContinuousStatistics):
    """Continuous statistics with triangle-specific coverage diagnostics."""

    def __init__(self) -> None:
        super().__init__()
        self.dex_unavailable: Counter[str] = Counter()
        self.disabled_markets: dict[str, str] = {}
        self.reference_input_amounts: dict[str, str] = {}

    def snapshot(
        self,
        *,
        started_at: str,
        duration_wall_seconds: float,
        tracker: CandidateTracker,
        streams: Mapping[str, PublicBookStream],
    ) -> dict[str, Any]:
        snapshot = super().snapshot(
            started_at=started_at,
            duration_wall_seconds=duration_wall_seconds,
            tracker=tracker,
            streams=streams,
        )
        snapshot.update(
            schema_version=2,
            mode="event_driven_cex_dex_cex_triangle",
            dex_unavailable=dict(sorted(self.dex_unavailable.items())),
            disabled_markets=dict(sorted(self.disabled_markets.items())),
            reference_input_amounts=dict(sorted(self.reference_input_amounts.items())),
        )
        return snapshot


def _midpoint(book: BookSnapshot) -> Decimal | None:
    if book.status != "ok" or not book.bids or not book.asks:
        return None
    bid, ask = book.bids[0][0], book.asks[0][0]
    if bid <= 0 or ask <= 0 or bid >= ask:
        return None
    return (bid + ask) / Decimal(2)


async def _reference_inputs(
    assets: Sequence[TriangleAsset],
    streams: Mapping[str, PublicBookStream],
    venues: Sequence[str],
    *,
    reference_notional_usdt: Decimal,
    wait_seconds: float,
) -> dict[str, Decimal]:
    """Size each DEX input once from a contemporaneous public CEX midpoint.

    A quote API expects an asset quantity, not USDT.  The fixed reference input
    makes calls comparable while avoiding a separate CEX REST polling path.
    It does *not* freeze the later depth-walk valuation: each candidate still
    uses the exact CEX quantities in the returned DEX quote.
    """

    unique = {asset.cex_symbol: asset for asset in assets}
    inputs: dict[str, Decimal] = {}
    deadline = time.monotonic() + max(0.1, wait_seconds)
    while len(inputs) < len(unique) and time.monotonic() < deadline:
        now = time.time_ns()
        for cex_ticker, asset in unique.items():
            if cex_ticker in inputs:
                continue
            for venue in venues:
                stream = streams.get(venue)
                if stream is None:
                    continue
                book = stream.nearest_snapshot(cex_symbol(asset, venue), now)
                price = _midpoint(book) if book is not None else None
                if price is not None:
                    inputs[cex_ticker] = reference_notional_usdt / price
                    break
        if len(inputs) < len(unique):
            await asyncio.sleep(0.05)
    return inputs


def calculate_triangle_cycle(
    *,
    market: TriangleMarket,
    dex_record: Mapping[str, Any],
    base_book: BookSnapshot,
    quote_book: BookSnapshot,
    cex_taker_fee_bps: Decimal,
    network_cost_floor_usdt: Decimal,
    max_response_skew_ms: Decimal,
    reference_notional_usdt: Decimal,
    cex_venue: str,
    cex_buy_taker_fee_bps: Decimal | None = None,
    cex_sell_taker_fee_bps: Decimal | None = None,
    cex_buy_fee_source: str | None = None,
    cex_sell_fee_source: str | None = None,
    cex_buy_fee_account_verified: bool | None = None,
    cex_sell_fee_account_verified: bool | None = None,
    cex_buy_fee_assumptions: Sequence[str] = (),
    cex_sell_fee_assumptions: Sequence[str] = (),
    cex_buy_fee_currency: FeeCurrency = "base",
    cex_sell_fee_currency: FeeCurrency = "quote",
) -> dict[str, Any]:
    """Walk two CEX books around one DEX exact-input quote.

    ``buy_base`` is ``B -> A`` on the DEX, so the full loop is buy B on the
    CEX, swap B for A on the DEX, then sell A on the same CEX.  ``sell_base``
    reverses it.  The buy leg is enlarged before walking its CEX depth so that
    a base-asset-denominated taker fee still leaves the exact DEX input.
    """

    direction = str(dex_record["direction"])
    base_amount = Decimal(str(dex_record["base_amount"]))
    quote_amount = Decimal(str(dex_record["quote_amount"]))
    buy_fee_bps = cex_buy_taker_fee_bps if cex_buy_taker_fee_bps is not None else cex_taker_fee_bps
    sell_fee_bps = cex_sell_taker_fee_bps if cex_sell_taker_fee_bps is not None else cex_taker_fee_bps
    for label, fee_bps in (("buy", buy_fee_bps), ("sell", sell_fee_bps)):
        if not fee_bps.is_finite() or fee_bps < 0 or fee_bps >= Decimal(10_000):
            raise ValueError(
                f"CEX {label} taker fee must be finite, non-negative, and below 10000 bps"
            )
    if base_amount <= 0 or quote_amount <= 0:
        raise ValueError("DEX quote amounts must be positive")

    if direction == "buy_base":
        cycle_direction = "buy_cex_quote_dex_sell_cex_base"
        buy_asset, buy_book, buy_amount = market.quote, quote_book, quote_amount
        sell_asset, sell_book, sell_amount = market.base, base_book, base_amount
    elif direction == "sell_base":
        cycle_direction = "buy_cex_base_sell_dex_sell_cex_quote"
        buy_asset, buy_book, buy_amount = market.base, base_book, base_amount
        sell_asset, sell_book, sell_amount = market.quote, quote_book, quote_amount
    else:
        raise ValueError(f"unsupported DEX direction: {direction}")

    buy_execution = cost_to_acquire(
        buy_amount,
        buy_book.asks,
        fee_bps=buy_fee_bps,
        fee_currency=cex_buy_fee_currency,
        base_currency=buy_asset.cex_symbol,
        quote_currency="USDT",
        fee_source=cex_buy_fee_source or "configured_cex_taker_fee",
        fee_quality=(
            "account_verified"
            if cex_buy_fee_account_verified is True
            else (
                "public_unverified"
                if cex_buy_fee_account_verified is False
                else "unknown"
            )
        ),
        state_version=(
            f"{buy_book.source}:update={buy_book.update_id}:"
            f"sequence={buy_book.cross_sequence}:"
            f"received={buy_book.response.received_realtime_ns}"
        ),
    )
    sell_execution = proceeds_from_sell(
        sell_amount,
        sell_book.bids,
        fee_bps=sell_fee_bps,
        fee_currency=cex_sell_fee_currency,
        base_currency=sell_asset.cex_symbol,
        quote_currency="USDT",
        fee_source=cex_sell_fee_source or "configured_cex_taker_fee",
        fee_quality=(
            "account_verified"
            if cex_sell_fee_account_verified is True
            else (
                "public_unverified"
                if cex_sell_fee_account_verified is False
                else "unknown"
            )
        ),
        state_version=(
            f"{sell_book.source}:update={sell_book.update_id}:"
            f"sequence={sell_book.cross_sequence}:"
            f"received={sell_book.response.received_realtime_ns}"
        ),
    )
    if not buy_execution.complete:
        if buy_execution.status == "insufficient_known_depth":
            raise ValueError(f"insufficient {buy_asset.cex_symbol}/USDT CEX ask depth")
        raise ValueError(
            f"invalid {buy_asset.cex_symbol}/USDT CEX ask book: {buy_execution.reason}"
        )
    if not sell_execution.complete:
        if sell_execution.status == "insufficient_known_depth":
            raise ValueError(f"insufficient {sell_asset.cex_symbol}/USDT CEX bid depth")
        raise ValueError(
            f"invalid {sell_asset.cex_symbol}/USDT CEX bid book: {sell_execution.reason}"
        )

    cex_buy_amount_before_fee = buy_execution.requested_book_base_quantity
    cex_buy_cost = buy_execution.gross_book_quote_amount
    cex_sell_gross = sell_execution.gross_book_quote_amount
    net_cost = -buy_execution.net_quote_movement
    net_proceeds = sell_execution.net_quote_movement
    gross_pnl = cex_sell_gross - cex_buy_cost
    net_before_network = net_proceeds - net_cost
    net_after_network = net_before_network - network_cost_floor_usdt
    gross_edge_bps = gross_pnl / cex_buy_cost * Decimal(10_000)
    net_before_bps = net_before_network / net_cost * Decimal(10_000)
    net_after_bps = net_after_network / net_cost * Decimal(10_000)
    dex_received_ns = int(dex_record["response_received_realtime_ns"])
    base_skew_ms = Decimal(abs(base_book.response.received_realtime_ns - dex_received_ns)) / Decimal(1_000_000)
    quote_skew_ms = Decimal(abs(quote_book.response.received_realtime_ns - dex_received_ns)) / Decimal(1_000_000)
    response_skew_ms = max(base_skew_ms, quote_skew_ms)
    timing_valid = response_skew_ms <= max_response_skew_ms
    if cex_buy_fee_account_verified is None and cex_sell_fee_account_verified is None:
        all_fee_rates_verified: bool | None = None
    else:
        all_fee_rates_verified = (
            cex_buy_fee_account_verified is True and cex_sell_fee_account_verified is True
        )

    return {
        "schema_version": 2,
        "round_id": dex_record.get("round_id"),
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "cex_venue": cex_venue,
        "cex_buy_symbol": cex_symbol(buy_asset, cex_venue),
        "cex_sell_symbol": cex_symbol(sell_asset, cex_venue),
        "cycle_direction": cycle_direction,
        "requested_notional_quote": str(dex_record["requested_notional_quote"]),
        "reference_input_asset": market.quote.symbol,
        "reference_notional_usdt": _decimal_text(reference_notional_usdt),
        "quote_symbol": "USDT",
        "base_amount": _decimal_text(base_amount),
        "quote_amount": _decimal_text(quote_amount),
        "asset_equivalence": market.asset_equivalence,
        "status": "ok" if timing_valid else "timing_skew_exceeded",
        "timing_valid": timing_valid,
        "response_skew_ms": round(float(response_skew_ms), 6),
        "base_cex_response_skew_ms": round(float(base_skew_ms), 6),
        "quote_cex_response_skew_ms": round(float(quote_skew_ms), 6),
        "max_response_skew_ms": _decimal_text(max_response_skew_ms),
        "dex_average_price_quote_per_base": str(dex_record.get("average_price_quote_per_base")),
        "cex_buy_amount_before_fee": _decimal_text(cex_buy_amount_before_fee),
        "cex_buy_vwap_usdt_per_asset": _decimal_text(cex_buy_cost / cex_buy_amount_before_fee),
        "cex_sell_vwap_usdt_per_asset": _decimal_text(cex_sell_gross / sell_amount),
        "gross_cost_quote": _decimal_text(cex_buy_cost),
        "gross_proceeds_quote": _decimal_text(cex_sell_gross),
        "gross_pnl_quote": _decimal_text(gross_pnl),
        "gross_edge_bps": round(float(gross_edge_bps), 6),
        "cex_taker_fee_bps_per_leg": (
            _decimal_text(cex_taker_fee_bps) if buy_fee_bps == sell_fee_bps else None
        ),
        "cex_buy_taker_fee_bps": _decimal_text(buy_fee_bps),
        "cex_sell_taker_fee_bps": _decimal_text(sell_fee_bps),
        "cex_buy_fee_source": cex_buy_fee_source,
        "cex_sell_fee_source": cex_sell_fee_source,
        "cex_buy_fee_account_verified": cex_buy_fee_account_verified,
        "cex_sell_fee_account_verified": cex_sell_fee_account_verified,
        "cex_buy_fee_assumptions": list(cex_buy_fee_assumptions),
        "cex_sell_fee_assumptions": list(cex_sell_fee_assumptions),
        "cex_buy_fee_currency_used": buy_execution.fee_currency,
        "cex_sell_fee_currency_used": sell_execution.fee_currency,
        "cex_buy_execution_estimate": buy_execution.as_dict(),
        "cex_sell_execution_estimate": sell_execution.as_dict(),
        "cex_fee_account_verified": all_fee_rates_verified,
        "candidate_eligible_with_account_verified_fee": all_fee_rates_verified is not False,
        "net_pnl_before_network_quote": _decimal_text(net_before_network),
        "net_edge_before_network_bps": round(float(net_before_bps), 6),
        "minimum_network_cost_quote": _decimal_text(network_cost_floor_usdt),
        "net_pnl_after_minimum_network_quote": _decimal_text(net_after_network),
        "net_edge_after_minimum_network_bps": round(float(net_after_bps), 6),
        "positive_before_network": timing_valid and net_before_network > 0,
        "positive_after_minimum_network": timing_valid and net_after_network > 0,
        "dex_fee_and_price_impact_included": True,
        "two_cex_depth_books_walked": True,
        "network_cost_is_floor_not_priority_auction": True,
        "transfer_rebalance_cost_included": False,
        "funding_or_borrow_cost_included": False,
        "dex_response_received_realtime_ns": dex_received_ns,
        "dex_request_rtt_ms": dex_record.get("request_rtt_ms"),
        "base_cex_response_received_realtime_ns": base_book.response.received_realtime_ns,
        "quote_cex_response_received_realtime_ns": quote_book.response.received_realtime_ns,
        "dex_fee_tier": dex_record.get("fee_tier"),
        "dex_fee_bps": dex_record.get("fee_bps"),
    }


def _calculation_error_cycle(
    *,
    market: TriangleMarket,
    venue: str,
    dex_record: Mapping[str, Any],
    error: BaseException,
) -> dict[str, Any]:
    return {
        "schema_version": 2,
        "round_id": dex_record.get("round_id"),
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "cex_venue": venue,
        "cycle_direction": (
            "buy_cex_quote_dex_sell_cex_base"
            if dex_record.get("direction") == "buy_base"
            else "buy_cex_base_sell_dex_sell_cex_quote"
        ),
        "requested_notional_quote": dex_record.get("requested_notional_quote"),
        "quote_symbol": "USDT",
        "status": "calculation_error",
        "error": f"{type(error).__name__}: {error}",
        "timing_valid": False,
    }


def _coverage_limited_cycle(
    *,
    market: TriangleMarket,
    venue: str,
    dex_record: Mapping[str, Any],
    status: str,
    detail: str,
) -> dict[str, Any]:
    """Represent an ordinary coverage limit without calling it a bug.

    A public top-five/twenty book can be valid yet too shallow for a requested
    exact amount.  That is useful negative evidence, but it must not pollute
    ``calculation_errors`` or trigger a needless diagnostic alarm every time a
    fresh CEX update arrives.
    """

    return {
        "schema_version": 2,
        "round_id": dex_record.get("round_id"),
        "market": market.name,
        "chain": market.chain,
        "dex_provider": market.provider,
        "dex_pair": market.dex_pair,
        "cex_venue": venue,
        "cycle_direction": (
            "buy_cex_quote_dex_sell_cex_base"
            if dex_record.get("direction") == "buy_base"
            else "buy_cex_base_sell_dex_sell_cex_quote"
        ),
        "requested_notional_quote": dex_record.get("requested_notional_quote"),
        "quote_symbol": "USDT",
        "status": status,
        "error": detail,
        "timing_valid": False,
        "positive_after_minimum_network": False,
    }


def _terminal_dex_unavailable(error: object) -> bool:
    """Identify a deterministic public-route miss, never a quota/network miss."""

    text = str(error).lower()
    return "http 400" in text or "http 404" in text or "could not find pool" in text


def _compact_candidate_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Persist only evidence needed to replay a candidate later.

    In particular, do not persist route plans, raw book levels, RPC payloads or
    every negative quote.  The event cap in the runner protects disk even if a
    market stays positive for a long time.
    """

    compact = {
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
            "max_net_edge_after_minimum_network_bps",
            "max_net_pnl_after_minimum_network_quote",
            "close_reason",
        )
        if key in event
    }
    best = event.get("best_cycle")
    if isinstance(best, Mapping):
        compact["best_cycle"] = {
            key: best.get(key)
            for key in (
                "round_id",
                "market",
                "chain",
                "dex_provider",
                "dex_pair",
                "cex_venue",
                "cex_buy_symbol",
                "cex_sell_symbol",
                "cycle_direction",
                "requested_notional_quote",
                "reference_input_asset",
                "reference_notional_usdt",
                "status",
                "timing_valid",
                "response_skew_ms",
                "max_response_skew_ms",
                "dex_request_rtt_ms",
                "cex_taker_fee_bps_per_leg",
                "cex_buy_taker_fee_bps",
                "cex_sell_taker_fee_bps",
                "cex_buy_fee_source",
                "cex_sell_fee_source",
                "cex_buy_fee_account_verified",
                "cex_sell_fee_account_verified",
                "cex_fee_account_verified",
                "minimum_network_cost_quote",
                "net_edge_after_minimum_network_bps",
                "net_pnl_after_minimum_network_quote",
            )
            if key in best
        }
    return compact


async def record_triangle_cycle_monitor(
    markets: Sequence[TriangleMarket],
    providers: Mapping[str, DexQuoteProvider],
    *,
    reference_notional_usdt: Decimal,
    duration_seconds: float | None,
    cex_venues: Sequence[str],
    cex_taker_fees: Mapping[str, Decimal],
    network_cost_floors: Mapping[str, Decimal],
    max_response_skew_ms: Decimal,
    max_dex_cache_age_ms: Decimal,
    output_directory: Path,
    proxy_url: str | None,
    timeout_seconds: float,
    history_capacity_per_symbol: int = 256,
    stats_flush_seconds: float = 2.0,
    max_persisted_candidate_events: int = 5_000,
    stonfi_min_round_interval_seconds: float = 1.0,
    uniswap_base_min_round_interval_seconds: float = 1.0,
    uniswap_polygon_min_round_interval_seconds: float = 1.0,
    raydium_429_cooldown_seconds: float = 120.0,
    raydium_429_min_request_interval_seconds: float = 3.0,
    stdout_candidates: bool = False,
    cex_streams: Mapping[str, PublicBookStream] | None = None,
    shared_provider_gates: Mapping[str, AsyncRequestPacer] | None = None,
    account_fee_rates: Mapping[tuple[str, str], SpotFeeRate] | None = None,
    fee_audit_file: Path | None = None,
    require_account_verified_fees_for_candidates: bool = True,
) -> dict[str, Any]:
    """Run a read-only broad triangle scan, optionally without a time limit."""

    if output_directory.exists():
        raise FileExistsError(f"Refusing to overwrite triangle monitor output: {output_directory}")
    if not markets:
        raise ValueError("at least one triangle market is required")
    if (
        (duration_seconds is not None and duration_seconds <= 0)
        or timeout_seconds <= 0
        or stats_flush_seconds <= 0
    ):
        raise ValueError("duration, timeout and stats flush interval must be positive")
    if reference_notional_usdt <= 0 or not reference_notional_usdt.is_finite():
        raise ValueError("reference USDT notional must be finite and positive")
    if max_response_skew_ms < 0 or max_dex_cache_age_ms < 0:
        raise ValueError("timing windows cannot be negative")
    if max_persisted_candidate_events <= 0 or history_capacity_per_symbol <= 0:
        raise ValueError("retention capacities must be positive")
    if raydium_429_cooldown_seconds < 0 or raydium_429_min_request_interval_seconds <= 0:
        raise ValueError("Raydium rate-limit recovery settings are invalid")
    if any(market.provider not in providers for market in markets):
        raise ValueError("every triangle market needs a DEX provider")
    if any(venue not in CEX_BOOK_ENDPOINTS for venue in cex_venues):
        raise ValueError("unsupported CEX venue")
    if any(venue not in cex_taker_fees for venue in cex_venues):
        raise ValueError("each selected CEX needs a configured taker fee")
    if any(market.chain not in network_cost_floors for market in markets):
        raise ValueError("each chain needs a configured minimum network cost")
    if fee_audit_file is not None and account_fee_rates is None:
        raise ValueError("fee_audit_file requires loaded account_fee_rates")
    for (venue, symbol), fee in (account_fee_rates or {}).items():
        if (
            venue.upper() not in CEX_BOOK_ENDPOINTS
            or not symbol
            or fee.venue.upper() != venue.upper()
            or fee.symbol.upper() != symbol.upper()
        ):
            raise ValueError("account fee-rate mapping contains an invalid venue or symbol")

    output_directory.mkdir(parents=True)
    network_route = configure_process_network_route(proxy_url)
    started_at = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()
    stats = TriangleStatistics()
    stats.candidate_event_limit = max_persisted_candidate_events
    tracker = CandidateTracker(Decimal("0"))
    stop_event = asyncio.Event()
    candidate_path = output_directory / "candidate_events.jsonl"
    stats_path = output_directory / "stats.json"
    candidate_path.touch(exist_ok=False)

    symbols_by_venue = {
        venue: sorted(
            {
                cex_symbol(asset, venue)
                for market in markets
                for asset in (market.base, market.quote)
            },
        )
        for venue in cex_venues
    }
    effective_fees: dict[tuple[str, str], SpotFeeRate] = {
        (venue, symbol): resolve_spot_fee_rate(
            venue=venue,
            symbol=symbol,
            fallback_taker_bps=cex_taker_fees[venue],
            account_fee_rates=account_fee_rates,
        )
        for venue, symbols in symbols_by_venue.items()
        for symbol in symbols
    }
    streams_to_start: dict[str, PublicBookStream] = dict(cex_streams or {})
    if not streams_to_start:
        streams_to_start = {
            venue: build_public_book_stream(
                venue,
                symbols_by_venue[venue],
                timeout_seconds=timeout_seconds,
                proxy_url=proxy_url,
                history_capacity_per_symbol=history_capacity_per_symbol,
            )
            for venue in cex_venues
        }
    unexpected_streams = set(streams_to_start) - set(cex_venues)
    if unexpected_streams:
        raise ValueError(f"test CEX streams contain unrequested venues: {sorted(unexpected_streams)}")
    active_streams: dict[str, PublicBookStream] = {}

    markets_by_venue_symbol: dict[tuple[str, str], list[TriangleMarket]] = defaultdict(list)
    for market in markets:
        for venue in cex_venues:
            markets_by_venue_symbol[(venue, cex_symbol(market.base, venue))].append(market)
            markets_by_venue_symbol[(venue, cex_symbol(market.quote, venue))].append(market)
    cache_by_market: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    cache_age_ns = int(max_dex_cache_age_ms * Decimal(1_000_000))
    reported_unavailable_books: set[str] = set()
    quote_gates = {
        "STONFI": AsyncRequestPacer(stonfi_min_round_interval_seconds),
        "UNISWAP_BASE": AsyncRequestPacer(uniswap_base_min_round_interval_seconds),
        "UNISWAP_POLYGON": AsyncRequestPacer(uniswap_polygon_min_round_interval_seconds),
    }
    quote_gates.update(shared_provider_gates or {})
    raydium_backoff_lock = asyncio.Lock()
    raydium_last_backoff_monotonic = 0.0

    async def backoff_raydium_after_429(error: object) -> None:
        """Install one shared circuit-breaker pause for every Raydium pair."""

        nonlocal raydium_last_backoff_monotonic
        async with raydium_backoff_lock:
            # Several workers may surface a response around the same instant.
            # Repeating the pause on every one would turn a short cooldown into
            # an unbounded delay, so one installation per five seconds is
            # enough; all providers use the same pacer object.
            now = time.monotonic()
            if now - raydium_last_backoff_monotonic < 5:
                return
            pacers = {
                getattr(provider, "request_pacer", None)
                for provider in providers.values()
                if isinstance(provider, RaydiumProvider)
            }
            for pacer in pacers:
                if isinstance(pacer, AsyncRequestPacer):
                    await pacer.defer(
                        cooldown_seconds=raydium_429_cooldown_seconds,
                        minimum_interval_seconds=raydium_429_min_request_interval_seconds,
                    )
            raydium_last_backoff_monotonic = now
            stats.add_error(
                kind="raydium_rate_limit_backoff",
                key="RAYDIUM",
                error=(
                    f"HTTP 429: pausing shared Raydium budget for "
                    f"{raydium_429_cooldown_seconds}s and raising its interval to at least "
                    f"{raydium_429_min_request_interval_seconds}s; source error: {str(error)[:160]}"
                ),
            )

    def persist_candidate_event(output: Any, event: Mapping[str, Any]) -> None:
        compact = _compact_candidate_event(event)
        # Terminal output is deliberately independent from on-disk retention.
        # A long-running monitor must remain observable after its bounded
        # evidence file reaches the cap; tmux scrollback is bounded separately
        # and no raw market stream is written to disk.
        if stdout_candidates:
            print(json.dumps(compact, ensure_ascii=False, separators=(",", ":")), flush=True)
        if stats.candidate_events_persisted >= max_persisted_candidate_events:
            stats.candidate_events_dropped += 1
            return
        output.write(json.dumps(compact, ensure_ascii=False, separators=(",", ":")) + "\n")
        stats.candidate_events_persisted += 1

    def report_missing_book(*, venue: str, market: TriangleMarket, missing: str) -> None:
        key = f"{venue}:{market.name}:{missing}"
        if key in reported_unavailable_books:
            return
        reported_unavailable_books.add(key)
        stats.cex_unavailable_symbols[key] += 1
        stats.add_error(
            kind="cex_symbol_unavailable",
            key=key,
            error=f"no public websocket book for {missing}",
        )

    def evaluate(
        output: Any,
        *,
        market: TriangleMarket,
        venue: str,
        dex_record: Mapping[str, Any],
        observed_realtime_ns: int,
        changed_book: BookSnapshot | None = None,
    ) -> None:
        stream = active_streams.get(venue)
        if stream is None:
            return
        target_ns = int(dex_record["response_received_realtime_ns"])
        base_symbol = cex_symbol(market.base, venue)
        quote_symbol = cex_symbol(market.quote, venue)
        base_book = (
            changed_book
            if changed_book is not None and changed_book.symbol == base_symbol
            else stream.nearest_snapshot(base_symbol, target_ns)
        )
        quote_book = (
            changed_book
            if changed_book is not None and changed_book.symbol == quote_symbol
            else stream.nearest_snapshot(quote_symbol, target_ns)
        )
        if base_book is None:
            report_missing_book(venue=venue, market=market, missing=base_symbol)
            return
        if quote_book is None:
            report_missing_book(venue=venue, market=market, missing=quote_symbol)
            return
        try:
            if str(dex_record.get("direction")) == "buy_base":
                buy_fee = effective_fees[(venue, quote_symbol)]
                sell_fee = effective_fees[(venue, base_symbol)]
            else:
                buy_fee = effective_fees[(venue, base_symbol)]
                sell_fee = effective_fees[(venue, quote_symbol)]
            cycle = calculate_triangle_cycle(
                market=market,
                dex_record=dex_record,
                base_book=base_book,
                quote_book=quote_book,
                cex_taker_fee_bps=cex_taker_fees[venue],
                cex_buy_taker_fee_bps=buy_fee.taker_buy_bps,
                cex_sell_taker_fee_bps=sell_fee.taker_sell_bps,
                cex_buy_fee_source=buy_fee.source,
                cex_sell_fee_source=sell_fee.source,
                cex_buy_fee_account_verified=buy_fee.account_verified,
                cex_sell_fee_account_verified=sell_fee.account_verified,
                cex_buy_fee_assumptions=buy_fee.assumptions,
                cex_sell_fee_assumptions=sell_fee.assumptions,
                network_cost_floor_usdt=network_cost_floors[market.chain],
                max_response_skew_ms=max_response_skew_ms,
                reference_notional_usdt=reference_notional_usdt,
                cex_venue=venue,
            )
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            if str(exc).startswith("insufficient "):
                cycle = _coverage_limited_cycle(
                    market=market,
                    venue=venue,
                    dex_record=dex_record,
                    status="insufficient_cex_depth",
                    detail=str(exc),
                )
            else:
                cycle = _calculation_error_cycle(
                    market=market,
                    venue=venue,
                    dex_record=dex_record,
                    error=exc,
                )
        stats.observe_cycle(cycle)
        if cycle.get("status") != "calculation_error":
            if not require_account_verified_fees_for_candidates:
                cycle["candidate_eligible_with_account_verified_fee"] = True
            for event in tracker.observe(cycle, observed_realtime_ns=observed_realtime_ns):
                persist_candidate_event(output, event)

    async def quote_worker(output: Any, market: TriangleMarket, input_amount: Decimal) -> None:
        provider = providers[market.provider]
        gate_key = _provider_gate_key(market)
        gate = quote_gates.get(gate_key) if gate_key else None
        request_pacer = getattr(provider, "request_pacer", None)
        pacing = None if isinstance(request_pacer, AsyncRequestPacer) else gate
        round_id = 0
        consecutive_terminal_route_misses = 0
        while not stop_event.is_set():
            if pacing is not None:
                await pacing.wait()
            try:
                records = await provider.quote_round(round_id, [input_amount])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                stats.provider_errors[market.provider] += 1
                stats.add_error(
                    kind="dex_provider_exception",
                    key=market.provider,
                    error=f"{type(exc).__name__}: {exc}",
                )
                await asyncio.sleep(0.25)
                continue
            stats.dex_rounds[market.provider] += 1
            stats.dex_records[market.provider] += len(records)
            stats.last_dex_quote_at = datetime.now(UTC).isoformat()
            for record in records:
                if record.get("status") == "request_error":
                    if _terminal_dex_unavailable(record.get("error")):
                        stats.dex_unavailable[market.provider] += 1
                    else:
                        stats.provider_errors[market.provider] += 1
                        stats.add_error(
                            kind="dex_request_error",
                            key=market.provider,
                            error=str(record.get("error", "request_error")),
                        )
                        if (
                            market.provider_kind == "raydium"
                            and "http 429" in str(record.get("error", "")).lower()
                        ):
                            await backoff_raydium_after_429(record.get("error"))
                elif record.get("status") != "ok":
                    stats.dex_unavailable[market.provider] += 1
            selected = _best_dex_records(records)
            terminal_miss = bool(records) and not selected and all(
                record.get("status") == "request_error"
                and _terminal_dex_unavailable(record.get("error"))
                for record in records
            )
            if terminal_miss:
                consecutive_terminal_route_misses += 1
                if consecutive_terminal_route_misses >= 2:
                    stats.disabled_markets[market.name] = (
                        "direct DEX route returned two consecutive terminal public API misses"
                    )
                    return
            else:
                consecutive_terminal_route_misses = 0
            for (_, _), record in selected.items():
                cache_by_market[market.name][
                    (str(record["requested_notional_quote"]), str(record["direction"]))
                ] = record
                received_ns = int(record["response_received_realtime_ns"])
                for venue in active_streams:
                    evaluate(
                        output,
                        market=market,
                        venue=venue,
                        dex_record=record,
                        observed_realtime_ns=received_ns,
                    )
            round_id += 1
            # Tests can supply an in-memory provider that returns immediately.
            # Yield so it cannot monopolise this event loop.
            await asyncio.sleep(0)

    async def cex_update_worker(output: Any, venue: str, stream: PublicBookStream) -> None:
        reported_terminal_error: str | None = None
        while not stop_event.is_set():
            try:
                book = await asyncio.wait_for(stream.next_update(), timeout=5.0)
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                error = getattr(stream, "error", None)
                if error and error != reported_terminal_error:
                    reported_terminal_error = str(error)
                    stats.cex_errors[f"{venue}:websocket"] += 1
                    stats.add_error(kind="cex_stream", key=venue, error=reported_terminal_error)
                continue
            stats.cex_updates[venue] += 1
            stats.last_cex_update_at = datetime.now(UTC).isoformat()
            for market in markets_by_venue_symbol.get((venue, book.symbol), ()):
                for record in tuple(cache_by_market[market.name].values()):
                    received_ns = int(record.get("response_received_monotonic_ns", 0))
                    if time.monotonic_ns() - received_ns > cache_age_ns:
                        continue
                    evaluate(
                        output,
                        market=market,
                        venue=venue,
                        dex_record=record,
                        observed_realtime_ns=book.response.received_realtime_ns,
                        changed_book=book,
                    )

    async def periodic_stats_writer() -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=stats_flush_seconds)
            except TimeoutError:
                atomic_json(
                    stats_path,
                    stats.snapshot(
                        started_at=started_at,
                        duration_wall_seconds=time.monotonic() - started_monotonic,
                        tracker=tracker,
                        streams=active_streams,
                    ),
                )

    manifest: dict[str, Any] = {
        "schema_version": 2,
        "status": "starting",
        "started_at": started_at,
        "stopped_at": None,
        "duration_requested_seconds": duration_seconds,
        "mode": "event_driven_cex_dex_cex_triangle",
        "universe": {
            "source_pairs": len(markets),
            "same_venue_directed_cycles_at_most": len(markets) * len(cex_venues) * 2,
            "markets": [asdict(market) for market in markets],
            "formula": "DEX A/B plus CEX A/USDT and B/USDT on the same venue",
        },
        "cex": {
            "venues": list(cex_venues),
            "websocket_symbols": symbols_by_venue,
            "taker_fee_bps_per_leg": {
                venue: _decimal_text(cex_taker_fees[venue]) for venue in cex_venues
            },
            "account_fee_audit": {
                "file": str(fee_audit_file.resolve()) if fee_audit_file is not None else None,
                "account_verified_effective_rates": sum(
                    fee.account_verified for fee in effective_fees.values()
                ),
                "credentials_read_by_this_monitor_process": False,
                "separate_read_only_audit_was_supplied": fee_audit_file is not None,
                "matching_effective_rates_by_symbol": {
                    f"{venue}:{symbol}": {
                        "taker_buy_bps": _decimal_text(fee.taker_buy_bps),
                        "taker_sell_bps": _decimal_text(fee.taker_sell_bps),
                        "account_verified": fee.account_verified,
                        "source": fee.source,
                        "assumptions": list(fee.assumptions),
                    }
                    for (venue, symbol), fee in sorted(effective_fees.items())
                },
                "candidate_policy": (
                    "both same-venue CEX legs need matching account-verified symbol fees"
                    if require_account_verified_fees_for_candidates
                    else "public baseline fees are allowed for research candidates and remain explicitly unverified"
                ),
            },
        },
        "dex": {
            "providers": [providers[market.provider].config() for market in markets],
            "reference_notional_usdt": _decimal_text(reference_notional_usdt),
            "reference_sizing": "fixed CEX-midpoint input amount selected once at startup",
            "exact_quote_cache_max_age_ms": _decimal_text(max_dex_cache_age_ms),
            "max_response_skew_ms": _decimal_text(max_response_skew_ms),
            "public_pacing": {
                "raydium_shared_min_request_interval_seconds": raydium_min_interval_from_providers(providers),
                "stonfi_shared_min_round_interval_seconds": stonfi_min_round_interval_seconds,
                "uniswap_base_min_round_interval_seconds": uniswap_base_min_round_interval_seconds,
                "uniswap_polygon_min_round_interval_seconds": uniswap_polygon_min_round_interval_seconds,
                "raydium_429_cooldown_seconds": raydium_429_cooldown_seconds,
                "raydium_429_min_request_interval_seconds": raydium_429_min_request_interval_seconds,
            },
        },
        "minimum_network_cost_quote_by_chain": {
            chain: _decimal_text(cost) for chain, cost in network_cost_floors.items()
        },
        "retention": {
            "raw_market_data_persisted": False,
            "max_persisted_candidate_events": max_persisted_candidate_events,
            "policy": "only compact candidate lifecycle events, aggregate stats and a bounded error ledger are persisted",
        },
        "network_route": network_route,
        "api_credentials_used": False,
        "wallet_or_private_key_used": False,
        "transactions_submitted": False,
        "model_scope": {
            "included": [
                "public CEX WebSocket depth retained in memory",
                "direct DEX exact-input quote with returned pool fee and price impact",
                "two same-venue CEX depth walks, side-specific account-audited taker fees when available and a minimum network-cost floor",
            ],
            "excluded": [
                "withdrawal, deposit, bridge, wrapper redemption and rebalance costs",
                "priority fee, inclusion probability and DEX state change before execution",
                "fill probability, inventory, borrow and capital costs",
                "cross-CEX inventory transfers and cross-venue CEX legs",
            ],
            "interpretation": (
                "A positive candidate is not an order instruction or proof of executable arbitrage. "
                + (
                    "Both CEX legs need account-verified fees; baseline-only positives are diagnostic only."
                    if require_account_verified_fees_for_candidates
                    else "Baseline-only positives may be retained as explicitly unverified research candidates."
                )
            ),
        },
        "files": {
            "candidate_events": str(candidate_path.resolve()),
            "stats": str(stats_path.resolve()),
            "manifest": str((output_directory / "manifest.json").resolve()),
        },
        "error": None,
        "warning": None,
    }
    atomic_json(output_directory / "manifest.json", manifest)

    final_status = "completed"
    final_error: str | None = None
    workers: list[asyncio.Task[None]] = []
    try:
        starts = await asyncio.gather(
            *(stream.start() for stream in streams_to_start.values()),
            return_exceptions=True,
        )
        for venue, stream, result in zip(
            streams_to_start,
            streams_to_start.values(),
            starts,
            strict=True,
        ):
            if isinstance(result, BaseException):
                stats.cex_errors[f"{venue}:websocket_start"] += 1
                stats.add_error(
                    kind="cex_stream_start",
                    key=venue,
                    error=f"{type(result).__name__}: {result}",
                )
                with contextlib.suppress(Exception):
                    await stream.close()
            else:
                active_streams[venue] = stream
        if not active_streams:
            raise RuntimeError("no CEX websocket stream produced an initial valid book")

        all_assets = tuple(
            {asset.cex_symbol: asset for market in markets for asset in (market.base, market.quote)}.values(),
        )
        reference_inputs = await _reference_inputs(
            all_assets,
            active_streams,
            cex_venues,
            reference_notional_usdt=reference_notional_usdt,
            wait_seconds=min(max(timeout_seconds * 2, 2), 20),
        )
        enabled_markets: list[TriangleMarket] = []
        for market in markets:
            input_amount = reference_inputs.get(market.quote.cex_symbol)
            if input_amount is None:
                stats.disabled_markets[market.name] = (
                    f"no initial CEX midpoint for DEX input {market.quote.cex_symbol}/USDT"
                )
                continue
            stats.reference_input_amounts[market.name] = _decimal_text(input_amount)
            enabled_markets.append(market)
        if not enabled_markets:
            raise RuntimeError("no triangle market received a reference CEX midpoint")

        with candidate_path.open("a", encoding="utf-8", buffering=1) as candidate_output:
            workers = [
                *(
                    asyncio.create_task(
                        quote_worker(
                            candidate_output,
                            market,
                            reference_inputs[market.quote.cex_symbol],
                        ),
                    )
                    for market in enabled_markets
                ),
                *(
                    asyncio.create_task(cex_update_worker(candidate_output, venue, stream))
                    for venue, stream in active_streams.items()
                ),
                asyncio.create_task(periodic_stats_writer()),
            ]
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
                for task in workers:
                    task.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
                workers.clear()
                close_reason = (
                    "requested_duration_elapsed" if duration_seconds is not None else "scanner_stopped"
                )
                for event in tracker.close_all(reason=close_reason):
                    persist_candidate_event(candidate_output, event)
    except asyncio.CancelledError:
        final_status = "stopped"
    except Exception as exc:
        final_status = "error"
        final_error = f"{type(exc).__name__}: {exc}"
    finally:
        stop_event.set()
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        for stream in streams_to_start.values():
            with contextlib.suppress(Exception):
                await stream.close()

    for venue, stream in active_streams.items():
        error = getattr(stream, "error", None)
        if error:
            stats.cex_errors.setdefault(f"{venue}:websocket", 1)
    elapsed = time.monotonic() - started_monotonic
    final_stats = stats.snapshot(
        started_at=started_at,
        duration_wall_seconds=elapsed,
        tracker=tracker,
        streams=active_streams,
    )
    final_stats["status"] = final_status
    atomic_json(stats_path, final_stats)
    manifest.update(
        status=final_status,
        stopped_at=datetime.now(UTC).isoformat(),
        duration_wall_seconds=round(elapsed, 6),
        error=final_error,
        warning=("one or more CEX websocket streams were unavailable" if stats.cex_errors else None),
    )
    atomic_json(output_directory / "manifest.json", manifest)
    return manifest


def raydium_min_interval_from_providers(providers: Mapping[str, DexQuoteProvider]) -> float | None:
    """Expose the common Raydium pacer interval without persisting object state."""

    for provider in providers.values():
        config = provider.config()
        if config.get("protocol") == "Raydium Route API v2":
            value = config.get("minimum_request_interval_seconds")
            return float(value) if value is not None else None
    return None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", type=_parse_market_names, default=list(DEFAULT_TRIANGLE_MARKET_NAMES))
    parser.add_argument(
        "--provider-kinds",
        type=_parse_provider_kinds,
        help="Optional comma-separated source subset, for example raydium or stonfi,uniswap_base",
    )
    parser.add_argument("--cex-venues", type=_parse_venues, default=["MEXC", "BYBIT", "OKX", "BINANCE"])
    parser.add_argument("--reference-notional-usdt", type=Decimal, default=Decimal("100"))
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--max-response-skew-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--max-dex-cache-age-ms", type=Decimal, default=Decimal("300"))
    parser.add_argument("--history-capacity-per-symbol", type=int, default=256)
    parser.add_argument("--stats-flush-seconds", type=float, default=2.0)
    parser.add_argument("--max-persisted-candidate-events", type=int, default=5_000)
    parser.add_argument("--minimum-network-costs", type=_parse_costs, default=dict(DEFAULT_NETWORK_COST_FLOORS))
    parser.add_argument("--cex-taker-fees-bps", type=_parse_fees, default=dict(DEFAULT_CEX_TAKER_FEES))
    parser.add_argument(
        "--cex-fee-audit-file",
        type=Path,
        help=(
            "Completed JSON from audit-cex-fees. Both CEX legs require matching account-verified "
            "symbol rates before a positive triangle is persisted."
        ),
    )
    parser.add_argument("--raydium-slippage-bps", type=int, default=50)
    parser.add_argument("--raydium-min-request-interval-seconds", type=float, default=0.65)
    parser.add_argument("--stonfi-slippage-tolerance", type=Decimal, default=Decimal("0.005"))
    parser.add_argument("--stonfi-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--uniswap-base-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--uniswap-polygon-min-round-interval-seconds", type=float, default=1.0)
    parser.add_argument("--raydium-429-cooldown-seconds", type=float, default=120.0)
    parser.add_argument("--raydium-429-min-request-interval-seconds", type=float, default=3.0)
    parser.add_argument("--base-rpc-url", default="https://mainnet-preconf.base.org")
    parser.add_argument("--polygon-rpc-url", default="https://polygon.drpc.org")
    parser.add_argument("--stdout-candidates", action="store_true")
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/triangle-cycles"))
    parser.add_argument("--run-id")
    return parser


def main() -> None:
    args = _parser().parse_args()
    timing_values = (
        args.duration_seconds,
        args.timeout_seconds,
        args.stats_flush_seconds,
        args.raydium_min_request_interval_seconds,
        args.stonfi_min_round_interval_seconds,
        args.uniswap_base_min_round_interval_seconds,
        args.uniswap_polygon_min_round_interval_seconds,
        args.raydium_429_min_request_interval_seconds,
    )
    if any(value <= 0 for value in timing_values):
        raise SystemExit("all monitor timing intervals must be positive")
    if (
        args.history_capacity_per_symbol <= 0
        or args.max_persisted_candidate_events <= 0
        or args.max_response_skew_ms < 0
        or args.max_dex_cache_age_ms < 0
        or args.reference_notional_usdt <= 0
        or not args.reference_notional_usdt.is_finite()
    ):
        raise SystemExit("capacities and reference notional must be positive; timing windows non-negative")
    if args.raydium_slippage_bps < 0:
        raise SystemExit("Raydium slippage cannot be negative")
    if args.raydium_429_cooldown_seconds < 0:
        raise SystemExit("Raydium 429 cooldown cannot be negative")
    if not Decimal(0) <= args.stonfi_slippage_tolerance < Decimal(1):
        raise SystemExit("STON.fi slippage tolerance must be in [0, 1)")
    try:
        account_fee_rates = (
            load_spot_fee_audit(args.cex_fee_audit_file)
            if args.cex_fee_audit_file is not None
            else None
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    run_id = args.run_id or default_run_id("triangle-cex-dex")
    validate_run_id(run_id)
    markets = [TRIANGLE_MARKET_BY_NAME[name] for name in args.markets]
    if args.provider_kinds is not None:
        allowed_kinds = set(args.provider_kinds)
        markets = [market for market in markets if market.provider_kind in allowed_kinds]
    if not markets:
        raise SystemExit("the selected market and provider-kind filters have no intersection")
    providers = build_triangle_providers(
        markets,
        base_rpc_url=args.base_rpc_url,
        polygon_rpc_url=args.polygon_rpc_url,
        fee_tiers=(100, 500, 3000),
        proxy_url=args.proxy_url,
        timeout_seconds=args.timeout_seconds,
        raydium_slippage_bps=args.raydium_slippage_bps,
        raydium_min_request_interval_seconds=args.raydium_min_request_interval_seconds,
        stonfi_slippage_tolerance=args.stonfi_slippage_tolerance,
    )
    manifest = asyncio.run(
        record_triangle_cycle_monitor(
            markets,
            providers,
            reference_notional_usdt=args.reference_notional_usdt,
            duration_seconds=args.duration_seconds,
            cex_venues=args.cex_venues,
            cex_taker_fees=args.cex_taker_fees_bps,
            network_cost_floors=args.minimum_network_costs,
            max_response_skew_ms=args.max_response_skew_ms,
            max_dex_cache_age_ms=args.max_dex_cache_age_ms,
            output_directory=args.output_root / run_id,
            proxy_url=args.proxy_url,
            timeout_seconds=args.timeout_seconds,
            history_capacity_per_symbol=args.history_capacity_per_symbol,
            stats_flush_seconds=args.stats_flush_seconds,
            max_persisted_candidate_events=args.max_persisted_candidate_events,
            stonfi_min_round_interval_seconds=args.stonfi_min_round_interval_seconds,
            uniswap_base_min_round_interval_seconds=args.uniswap_base_min_round_interval_seconds,
            uniswap_polygon_min_round_interval_seconds=args.uniswap_polygon_min_round_interval_seconds,
            raydium_429_cooldown_seconds=args.raydium_429_cooldown_seconds,
            raydium_429_min_request_interval_seconds=args.raydium_429_min_request_interval_seconds,
            stdout_candidates=args.stdout_candidates,
            account_fee_rates=account_fee_rates,
            fee_audit_file=args.cex_fee_audit_file,
        ),
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    if manifest["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
