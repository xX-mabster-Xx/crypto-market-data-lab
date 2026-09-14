"""Read-only, event-driven strategy analysis for the unified perp data plane.

The collector already owns all public WebSocket/RPC connections.  This module
is only a consumer of that shared bus: it never opens a venue connection,
places an order, asks for a wallet, or serialises raw market data.

It evaluates the strategy families for which the currently normalised public
events contain enough information:

* a simultaneous, pre-positioned CEX spot-to-spot inventory cycle;
* a two-perpetual flat price cycle across venues;
* a delta-neutral cross-perpetual funding carry, normalised to one hour;
* spot <-> perpetual carry using public CEX books; and
* DEX exact-input entry-basis observations against a matching perpetual; and
* conservative DEX <-> perpetual immediate-close models when a matching
  reverse exact-input DEX quote is already present on the shared bus.

The DEX families never open a new connection or request a quote.  A paired
reverse quote is accepted only when it is from the same provider, market,
round, and exact raw base quantity.  Even then it is an independent public
pre-trade simulation, not proof that two swaps can execute against a single
post-trade pool state.  Every positive lifecycle record is therefore a
research candidate, never an execution instruction.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from market_data_lab.account_fee_audit import SpotFeeRate
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.cex_dex_cycles import DEFAULT_NETWORK_COST_FLOORS
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.contract_models import PerpContractModel
from market_data_lab.contract_models import resolve_perp_contract_model
from market_data_lab.exact_quote_pair_cache import ExactQuotePair
from market_data_lab.exact_quote_pair_cache import ExactQuotePairCache
from market_data_lab.exact_quote_pair_cache import quote_received_before
from market_data_lab.funding_model import FundingProjection
from market_data_lab.funding_model import project_common_rate_discrete_funding
from market_data_lab.live_common import atomic_json
from market_data_lab.perp_venue_feeds import PerpQuoteEvent
from market_data_lab.numeric_text import canonical_decimal_text
from market_data_lab.polling_quote_sources import ExactInputQuote
from market_data_lab.quantity_lattice import round_down_to_common_quantity_lattice
from market_data_lab.realtime_scanner import MarketEvent
from market_data_lab.realtime_scanner import SourceEpochChange
from market_data_lab.rolling_cycle_monitor import DEFAULT_CEX_TAKER_FEES
from market_data_lab.solana_realtime_scanner import CexTopOfBookEvent
from market_data_lab.strategy_quality import derive_candidate_quality

from market_data_lab.amm_simulation.contracts import SequentialUnwindResult
from market_data_lab.amm_simulation.replay import SimulationEvidenceBundle, save_evidence_bundle


# These are deliberately conservative modelling baselines, not assertions
# about a user's fee tier, fee-token discount, or private agreement.  Paradex
# can expose a public market fee in its contract metadata; every other unknown
# perp venue is modelled at a deliberately non-zero fallback and remains
# explicitly unverified in the output.
DEFAULT_PUBLIC_SPOT_TAKER_FEES: dict[str, Decimal] = {
    **DEFAULT_CEX_TAKER_FEES,
    "BITGET": Decimal("10"),
}
BYBIT_LINEAR_PUBLIC_VIP0_TAKER_BPS = Decimal("5.5")
UNKNOWN_PUBLIC_PERP_TAKER_BPS = Decimal("10")
STABLE_QUOTES = ("USDT", "USDC", "USD1", "USDE", "USD")
_SYMBOL_SEPARATOR = re.compile(r"[-_/:]", re.ASCII)
STRATEGY_FAMILIES = (
    "cex_spot_cross_venue_inventory_cycle",
    "perp_perp_flat_price_cycle",
    "cross_perp_funding_carry",
    "spot_perp_flat_price_cycle",
    "spot_perp_funding_carry",
    "dex_perp_entry_hedge",
    "dex_perp_paired_exact_quote_flat_model",
    "dex_perp_paired_exact_quote_funding_scenario",
    "dex_perp_sequential_flat_model",
)


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return canonical_decimal_text(value)


def _utc_iso_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, UTC).isoformat()


def _positive_decimal(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def _age_ms(*, now_ns: int, then_ns: int | None) -> float | None:
    if then_ns is None:
        return None
    return round(max(0, now_ns - then_ns) / 1_000_000, 3)


def _stable_base_and_quote(symbol: str) -> tuple[str, str] | None:
    normalized = _SYMBOL_SEPARATOR.sub("", symbol.upper())
    for quote in STABLE_QUOTES:
        if normalized.endswith(quote) and len(normalized) > len(quote):
            return normalized[: -len(quote)], quote
    return None


@dataclass(frozen=True, slots=True)
class _Fee:
    taker_bps: Decimal
    source: str
    account_verified: bool


@dataclass(frozen=True, slots=True)
class _PerpLeg:
    venue: str
    venue_symbol: str
    base: str
    settlement: str
    best_bid: Decimal | None
    best_bid_size: Decimal | None
    best_ask: Decimal | None
    best_ask_size: Decimal | None
    book_received_realtime_ns: int | None
    book_received_monotonic_ns: int | None
    funding_rate: Decimal | None
    funding_rate_short: Decimal | None
    funding_interval_minutes: int | None
    funding_rate_kind: str | None
    next_funding_time_ms: int | None
    context_received_realtime_ns: int | None
    context_received_monotonic_ns: int | None
    mark_price: Decimal | None
    index_price: Decimal | None
    quantity_step: Decimal | None
    minimum_order_quantity: Decimal | None
    public_taker_fee_bps: Decimal | None
    fee_source: str | None
    contract_type: str | None
    contract_model: PerpContractModel
    execution_model: str | None
    source: str
    source_epoch: int

    @property
    def key(self) -> tuple[str, str]:
        return self.venue.upper(), self.venue_symbol.upper()

    @property
    def executable_bbo(self) -> bool:
        return (
            _positive_decimal(self.best_bid)
            and _positive_decimal(self.best_ask)
            and _positive_decimal(self.best_bid_size)
            and _positive_decimal(self.best_ask_size)
            and (
                self.execution_model is None
                or "indicative" not in self.execution_model.lower()
            )
        )


@dataclass(frozen=True, slots=True)
class _SpotLeg:
    venue: str
    symbol: str
    base: str
    quote: str
    best_bid: Decimal | None
    best_bid_size: Decimal | None
    best_ask: Decimal | None
    best_ask_size: Decimal | None
    received_realtime_ns: int
    received_monotonic_ns: int
    source: str
    source_epoch: int

    @property
    def key(self) -> tuple[str, str]:
        return self.venue.upper(), self.symbol.upper()

    @property
    def executable_bbo(self) -> bool:
        return (
            _positive_decimal(self.best_bid)
            and _positive_decimal(self.best_ask)
            and _positive_decimal(self.best_bid_size)
            and _positive_decimal(self.best_ask_size)
        )


@dataclass
class _ActiveCandidate:
    key: str
    analysis_kind: str
    started_realtime_ns: int
    started_monotonic_ns: int
    started_at: str
    last_seen_realtime_ns: int
    last_seen_monotonic_ns: int
    last_seen_at: str
    observations: int
    max_edge_bps: Decimal
    max_pnl_usdt: Decimal
    best_cycle: dict[str, Any]
    persisted: bool


class UnifiedPerpAnalyzer:
    """Compare compatible bounded perp, spot, and exact-quote states.

    The hot path only replaces one latest record and marks its base asset
    dirty.  Decimal calculations happen in a coalesced worker, so very busy
    feeds such as dYdX cannot starve the common collector bus.
    """

    def __init__(
        self,
        *,
        output_directory: Path,
        fee_audit_file: Path | None = None,
        max_response_skew_ms: Decimal = Decimal("1000"),
        max_book_age_ms: Decimal = Decimal("500"),
        max_exact_quote_age_ms: Decimal = Decimal("1500"),
        max_funding_context_age_ms: Decimal = Decimal("300000"),
        coalesce_interval_ms: float = 50.0,
        target_notional_usdt: Decimal = Decimal("100"),
        funding_horizon_hours: Decimal = Decimal("1"),
        candidate_min_persistence_ms: Decimal = Decimal("500"),
        max_candidate_events: int = 5_000,
        max_exact_quote_pair_cache_buckets: int = 2_048,
        max_exact_quote_pair_cache_records_per_bucket: int = 4,
        sequential_amm_simulator: Any = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        realtime_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        if not _positive_decimal(max_response_skew_ms):
            raise ValueError("max_response_skew_ms must be finite and positive")
        if not _positive_decimal(max_book_age_ms):
            raise ValueError("max_book_age_ms must be finite and positive")
        if not _positive_decimal(max_exact_quote_age_ms):
            raise ValueError("max_exact_quote_age_ms must be finite and positive")
        if not _positive_decimal(max_funding_context_age_ms):
            raise ValueError("max_funding_context_age_ms must be finite and positive")
        if coalesce_interval_ms <= 0:
            raise ValueError("coalesce_interval_ms must be positive")
        if not _positive_decimal(target_notional_usdt):
            raise ValueError("target_notional_usdt must be finite and positive")
        if not _positive_decimal(funding_horizon_hours):
            raise ValueError("funding_horizon_hours must be finite and positive")
        if (
            not candidate_min_persistence_ms.is_finite()
            or candidate_min_persistence_ms < 0
        ):
            raise ValueError("candidate_min_persistence_ms must be finite and non-negative")
        if max_candidate_events <= 0:
            raise ValueError("max_candidate_events must be positive")

        self.output_directory = output_directory
        self.analysis_directory = output_directory / "perp_analysis"
        self.candidate_events_path = self.analysis_directory / "candidate_events.jsonl"
        self.stats_path = self.analysis_directory / "stats.json"
        self.capabilities_path = self.analysis_directory / "capabilities.json"
        self.max_response_skew_ms = max_response_skew_ms
        self.max_book_age_ms = max_book_age_ms
        self.max_exact_quote_age_ms = max_exact_quote_age_ms
        self.max_funding_context_age_ms = max_funding_context_age_ms
        self.coalesce_interval_seconds = coalesce_interval_ms / 1_000
        self.target_notional_usdt = target_notional_usdt
        self.funding_horizon_hours = funding_horizon_hours
        self.candidate_min_persistence_ms = candidate_min_persistence_ms
        self._candidate_min_persistence_ns = int(candidate_min_persistence_ms * Decimal(1_000_000))
        self.max_candidate_events = max_candidate_events
        self._monotonic_ns = monotonic_ns
        self._realtime_ns = realtime_ns
        self._sequential_amm_simulator = sequential_amm_simulator
        self._sequential_evidence: dict[str, SequentialUnwindResult] = {}
        self._sequential_evidence_paths: dict[str, str] = {}
        self._max_sequential_evidence_records = 256

        self._perps: dict[tuple[str, str], _PerpLeg] = {}
        self._perp_keys_by_base: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._spots: dict[tuple[str, str], _SpotLeg] = {}
        self._spot_keys_by_base: dict[str, set[tuple[str, str]]] = defaultdict(set)
        self._direct_markets = {market.provider: market for market in MARKETS.values()}
        self._dex_quotes: dict[tuple[str, str, str], ExactInputQuote] = {}
        self._dex_quote_provenance: dict[tuple[str, str, str], tuple[str, int]] = {}
        self._current_source_epochs: dict[str, int] = {}
        self._dex_keys_by_base: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
        self._observed_exact_quote_providers: set[str] = set()
        self._exact_quote_pair_cache = ExactQuotePairCache(
            max_buckets=max_exact_quote_pair_cache_buckets,
            max_records_per_bucket=max_exact_quote_pair_cache_records_per_bucket,
        )
        self._linear_books: dict[
            tuple[str, str], tuple[CexTopOfBookEvent, str, int]
        ] = {}
        self._linear_contexts: dict[tuple[str, str], tuple[object, str, int]] = {}

        self._dirty_bases: set[str] = set()
        self._wake = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False
        self._active: dict[str, _ActiveCandidate] = {}
        self._candidate_events: deque[dict[str, Any]] = deque()
        self._pending_candidate_events: deque[dict[str, Any]] = deque()
        self._candidate_events_dirty = False
        # Capability evidence is metadata, not a per-tick journal.  Rewrite
        # this small manifest only when the set or semantics of observed
        # markets changes.
        self._capabilities_dirty = True
        self._last_candidate_events_flush_monotonic = 0.0
        self._candidate_events_flush_interval_seconds = 5.0
        self._candidate_started = 0
        self._candidate_improved = 0
        self._candidate_closed = 0
        self._counts: Counter[str] = Counter()
        self._route_stats: dict[str, dict[str, Any]] = {}
        self._recent_errors: deque[str] = deque(maxlen=20)
        # Candidate lifecycles can close and reopen with every brief BBO or
        # funding-context change. Keep a compact bounded lifecycle journal and
        # make the operator console readable.
        self._last_console_candidate_report_ns: dict[str, int] = {}
        self._console_candidate_report_interval_ns = 5_000_000_000
        self._spot_fee_rates, self._fee_audit_error = self._load_fee_audit(fee_audit_file)
        self._started_at = datetime.now(UTC).isoformat()

    @staticmethod
    def _load_fee_audit(
        fee_audit_file: Path | None,
    ) -> tuple[dict[tuple[str, str], SpotFeeRate], str | None]:
        if fee_audit_file is None:
            return {}, None
        try:
            return load_spot_fee_audit(fee_audit_file), None
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            return {}, f"{type(exc).__name__}: {exc}"[:512]

    def _spot_fee(self, spot: _SpotLeg, *, side: str) -> _Fee:
        audited = self._spot_fee_rates.get((spot.venue.upper(), spot.symbol.upper()))
        if audited is not None:
            return _Fee(
                taker_bps=(audited.taker_buy_bps if side == "buy" else audited.taker_sell_bps),
                source=audited.source,
                account_verified=audited.account_verified,
            )
        fallback = DEFAULT_PUBLIC_SPOT_TAKER_FEES.get(spot.venue.upper(), Decimal("10"))
        return _Fee(
            taker_bps=fallback,
            source="configured_public_baseline_not_account_verified",
            account_verified=False,
        )

    @staticmethod
    def _perp_fee(perp: _PerpLeg) -> _Fee:
        if _positive_decimal(perp.public_taker_fee_bps) or perp.public_taker_fee_bps == Decimal("0"):
            return _Fee(
                taker_bps=perp.public_taker_fee_bps or Decimal("0"),
                source=perp.fee_source or "public_contract_fee_metadata",
                account_verified=False,
            )
        if perp.venue.upper() == "BYBIT":
            return _Fee(
                taker_bps=BYBIT_LINEAR_PUBLIC_VIP0_TAKER_BPS,
                source="bybit_public_vip0_standard_schedule_not_account_verified",
                account_verified=False,
            )
        return _Fee(
            taker_bps=UNKNOWN_PUBLIC_PERP_TAKER_BPS,
            source="conservative_unknown_public_perp_taker_baseline_not_account_verified",
            account_verified=False,
        )

    @staticmethod
    def _buy_cost(quantity: Decimal, price: Decimal, fee: _Fee) -> Decimal:
        return quantity * price * (Decimal("1") + fee.taker_bps / Decimal(10_000))

    @staticmethod
    def _sell_proceeds(quantity: Decimal, price: Decimal, fee: _Fee) -> Decimal:
        return quantity * price * (Decimal("1") - fee.taker_bps / Decimal(10_000))

    @staticmethod
    def _perp_record(perp: _PerpLeg, fee: _Fee) -> dict[str, Any]:
        return {
            "venue": perp.venue,
            "venue_symbol": perp.venue_symbol,
            "base": perp.base,
            "settlement": perp.settlement,
            "best_bid": _decimal_text(perp.best_bid),
            "best_bid_size": _decimal_text(perp.best_bid_size),
            "best_ask": _decimal_text(perp.best_ask),
            "best_ask_size": _decimal_text(perp.best_ask_size),
            "book_received_realtime_ns": perp.book_received_realtime_ns,
            "funding_rate": _decimal_text(perp.funding_rate),
            "funding_rate_short": _decimal_text(perp.funding_rate_short),
            "funding_interval_minutes": perp.funding_interval_minutes,
            "funding_rate_kind": perp.funding_rate_kind,
            "next_funding_time_ms": perp.next_funding_time_ms,
            "context_received_realtime_ns": perp.context_received_realtime_ns,
            "mark_price": _decimal_text(perp.mark_price),
            "index_price": _decimal_text(perp.index_price),
            "quantity_step": _decimal_text(perp.quantity_step),
            "minimum_order_quantity": _decimal_text(perp.minimum_order_quantity),
            "contract_type": perp.contract_type,
            "contract_model": perp.contract_model.as_dict(),
            "execution_model": perp.execution_model,
            "taker_fee_bps_used": _decimal_text(fee.taker_bps),
            "fee_source": fee.source,
            "fee_account_verified": fee.account_verified,
        }

    @staticmethod
    def _spot_record(spot: _SpotLeg, fee_buy: _Fee, fee_sell: _Fee) -> dict[str, Any]:
        return {
            "venue": spot.venue,
            "symbol": spot.symbol,
            "base": spot.base,
            "quote": spot.quote,
            "best_bid": _decimal_text(spot.best_bid),
            "best_bid_size": _decimal_text(spot.best_bid_size),
            "best_ask": _decimal_text(spot.best_ask),
            "best_ask_size": _decimal_text(spot.best_ask_size),
            "received_realtime_ns": spot.received_realtime_ns,
            "taker_buy_fee_bps_used": _decimal_text(fee_buy.taker_bps),
            "taker_sell_fee_bps_used": _decimal_text(fee_sell.taker_bps),
            "fee_source": fee_buy.source,
            "fee_account_verified": fee_buy.account_verified and fee_sell.account_verified,
        }

    @staticmethod
    def _perp_capability_signature(perp: _PerpLeg) -> tuple[object, ...]:
        """Fields that alter what a public perp state is safe to model."""

        return (
            perp.base,
            perp.settlement,
            perp.source,
            perp.execution_model,
            perp.contract_type,
            perp.contract_model.model_id,
            perp.contract_model.supported,
            perp.contract_model.reason,
            perp.funding_rate is not None,
            perp.funding_rate_short is not None,
            perp.funding_rate_kind,
            perp.funding_interval_minutes,
            perp.next_funding_time_ms is not None,
            perp.index_price is not None,
            perp.mark_price is not None,
            perp.public_taker_fee_bps is not None,
        )

    @staticmethod
    def _spot_capability_signature(spot: _SpotLeg) -> tuple[str, str, str, str]:
        return spot.venue, spot.symbol, spot.base, spot.quote

    def _capability_manifest(self) -> dict[str, Any]:
        """Build stable, non-price evidence for the current public inputs.

        This is intentionally a capability declaration, not a market-data
        dump: it contains no BBO values, quantities, funding rates, or PnL.
        That makes the persisted file useful for reproducing model limits
        without reintroducing a raw-tick retention path.
        """

        perpetual_markets = []
        for perp in sorted(self._perps.values(), key=lambda item: (item.venue, item.venue_symbol)):
            funding_reference = (
                "venue_index_or_oracle"
                if _positive_decimal(perp.index_price)
                else ("venue_mark" if _positive_decimal(perp.mark_price) else None)
            )
            perpetual_markets.append(
                {
                    "venue": perp.venue,
                    "venue_symbol": perp.venue_symbol,
                    "base": perp.base,
                    "settlement": perp.settlement,
                    "source": perp.source,
                    "execution_model": perp.execution_model,
                    "contract_type": perp.contract_type,
                    "contract_model": perp.contract_model.as_dict(),
                    "public_taker_fee_available": perp.public_taker_fee_bps is not None,
                    "funding": {
                        "rate_observed": perp.funding_rate is not None,
                        "side_specific_rate_observed": perp.funding_rate_short is not None,
                        "rate_kind": perp.funding_rate_kind,
                        "interval_minutes": perp.funding_interval_minutes,
                        "next_event_observed": perp.next_funding_time_ms is not None,
                        "reference_price_available": funding_reference,
                        "reference_price_funding_semantics_verified": False,
                    },
                }
            )
        spot_markets = [
            {
                "venue": spot.venue,
                "symbol": spot.symbol,
                "base": spot.base,
                "quote": spot.quote,
            }
            for spot in sorted(self._spots.values(), key=lambda item: (item.venue, item.symbol))
        ]
        exact_quote_markets = []
        for provider in sorted(self._observed_exact_quote_providers):
            market = self._direct_markets.get(provider)
            if market is None:
                continue
            exact_quote_markets.append(
                {
                    "provider": provider,
                    "chain": market.chain,
                    "pair": market.dex_pair,
                    "base": market.cex_base_symbol,
                }
            )
        return {
            "schema_version": 1,
            "mode": "read_only_public_market_research",
            "execution_enabled": False,
            "collector_connections_opened_by_analyzer": 0,
            "strategy_families": list(STRATEGY_FAMILIES),
            "markets": {
                "perpetuals": perpetual_markets,
                "spots": spot_markets,
                "exact_quote_sources_observed": exact_quote_markets,
            },
            "limitations": [
                "no_orders_wallets_or_transactions",
                "account_specific_fee_tier_margin_collateral_and_balances_not_verified",
                "only_linear_base_quantity_perpetual_v1_is_modelled",
                "cross_settlement_routes_require_an_explicit_executable_fx_model",
                "funding_reference_and_event_eligibility_semantics_remain_venue_specific",
                "exact_dex_quotes_are_public_pre_trade_simulations_not_post_trade_pool_state",
            ],
        }

    def _flush_capability_manifest(self) -> None:
        if not self._capabilities_dirty or not self.output_directory.exists():
            return
        self.analysis_directory.mkdir(parents=True, exist_ok=True)
        atomic_json(self.capabilities_path, self._capability_manifest())
        self._capabilities_dirty = False

    def _ensure_worker(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._run_worker())

    def _mark_dirty(self, base: str) -> None:
        normalized = base.upper()
        if normalized:
            self._dirty_bases.add(normalized)
            self._wake.set()

    def _store_perp(self, perp: _PerpLeg) -> None:
        key = perp.key
        old = self._perps.get(key)
        if old is not None and old.base != perp.base:
            self._perp_keys_by_base[old.base].discard(key)
        if old is None or (
            self._perp_capability_signature(old)
            != self._perp_capability_signature(perp)
        ):
            self._capabilities_dirty = True
        self._perps[key] = perp
        self._perp_keys_by_base[perp.base].add(key)
        self._mark_dirty(perp.base)

    def _update_generic_perp(self, event: MarketEvent) -> None:
        value = event.value
        assert isinstance(value, PerpQuoteEvent)
        base = value.base.upper()
        settlement = value.settlement.upper()
        if not base or not settlement:
            self._counts["malformed_perp_events"] += 1
            return
        self._store_perp(
            _PerpLeg(
                venue=value.venue.upper(),
                venue_symbol=value.venue_symbol.upper(),
                base=base,
                settlement=settlement,
                best_bid=value.best_bid,
                best_bid_size=value.best_bid_size,
                best_ask=value.best_ask,
                best_ask_size=value.best_ask_size,
                book_received_realtime_ns=value.book_received_realtime_ns,
                book_received_monotonic_ns=value.book_received_monotonic_ns,
                funding_rate=value.funding_rate,
                funding_rate_short=value.funding_rate_short,
                funding_interval_minutes=value.funding_interval_minutes,
                funding_rate_kind=value.funding_rate_kind,
                next_funding_time_ms=value.next_funding_time_ms,
                context_received_realtime_ns=value.context_received_realtime_ns,
                context_received_monotonic_ns=value.context_received_monotonic_ns,
                mark_price=value.mark_price,
                index_price=value.index_price,
                quantity_step=value.quantity_step,
                minimum_order_quantity=value.minimum_order_quantity,
                public_taker_fee_bps=value.public_taker_fee_bps,
                fee_source=value.fee_source,
                contract_type=value.contract_type,
                contract_model=resolve_perp_contract_model(value.contract_type),
                execution_model=value.execution_model,
                source=event.source,
                source_epoch=event.source_epoch,
            ),
        )
        self._counts["perp_quote_events"] += 1

    def _update_spot(self, event: MarketEvent) -> None:
        value = event.value
        assert isinstance(value, CexTopOfBookEvent)
        parsed = _stable_base_and_quote(value.symbol)
        if parsed is None:
            self._counts["unmapped_spot_book_events"] += 1
            return
        base, quote = parsed
        spot = _SpotLeg(
            venue=value.venue.upper(),
            symbol=value.symbol.upper(),
            base=base,
            quote=quote,
            best_bid=value.best_bid,
            best_bid_size=value.best_bid_size,
            best_ask=value.best_ask,
            best_ask_size=value.best_ask_size,
            received_realtime_ns=value.received_realtime_ns,
            received_monotonic_ns=value.received_monotonic_ns,
            source=event.source,
            source_epoch=event.source_epoch,
        )
        old = self._spots.get(spot.key)
        if old is None or (
            self._spot_capability_signature(old)
            != self._spot_capability_signature(spot)
        ):
            self._capabilities_dirty = True
        self._spots[spot.key] = spot
        self._spot_keys_by_base[base].add(spot.key)
        self._counts["spot_book_events"] += 1
        self._mark_dirty(base)

    def _update_linear_book(self, event: MarketEvent) -> None:
        value = event.value
        assert isinstance(value, CexTopOfBookEvent)
        key = value.venue.upper(), value.symbol.upper()
        self._linear_books[key] = (value, event.source, event.source_epoch)
        self._refresh_bybit_linear(key)

    def _update_linear_context(self, event: MarketEvent) -> None:
        value = event.value
        symbol = getattr(value, "symbol", None)
        if not isinstance(symbol, str) or not symbol:
            self._counts["malformed_linear_context_events"] += 1
            return
        key = "BYBIT", symbol.upper()
        self._linear_contexts[key] = (value, event.source, event.source_epoch)
        self._refresh_bybit_linear(key)

    def _refresh_bybit_linear(self, key: tuple[str, str]) -> None:
        book_state = self._linear_books.get(key)
        if book_state is None:
            return
        book, source, source_epoch = book_state
        parsed = _stable_base_and_quote(book.symbol)
        if parsed is None:
            self._counts["unmapped_linear_book_events"] += 1
            return
        base, settlement = parsed
        context_state = self._linear_contexts.get(key)
        context = (
            context_state[0]
            if context_state is not None
            and context_state[1] == source
            and context_state[2] == source_epoch
            else None
        )
        context_received = getattr(context, "received_realtime_ns", None)
        context_received_monotonic = getattr(context, "received_monotonic_ns", None)
        self._store_perp(
            _PerpLeg(
                venue="BYBIT",
                venue_symbol=book.symbol.upper(),
                base=base,
                settlement=settlement,
                best_bid=book.best_bid,
                best_bid_size=book.best_bid_size,
                best_ask=book.best_ask,
                best_ask_size=book.best_ask_size,
                book_received_realtime_ns=book.received_realtime_ns,
                book_received_monotonic_ns=book.received_monotonic_ns,
                funding_rate=getattr(context, "funding_rate", None),
                funding_rate_short=None,
                # The public ticker contains the next timestamp but not a
                # verified per-contract interval in this shared source.  Do
                # not silently assume eight hours for every instrument.
                funding_interval_minutes=None,
                funding_rate_kind=(
                    "public_current_rate_interval_not_confirmed"
                    if context is not None
                    else None
                ),
                next_funding_time_ms=getattr(context, "next_funding_time_ms", None),
                context_received_realtime_ns=(
                    context_received if isinstance(context_received, int) else None
                ),
                context_received_monotonic_ns=(
                    context_received_monotonic
                    if isinstance(context_received_monotonic, int)
                    else None
                ),
                mark_price=getattr(context, "mark_price", None),
                index_price=getattr(context, "index_price", None),
                quantity_step=None,
                minimum_order_quantity=None,
                public_taker_fee_bps=BYBIT_LINEAR_PUBLIC_VIP0_TAKER_BPS,
                fee_source="bybit_public_vip0_standard_schedule_not_account_verified",
                contract_type="linear_perpetual",
                contract_model=resolve_perp_contract_model("linear_perpetual"),
                execution_model="central_limit_order_book",
                source=source,
                source_epoch=source_epoch,
            ),
        )
        self._counts["linear_perp_book_or_context_events"] += 1

    def handle_source_epoch_change(
        self,
        change: SourceEpochChange | str,
        old_epoch: int | None = None,
        new_epoch: int | None = None,
    ) -> None:
        """Purge every cached leg owned by the previous source epoch."""

        if isinstance(change, SourceEpochChange):
            source = change.source
            epoch = change.source_epoch
        else:
            if new_epoch is None:
                raise TypeError("new_epoch is required for the legacy epoch callback")
            source = change
            epoch = new_epoch
        current = self._current_source_epochs.get(source)
        if current is not None and epoch <= current:
            if epoch == current:
                return
            raise RuntimeError(f"source epoch regressed for {source!r}: {epoch} < {current}")
        self._current_source_epochs[source] = epoch
        invalidated_dex = self._purge_dex_source_epoch(source, epoch - 1)
        invalidated_spots = 0
        invalidated_perps = 0
        for key, leg in tuple(self._spots.items()):
            if leg.source == source and leg.source_epoch < epoch:
                self._spots.pop(key, None)
                self._spot_keys_by_base[leg.base].discard(key)
                if not self._spot_keys_by_base[leg.base]:
                    self._spot_keys_by_base.pop(leg.base, None)
                invalidated_spots += 1
        for key, leg in tuple(self._perps.items()):
            if leg.source == source and leg.source_epoch < epoch:
                self._perps.pop(key, None)
                self._perp_keys_by_base[leg.base].discard(key)
                if not self._perp_keys_by_base[leg.base]:
                    self._perp_keys_by_base.pop(leg.base, None)
                invalidated_perps += 1
        self._linear_books = {
            key: value
            for key, value in self._linear_books.items()
            if not (value[1] == source and value[2] < epoch)
        }
        self._linear_contexts = {
            key: value
            for key, value in self._linear_contexts.items()
            if not (value[1] == source and value[2] < epoch)
        }
        self._dirty_bases.clear()
        now_realtime_ns = self._realtime_ns()
        now_monotonic_ns = self._monotonic_ns()
        for key, state in tuple(self._active.items()):
            self._active.pop(key)
            self._candidate_closed += 1
            if state.persisted:
                self._persist_candidate_event(
                    self._candidate_event(
                        "candidate_closed",
                        state,
                        observed_realtime_ns=now_realtime_ns,
                        observed_monotonic_ns=now_monotonic_ns,
                        close_reason="source_epoch_advanced",
                    ),
                )
            else:
                self._counts["candidate_shorter_than_minimum_persistence"] += 1
        if invalidated_dex:
            self._exact_quote_pair_cache = ExactQuotePairCache(
                max_buckets=self._exact_quote_pair_cache.max_buckets,
                max_records_per_bucket=self._exact_quote_pair_cache.max_records_per_bucket,
            )
        self._counts["epoch_invalidated_spot_legs"] += invalidated_spots
        self._counts["epoch_invalidated_perp_legs"] += invalidated_perps
        self._capabilities_dirty = True

    def _remove_dex_quote_key(self, key: tuple[str, str, str]) -> bool:
        """Remove a DEX quote key from primary cache and all secondary indexes."""
        provider, _slot_id, _direction = key
        self._dex_quote_provenance.pop(key, None)
        if key in self._dex_quotes:
            self._dex_quotes.pop(key, None)
            for base_set in self._dex_keys_by_base.values():
                base_set.discard(key)
            self._capabilities_dirty = True
            return True
        return False

    def _purge_dex_provider(self, provider: str) -> int:
        """Remove all DEX quotes for a single provider."""
        removed = 0
        keys_to_remove = [
            key for key in self._dex_keys_by_base.get(provider.upper(), set())
            if key in self._dex_quotes
        ]
        # Also check all base sets since provider isn't the dict key
        all_provider_keys = [key for key in self._dex_quotes if key[0] == provider]
        for key in all_provider_keys:
            if self._remove_dex_quote_key(key):
                removed += 1
        for base in list(self._dex_keys_by_base):
            self._dex_keys_by_base[base].difference_update(
                key for key in all_provider_keys
            )
            if not self._dex_keys_by_base[base]:
                self._dex_keys_by_base.pop(base, None)
        if removed:
            self._capabilities_dirty = True
        return removed

    def _purge_dex_source_epoch(self, source: str, old_epoch: int) -> int:
        """Purge DEX quotes belonging to a source epoch that no longer owns them."""
        invalidated = 0
        for key in list(self._dex_quotes.keys()):
            provenance = self._dex_quote_provenance.get(key)
            if provenance is not None and provenance[0] == source and provenance[1] <= old_epoch:
                if self._remove_dex_quote_key(key):
                    invalidated += 1
        if invalidated:
            for base in list(self._dex_keys_by_base):
                empty = all(
                    key not in self._dex_quotes for key in self._dex_keys_by_base.get(base, set())
                )
                if empty:
                    self._dex_keys_by_base.pop(base, None)
            self._capabilities_dirty = True
        self._counts["epoch_invalidated_dex_quotes"] += invalidated
        return invalidated

    def _accept_event_epoch(self, event: MarketEvent) -> bool:
        current = self._current_source_epochs.get(event.source)
        if current is None:
            self._current_source_epochs[event.source] = event.source_epoch
            return True
        if event.source_epoch < current:
            self._counts["old_epoch_events_rejected"] += 1
            return False
        if event.source_epoch > current:
            raise RuntimeError(
                f"event epoch {event.source_epoch} for {event.source!r} arrived before "
                f"SourceEpochChange from epoch {current}",
            )
        return True

    def _prune_expired_dex_quotes(self, now_monotonic_ns: int) -> int:
        """Physically remove DEX quotes older than the freshness TTL."""
        freshness_ns = int(self.max_response_skew_ms * Decimal(1_000_000))
        cutoff = now_monotonic_ns - freshness_ns
        pruned = 0
        for key in list(self._dex_quotes.keys()):
            quote = self._dex_quotes.get(key)
            if quote is not None and quote.response_received_monotonic_ns < cutoff:
                if self._remove_dex_quote_key(key):
                    pruned += 1
        if pruned:
            for base in list(self._dex_keys_by_base):
                empty = all(
                    key not in self._dex_quotes for key in self._dex_keys_by_base.get(base, set())
                )
                if empty:
                    self._dex_keys_by_base.pop(base, None)
            self._capabilities_dirty = True
            self._counts["stale_dex_prunes"] += pruned
        return pruned

    def _update_exact_quote(self, event: MarketEvent, value: ExactInputQuote) -> None:
        market = self._direct_markets.get(value.provider)
        if market is None:
            self._counts["unmapped_exact_quote_events"] += 1
            return
        if (
            value.status != "ok"
            or value.direction not in {"buy_base", "sell_base"}
            or not _positive_decimal(value.base_amount)
            or not _positive_decimal(value.quote_amount)
            or value.requested_notional_quote is None
        ):
            self._counts["malformed_exact_quote_events"] += 1
            return
        slot_id = value.quote_slot_id
        if not slot_id:
            self._counts["malformed_exact_quote_events"] += 1
            return
        key = value.provider, slot_id, value.direction
        previous = self._dex_quotes.get(key)
        if previous is not None and value.source_epoch < previous.source_epoch:
            self._counts["old_epoch_exacts_ignored"] += 1
            return
        if previous is not None and quote_received_before(value, previous):
            self._counts["exact_quote_out_of_order_ignored"] += 1
            return
        self._dex_quotes[key] = value
        self._dex_quote_provenance[key] = (event.source, event.source_epoch)
        self._dex_keys_by_base[market.cex_base_symbol.upper()].add(key)
        if value.provider not in self._observed_exact_quote_providers:
            self._observed_exact_quote_providers.add(value.provider)
            self._capabilities_dirty = True
        if self._exact_quote_pair_cache.put(value):
            self._counts["exact_quote_pair_cache_records"] += 1
        self._counts["exact_quote_events"] += 1
        self._mark_dirty(market.cex_base_symbol)

    async def handle_event(self, event: MarketEvent) -> None:
        """Accept a public event without blocking the collector."""

        if self._closed:
            return
        if not self._accept_event_epoch(event):
            return
        self._ensure_worker()
        self._counts["events_seen"] += 1
        if isinstance(event.value, PerpQuoteEvent):
            self._update_generic_perp(event)
            return
        if event.kind == "order_book" and isinstance(event.value, CexTopOfBookEvent):
            if event.value.category == "spot":
                self._update_spot(event)
            elif event.value.category == "linear":
                self._update_linear_book(event)
            return
        if event.kind == "perp_context" and event.source == "cex:BYBIT:linear":
            self._update_linear_context(event)
            return
        if event.kind == "exact_input_quote" and isinstance(event.value, ExactInputQuote):
            quote = (
                event.value
                if event.value.source_epoch == event.source_epoch
                else replace(event.value, source_epoch=event.source_epoch)
            )
            self._update_exact_quote(event, quote)

    def _perps_for_base(self, base: str) -> tuple[_PerpLeg, ...]:
        return tuple(
            self._perps[key]
            for key in sorted(self._perp_keys_by_base.get(base, ()))
            if key in self._perps
            and self._perps[key].base == base
            and self._current_source_epochs.get(
                self._perps[key].source,
                self._perps[key].source_epoch,
            ) == self._perps[key].source_epoch
        )

    def _spots_for_base(self, base: str) -> tuple[_SpotLeg, ...]:
        return tuple(
            self._spots[key]
            for key in sorted(self._spot_keys_by_base.get(base, ()))
            if key in self._spots
            and self._spots[key].base == base
            and self._current_source_epochs.get(
                self._spots[key].source,
                self._spots[key].source_epoch,
            ) == self._spots[key].source_epoch
        )

    def _quotes_for_base(self, base: str) -> tuple[tuple[ExactInputQuote, Any], ...]:
        result: list[tuple[ExactInputQuote, Any]] = []
        for key in sorted(self._dex_keys_by_base.get(base, ())):
            quote = self._dex_quotes.get(key)
            if quote is None:
                continue
            provenance = self._dex_quote_provenance.get(key)
            if provenance is None or self._current_source_epochs.get(
                provenance[0], provenance[1],
            ) != provenance[1]:
                continue
            market = self._direct_markets.get(quote.provider)
            if market is not None:
                result.append((quote, market))
        return tuple(result)

    def _paired_reverse_dex_quote(
        self,
        quote: ExactInputQuote,
        *,
        now_monotonic_ns: int,
    ) -> tuple[ExactQuotePair | None, str]:
        """Find a fresh exact unwind without assuming one latest quote is it.

        Providers may publish multiple fee tiers/routes for one notional.  The
        bounded cache retains those compact records by actual raw base amount,
        which lets a matching reverse survive a later incompatible overwrite.
        A source round is quality evidence, not the sole identity condition.
        """

        return self._exact_quote_pair_cache.best_reverse_for(
            quote,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ns=int(self.max_exact_quote_age_ms * Decimal(1_000_000)),
        )

    def _sequential_result_for(
        self,
        quote: ExactInputQuote,
    ) -> SequentialUnwindResult | None:
        """Return a sequential AMM unwind for a configured shadow simulator.

        The simulator is an injectable, read-only service that already holds an
        immutable snapshot for the allowlisted pool.  When it is not configured
        (the default) this method returns ``None`` and the legacy paired path is
        unchanged.
        """

        if self._sequential_amm_simulator is None:
            return None
        simulate = getattr(self._sequential_amm_simulator, "simulate_buy_sell", None)
        if not callable(simulate):
            self._counts["dex_perp_sequential_simulator_unavailable"] += 1
            return None
        try:
            result = simulate(quote)
        except Exception as exc:  # noqa: BLE001
            self._counts["dex_perp_sequential_simulator_error"] += 1
            self._recent_errors.append(f"sequential: {type(exc).__name__}: {exc}"[:512])
            return None
        if result is None:
            return None
        if not isinstance(result, SequentialUnwindResult):
            self._counts["dex_perp_sequential_malformed_result"] += 1
            return None
        # The first vertical slice is intentionally strict for real Raydium
        # CPMM observations.  A few historical unit fixtures use a synthetic
        # protocol and omit canonical mint IDs; those remain on the legacy
        # injectable seam, while every live CPMM result must carry complete
        # identity/quantity/evidence binding.
        strict_binding = (
            quote.protocol == "raydium_cpmm"
            or quote.input_asset_id is not None
            or quote.output_asset_id is not None
        )
        if strict_binding:
            if result.provider != "raydium_cpmm":
                self._counts["dex_perp_sequential_protocol_mismatch"] += 1
                return None
            if not result.evidence_hash:
                self._counts["dex_perp_sequential_evidence_missing"] += 1
                return None
            if (
                result.worker_generation is None
                or result.worker_generation <= 0
                or result.source_epoch is None
                or result.boot_id is None
                or result.context_slot is None
            ):
                self._counts["dex_perp_sequential_generation_mismatch"] += 1
                return None
            if (
                not isinstance(quote.input_asset_id, str)
                or quote.input_asset_id != result.stable_asset_id
                or not isinstance(quote.output_asset_id, str)
                or quote.output_asset_id != result.base_asset_id
            ):
                self._counts["dex_perp_sequential_asset_binding_mismatch"] += 1
                return None
            if (
                not isinstance(quote.input_amount_raw, int)
                or quote.input_amount_raw != result.buy_input_raw
                or result.sell_input_raw != result.buy_output_raw
            ):
                self._counts["dex_perp_sequential_raw_quantity_mismatch"] += 1
                return None
            describe = getattr(self._sequential_amm_simulator, "describe", None)
            metadata = describe() if callable(describe) else None
            if not isinstance(metadata, Mapping):
                self._counts["dex_perp_sequential_metadata_missing"] += 1
                return None
            if (
                metadata.get("provider") != result.provider
                or metadata.get("pool_id") != result.pool_id
                or metadata.get("stable_asset_id") != result.stable_asset_id
                or metadata.get("base_asset_id") != result.base_asset_id
                or not isinstance(metadata.get("perp_symbol"), str)
                or metadata.get("snapshot_id") != result.snapshot_id
                or metadata.get("snapshot_hash") != result.snapshot_hash
                or metadata.get("worker_generation") != result.worker_generation
                or metadata.get("source_epoch") != result.source_epoch
                or metadata.get("boot_id") != result.boot_id
                or metadata.get("context_slot") != result.context_slot
            ):
                self._counts["dex_perp_sequential_snapshot_binding_mismatch"] += 1
                return None
            if self._persist_sequential_evidence(result) is None:
                self._counts["dex_perp_sequential_evidence_unavailable"] += 1
                return None
        return result

    @staticmethod
    def _asset_decimals(asset_id: str) -> int | None:
        """Extract decimals from the canonical ``...:<mint>:<decimals>`` ID."""

        if not isinstance(asset_id, str):
            return None
        tail = asset_id.rsplit(":", 1)[-1]
        try:
            decimals = int(tail)
        except (TypeError, ValueError):
            return None
        return decimals if decimals >= 0 else None

    def _sequential_metadata(self) -> Mapping[str, object] | None:
        describe = getattr(self._sequential_amm_simulator, "describe", None)
        metadata = describe() if callable(describe) else None
        return metadata if isinstance(metadata, Mapping) else None

    def _persist_sequential_evidence(self, result: SequentialUnwindResult) -> str | None:
        """Persist one deduplicated, bounded evidence artifact for replay."""

        if not result.evidence_hash:
            return None
        existing = self._sequential_evidence_paths.get(result.evidence_hash)
        if existing is not None:
            return existing
        getter = getattr(self._sequential_amm_simulator, "evidence_bundle", None)
        bundle = getter() if callable(getter) else None
        if not isinstance(bundle, SimulationEvidenceBundle):
            return None
        if bundle.evidence_hash != result.evidence_hash:
            return None
        evidence_dir = self.analysis_directory / "sequential_evidence"
        evidence_dir.mkdir(parents=True, exist_ok=True)
        path = evidence_dir / f"{result.evidence_hash}.json"
        try:
            if not path.exists():
                save_evidence_bundle(bundle, path)
        except OSError as exc:
            self._recent_errors.append(f"sequential evidence: {type(exc).__name__}: {exc}"[:512])
            return None
        self._sequential_evidence_paths[result.evidence_hash] = str(path)
        while len(self._sequential_evidence_paths) > self._max_sequential_evidence_records:
            old_hash = next(iter(self._sequential_evidence_paths))
            old_path = self._sequential_evidence_paths.pop(old_hash)
            if old_hash != result.evidence_hash:
                try:
                    Path(old_path).unlink(missing_ok=True)
                except OSError:
                    pass
        return str(path)

    def _timing(self, *timestamps: int | None) -> tuple[bool, float | None]:
        if not timestamps or any(value is None for value in timestamps):
            return False, None
        values = [int(value) for value in timestamps if value is not None]
        skew_ms = Decimal(max(values) - min(values)) / Decimal(1_000_000)
        return skew_ms <= self.max_response_skew_ms, float(skew_ms)

    @staticmethod
    def _receipt_freshness(
        *,
        now_realtime_ns: int,
        now_monotonic_ns: int,
        max_age_ms: Decimal,
        receipts: Mapping[str, tuple[int | None, int | None]],
    ) -> tuple[bool, dict[str, dict[str, float | None]]]:
        """Require both local clocks to support the freshness claim.

        UTC timestamps are evidence and catch a long suspend, while monotonic
        receipt time is immune to ordinary wall-clock adjustments.  A jump in
        either direction can only make the route less eligible, never fresher.
        """

        ages: dict[str, dict[str, float | None]] = {}
        fresh = bool(receipts)
        for name, (received_realtime_ns, received_monotonic_ns) in receipts.items():
            realtime_age = _age_ms(
                now_ns=now_realtime_ns,
                then_ns=received_realtime_ns,
            )
            monotonic_age = _age_ms(
                now_ns=now_monotonic_ns,
                then_ns=received_monotonic_ns,
            )
            ages[name] = {
                "realtime": realtime_age,
                "monotonic": monotonic_age,
            }
            if (
                realtime_age is None
                or monotonic_age is None
                or Decimal(str(realtime_age)) > max_age_ms
                or Decimal(str(monotonic_age)) > max_age_ms
            ):
                fresh = False
        return fresh, ages

    def _funding_projection(
        self,
        *,
        perp: _PerpLeg,
        side: str,
        quantity: Decimal,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> tuple[FundingProjection, dict[str, Any]]:
        context_fresh, context_ages = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_funding_context_age_ms,
            receipts={
                "funding_context": (
                    perp.context_received_realtime_ns,
                    perp.context_received_monotonic_ns,
                ),
            },
        )
        detail: dict[str, Any] = {
            "funding_rate": _decimal_text(perp.funding_rate),
            "funding_rate_short": _decimal_text(perp.funding_rate_short),
            "funding_interval_minutes": perp.funding_interval_minutes,
            "funding_rate_kind": perp.funding_rate_kind,
            "next_funding_time_ms": perp.next_funding_time_ms,
            "funding_context_age_ms": context_ages["funding_context"],
            "funding_context_fresh": context_fresh,
            "funding_context_freshness_clock": "local_realtime_and_monotonic",
            "side": side,
            "normalization": "positive common funding rate means longs pay shorts",
        }
        reference_price: Decimal | None = None
        reference_price_kind: str | None = None
        if _positive_decimal(perp.index_price):
            reference_price = perp.index_price
            reference_price_kind = "venue_index_or_oracle"
        elif _positive_decimal(perp.mark_price):
            reference_price = perp.mark_price
            reference_price_kind = "venue_mark"
        detail.update(
            {
                "reference_price": _decimal_text(reference_price),
                "reference_price_kind": reference_price_kind,
                # The venue-specific ContractModel must later certify which
                # reference actually determines settlement.  Do not substitute
                # a current BBO here merely to get a funding number.
                "reference_price_funding_semantics_verified": False,
            },
        )
        if not context_fresh:
            projection = FundingProjection(
                normalized_cashflow_per_hour=None,
                scheduled_cashflow_for_horizon=None,
                scheduled_event_times_ms=(),
                quality="unknown",
                reason="funding_context_stale",
                reference_price=reference_price,
                reference_price_kind=reference_price_kind,
            )
            detail["available"] = False
            detail["funding_projection_quality"] = projection.quality
            detail["funding_projection_reason"] = projection.reason
            detail["funding_horizon_model_complete"] = False
            return projection, detail
        if perp.funding_rate_short is not None:
            projection = FundingProjection(
                normalized_cashflow_per_hour=None,
                scheduled_cashflow_for_horizon=None,
                scheduled_event_times_ms=(),
                quality="unknown",
                reason="side_specific_funding_semantics_not_modelled",
                reference_price=reference_price,
                reference_price_kind=reference_price_kind,
            )
            detail["available"] = False
            detail["funding_projection_quality"] = projection.quality
            detail["funding_projection_reason"] = projection.reason
            detail["funding_horizon_model_complete"] = False
            return projection, detail
        projection = project_common_rate_discrete_funding(
            rate=perp.funding_rate,
            rate_kind=perp.funding_rate_kind,
            interval_minutes=perp.funding_interval_minutes,
            next_event_time_ms=perp.next_funding_time_ms,
            reference_price=reference_price,
            reference_price_kind=reference_price_kind,
            quantity=quantity,
            side=side,
            now_realtime_ns=now_ns,
            horizon_hours=self.funding_horizon_hours,
        )
        detail.update(
            {
                # A known discrete event can be sufficient for a horizon
                # cashflow even when this venue has not published a reliable
                # recurring interval.  Do not hide that evidence merely
                # because it cannot be normalised to an hourly comparison.
                "available": (
                    projection.normalized_cashflow_per_hour is not None
                    or projection.horizon_model_complete
                ),
                "funding_rate_per_hour": _decimal_text(
                    projection.normalized_cashflow_per_hour
                ),
                "scheduled_funding_cashflow_for_horizon_usdt": _decimal_text(
                    projection.scheduled_cashflow_for_horizon
                ),
                "scheduled_funding_event_times_ms": list(
                    projection.scheduled_event_times_ms
                ),
                "funding_projection_quality": projection.quality,
                "funding_projection_reason": projection.reason,
                "funding_horizon_model_complete": projection.horizon_model_complete,
                "side_specific_rate_present_not_used": False,
            },
        )
        return projection, detail

    @staticmethod
    def _normalise_quantity(quantity: Decimal, *perps: _PerpLeg) -> Decimal | None:
        if not _positive_decimal(quantity):
            return None
        result = round_down_to_common_quantity_lattice(
            quantity,
            (perp.quantity_step for perp in perps),
        )
        if not _positive_decimal(result):
            return None
        if any(
            _positive_decimal(perp.minimum_order_quantity)
            and result < perp.minimum_order_quantity
            for perp in perps
        ):
            return None
        return result

    def _candidate_quantity(
        self,
        *,
        buy_price: Decimal,
        buy_size: Decimal | None,
        sell_size: Decimal | None,
        perps: tuple[_PerpLeg, ...],
        extra_limits: tuple[Decimal | None, ...] = (),
    ) -> Decimal | None:
        if not _positive_decimal(buy_price) or not _positive_decimal(buy_size) or not _positive_decimal(sell_size):
            return None
        quantity = min(self.target_notional_usdt / buy_price, buy_size, sell_size)
        for limit in extra_limits:
            if not _positive_decimal(limit):
                return None
            quantity = min(quantity, limit)
        return self._normalise_quantity(quantity, *perps)

    async def _run_worker(self) -> None:
        try:
            while not self._closed:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=self.coalesce_interval_seconds)
                except TimeoutError:
                    pass
                self._wake.clear()
                try:
                    bases = tuple(sorted(self._dirty_bases))[:64]
                    self._dirty_bases.difference_update(bases)
                    for base in bases:
                        self._evaluate_base(base)
                    if self._dirty_bases:
                        self._wake.set()
                    self._close_stale_candidates()
                    self._prune_expired_dex_quotes(self._monotonic_ns())
                    await asyncio.sleep(0)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._counts["worker_errors"] += 1
                    self._recent_errors.append(f"worker: {type(exc).__name__}: {exc}"[:512])
                    await asyncio.sleep(self.coalesce_interval_seconds)
        except asyncio.CancelledError:
            raise

    def _evaluate_base(self, base: str) -> None:
        spots = self._spots_for_base(base)
        now_ns = self._realtime_ns()
        now_monotonic_ns = self._monotonic_ns()
        # A CEX-to-CEX trade is a complete paired inventory cycle when both
        # venues already hold the required base and quote balances.  It is
        # deliberately evaluated independently of whether this base also has
        # a perpetual market.
        for index, buy_spot in enumerate(spots):
            for sell_spot in spots[index + 1 :]:
                self._evaluate_spot_pair(
                    buy_spot,
                    sell_spot,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                self._evaluate_spot_pair(
                    sell_spot,
                    buy_spot,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )

        perps = self._perps_for_base(base)
        if not perps:
            return
        for index, long_perp in enumerate(perps):
            for short_perp in perps[index + 1 :]:
                self._evaluate_perp_pair(
                    long_perp,
                    short_perp,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                self._evaluate_perp_pair(
                    short_perp,
                    long_perp,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
        for spot in spots:
            for perp in perps:
                self._evaluate_spot_perp(
                    spot,
                    perp,
                    direction="long_spot_short_perp",
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                self._evaluate_spot_perp(
                    spot,
                    perp,
                    direction="short_spot_long_perp",
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
        for quote, market in self._quotes_for_base(base):
            for perp in perps:
                self._evaluate_dex_perp(
                    quote,
                    market,
                    perp,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )

    def _evaluate_spot_pair(
        self,
        buy_spot: _SpotLeg,
        sell_spot: _SpotLeg,
        *,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        """Model buy on one CEX and simultaneous sell on another.

        The pair is globally flat in the base asset only when inventory is
        pre-positioned at both venues.  It is therefore a valid instantaneous
        cash-flow model, while rebalancing remains a stated execution blocker
        rather than an invented zero cost.
        """

        self._counts["spot_spot_pair_considered"] += 1
        if (
            buy_spot.key == sell_spot.key
            or buy_spot.base != sell_spot.base
            or buy_spot.quote not in STABLE_QUOTES
            or sell_spot.quote not in STABLE_QUOTES
        ):
            self._counts["spot_spot_incompatible"] += 1
            return
        if buy_spot.quote != sell_spot.quote:
            # A stable-looking ticker is not an FX rate.  Keep the native
            # quote assets separate until an executable conversion model is
            # supplied, rather than turning USDC/USDT into a free parity.
            self._counts["spot_spot_cross_settlement_fx_unavailable"] += 1
            return
        if not buy_spot.executable_bbo or not sell_spot.executable_bbo:
            self._counts["spot_spot_missing_executable_bbo"] += 1
            return
        assert buy_spot.best_ask is not None
        assert buy_spot.best_ask_size is not None
        assert sell_spot.best_bid is not None
        assert sell_spot.best_bid_size is not None
        quantity = self._candidate_quantity(
            buy_price=buy_spot.best_ask,
            buy_size=buy_spot.best_ask_size,
            sell_size=sell_spot.best_bid_size,
            perps=(),
        )
        if quantity is None:
            self._counts["spot_spot_insufficient_visible_size"] += 1
            return
        buy_fee = self._spot_fee(buy_spot, side="buy")
        sell_fee = self._spot_fee(sell_spot, side="sell")
        buy_cost = self._buy_cost(quantity, buy_spot.best_ask, buy_fee)
        sell_proceeds = self._sell_proceeds(quantity, sell_spot.best_bid, sell_fee)
        response_skew_valid, response_skew_ms = self._timing(
            buy_spot.received_realtime_ns,
            sell_spot.received_realtime_ns,
        )
        market_data_fresh, market_data_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "buy_spot_bbo": (
                    buy_spot.received_realtime_ns,
                    buy_spot.received_monotonic_ns,
                ),
                "sell_spot_bbo": (
                    sell_spot.received_realtime_ns,
                    sell_spot.received_monotonic_ns,
                ),
            },
        )
        timing_valid = response_skew_valid and market_data_fresh
        route_id = (
            f"buy_spot:{buy_spot.venue}:{buy_spot.symbol}|"
            f"sell_spot:{sell_spot.venue}:{sell_spot.symbol}"
        )
        common = {
            "route_id": route_id,
            "base": buy_spot.base,
            "direction": "buy_first_spot_sell_second_spot",
            "hedged_base_quantity": _decimal_text(quantity),
            "target_notional_usdt": _decimal_text(self.target_notional_usdt),
            "visible_entry_notional_usdt": _decimal_text(quantity * buy_spot.best_ask),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "pnl_currency": buy_spot.quote,
            "settlement_parity_assumption": "same_settlement_currency_no_fx_conversion",
            "buy_spot": self._spot_record(buy_spot, buy_fee, buy_fee),
            "sell_spot": self._spot_record(sell_spot, sell_fee, sell_fee),
            "buy_cost_usdt": _decimal_text(buy_cost),
            "sell_proceeds_usdt": _decimal_text(sell_proceeds),
            "inventory_requirement": (
                "requires_prepositioned_quote_on_buy_venue_and_base_on_sell_venue"
            ),
            "rebalance_cost_included": False,
            "top_of_book_only": True,
        }
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="spot_spot_inventory_cycle",
                strategy="cex_spot_cross_venue_inventory_cycle",
                common=common,
                timing_valid=timing_valid,
                notional=quantity * buy_spot.best_ask,
                net_before_funding=sell_proceeds - buy_cost,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=Decimal("0"),
                # There is no open derivative position to close: the two
                # simultaneous fills leave global base exposure unchanged.
                full_exit_model=True,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _evaluate_perp_pair(
        self,
        long_perp: _PerpLeg,
        short_perp: _PerpLeg,
        *,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        """Model a long leg on ``long_perp`` and a short leg on ``short_perp``."""

        self._counts["perp_perp_pair_considered"] += 1
        if (
            long_perp.base != short_perp.base
            or long_perp.key == short_perp.key
            or long_perp.settlement not in STABLE_QUOTES
            or short_perp.settlement not in STABLE_QUOTES
        ):
            self._counts["perp_perp_incompatible"] += 1
            return
        if long_perp.settlement != short_perp.settlement:
            self._counts["perp_perp_cross_settlement_fx_unavailable"] += 1
            return
        if not long_perp.contract_model.supported or not short_perp.contract_model.supported:
            # The common cash-flow formula is deliberately limited to the
            # typed linear/base-quantity model.  Never treat an inverse or
            # unclassified quote as if its BBO size were base quantity.
            self._counts["perp_perp_unsupported_contract_model"] += 1
            return
        if not long_perp.executable_bbo or not short_perp.executable_bbo:
            self._counts["perp_perp_missing_executable_bbo"] += 1
            return
        assert long_perp.best_ask is not None
        assert long_perp.best_bid is not None
        assert long_perp.best_ask_size is not None
        assert short_perp.best_bid is not None
        assert short_perp.best_ask is not None
        assert short_perp.best_bid_size is not None
        quantity = self._candidate_quantity(
            buy_price=long_perp.best_ask,
            buy_size=long_perp.best_ask_size,
            sell_size=short_perp.best_bid_size,
            perps=(long_perp, short_perp),
        )
        if quantity is None:
            self._counts["perp_perp_insufficient_visible_size_or_contract_step"] += 1
            return
        long_fee = self._perp_fee(long_perp)
        short_fee = self._perp_fee(short_perp)
        response_skew_valid, response_skew_ms = self._timing(
            long_perp.book_received_realtime_ns,
            short_perp.book_received_realtime_ns,
        )
        market_data_fresh, market_data_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "long_perp_bbo": (
                    long_perp.book_received_realtime_ns,
                    long_perp.book_received_monotonic_ns,
                ),
                "short_perp_bbo": (
                    short_perp.book_received_realtime_ns,
                    short_perp.book_received_monotonic_ns,
                ),
            },
        )
        timing_valid = response_skew_valid and market_data_fresh
        open_cashflow = (
            self._sell_proceeds(quantity, short_perp.best_bid, short_fee)
            - self._buy_cost(quantity, long_perp.best_ask, long_fee)
        )
        close_cashflow = (
            self._sell_proceeds(quantity, long_perp.best_bid, long_fee)
            - self._buy_cost(quantity, short_perp.best_ask, short_fee)
        )
        flat_now_pnl = open_cashflow + close_cashflow
        notional = quantity * long_perp.best_ask
        route_id = (
            f"long:{long_perp.venue}:{long_perp.venue_symbol}|"
            f"short:{short_perp.venue}:{short_perp.venue_symbol}"
        )
        common = {
            "route_id": route_id,
            "base": long_perp.base,
            "direction": "long_first_perp_short_second_perp",
            "hedged_base_quantity": _decimal_text(quantity),
            "target_notional_usdt": _decimal_text(self.target_notional_usdt),
            "visible_entry_notional_usdt": _decimal_text(notional),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "pnl_currency": long_perp.settlement,
            "settlement_parity_assumption": "same_settlement_currency_no_fx_conversion",
            "long_perp": self._perp_record(long_perp, long_fee),
            "short_perp": self._perp_record(short_perp, short_fee),
            "open_cashflow_usdt": _decimal_text(open_cashflow),
            "current_close_cashflow_usdt": _decimal_text(close_cashflow),
            "top_of_book_only": True,
        }
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="perp_perp_flat_price_cycle",
                strategy="perp_perp_flat_price_cycle",
                common=common,
                timing_valid=timing_valid,
                notional=notional,
                net_before_funding=flat_now_pnl,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=Decimal("0"),
                full_exit_model=True,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )
        long_funding, long_funding_detail = self._funding_projection(
            perp=long_perp,
            side="long",
            quantity=quantity,
            now_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        short_funding, short_funding_detail = self._funding_projection(
            perp=short_perp,
            side="short",
            quantity=quantity,
            now_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        if (
            (
                long_funding.normalized_cashflow_per_hour is None
                and not long_funding.horizon_model_complete
            )
            or (
                short_funding.normalized_cashflow_per_hour is None
                and not short_funding.horizon_model_complete
            )
        ):
            self._counts["perp_perp_funding_context_unavailable"] += 1
            return
        funding_horizon_model_complete = (
            long_funding.horizon_model_complete and short_funding.horizon_model_complete
        )
        funding_horizon_pnl = (
            long_funding.scheduled_cashflow_for_horizon
            + short_funding.scheduled_cashflow_for_horizon
            if funding_horizon_model_complete
            and long_funding.scheduled_cashflow_for_horizon is not None
            and short_funding.scheduled_cashflow_for_horizon is not None
            else None
        )
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="perp_perp_funding_carry",
                strategy="cross_perp_funding_carry",
                common=common,
                timing_valid=timing_valid,
                notional=notional,
                net_before_funding=flat_now_pnl,
                funding_per_hour=(
                    long_funding.normalized_cashflow_per_hour
                    + short_funding.normalized_cashflow_per_hour
                    if long_funding.normalized_cashflow_per_hour is not None
                    and short_funding.normalized_cashflow_per_hour is not None
                    else None
                ),
                funding_details=(long_funding_detail, short_funding_detail),
                network_reserve=Decimal("0"),
                full_exit_model=True,
                funding_horizon_pnl=funding_horizon_pnl,
                funding_horizon_model_complete=funding_horizon_model_complete,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _evaluate_spot_perp(
        self,
        spot: _SpotLeg,
        perp: _PerpLeg,
        *,
        direction: str,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        self._counts["spot_perp_pair_considered"] += 1
        if spot.quote not in STABLE_QUOTES or perp.settlement not in STABLE_QUOTES:
            self._counts["spot_perp_incompatible_settlement"] += 1
            return
        if spot.quote != perp.settlement:
            self._counts["spot_perp_cross_settlement_fx_unavailable"] += 1
            return
        if not perp.contract_model.supported:
            self._counts["spot_perp_unsupported_contract_model"] += 1
            return
        if not spot.executable_bbo or not perp.executable_bbo:
            self._counts["spot_perp_missing_executable_bbo"] += 1
            return
        assert spot.best_bid is not None
        assert spot.best_ask is not None
        assert spot.best_bid_size is not None
        assert spot.best_ask_size is not None
        assert perp.best_bid is not None
        assert perp.best_ask is not None
        assert perp.best_bid_size is not None
        assert perp.best_ask_size is not None
        spot_buy = self._spot_fee(spot, side="buy")
        spot_sell = self._spot_fee(spot, side="sell")
        perp_fee = self._perp_fee(perp)
        if direction == "long_spot_short_perp":
            quantity = self._candidate_quantity(
                buy_price=spot.best_ask,
                buy_size=spot.best_ask_size,
                sell_size=perp.best_bid_size,
                perps=(perp,),
            )
            if quantity is None:
                self._counts["spot_perp_insufficient_visible_size_or_contract_step"] += 1
                return
            open_cashflow = (
                self._sell_proceeds(quantity, perp.best_bid, perp_fee)
                - self._buy_cost(quantity, spot.best_ask, spot_buy)
            )
            close_cashflow = (
                self._sell_proceeds(quantity, spot.best_bid, spot_sell)
                - self._buy_cost(quantity, perp.best_ask, perp_fee)
            )
            funding, funding_detail = self._funding_projection(
                perp=perp,
                side="short",
                quantity=quantity,
                now_ns=now_ns,
                now_monotonic_ns=now_monotonic_ns,
            )
            inventory_note = "requires_quote_cash_and_perp_collateral"
            notional = quantity * spot.best_ask
        elif direction == "short_spot_long_perp":
            quantity = self._candidate_quantity(
                buy_price=perp.best_ask,
                buy_size=perp.best_ask_size,
                sell_size=spot.best_bid_size,
                perps=(perp,),
            )
            if quantity is None:
                self._counts["spot_perp_insufficient_visible_size_or_contract_step"] += 1
                return
            open_cashflow = (
                self._sell_proceeds(quantity, spot.best_bid, spot_sell)
                - self._buy_cost(quantity, perp.best_ask, perp_fee)
            )
            close_cashflow = (
                self._sell_proceeds(quantity, perp.best_bid, perp_fee)
                - self._buy_cost(quantity, spot.best_ask, spot_buy)
            )
            funding, funding_detail = self._funding_projection(
                perp=perp,
                side="long",
                quantity=quantity,
                now_ns=now_ns,
                now_monotonic_ns=now_monotonic_ns,
            )
            inventory_note = "requires_existing_spot_inventory_or_verified_borrow"
            notional = quantity * perp.best_ask
        else:
            raise ValueError(f"unsupported spot/perp direction: {direction}")
        response_skew_valid, response_skew_ms = self._timing(
            spot.received_realtime_ns,
            perp.book_received_realtime_ns,
        )
        market_data_fresh, market_data_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "spot_bbo": (spot.received_realtime_ns, spot.received_monotonic_ns),
                "perp_bbo": (
                    perp.book_received_realtime_ns,
                    perp.book_received_monotonic_ns,
                ),
            },
        )
        timing_valid = response_skew_valid and market_data_fresh
        route_id = f"spot:{spot.venue}:{spot.symbol}|perp:{perp.venue}:{perp.venue_symbol}|{direction}"
        common = {
            "route_id": route_id,
            "base": spot.base,
            "direction": direction,
            "hedged_base_quantity": _decimal_text(quantity),
            "target_notional_usdt": _decimal_text(self.target_notional_usdt),
            "visible_entry_notional_usdt": _decimal_text(notional),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "pnl_currency": spot.quote,
            "settlement_parity_assumption": "same_settlement_currency_no_fx_conversion",
            "spot": self._spot_record(spot, spot_buy, spot_sell),
            "perp": self._perp_record(perp, perp_fee),
            "open_cashflow_usdt": _decimal_text(open_cashflow),
            "current_close_cashflow_usdt": _decimal_text(close_cashflow),
            "inventory_requirement": inventory_note,
            "top_of_book_only": True,
        }
        flat_now_pnl = open_cashflow + close_cashflow
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="spot_perp_flat_price_cycle",
                strategy="spot_perp_flat_price_cycle",
                common=common,
                timing_valid=timing_valid,
                notional=notional,
                net_before_funding=flat_now_pnl,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=Decimal("0"),
                full_exit_model=True,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )
        if (
            funding.normalized_cashflow_per_hour is None
            and not funding.horizon_model_complete
        ):
            self._counts["spot_perp_funding_context_unavailable"] += 1
            return
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="spot_perp_funding_carry",
                strategy="spot_perp_funding_carry",
                common=common,
                timing_valid=timing_valid,
                notional=notional,
                net_before_funding=flat_now_pnl,
                funding_per_hour=funding.normalized_cashflow_per_hour,
                funding_details=(funding_detail,),
                network_reserve=Decimal("0"),
                full_exit_model=True,
                funding_horizon_pnl=funding.scheduled_cashflow_for_horizon,
                funding_horizon_model_complete=funding.horizon_model_complete,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _evaluate_dex_perp(
        self,
        quote: ExactInputQuote,
        market: Any,
        perp: _PerpLeg,
        *,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        self._counts["dex_perp_pair_considered"] += 1
        if not perp.contract_model.supported:
            self._counts["dex_perp_unsupported_contract_model"] += 1
            return
        market_quote = getattr(market, "quote_symbol", None)
        if (
            not isinstance(market_quote, str)
            or market_quote.upper() != perp.settlement
        ):
            self._counts["dex_perp_cross_settlement_fx_unavailable"] += 1
            return
        if (
            not perp.executable_bbo
            or quote.quote_amount is None
            or (quote.direction == "sell_base" and quote.base_amount is None)
            or (quote.direction == "buy_base" and quote.input_amount_raw is None
                and self._sequential_amm_simulator is not None)
        ):
            self._counts["dex_perp_missing_executable_bbo_or_quote"] += 1
            return
        if quote.direction == "buy_base":
            hedge_side = "short"
            hedge_price = perp.best_bid
            hedge_size = perp.best_bid_size
            direction = "long_dex_short_perp"
        elif quote.direction == "sell_base":
            hedge_side = "long"
            hedge_price = perp.best_ask
            hedge_size = perp.best_ask_size
            direction = "existing_dex_inventory_long_perp"
        else:
            self._counts["dex_perp_malformed_direction"] += 1
            return
        if not _positive_decimal(hedge_price) or not _positive_decimal(hedge_size):
            self._counts["dex_perp_missing_executable_bbo_or_quote"] += 1
            return
        if quote.direction == "sell_base" or self._sequential_amm_simulator is None:
            if quote.base_amount is None:
                self._counts["dex_perp_missing_executable_bbo_or_quote"] += 1
                return
            quantity = self._normalise_quantity(quote.base_amount, perp)
            if quantity is None or quantity > hedge_size:
                self._counts["dex_perp_insufficient_visible_size_or_contract_step"] += 1
                return
            residual_bps = (quote.base_amount - quantity).copy_abs() / quote.base_amount * Decimal(10_000)
            if residual_bps > Decimal("5"):
                self._counts["dex_perp_hedge_residual_too_large"] += 1
                return
            if quantity != quote.base_amount:
                self._counts["dex_perp_exact_quote_quantity_mismatch"] += 1
                return
        if quote.direction == "buy_base":
            sequential = self._sequential_result_for(quote)
            if self._sequential_amm_simulator is not None:
                # When the vertical slice is enabled, a missing local result
                # is a typed refusal.  Do not fall back to a remote quote as
                # post-trade pool state.
                if sequential is None:
                    self._counts["dex_perp_sequential_unavailable_no_fallback"] += 1
                    return
                strict_binding = (
                    quote.protocol == "raydium_cpmm"
                    or quote.input_asset_id is not None
                    or quote.output_asset_id is not None
                )
                if strict_binding:
                    base_decimals = self._asset_decimals(sequential.base_asset_id)
                    local_quantity = (
                        Decimal(sequential.buy_output_raw).scaleb(-base_decimals)
                        if base_decimals is not None else None
                    )
                    quantity = (
                        self._normalise_quantity(local_quantity, perp)
                        if local_quantity is not None else None
                    )
                    if (
                        quantity is None
                        or quantity != local_quantity
                        or quantity > hedge_size
                    ):
                        self._counts["dex_perp_sequential_lot_or_size_mismatch"] += 1
                        return
                else:
                    quantity = self._normalise_quantity(quote.base_amount, perp)
                    if quantity is None or quantity > hedge_size:
                        self._counts["dex_perp_insufficient_visible_size_or_contract_step"] += 1
                        return
                self._counts["dex_perp_sequential_quote_matched"] += 1
                self._evaluate_dex_perp_sequential(
                    sequential,
                    market,
                    perp,
                    quantity=quantity,
                    strict_binding=strict_binding,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                return
            if sequential is not None:
                self._counts["dex_perp_sequential_quote_matched"] += 1
                self._evaluate_dex_perp_sequential(
                    sequential,
                    market,
                    perp,
                    quantity=quantity,
                    strict_binding=(
                        quote.protocol == "raydium_cpmm"
                        or quote.input_asset_id is not None
                        or quote.output_asset_id is not None
                    ),
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                return
            # A DEX quote is exact only at its own raw base amount.  Do not
            # linearly rescale a reverse curve after a perp lot-size round.
            quote_pair, pairing = self._paired_reverse_dex_quote(
                quote,
                now_monotonic_ns=now_monotonic_ns,
            )
            if quote_pair is not None:
                self._counts["dex_perp_reverse_quote_pair_matched"] += 1
                self._evaluate_dex_perp_paired_exact_quote(
                    quote_pair,
                    market,
                    perp,
                    quantity=quantity,
                    now_ns=now_ns,
                    now_monotonic_ns=now_monotonic_ns,
                )
                return
            self._counts[f"dex_perp_reverse_quote_{pairing}"] += 1
        dex_quote_value = quote.quote_amount
        perp_fee = self._perp_fee(perp)
        if quote.direction == "buy_base":
            assert perp.best_bid is not None
            entry_basis_difference = self._sell_proceeds(quantity, perp.best_bid, perp_fee) - dex_quote_value
        else:
            assert perp.best_ask is not None
            entry_basis_difference = dex_quote_value - self._buy_cost(quantity, perp.best_ask, perp_fee)
        network_floor = DEFAULT_NETWORK_COST_FLOORS.get(market.chain, Decimal("0"))
        # A position needs at least an entry and an eventual exit transaction.
        network_reserve = network_floor * Decimal("2")
        response_skew_valid, response_skew_ms = self._timing(
            quote.response_received_realtime_ns,
            perp.book_received_realtime_ns,
        )
        market_data_fresh, market_data_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_exact_quote_age_ms,
            receipts={
                "dex_entry_quote": (
                    quote.response_received_realtime_ns,
                    quote.response_received_monotonic_ns,
                ),
            },
        )
        perp_book_fresh, perp_book_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "perp_bbo": (
                    perp.book_received_realtime_ns,
                    perp.book_received_monotonic_ns,
                ),
            },
        )
        market_data_ages_ms.update(perp_book_ages_ms)
        market_data_fresh = market_data_fresh and perp_book_fresh
        timing_valid = response_skew_valid and market_data_fresh
        route_id = f"dex:{market.name}|perp:{perp.venue}:{perp.venue_symbol}|{direction}"
        common = {
            "route_id": route_id,
            "base": market.cex_base_symbol,
            "direction": direction,
            "hedged_base_quantity": _decimal_text(quantity),
            "requested_notional_quote": _decimal_text(quote.requested_notional_quote),
            "reference_notional_usdt": _decimal_text(quote.reference_notional_usdt),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "dex_market": market.name,
            "dex_chain": market.chain,
            "dex_pair": market.dex_pair,
            "asset_equivalence": market.asset_equivalence,
            "dex_provider": quote.provider,
            "dex_source_epoch": quote.source_epoch,
            "dex_direction": quote.direction,
            "pnl_currency": market_quote.upper(),
            "dex_quote_amount": _decimal_text(dex_quote_value),
            "dex_quote_fee_and_price_impact_included": True,
            "dex_request_rtt_ms": quote.request_rtt_ms,
            "perp": self._perp_record(perp, perp_fee),
            # Opening a perpetual does not generate a spot-sale cash flow.
            # This is an entry basis diagnostic, not realized PnL.
            "entry_basis_difference_usdt": _decimal_text(entry_basis_difference),
            "entry_basis_difference_bps": _decimal_text(
                entry_basis_difference / dex_quote_value * Decimal(10_000)
            ),
            "entry_basis_is_not_realised_pnl": True,
            "funding_scenario_not_evaluated_without_close_model": True,
            "hedge_residual_bps": _decimal_text(residual_bps),
            "top_of_book_only": True,
            "reverse_dex_exact_quote_for_same_base_quantity_available": False,
        }
        # No reverse curve for this exact base quantity is held in the shared
        # state, so this remains an entry-basis observation rather than a
        # candidate that claims a closed PnL.
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="dex_perp_entry_hedge",
                strategy="dex_perp_entry_hedge",
                common=common,
                timing_valid=timing_valid,
                notional=dex_quote_value,
                net_before_funding=entry_basis_difference,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=network_reserve,
                full_exit_model=False,
                pnl_model_complete=False,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _evaluate_dex_perp_paired_exact_quote(
        self,
        quote_pair: ExactQuotePair,
        market: Any,
        perp: _PerpLeg,
        *,
        quantity: Decimal,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        """Model an immediate DEX/perp unwind from an exact DEX quote pair.

        This covers a *current* long-DEX/short-perp close only.  It does not
        treat short-perp opening proceeds as spot cash and it does not infer a
        reverse DEX price by division.  The reverse DEX quote is a separate,
        current public simulation, so the result remains non-candidate
        reconnaissance until post-trade pool-state simulation is implemented.
        """

        quote = quote_pair.entry
        reverse_quote = quote_pair.exit
        if (
            quote.quote_amount is None
            or reverse_quote.quote_amount is None
            or not _positive_decimal(perp.best_bid)
            or not _positive_decimal(perp.best_ask)
        ):
            self._counts["dex_perp_paired_quote_missing_values"] += 1
            return
        perp_fee = self._perp_fee(perp)
        dex_roundtrip_pnl = reverse_quote.quote_amount - quote.quote_amount
        # For a short perpetual, opening at the bid and immediately closing at
        # the ask leaves only the spread and both taker fees as PnL.  Margin is
        # intentionally not represented as a cash-flow profit.
        perp_roundtrip_pnl = (
            self._sell_proceeds(quantity, perp.best_bid, perp_fee)
            - self._buy_cost(quantity, perp.best_ask, perp_fee)
        )
        flat_now_pnl = dex_roundtrip_pnl + perp_roundtrip_pnl
        network_floor = DEFAULT_NETWORK_COST_FLOORS.get(market.chain, Decimal("0"))
        network_reserve = network_floor * Decimal("2")
        response_skew_valid, response_skew_ms = self._timing(
            quote.response_received_realtime_ns,
            reverse_quote.response_received_realtime_ns,
            perp.book_received_realtime_ns,
        )
        dex_quotes_fresh, market_data_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_exact_quote_age_ms,
            receipts={
                "dex_entry_quote": (
                    quote.response_received_realtime_ns,
                    quote.response_received_monotonic_ns,
                ),
                "dex_exit_quote": (
                    reverse_quote.response_received_realtime_ns,
                    reverse_quote.response_received_monotonic_ns,
                ),
            },
        )
        perp_book_fresh, perp_book_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "perp_bbo": (
                    perp.book_received_realtime_ns,
                    perp.book_received_monotonic_ns,
                ),
            },
        )
        market_data_ages_ms.update(perp_book_ages_ms)
        market_data_fresh = dex_quotes_fresh and perp_book_fresh
        timing_valid = response_skew_valid and market_data_fresh
        funding, funding_detail = self._funding_projection(
            perp=perp,
            side="short",
            quantity=quantity,
            now_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
        )
        route_id = (
            f"dex:{market.name}|perp:{perp.venue}:{perp.venue_symbol}|"
            "long_dex_short_perp"
        )
        common = {
            "route_id": route_id,
            "base": market.cex_base_symbol,
            "direction": "long_dex_short_perp",
            "hedged_base_quantity": _decimal_text(quantity),
            "requested_notional_quote": _decimal_text(quote.requested_notional_quote),
            "reference_notional_usdt": _decimal_text(quote.reference_notional_usdt),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "dex_market": market.name,
            "dex_chain": market.chain,
            "dex_pair": market.dex_pair,
            "asset_equivalence": market.asset_equivalence,
            "dex_provider": quote.provider,
            "dex_source_epoch": quote.source_epoch,
            "dex_entry_direction": quote.direction,
            "dex_exit_direction": reverse_quote.direction,
            "pnl_currency": perp.settlement,
            "dex_entry_exact_quote_amount": _decimal_text(quote.quote_amount),
            "dex_exit_exact_quote_amount": _decimal_text(reverse_quote.quote_amount),
            "dex_entry_output_base_raw": quote.output_amount_raw,
            "dex_exit_input_base_raw": reverse_quote.input_amount_raw,
            "dex_roundtrip_pnl_usdt": _decimal_text(dex_roundtrip_pnl),
            "perp_short_roundtrip_pnl_usdt": _decimal_text(perp_roundtrip_pnl),
            "immediate_flat_pnl_before_network_usdt": _decimal_text(flat_now_pnl),
            "dex_quote_fee_and_price_impact_included": True,
            "dex_entry_request_rtt_ms": quote.request_rtt_ms,
            "dex_exit_request_rtt_ms": reverse_quote.request_rtt_ms,
            "perp": self._perp_record(perp, perp_fee),
            "top_of_book_only": True,
            "reverse_dex_exact_quote_for_same_base_quantity_available": True,
            "reverse_dex_pairing": quote_pair.pairing_quality,
            "reverse_dex_same_source_epoch": quote_pair.same_source_epoch,
            "reverse_dex_same_source_round": quote_pair.same_source_round,
            "reverse_dex_same_block_number": quote_pair.same_block_number,
            # The quote pair describes two independent pre-trade simulations.
            # It is deliberately not presented as an atomic pool round trip.
            "dex_post_trade_pool_state_simulated": False,
            "candidate_eligibility_blocker": (
                "dex_paired_quotes_do_not_simulate_post_trade_pool_state"
            ),
        }
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="dex_perp_paired_exact_quote_flat_model",
                strategy="dex_perp_paired_exact_quote_flat_model",
                common=common,
                timing_valid=timing_valid,
                notional=quote.quote_amount,
                net_before_funding=flat_now_pnl,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=network_reserve,
                full_exit_model=True,
                candidate_eligible=False,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )
        if (
            funding.normalized_cashflow_per_hour is None
            and not funding.horizon_model_complete
        ):
            self._counts["dex_perp_paired_quote_funding_context_unavailable"] += 1
            return
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="dex_perp_paired_exact_quote_funding_scenario",
                strategy="dex_perp_paired_exact_quote_funding_scenario",
                common=common,
                timing_valid=timing_valid,
                notional=quote.quote_amount,
                net_before_funding=flat_now_pnl,
                funding_per_hour=funding.normalized_cashflow_per_hour,
                funding_details=(funding_detail,),
                network_reserve=network_reserve,
                full_exit_model=True,
                candidate_eligible=False,
                funding_horizon_pnl=funding.scheduled_cashflow_for_horizon,
                funding_horizon_model_complete=funding.horizon_model_complete,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _evaluate_dex_perp_sequential(
        self,
        result: SequentialUnwindResult,
        market: Any,
        perp: _PerpLeg,
        *,
        quantity: Decimal,
        strict_binding: bool = True,
        now_ns: int,
        now_monotonic_ns: int,
    ) -> None:
        """Model one sequential AMM buy->sell unwind against a compatible perp.

        The DEX contribution is the final stable balance minus the initial
        stable balance from an explicit post-trade pool-state simulation.  The
        notional short-perp roundtrip is short PnL minus both taker fees and is
        deliberately not treated as a realised spot cash flow.  This row is
        research evidence only and never changes candidate lifecycle on its
        own; ``execution_ready`` stays false.
        """

        self._counts["dex_perp_sequential_models"] += 1
        if not _positive_decimal(perp.best_bid) or not _positive_decimal(perp.best_ask):
            self._counts["dex_perp_sequential_missing_bbo"] += 1
            return
        strict_binding = strict_binding and result.provider == "raydium_cpmm" and bool(result.evidence_hash)
        metadata = self._sequential_metadata()
        if strict_binding:
            if metadata is None:
                self._counts["dex_perp_sequential_metadata_missing"] += 1
                return
            expiry = metadata.get("state_valid_until_monotonic_ns")
            if isinstance(expiry, int) and now_monotonic_ns >= expiry:
                self._counts["dex_perp_sequential_snapshot_expired"] += 1
                return
            received = metadata.get("snapshot_received_monotonic_ns")
            if isinstance(received, int):
                age_ms = max(0, now_monotonic_ns - received) / 1_000_000
                if age_ms > float(self.max_exact_quote_age_ms):
                    self._counts["dex_perp_sequential_snapshot_stale"] += 1
                    return
            base_decimals = self._asset_decimals(result.base_asset_id)
            if base_decimals is None:
                self._counts["dex_perp_sequential_asset_binding_mismatch"] += 1
                return
            actual_quantity = Decimal(result.buy_output_raw).scaleb(-base_decimals)
            if quantity != actual_quantity:
                self._counts["dex_perp_sequential_raw_quantity_mismatch"] += 1
                return
            expected_perp_symbol = metadata.get("perp_symbol")
            if (
                not isinstance(expected_perp_symbol, str)
                or expected_perp_symbol.upper() != str(market.cex_base_symbol).upper()
                or expected_perp_symbol.upper() != perp.base.upper()
            ):
                self._counts["dex_perp_sequential_asset_binding_mismatch"] += 1
                return
        perp_fee = self._perp_fee(perp)
        dex_roundtrip_pnl = result.final_stable - result.initial_stable
        perp_roundtrip_pnl = (
            self._sell_proceeds(quantity, perp.best_bid, perp_fee)
            - self._buy_cost(quantity, perp.best_ask, perp_fee)
        )
        flat_now_pnl = dex_roundtrip_pnl + perp_roundtrip_pnl
        network_floor = DEFAULT_NETWORK_COST_FLOORS.get(market.chain, Decimal("0"))
        network_reserve = network_floor * Decimal("2")
        snapshot_received = (
            metadata.get("snapshot_received_monotonic_ns")
            if isinstance(metadata, Mapping)
            else None
        )
        snapshot_received_realtime = (
            metadata.get("snapshot_received_realtime_ns")
            if isinstance(metadata, Mapping)
            else None
        )
        response_skew_valid, response_skew_ms = self._timing(
            snapshot_received if isinstance(snapshot_received, int) else now_monotonic_ns,
            perp.book_received_monotonic_ns,
        )
        market_data_ages_ms: dict[str, object] = {}
        dex_fresh, dex_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_exact_quote_age_ms,
            receipts={
                "sequential_snapshot": (
                    (
                        snapshot_received_realtime
                        if isinstance(snapshot_received_realtime, int)
                        else (now_ns if not strict_binding else None)
                    ),
                    (
                        snapshot_received
                        if isinstance(snapshot_received, int)
                        else (now_monotonic_ns if not strict_binding else None)
                    ),
                ),
            },
        )
        market_data_ages_ms.update(dex_ages_ms)
        perp_book_fresh, perp_book_ages_ms = self._receipt_freshness(
            now_realtime_ns=now_ns,
            now_monotonic_ns=now_monotonic_ns,
            max_age_ms=self.max_book_age_ms,
            receipts={
                "perp_bbo": (
                    perp.book_received_realtime_ns,
                    perp.book_received_monotonic_ns,
                ),
            },
        )
        market_data_ages_ms.update(perp_book_ages_ms)
        market_data_fresh = dex_fresh and perp_book_fresh
        timing_valid = response_skew_valid and market_data_fresh
        route_id = (
            f"dex:{market.name}|a:{market.dex_pair}|perp:{perp.venue}:{perp.venue_symbol}|"
            "sequential_long_dex_inventory_short_perp"
        )
        common = {
            "route_id": route_id,
            "base": market.cex_base_symbol,
            "direction": "sequential_dex_long_inventory_short_perp",
            "hedged_base_quantity": _decimal_text(quantity),
            "model_notional_usdt": _decimal_text(result.initial_stable),
            "reference_notional_usdt": _decimal_text(self.target_notional_usdt),
            "response_skew_ms": response_skew_ms,
            "response_skew_valid": response_skew_valid,
            "market_data_fresh": market_data_fresh,
            "market_data_ages_ms": market_data_ages_ms,
            "market_data_freshness_clock": "local_realtime_and_monotonic",
            "dex_market": market.name,
            "dex_chain": market.chain,
            "dex_pair": market.dex_pair,
            "asset_equivalence": market.asset_equivalence,
            "dex_provider": result.provider,
            "pnl_currency": perp.settlement,
            "dex_sequential_snapshot_id": result.snapshot_id,
            "dex_sequential_snapshot_hash": result.snapshot_hash,
            "dex_sequential_evidence_hash": result.evidence_hash,
            "dex_sequential_evidence_path": self._sequential_evidence_paths.get(
                result.evidence_hash or "",
            ),
            "dex_sequential_scenario_kind": result.scenario_kind,
            "dex_sequential_pool_id": result.pool_id,
            "dex_sequential_worker_generation": result.worker_generation,
            "dex_sequential_source_epoch": (
                metadata.get("source_epoch") if isinstance(metadata, Mapping) else None
            ),
            "dex_sequential_context_slot": (
                metadata.get("context_slot") if isinstance(metadata, Mapping) else None
            ),
            "dex_sequential_boot_id": (
                metadata.get("boot_id") if isinstance(metadata, Mapping) else None
            ),
            "dex_buy_leg_id": result.buy_leg_id,
            "dex_sell_leg_id": result.sell_leg_id,
            "dex_buy_input_raw": result.buy_input_raw,
            "dex_buy_output_raw": result.buy_output_raw,
            "dex_sell_input_raw": result.sell_input_raw,
            "dex_sell_output_raw": result.sell_output_raw,
            "dex_initial_stable_raw": result.buy_input_raw,
            "dex_final_stable_raw": result.sell_output_raw,
            "dex_initial_stable": _decimal_text(result.initial_stable),
            "dex_final_stable": _decimal_text(result.final_stable),
            "dex_roundtrip_pnl_usdt": _decimal_text(dex_roundtrip_pnl),
            "perp_short_roundtrip_pnl_usdt": _decimal_text(perp_roundtrip_pnl),
            "immediate_flat_pnl_before_network_usdt": _decimal_text(flat_now_pnl),
            "dex_quote_fee_and_price_impact_included": True,
            "top_of_book_only": True,
            "perp": self._perp_record(perp, perp_fee),
            "dex_post_trade_pool_state_simulated": True,
            "execution_ready": False,
        }
        self._observe_cycle(
            self._make_cycle(
                analysis_kind="dex_perp_sequential_flat_model",
                strategy="dex_perp_sequential_flat_model",
                common=common,
                timing_valid=timing_valid,
                notional=result.initial_stable,
                net_before_funding=flat_now_pnl,
                funding_per_hour=None,
                funding_details=(),
                network_reserve=network_reserve,
                full_exit_model=True,
                candidate_eligible=False,
                observed_realtime_ns=now_ns,
            ),
            observed_realtime_ns=now_ns,
            observed_monotonic_ns=now_monotonic_ns,
        )

    def _make_cycle(
        self,
        *,
        analysis_kind: str,
        strategy: str,
        common: Mapping[str, Any],
        timing_valid: bool,
        notional: Decimal,
        net_before_funding: Decimal,
        funding_per_hour: Decimal | None,
        funding_details: tuple[Mapping[str, Any], ...],
        network_reserve: Decimal,
        full_exit_model: bool,
        observed_realtime_ns: int,
        candidate_eligible: bool | None = None,
        pnl_model_complete: bool = True,
        funding_horizon_pnl: Decimal | None = None,
        funding_horizon_model_complete: bool | None = None,
    ) -> dict[str, Any]:
        if not _positive_decimal(notional):
            raise ValueError("strategy notional must be finite and positive")
        has_funding_model = bool(funding_details)
        if not pnl_model_complete and (
            has_funding_model
            or funding_per_hour is not None
            or funding_horizon_pnl is not None
        ):
            raise ValueError("incomplete PnL model cannot project funding")
        if not has_funding_model:
            if funding_per_hour is not None or funding_horizon_pnl is not None:
                raise ValueError("funding values need funding evidence details")
            funding_horizon_model_complete = True
        elif funding_horizon_model_complete is None:
            # A per-hour normalisation is useful research data, but it is not
            # an event-calendar cashflow by itself.  Conversely, a known
            # future event can produce a valid horizon cashflow even without
            # a confirmed recurring interval.
            funding_horizon_model_complete = funding_horizon_pnl is not None
        if (
            has_funding_model
            and funding_horizon_model_complete
            and funding_horizon_pnl is None
        ):
            raise ValueError("complete funding horizon needs an explicit cashflow")
        if (
            has_funding_model
            and not funding_horizon_model_complete
            and funding_horizon_pnl is not None
        ):
            raise ValueError("incomplete funding horizon cannot include a cashflow")
        if pnl_model_complete:
            after_network = net_before_funding - network_reserve
            funding_for_horizon = (
                funding_horizon_pnl
                if has_funding_model and funding_horizon_model_complete
                else (Decimal("0") if not has_funding_model else None)
            )
            modeled_pnl: Decimal | None = after_network + (funding_for_horizon or Decimal("0"))
            edge_bps: Decimal | None = modeled_pnl / notional * Decimal(10_000)
            candidate_eligible = (
                full_exit_model
                if candidate_eligible is None
                else full_exit_model and candidate_eligible
            )
            if has_funding_model:
                candidate_eligible = (
                    candidate_eligible
                    and bool(funding_details)
                    and funding_horizon_model_complete
                )
            positive = modeled_pnl > 0
        else:
            funding_for_horizon = None
            funding_horizon_model_complete = False
            modeled_pnl = None
            edge_bps = None
            candidate_eligible = False
            positive = False
        fee_verified = all(
            leg.get("fee_account_verified") is True
            for field in (
                "long_perp",
                "short_perp",
                "perp",
                "spot",
                "buy_spot",
                "sell_spot",
            )
            if isinstance((leg := common.get(field)), Mapping)
        )
        has_exact_dex_quote = any(
            field in common
            for field in (
                "dex_quote_amount",
                "dex_entry_exact_quote_amount",
                "dex_exit_exact_quote_amount",
            )
        )
        if not has_funding_model:
            funding_quality = "none_required"
        elif funding_horizon_model_complete and all(
            item.get("funding_projection_quality") == "projected"
            for item in funding_details
        ):
            funding_quality = "projected"
        else:
            funding_quality = "unknown"
        quality = derive_candidate_quality(
            analysis_kind=analysis_kind,
            timing_valid=timing_valid,
            pnl_model_complete=pnl_model_complete,
            full_exit_model=full_exit_model,
            funding_quality=funding_quality,
            has_exact_dex_quote=has_exact_dex_quote,
            top_of_book_only=common.get("top_of_book_only") is True,
            dex_post_trade_pool_state_simulated=(
                common.get("dex_post_trade_pool_state_simulated")
                if isinstance(common.get("dex_post_trade_pool_state_simulated"), bool)
                else None
            ),
        )
        payload: dict[str, Any] = {
            "schema_version": 1,
            "status": "ok",
            "analysis_kind": analysis_kind,
            "strategy": strategy,
            "analysis_notional_bucket_usdt": _decimal_text(self.target_notional_usdt),
            "model_notional_usdt": _decimal_text(notional),
            # Existing field names retain their legacy suffix for downstream
            # compatibility.  This field is authoritative: no FX conversion
            # is inferred by the model.
            "model_currency": common.get("pnl_currency"),
            "timing_valid": timing_valid,
            "pnl_model_complete": pnl_model_complete,
            "net_before_network_and_funding_usdt": (
                _decimal_text(net_before_funding) if pnl_model_complete else None
            ),
            "minimum_network_reserve_usdt": _decimal_text(network_reserve),
            "network_reserve_included": pnl_model_complete and network_reserve > 0,
            "funding_horizon_hours": (
                _decimal_text(self.funding_horizon_hours) if pnl_model_complete else None
            ),
            "funding_pnl_per_hour_usdt": _decimal_text(funding_per_hour),
            "funding_pnl_for_horizon_usdt": _decimal_text(funding_for_horizon),
            "funding_horizon_model_complete": funding_horizon_model_complete,
            "funding_not_included_in_net_pnl": (
                has_funding_model and not funding_horizon_model_complete
            ),
            "funding_details": [dict(item) for item in funding_details],
            "net_pnl_after_modeled_costs_usdt": _decimal_text(modeled_pnl),
            "net_edge_after_modeled_costs_bps": _decimal_text(edge_bps),
            "positive_after_modeled_costs": positive,
            "candidate_eligible": candidate_eligible,
            "candidate_eligible_with_account_verified_fees": fee_verified and candidate_eligible,
            "full_exit_model": full_exit_model,
            "execution_ready": False,
            "quality": quality.as_dict(),
            "observed_at": _utc_iso_from_ns(observed_realtime_ns),
        }
        payload.update(common)
        return payload

    @staticmethod
    def _cycle_key(cycle: Mapping[str, Any], analysis_kind: str) -> str:
        return "|".join(
            (
                analysis_kind,
                str(cycle.get("route_id")),
                str(cycle.get("analysis_notional_bucket_usdt")),
            ),
        )

    @staticmethod
    def _as_decimal(cycle: Mapping[str, Any], field: str) -> Decimal | None:
        try:
            value = Decimal(str(cycle[field]))
        except (InvalidOperation, KeyError, TypeError, ValueError):
            return None
        return value if value.is_finite() else None

    def _is_modelled_candidate(self, cycle: Mapping[str, Any]) -> bool:
        edge = self._as_decimal(cycle, "net_edge_after_modeled_costs_bps")
        return bool(
            cycle.get("status") == "ok"
            and cycle.get("timing_valid") is True
            and cycle.get("candidate_eligible") is True
            and cycle.get("positive_after_modeled_costs") is True
            and edge is not None
            and edge >= 0
        )

    @staticmethod
    def _execution_blockers(cycle: Mapping[str, Any]) -> list[str]:
        blockers = [
            "public_data_model_only_no_orders_wallet_or_transactions",
            "independent_venue_balances_collateral_and_rebalance_not_verified",
            "top_of_book_visible_liquidity_only_full_depth_and_fill_risk_not_simulated",
        ]
        if cycle.get("candidate_eligible_with_account_verified_fees") is not True:
            blockers.append("account_specific_spot_or_perp_taker_fees_not_verified")
        if cycle.get("market_data_fresh") is False:
            blockers.append("state_stale")
        if cycle.get("response_skew_valid") is False:
            blockers.append("leg_receive_skew_exceeds_limit")
        if cycle.get("funding_details"):
            blockers.append("future_funding_rate_and_settlement_not_guaranteed")
        if (
            cycle.get("funding_details")
            and cycle.get("funding_horizon_model_complete") is not True
        ):
            blockers.append("funding_time_ambiguous")
        if cycle.get("full_exit_model") is not True:
            blockers.append("reverse_dex_exit_curve_for_same_base_quantity_not_modelled")
        if cycle.get("pnl_model_complete") is False:
            blockers.append("complete_pnl_close_model_not_available")
        if cycle.get("dex_post_trade_pool_state_simulated") is False:
            blockers.append("dex_paired_quotes_do_not_simulate_post_trade_pool_state")
        eligibility_blocker = cycle.get("candidate_eligibility_blocker")
        if isinstance(eligibility_blocker, str) and eligibility_blocker:
            blockers.append(eligibility_blocker)
        if cycle.get("inventory_requirement"):
            blockers.append(str(cycle["inventory_requirement"]))
        if cycle.get("network_reserve_included") is not True:
            blockers.append("venue_specific_onchain_gas_or_withdrawal_cost_not_modelled")
        return list(dict.fromkeys(blockers))

    def _candidate_event(
        self,
        event: str,
        state: _ActiveCandidate,
        *,
        observed_realtime_ns: int,
        observed_monotonic_ns: int,
        current_cycle: Mapping[str, Any] | None = None,
        close_reason: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "analysis_kind": state.analysis_kind,
            "candidate_key": state.key,
            "candidate_model": "timing_valid_public_fee_model_after_visible_bbo_and_horizon_costs",
            "execution_ready": False,
            "started_at": state.started_at,
            "last_seen_at": state.last_seen_at,
            "event_at": _utc_iso_from_ns(observed_realtime_ns),
            "duration_seconds": round(
                max(0, observed_monotonic_ns - state.started_monotonic_ns) / 1_000_000_000,
                6,
            ),
            "positive_observations": state.observations,
            "max_net_edge_after_modeled_costs_bps": _decimal_text(state.max_edge_bps),
            "max_net_pnl_after_modeled_costs_usdt": _decimal_text(state.max_pnl_usdt),
            "best_cycle": state.best_cycle,
            "execution_blockers": self._execution_blockers(state.best_cycle),
        }
        if current_cycle is not None:
            payload["current_cycle"] = dict(current_cycle)
        if close_reason is not None:
            payload["close_reason"] = close_reason
        return payload

    @staticmethod
    def _compact_candidate_cycle(cycle: Mapping[str, Any]) -> dict[str, Any]:
        """Keep research-relevant execution metadata, not a raw book snapshot."""

        fields = (
            "schema_version",
            "status",
            "analysis_kind",
            "strategy",
            "route_id",
            "base",
            "direction",
            "observed_at",
            "analysis_notional_bucket_usdt",
            "model_notional_usdt",
            "model_currency",
            "pnl_currency",
            "visible_entry_notional_usdt",
            "hedged_base_quantity",
            "response_skew_ms",
            "response_skew_valid",
            "market_data_fresh",
            "market_data_ages_ms",
            "market_data_freshness_clock",
            "settlement_parity_assumption",
            "open_cashflow_usdt",
            "current_close_cashflow_usdt",
            "entry_basis_difference_usdt",
            "entry_basis_difference_bps",
            "entry_basis_is_not_realised_pnl",
            "funding_scenario_not_evaluated_without_close_model",
            "net_before_network_and_funding_usdt",
            "minimum_network_reserve_usdt",
            "network_reserve_included",
            "funding_horizon_hours",
            "funding_pnl_per_hour_usdt",
            "funding_pnl_for_horizon_usdt",
            "funding_horizon_model_complete",
            "funding_not_included_in_net_pnl",
            "net_pnl_after_modeled_costs_usdt",
            "net_edge_after_modeled_costs_bps",
            "positive_after_modeled_costs",
            "candidate_eligible",
            "candidate_eligible_with_account_verified_fees",
            "full_exit_model",
            "pnl_model_complete",
            "execution_ready",
            "quality",
            "inventory_requirement",
            "rebalance_cost_included",
            "top_of_book_only",
            "dex_market",
            "dex_chain",
            "dex_pair",
            "asset_equivalence",
            "dex_provider",
            "dex_source_epoch",
            "dex_direction",
            "dex_quote_amount",
            "dex_request_rtt_ms",
            "hedge_residual_bps",
            "reverse_dex_exact_quote_for_same_base_quantity_available",
            "dex_entry_direction",
            "dex_exit_direction",
            "dex_entry_exact_quote_amount",
            "dex_exit_exact_quote_amount",
            "dex_entry_output_base_raw",
            "dex_exit_input_base_raw",
            "dex_roundtrip_pnl_usdt",
            "perp_short_roundtrip_pnl_usdt",
            "immediate_flat_pnl_before_network_usdt",
            "reverse_dex_pairing",
            "reverse_dex_same_source_epoch",
            "reverse_dex_same_source_round",
            "reverse_dex_same_block_number",
            "dex_sequential_snapshot_id",
            "dex_sequential_snapshot_hash",
            "dex_sequential_evidence_hash",
            "dex_sequential_scenario_kind",
            "dex_sequential_pool_id",
            "dex_sequential_worker_generation",
            "dex_buy_leg_id",
            "dex_sell_leg_id",
            "dex_buy_input_raw",
            "dex_buy_output_raw",
            "dex_sell_input_raw",
            "dex_sell_output_raw",
            "dex_initial_stable_raw",
            "dex_final_stable_raw",
            "dex_initial_stable",
            "dex_final_stable",
            "dex_post_trade_pool_state_simulated",
            "candidate_eligibility_blocker",
        )
        result = {field: cycle[field] for field in fields if field in cycle}
        funding_fields = (
            "funding_rate",
            "funding_rate_short",
            "funding_interval_minutes",
            "funding_rate_kind",
            "next_funding_time_ms",
            "funding_context_age_ms",
            "funding_context_fresh",
            "side",
            "available",
            "reference_price",
            "reference_price_kind",
            "reference_price_funding_semantics_verified",
            "funding_rate_per_hour",
            "scheduled_funding_cashflow_for_horizon_usdt",
            "scheduled_funding_event_times_ms",
            "funding_projection_quality",
            "funding_projection_reason",
            "funding_horizon_model_complete",
            "side_specific_rate_present_not_used",
        )
        funding_details = cycle.get("funding_details")
        if isinstance(funding_details, list):
            result["funding_details"] = [
                {field: item[field] for field in funding_fields if field in item}
                for item in funding_details
                if isinstance(item, Mapping)
            ]
        leg_fields = (
            "venue",
            "venue_symbol",
            "symbol",
            "base",
            "quote",
            "settlement",
            "best_bid",
            "best_bid_size",
            "best_ask",
            "best_ask_size",
            "taker_fee_bps_used",
            "taker_buy_fee_bps_used",
            "taker_sell_fee_bps_used",
            "fee_source",
            "fee_account_verified",
            "funding_rate",
            "funding_interval_minutes",
            "funding_rate_kind",
            "next_funding_time_ms",
            "execution_model",
        )
        for name in ("buy_spot", "sell_spot", "spot", "perp", "long_perp", "short_perp"):
            leg = cycle.get(name)
            if isinstance(leg, Mapping):
                result[name] = {field: leg[field] for field in leg_fields if field in leg}
        return result

    def _compact_candidate_event(self, event: Mapping[str, Any]) -> dict[str, Any]:
        fields = (
            "schema_version",
            "event",
            "analysis_kind",
            "candidate_key",
            "candidate_model",
            "execution_ready",
            "started_at",
            "last_seen_at",
            "event_at",
            "duration_seconds",
            "positive_observations",
            "max_net_edge_after_modeled_costs_bps",
            "max_net_pnl_after_modeled_costs_usdt",
            "execution_blockers",
            "close_reason",
        )
        result = {field: event[field] for field in fields if field in event}
        for name in ("best_cycle", "current_cycle"):
            cycle = event.get(name)
            if isinstance(cycle, Mapping):
                result[name] = self._compact_candidate_cycle(cycle)
        return result

    def _flush_candidate_events(self, *, force: bool = False) -> None:
        if not self._candidate_events_dirty:
            return
        now = time.monotonic()
        if (
            not force
            and now - self._last_candidate_events_flush_monotonic
            < self._candidate_events_flush_interval_seconds
        ):
            return
        self.analysis_directory.mkdir(parents=True, exist_ok=True)
        # Terminal summaries are immutable.  Appending only the pending batch
        # avoids rewriting the full bounded journal every few seconds as it
        # approaches its cap.  Once the count cap is reached, persistence
        # stops entirely; active state remains in the compact stats snapshot.
        batch = tuple(self._pending_candidate_events)
        if batch:
            with self.candidate_events_path.open("a", encoding="utf-8") as output:
                for row in batch:
                    output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
            self._pending_candidate_events.clear()
        self._candidate_events_dirty = False
        self._last_candidate_events_flush_monotonic = now
        self._counts["candidate_event_flushes"] += 1
        self._counts["candidate_events_on_disk"] = len(self._candidate_events)

    def _persist_candidate_event(self, event: dict[str, Any]) -> None:
        if len(self._candidate_events) >= self.max_candidate_events:
            self._counts["candidate_events_dropped_after_limit"] += 1
            return
        compact = self._compact_candidate_event(event)
        self._candidate_events.append(compact)
        self._pending_candidate_events.append(compact)
        self._candidate_events_dirty = True
        self._counts["candidate_events_written"] += 1

    def _report_candidate_console(self, event: Mapping[str, Any]) -> None:
        if event["event"] in {"candidate_started", "candidate_improved"}:
            candidate_key = str(event["candidate_key"])
            observed_ns = self._monotonic_ns()
            last_report_ns = self._last_console_candidate_report_ns.get(candidate_key)
            if (
                last_report_ns is not None
                and observed_ns - last_report_ns < self._console_candidate_report_interval_ns
            ):
                self._counts["console_candidate_reports_suppressed"] += 1
                return
            self._last_console_candidate_report_ns[candidate_key] = observed_ns
            cycle = event["best_cycle"]
            print(
                "[perp] "
                f"{event['event']} {event['analysis_kind']} "
                f"{cycle.get('base')} {cycle.get('direction')} "
                f"edge={event['max_net_edge_after_modeled_costs_bps']}bps "
                "modelled; not execution-ready",
                flush=True,
            )

    def _observe_candidate(
        self,
        cycle: dict[str, Any],
        *,
        analysis_kind: str,
        observed_realtime_ns: int,
        observed_monotonic_ns: int,
    ) -> None:
        key = self._cycle_key(cycle, analysis_kind)
        current = self._active.get(key)
        if not self._is_modelled_candidate(cycle):
            if current is not None:
                current.last_seen_realtime_ns = observed_realtime_ns
                current.last_seen_monotonic_ns = observed_monotonic_ns
                current.last_seen_at = _utc_iso_from_ns(observed_realtime_ns)
                self._active.pop(key)
                self._candidate_closed += 1
                if current.persisted:
                    self._persist_candidate_event(
                        self._candidate_event(
                            "candidate_closed",
                            current,
                            observed_realtime_ns=observed_realtime_ns,
                            observed_monotonic_ns=observed_monotonic_ns,
                            current_cycle=cycle,
                            close_reason="not_positive_not_eligible_or_timing_invalid",
                        ),
                    )
                else:
                    self._counts["candidate_shorter_than_minimum_persistence"] += 1
            return
        edge = self._as_decimal(cycle, "net_edge_after_modeled_costs_bps")
        pnl = self._as_decimal(cycle, "net_pnl_after_modeled_costs_usdt")
        if edge is None or pnl is None:
            return
        if current is None:
            current = _ActiveCandidate(
                key=key,
                analysis_kind=analysis_kind,
                started_realtime_ns=observed_realtime_ns,
                started_monotonic_ns=observed_monotonic_ns,
                started_at=_utc_iso_from_ns(observed_realtime_ns),
                last_seen_realtime_ns=observed_realtime_ns,
                last_seen_monotonic_ns=observed_monotonic_ns,
                last_seen_at=_utc_iso_from_ns(observed_realtime_ns),
                observations=1,
                max_edge_bps=edge,
                max_pnl_usdt=pnl,
                best_cycle=cycle,
                persisted=self._candidate_min_persistence_ns == 0,
            )
            self._active[key] = current
            self._candidate_started += 1
            if current.persisted:
                self._report_candidate_console(
                    self._candidate_event(
                        "candidate_started",
                        current,
                        observed_realtime_ns=observed_realtime_ns,
                        observed_monotonic_ns=observed_monotonic_ns,
                        current_cycle=cycle,
                    ),
                )
            else:
                self._counts["candidate_pending_started"] += 1
            return
        current.last_seen_realtime_ns = observed_realtime_ns
        current.last_seen_monotonic_ns = observed_monotonic_ns
        current.last_seen_at = _utc_iso_from_ns(observed_realtime_ns)
        current.observations += 1
        improved = edge > current.max_edge_bps
        if improved:
            current.max_edge_bps = edge
            current.max_pnl_usdt = max(current.max_pnl_usdt, pnl)
            current.best_cycle = cycle
            self._candidate_improved += 1
        else:
            current.max_pnl_usdt = max(current.max_pnl_usdt, pnl)

        if not current.persisted:
            if observed_monotonic_ns - current.started_monotonic_ns < self._candidate_min_persistence_ns:
                self._counts["candidate_pending_observations"] += 1
                return
            current.persisted = True
            self._counts["candidate_matured"] += 1
            self._report_candidate_console(
                self._candidate_event(
                    "candidate_started",
                    current,
                    observed_realtime_ns=observed_realtime_ns,
                    observed_monotonic_ns=observed_monotonic_ns,
                    current_cycle=cycle,
                ),
            )
            return
        if improved:
            self._report_candidate_console(
                self._candidate_event(
                    "candidate_improved",
                    current,
                    observed_realtime_ns=observed_realtime_ns,
                    observed_monotonic_ns=observed_monotonic_ns,
                    current_cycle=cycle,
                ),
            )

    def _observe_cycle(self, cycle: dict[str, Any], *, observed_realtime_ns: int, observed_monotonic_ns: int) -> None:
        analysis_kind = str(cycle["analysis_kind"])
        self._counts["strategy_evaluations"] += 1
        self._counts[f"{analysis_kind}_evaluations"] += 1
        timing_valid = cycle.get("timing_valid") is True
        positive = cycle.get("positive_after_modeled_costs") is True
        eligible = cycle.get("candidate_eligible") is True
        if timing_valid:
            self._counts["timing_valid_evaluations"] += 1
        if positive:
            self._counts["positive_after_modeled_costs_evaluations"] += 1
            if timing_valid:
                self._counts["timing_valid_positive_after_modeled_costs_evaluations"] += 1
        if eligible:
            self._counts["candidate_eligible_evaluations"] += 1
        route_key = self._cycle_key(cycle, analysis_kind)
        route = self._route_stats.setdefault(
            route_key,
            {
                "analysis_kind": analysis_kind,
                "strategy": cycle.get("strategy"),
                "route_id": cycle.get("route_id"),
                "base": cycle.get("base"),
                "direction": cycle.get("direction"),
                "evaluations": 0,
                "timing_valid": 0,
                "positive_after_modeled_costs": 0,
                "timing_valid_positive_after_modeled_costs": 0,
                "candidate_eligible": cycle.get("candidate_eligible"),
                "best_timing_valid_net_edge_after_modeled_costs_bps": None,
                "best_timing_valid_cycle": None,
                "best_timing_valid_entry_basis_difference_bps": None,
                "best_timing_valid_entry_basis_cycle": None,
            },
        )
        route["evaluations"] += 1
        if timing_valid:
            route["timing_valid"] += 1
        if positive:
            route["positive_after_modeled_costs"] += 1
            if timing_valid:
                route["timing_valid_positive_after_modeled_costs"] += 1
        edge = self._as_decimal(cycle, "net_edge_after_modeled_costs_bps")
        if edge is not None and timing_valid:
            best = route["best_timing_valid_net_edge_after_modeled_costs_bps"]
            if best is None or edge > Decimal(str(best)):
                route["best_timing_valid_net_edge_after_modeled_costs_bps"] = _decimal_text(edge)
                route["best_timing_valid_cycle"] = cycle
        entry_basis = self._as_decimal(cycle, "entry_basis_difference_bps")
        if entry_basis is not None:
            self._counts["entry_basis_observations"] += 1
            if timing_valid:
                self._counts["timing_valid_entry_basis_observations"] += 1
                best_entry_basis = route["best_timing_valid_entry_basis_difference_bps"]
                if best_entry_basis is None or entry_basis > Decimal(str(best_entry_basis)):
                    route["best_timing_valid_entry_basis_difference_bps"] = _decimal_text(entry_basis)
                    route["best_timing_valid_entry_basis_cycle"] = cycle
                    # Keep the established field useful to consumers of the
                    # entry-only list while its PnL-specific peer stays null.
                    if route["best_timing_valid_cycle"] is None:
                        route["best_timing_valid_cycle"] = cycle
        self._observe_candidate(
            cycle,
            analysis_kind=analysis_kind,
            observed_realtime_ns=observed_realtime_ns,
            observed_monotonic_ns=observed_monotonic_ns,
        )

    def _close_stale_candidates(self) -> None:
        now_realtime_ns = self._realtime_ns()
        now_monotonic_ns = self._monotonic_ns()
        # A route with no new evaluation cannot remain active longer than the
        # strictest universal BBO freshness budget, even if its last observed
        # receive skew happened to be wider.
        max_idle_ns = int(
            min(self.max_response_skew_ms, self.max_book_age_ms)
            * Decimal(1_000_000)
        )
        for key, state in tuple(self._active.items()):
            if now_monotonic_ns - state.last_seen_monotonic_ns <= max_idle_ns:
                continue
            self._active.pop(key)
            self._candidate_closed += 1
            if state.persisted:
                self._persist_candidate_event(
                    self._candidate_event(
                        "candidate_closed",
                        state,
                        observed_realtime_ns=now_realtime_ns,
                        observed_monotonic_ns=now_monotonic_ns,
                        close_reason="no_fresh_timing_valid_evaluation",
                    ),
                )
            else:
                self._counts["candidate_shorter_than_minimum_persistence"] += 1

    def snapshot(self) -> Mapping[str, Any]:
        # ``snapshot`` is called by the scanner's status writer every two
        # seconds.  Batch candidate persistence here so hot micro-windows do
        # not turn into synchronous disk writes on the event-consumer path.
        self._flush_candidate_events()
        self._flush_capability_manifest()
        ranked_routes = sorted(
            (
                item
                for item in self._route_stats.values()
                if item["best_timing_valid_net_edge_after_modeled_costs_bps"] is not None
            ),
            key=lambda item: Decimal(str(item["best_timing_valid_net_edge_after_modeled_costs_bps"])),
            reverse=True,
        )
        ranked_entry_basis_routes = sorted(
            (
                item
                for item in self._route_stats.values()
                if item["best_timing_valid_entry_basis_difference_bps"] is not None
            ),
            key=lambda item: Decimal(str(item["best_timing_valid_entry_basis_difference_bps"])),
            reverse=True,
        )
        # Entry-basis observations and conservative paired-close models can be
        # valuable research, but neither qualifies as a candidate until the
        # necessary model-quality checks are satisfied. Keep them separate.
        top_routes = [item for item in ranked_routes if item["candidate_eligible"]][:40]
        top_unqualified_closed_models = [
            item for item in ranked_routes if not item["candidate_eligible"]
        ][:40]
        top_entry_only_signals = ranked_entry_basis_routes[:40]
        unsupported_contract_states = [
            {
                "venue": perp.venue,
                "venue_symbol": perp.venue_symbol,
                "base": perp.base,
                "contract_type": perp.contract_type,
                "reason": perp.contract_model.reason,
            }
            for perp in sorted(
                self._perps.values(),
                key=lambda item: (item.venue, item.venue_symbol),
            )
            if not perp.contract_model.supported
        ]
        payload: dict[str, Any] = {
            "schema_version": 1,
            "status": "closed" if self._closed else "running",
            "mode": "event_driven_unified_perp_strategy_analysis",
            "started_at": self._started_at,
            "updated_at": _utc_iso_from_ns(self._realtime_ns()),
            "collector_connections_opened_by_analyzer": 0,
            "raw_market_data_persisted": False,
            "strategy_families": list(STRATEGY_FAMILIES),
            "capability_manifest": {
                "path": "perp_analysis/capabilities.json",
                "write_policy": "on_observed_capability_change_only",
                "raw_market_data_included": False,
                "dirty_in_memory": self._capabilities_dirty,
            },
            "candidate_event_persistence": {
                "path": "perp_analysis/candidate_events.jsonl",
                "format": "bounded_compact_perp_candidate_terminal_summary_v4",
                "retained": len(self._candidate_events),
                "maximum": self.max_candidate_events,
                "write_mode": "batched_append_until_count_cap",
                "event_policy": "persist_terminal_summary_only; active_routes_are_in_stats",
                "at_cap_policy": "keep_existing_terminal_summaries; stop_disk_persistence",
                "flush_interval_seconds": self._candidate_events_flush_interval_seconds,
                "minimum_persistence_ms": _decimal_text(self.candidate_min_persistence_ms),
                "dirty_in_memory": self._candidate_events_dirty,
            },
            "timing": {
                "max_response_skew_ms": _decimal_text(self.max_response_skew_ms),
                "max_book_age_ms": _decimal_text(self.max_book_age_ms),
                "max_exact_quote_age_ms": _decimal_text(self.max_exact_quote_age_ms),
                "max_funding_context_age_ms": _decimal_text(self.max_funding_context_age_ms),
                "freshness_clock": "local_realtime_and_monotonic",
                "evaluation_coalesce_interval_ms": round(self.coalesce_interval_seconds * 1_000, 3),
            },
            "sizing": {
                "target_notional_usdt": _decimal_text(self.target_notional_usdt),
                "funding_horizon_hours": _decimal_text(self.funding_horizon_hours),
                "perp_depth_model": "visible_best_bid_ask_quantity_only",
                "perp_contract_model_policy": (
                    "linear_base_quantity_perpetual_v1_only; "
                    "inverse_quanto_multiplier_and_unclassified_contracts_are_not_modelled"
                ),
            },
            "fee_policy": {
                "spot": "account_fee_audit_when_available_else_public_baseline",
                "perp": "public_contract_fee_when_available_else_conservative_10bps_baseline",
                "bybit_linear": "public_vip0_5_5bps_baseline_not_account_verified",
                "spot_fee_audit_loaded_rates": len(self._spot_fee_rates),
                "spot_fee_audit_load_error": self._fee_audit_error,
            },
            "coverage": {
                "perp_latest_states": len(self._perps),
                "perp_bases": len(self._perp_keys_by_base),
                "spot_latest_states": len(self._spots),
                "direct_exact_quote_states": len(self._dex_quotes),
                "exact_quote_pair_cache": self._exact_quote_pair_cache.snapshot(),
                "sequential_evidence": {
                    "directory": str(self.analysis_directory / "sequential_evidence"),
                    "retained": len(self._sequential_evidence_paths),
                    "maximum": self._max_sequential_evidence_records,
                    "hashes": list(self._sequential_evidence_paths),
                },
                "supported_linear_perp_contract_states": sum(
                    perp.contract_model.supported for perp in self._perps.values()
                ),
                "unsupported_perp_contract_states": unsupported_contract_states,
            },
            "counts": dict(sorted(self._counts.items())),
            "source_epochs": dict(sorted(self._current_source_epochs.items())),
           "candidate_lifecycle": {
               "started": self._candidate_started,
               "improved": self._candidate_improved,
               "closed": self._candidate_closed,
               "active": len(self._active),
               "persisted_active": sum(state.persisted for state in self._active.values()),
               "pending_active": sum(not state.persisted for state in self._active.values()),
           },
            "observability": {
                "received_events": self._counts.get("events_seen", 0),
                "accepted_events": (
                    self._counts.get("dex_quote_events", 0)
                    + self._counts.get("perp_book_events", 0)
                    + self._counts.get("linear_book_events", 0)
                ),
                "rejected_state_events": (
                    self._counts.get("old_epoch_exacts_ignored", 0)
                    + self._counts.get("old_epoch_events_rejected", 0)
                    + self._counts.get("malformed_exact_quote_events", 0)
                    + self._counts.get("malformed_perp_events", 0)
                    + self._counts.get("unmapped_spot_book_events", 0)
                ),
                "pending_keys": len(self._dex_quotes),
                "state_key_count": (
                    len(self._dex_quotes)
                    + sum(len(v) for v in self._dex_keys_by_base.values())
                ),
                "state_key_retirements": (
                    self._counts.get("stale_dex_prunes", 0)
                    + self._counts.get("epoch_invalidated_dex_quotes", 0)
                ),
                "last_error": (
                    self._recent_errors[-1] if self._recent_errors else None
                ),
            },
            "route_count": len(self._route_stats),
            "top_routes": top_routes,
            "top_unqualified_closed_models": top_unqualified_closed_models,
            "top_reconnaissance_entry_signals": top_entry_only_signals,
            "top_routes_policy": (
                "top_routes_are_timing_valid_and_candidate_eligible_pnl_models_only; "
                "top_unqualified_closed_models_are_non-candidate_pnl_models; "
                "top_reconnaissance_entry_signals_are_basis_diagnostics_not_pnl"
            ),
            "recent_calculation_errors": list(self._recent_errors),
            "execution_note": (
                "positive rows are public-data models only. They do not submit orders and exclude "
                "account-specific fees, collateral, liquidation, rebalance, withdrawals, venue gas, "
                "future funding changes, and realised multi-leg fill risk."
            ),
        }
        if self.output_directory.exists():
            self.analysis_directory.mkdir(parents=True, exist_ok=True)
            atomic_json(self.stats_path, payload)
        return payload

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        now = self._realtime_ns()
        now_monotonic_ns = self._monotonic_ns()
        for key, state in tuple(self._active.items()):
            self._active.pop(key)
            self._candidate_closed += 1
            if state.persisted:
                self._persist_candidate_event(
                    self._candidate_event(
                        "candidate_closed",
                        state,
                        observed_realtime_ns=now,
                        observed_monotonic_ns=now_monotonic_ns,
                        close_reason="scanner_shutdown",
                    ),
                )
            else:
                self._counts["candidate_shorter_than_minimum_persistence"] += 1
        self._flush_candidate_events(force=True)
        self.snapshot()
