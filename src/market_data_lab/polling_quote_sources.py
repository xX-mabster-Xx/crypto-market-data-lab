"""Thin adapters from existing public exact-quote APIs to ``MarketEvent``.

Several DEXes expose an execution-curve simulation rather than a streaming
order book.  They still belong in the common data plane, but their semantics
must remain explicit: one event is an exact-input quote for a particular size,
not a pretend mid-price or an executable order.

The adapter discards vendor payloads and route plans immediately after
normalising a compact observation.  Only that compact current state and the
bounded scanner window remain in RAM; nothing is written as raw quote data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import time
import urllib.parse
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from typing import Any

from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import DexQuoteProvider
from market_data_lab.dex_quotes import quote_route_labels
from market_data_lab.numeric_text import canonical_decimal_text
from market_data_lab.quote_broker import QuoteKey
from market_data_lab.quote_broker import QuoteResult
from market_data_lab.realtime_scanner import MarketEvent


Publish = Callable[[MarketEvent], Awaitable[None]]
_URL_PATTERN = re.compile(r"(?:https?|wss?)://[^\s\"']+")
_RATE_LIMIT_PATTERN = re.compile(r"(?:\b429\b|rate[ -]?limit|too many requests)", re.IGNORECASE)
_TERMINAL_ROUTE_PATTERN = re.compile(
    r"(?:could not find pool|insufficient liquidity|no active quote route)",
    re.IGNORECASE,
)


def _safe_error(value: object) -> str | None:
    if value is None:
        return None

    def redact(match: re.Match[str]) -> str:
        parsed = urllib.parse.urlsplit(match.group(0))
        host = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port is not None else ""
        return f"{parsed.scheme}://{host}{port}"

    return _URL_PATTERN.sub(redact, str(value))[:512]


def _as_int(value: object) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _as_decimal(value: object) -> Decimal | None:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return result if result.is_finite() else None


def _is_rate_limited(record: Mapping[str, Any]) -> bool:
    return bool(_RATE_LIMIT_PATTERN.search(str(record.get("error", ""))))


def _is_terminal_route_miss(record: Mapping[str, Any]) -> bool:
    return bool(_TERMINAL_ROUTE_PATTERN.search(str(record.get("error", ""))))


@dataclass(frozen=True, slots=True)
class ExactInputQuote:
    """Compact normalised exact-input DEX quote retained by the common store."""

    provider: str
    chain: str | None
    protocol: str | None
    source_kind: str | None
    pair: str | None
    direction: str | None
    round_id: int | None
    requested_notional_quote: Decimal | None
    # For a cross-asset pair, this is separate from the native DEX input
    # amount above.  It makes an approximately common-USD quote bucket
    # explicit without sending an invented price to a vendor API.
    reference_notional_usdt: Decimal | None
    # These normalized human-unit amounts are needed by an event-driven
    # consumer that walks a current CEX depth book.  Keeping them alongside a
    # compact DEX quote does not persist raw vendor payloads and avoids having
    # an evaluator re-query the DEX merely to reconstruct its sizing.
    base_amount: Decimal | None
    quote_amount: Decimal | None
    input_symbol: str | None
    output_symbol: str | None
    input_amount_raw: int | None
    output_amount_raw: int | None
    average_price_quote_per_base: Decimal | None
    fee_bps: Decimal | None
    request_rtt_ms: float | None
    status: str
    error: str | None
    response_received_realtime_ns: int
    response_received_monotonic_ns: int
    block_number: int | None
    # Stamped by the common collector supervisor before strategy consumers
    # cache the quote.  A matching local round number from a prior reconnect
    # is not compatible with the current source epoch.
    source_epoch: int = 0
    input_asset_id: str | None = None
    output_asset_id: str | None = None
    broker_endpoint_generation: str | None = None
    broker_quota_domain: str | None = None
    broker_policy_fingerprint: str | None = None
    quote_slot_id: str | None = None
    route_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QuoteRoundInput:
    """Exact DEX input amount with a stable logical quote slot identifier."""

    amount: Decimal
    reference_notional_usdt: Decimal | None = None
    quote_slot_id: str | None = None


def _static_slot_id(amount: Decimal) -> str:
    """Build a stable slot id for a static (non-dynamic) notional amount."""

    return f"notional:{canonical_decimal_text(amount)}"


def _slot_id_from_record(record: Mapping[str, Any], source_name: str) -> str | None:
    """Extract or derive a stable quote_slot_id from a provider record.

    Records that carry an explicit ``quote_slot_id`` use it directly.  Otherwise
    the slot is derived from the ``requested_notional_quote`` and ``direction``
    using canonical decimal text, so dynamic-size quotes share one identity.
    Malformed records that lack both a slot and a notional are classified as
    unmapped rather than generating a new arbitrary key per payload.
    """

    explicit = record.get("quote_slot_id")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()[:128]
    notional = _as_decimal(record.get("requested_notional_quote"))
    direction = record.get("direction")
    if notional is None or not notional.is_finite() or notional <= 0:
        return None
    if not isinstance(direction, str) or direction not in {"buy_base", "sell_base"}:
        return None
    return f"notional:{canonical_decimal_text(notional)}:{direction}"


@dataclass
class PollingDexQuoteSource:
    """Publish one provider's public exact-input quote stream.

    ``shared_quota_pacer`` is deliberately injected for APIs that do not
    impose their own pacing.  A provider that already has a shared internal
   pacer (Raydium/Jupiter) leaves it ``None``; that pacer is still shared
    across sibling sources, so adding pairs never multiplies the quota.
   """

    provider: DexQuoteProvider
    notionals: Sequence[Decimal]
    notional_supplier: Callable[[], Sequence[QuoteRoundInput]] | None = None
    minimum_round_interval_seconds: float = 0.0
    shared_quota_pacer: AsyncRequestPacer | None = None
    rate_limit_circuit_breaker_events: int | None = None
    rate_limit_circuit_breaker_seconds: float = 900.0
    terminal_route_circuit_breaker_events: int | None = None
    terminal_route_circuit_breaker_seconds: float = 900.0
    quote_ttl_seconds: float = 1.5
    quota_domain: str | None = None
    name: str = field(init=False)
    _endpoint_generation: str = field(init=False, repr=False)
    _policy_fingerprint: str = field(init=False, repr=False)
    _provider_descriptor: Mapping[str, Any] = field(init=False, repr=False)
    _rounds: int = field(default=0, init=False, repr=False)
    _observations: int = field(default=0, init=False, repr=False)
    _statuses: Counter[str] = field(default_factory=Counter, init=False, repr=False)
    _last_error: str | None = field(default=None, init=False, repr=False)
    _last_rtt_ms: float | None = field(default=None, init=False, repr=False)
    _last_event_monotonic_ns: int | None = field(default=None, init=False, repr=False)
    _recent_errors: deque[str] = field(default_factory=lambda: deque(maxlen=8), init=False, repr=False)
    _consecutive_exceptions: int = field(default=0, init=False, repr=False)
    _rate_limit_events: int = field(default=0, init=False, repr=False)
    _rate_limit_streak: int = field(default=0, init=False, repr=False)
    _last_rate_limit_error: str | None = field(default=None, init=False, repr=False)
    _rate_limit_circuit_breaker_until: float | None = field(default=None, init=False, repr=False)
    _terminal_route_events: int = field(default=0, init=False, repr=False)
    _last_terminal_route_error: str | None = field(default=None, init=False, repr=False)
    _terminal_route_circuit_breaker_until: float | None = field(default=None, init=False, repr=False)
    _last_round_inputs: tuple[QuoteRoundInput, ...] = field(default=(), init=False, repr=False)

    def __post_init__(self) -> None:
        if self.notional_supplier is None and (
            not self.notionals
            or any(value <= 0 or not value.is_finite() for value in self.notionals)
        ):
            raise ValueError("quote source needs finite positive notionals or a supplier")
        if self.notionals and any(value <= 0 or not value.is_finite() for value in self.notionals):
            raise ValueError("quote source notionals must be finite and positive")
        if self.minimum_round_interval_seconds < 0:
            raise ValueError("minimum quote round interval cannot be negative")
        if self.rate_limit_circuit_breaker_events is not None and self.rate_limit_circuit_breaker_events <= 0:
            raise ValueError("rate-limit circuit-breaker threshold must be positive")
        if self.rate_limit_circuit_breaker_seconds <= 0:
            raise ValueError("rate-limit circuit-breaker duration must be positive")
        if (
            self.terminal_route_circuit_breaker_events is not None
            and self.terminal_route_circuit_breaker_events <= 0
        ):
            raise ValueError("terminal-route circuit-breaker threshold must be positive")
        if self.terminal_route_circuit_breaker_seconds <= 0:
            raise ValueError("terminal-route circuit-breaker duration must be positive")
        if not math.isfinite(self.quote_ttl_seconds) or self.quote_ttl_seconds <= 0:
            raise ValueError("quote TTL must be positive")
        provider_name = str(getattr(self.provider, "name", "")).strip()
        if not provider_name:
            raise ValueError("quote provider needs a non-empty name")
        if self.quota_domain is not None and not self.quota_domain.strip():
            raise ValueError("quota domain must be non-empty when supplied")
        self.name = f"dexquote:{provider_name}"
        descriptor_factory = getattr(self.provider, "config", None)
        descriptor: Mapping[str, Any] = {}
        if callable(descriptor_factory):
            candidate = descriptor_factory()
            if isinstance(candidate, Mapping):
                descriptor = dict(candidate)
        self._provider_descriptor = descriptor
        policy_encoded = json.dumps(
            descriptor or {"provider": provider_name, "implementation": type(self.provider).__name__},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        endpoint_descriptor = {
            name: descriptor[name]
            for name in ("chain", "endpoint_origin", "protocol", "source_kind")
            if name in descriptor
        }
        endpoint_encoded = json.dumps(
            endpoint_descriptor
            or {"provider": provider_name, "implementation": type(self.provider).__name__},
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        self._endpoint_generation = (
            f"endpoint-sha256:{hashlib.sha256(endpoint_encoded).hexdigest()}"
        )
        self._policy_fingerprint = (
            f"config-sha256:{hashlib.sha256(policy_encoded).hexdigest()}"
        )
        if self.quota_domain is None:
            self.quota_domain = f"provider:{provider_name.upper()}"

    def describe(self) -> Mapping[str, Any]:
        return {
            "source": self.name,
            "provider": str(self.provider.name),
            "mode": "public_exact_input_quote_polling",
            "notionals_quote": [format(value, "f") for value in self.notionals],
            "dynamic_notional_supplier": self.notional_supplier is not None,
            "minimum_round_interval_seconds": self.minimum_round_interval_seconds,
            "uses_shared_quota_pacer": self.shared_quota_pacer is not None,
            "quote_ttl_seconds": self.quote_ttl_seconds,
            "quota_domain": self.quota_domain,
            "broker_endpoint_generation": self._endpoint_generation,
            "broker_policy_fingerprint": self._policy_fingerprint,
            "raw_vendor_payload_persisted": False,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }

    def status(self) -> Mapping[str, Any]:
        age_ms: float | None = None
        if self._last_event_monotonic_ns is not None:
            age_ms = max(0.0, (time.monotonic_ns() - self._last_event_monotonic_ns) / 1_000_000)
        circuit_breaker_seconds_remaining = (
            max(0.0, self._rate_limit_circuit_breaker_until - time.monotonic())
            if self._rate_limit_circuit_breaker_until is not None
            else 0.0
        )
        terminal_route_circuit_breaker_seconds_remaining = (
            max(0.0, self._terminal_route_circuit_breaker_until - time.monotonic())
            if self._terminal_route_circuit_breaker_until is not None
            else 0.0
        )
        return {
            "rounds": self._rounds,
            "observations": self._observations,
            "status_counts": dict(self._statuses),
            "last_error": self._last_error,
            "recent_errors": list(self._recent_errors),
            "last_request_rtt_ms": self._last_rtt_ms,
            "last_quote_age_ms": round(age_ms, 3) if age_ms is not None else None,
            "last_round_inputs": [
                {
                    "amount": format(item.amount, "f"),
                    "reference_notional_usdt": (
                        format(item.reference_notional_usdt, "f")
                        if item.reference_notional_usdt is not None
                        else None
                     ),
                }
                for item in self._last_round_inputs
            ],
            "rate_limit_events": self._rate_limit_events,
            "rate_limit_streak": self._rate_limit_streak,
            "last_rate_limit_error": self._last_rate_limit_error,
            "rate_limit_circuit_breaker_active": circuit_breaker_seconds_remaining > 0,
            "rate_limit_circuit_breaker_seconds_remaining": round(
                circuit_breaker_seconds_remaining,
                3,
            ),
            "terminal_route_events": self._terminal_route_events,
            "last_terminal_route_error": self._last_terminal_route_error,
            "terminal_route_circuit_breaker_active": (
                terminal_route_circuit_breaker_seconds_remaining > 0
            ),
            "terminal_route_circuit_breaker_seconds_remaining": round(
                terminal_route_circuit_breaker_seconds_remaining,
                3,
            ),
        }

    def _asset_id(self, *, direction: str, input_side: bool) -> str | None:
        base = getattr(self.provider, "base", None)
        quote = getattr(self.provider, "quote", None)
        if direction == "buy_base":
            asset = quote if input_side else base
        elif direction == "sell_base":
            asset = base if input_side else quote
        else:
            return None
        address = getattr(asset, "address", None)
        decimals = getattr(asset, "decimals", None)
        chain = self._provider_descriptor.get("chain")
        chain_id = self._provider_descriptor.get("chain_id")
        if (
            not isinstance(chain, str)
            or not chain.strip()
            or not isinstance(address, str)
            or not address.strip()
            or not isinstance(decimals, int)
            or decimals < 0
        ):
            return None
        if chain.lower() == "solana" and chain_id is None:
            # Keep the public route observation in the same canonical asset
            # namespace as immutable local-worker snapshots.  Without this
            # boundary normalization every real CPMM result is rejected even
            # when its mint and decimals are identical.
            return f"solana:mainnet:{address}:{decimals}"
        namespace = f"eip155:{chain_id}" if isinstance(chain_id, int) else chain.lower()
        identifier = address.lower() if namespace.startswith("eip155:") else address
        return f"{namespace}:{identifier}:decimals:{decimals}"

    def _normalise(self, record: Mapping[str, Any]) -> ExactInputQuote:
        now_realtime_ns = time.time_ns()
        now_monotonic_ns = time.monotonic_ns()
        realtime_ns = _as_int(record.get("response_received_realtime_ns")) or now_realtime_ns
        monotonic_ns = _as_int(record.get("response_received_monotonic_ns")) or now_monotonic_ns
        chain_context = record.get("chain_context")
        block_number = (
            _as_int(chain_context.get("block_number"))
            if isinstance(chain_context, Mapping)
            else None
        )
        rtt = record.get("request_rtt_ms")
        request_rtt_ms = float(rtt) if isinstance(rtt, (int, float)) else None
        direction = (
            str(record["direction"])[:32]
            if isinstance(record.get("direction"), str)
            else None
        )
        return ExactInputQuote(
            provider=str(record.get("provider", ""))[:96],
            chain=str(record["chain"])[:64] if isinstance(record.get("chain"), str) else None,
            protocol=(str(record["protocol"])[:128] if isinstance(record.get("protocol"), str) else None),
            source_kind=(str(record["source_kind"])[:96] if isinstance(record.get("source_kind"), str) else None),
            pair=str(record["pair"])[:128] if isinstance(record.get("pair"), str) else None,
            direction=direction,
            round_id=_as_int(record.get("round_id")),
            requested_notional_quote=_as_decimal(record.get("requested_notional_quote")),
            reference_notional_usdt=_as_decimal(record.get("reference_notional_usdt")),
            base_amount=_as_decimal(record.get("base_amount")),
            quote_amount=_as_decimal(record.get("quote_amount")),
            input_symbol=(str(record["input_symbol"])[:64] if isinstance(record.get("input_symbol"), str) else None),
            output_symbol=(str(record["output_symbol"])[:64] if isinstance(record.get("output_symbol"), str) else None),
            input_amount_raw=_as_int(record.get("input_amount_raw")),
            output_amount_raw=_as_int(record.get("output_amount_raw")),
            average_price_quote_per_base=_as_decimal(record.get("average_price_quote_per_base")),
            fee_bps=_as_decimal(record.get("fee_bps")),
            request_rtt_ms=request_rtt_ms,
            status=str(record.get("status", "request_error"))[:64],
            error=_safe_error(record.get("error")),
            response_received_realtime_ns=realtime_ns,
            response_received_monotonic_ns=monotonic_ns,
            block_number=block_number,
            input_asset_id=(
                self._asset_id(direction=direction, input_side=True)
                if direction is not None
                else None
            ),
            output_asset_id=(
                self._asset_id(direction=direction, input_side=False)
                if direction is not None
                else None
            ),
            broker_endpoint_generation=self._endpoint_generation,
            broker_quota_domain=self.quota_domain,
            broker_policy_fingerprint=self._policy_fingerprint,
            quote_slot_id=_slot_id_from_record(record, self.name),
            route_ids=quote_route_labels(record),
        )

    def broker_result(self, quote: ExactInputQuote) -> QuoteResult | None:
        """Project one usable polling observation into the shared Broker cache.

        The conversion is intentionally strict.  Legacy/synthetic events that
        lack canonical contract-or-mint identifiers remain available to the
        old analyzers, but cannot masquerade as reusable broker evidence.
        """

        if (
            quote.status != "ok"
            or quote.provider != getattr(self.provider, "name", None)
            or not quote.chain
            or not quote.input_asset_id
            or not quote.output_asset_id
            or not quote.broker_endpoint_generation
            or not quote.broker_quota_domain
            or not quote.broker_policy_fingerprint
            or not isinstance(quote.input_amount_raw, int)
            or quote.input_amount_raw <= 0
            or not isinstance(quote.output_amount_raw, int)
            or quote.output_amount_raw <= 0
        ):
            return None
        ttl_ns = int(self.quote_ttl_seconds * 1_000_000_000)
        state_fingerprint = (
            f"source_epoch:{quote.source_epoch}:block:{quote.block_number}"
            if quote.block_number is not None
            else f"source_epoch:{quote.source_epoch}:ttl_ns:{ttl_ns}"
        )
        key = QuoteKey(
            provider=quote.provider,
            endpoint_generation=quote.broker_endpoint_generation,
            quota_domain=quote.broker_quota_domain,
            chain=quote.chain,
            input_asset_id=quote.input_asset_id,
            output_asset_id=quote.output_asset_id,
            amount_raw=quote.input_amount_raw,
            mode="exact_in",
            route_constraints_fingerprint=quote.broker_policy_fingerprint,
            fee_policy_fingerprint=quote.broker_policy_fingerprint,
            slippage_policy_fingerprint=quote.broker_policy_fingerprint,
            required_state_fingerprint=state_fingerprint,
            minimum_state_quality=(
                "pinned_block" if quote.block_number is not None else "locally_received_ttl"
            ),
        )
        rtt_ns = (
            max(0, int(quote.request_rtt_ms * 1_000_000))
            if quote.request_rtt_ms is not None
            else 0
        )
        request_started_ns = max(1, quote.response_received_monotonic_ns - rtt_ns)
        fee_breakdown: tuple[Mapping[str, object], ...] = ()
        if quote.fee_bps is not None:
            fee_breakdown = (
                {
                    "kind": "provider_reported_fee_bps",
                    "amount_bps": format(quote.fee_bps, "f"),
                    "included_in_output": True,
                },
            )
        return QuoteResult(
            request_id=(
                f"observed:{self.name}:{quote.source_epoch}:"
                f"{quote.round_id}:{quote.response_received_monotonic_ns}"
            ),
            key=key,
            status="ok",
            reason=None,
            requested_input_raw=quote.input_amount_raw,
            requested_output_raw=None,
            actual_input_raw=quote.input_amount_raw,
            actual_output_raw=quote.output_amount_raw,
            expected_output_raw=quote.output_amount_raw,
            minimum_accepted_output_raw=None,
            maximum_accepted_input_raw=None,
            request_started_monotonic_ns=request_started_ns,
            response_received_monotonic_ns=quote.response_received_monotonic_ns,
            response_received_realtime_ns=quote.response_received_realtime_ns,
            published_monotonic_ns=quote.response_received_monotonic_ns,
            ttl_ns=ttl_ns,
            exactness="exact_input",
            firmness="indicative",
            consistency=("pinned_block" if quote.block_number is not None else "unpinned"),
            state_after_capability="unsupported",
            route_ids=quote.route_ids,
            fee_breakdown=fee_breakdown,
            block_number=quote.block_number,
            served_from="observed_polling",
        )

    @staticmethod
    def _summary(quote: ExactInputQuote) -> dict[str, Any]:
        return {
            "provider": quote.provider,
            "chain": quote.chain,
            "protocol": quote.protocol,
            "source_kind": quote.source_kind,
            "pair": quote.pair,
            "direction": quote.direction,
            "round_id": quote.round_id,
            "requested_notional_quote": (
                format(quote.requested_notional_quote, "f")
                if quote.requested_notional_quote is not None
                else None
            ),
            "reference_notional_usdt": (
                format(quote.reference_notional_usdt, "f")
                if quote.reference_notional_usdt is not None
                else None
            ),
            "base_amount": format(quote.base_amount, "f") if quote.base_amount is not None else None,
            "quote_amount": format(quote.quote_amount, "f") if quote.quote_amount is not None else None,
            "status": quote.status,
            "average_price_quote_per_base": (
                format(quote.average_price_quote_per_base, "f")
                if quote.average_price_quote_per_base is not None
                else None
            ),
            "fee_bps": format(quote.fee_bps, "f") if quote.fee_bps is not None else None,
            "request_rtt_ms": quote.request_rtt_ms,
            "block_number": quote.block_number,
            "error": quote.error,
            "quote_slot_id": quote.quote_slot_id,
        }

    async def _publish_record(self, publish: Publish, record: Mapping[str, Any]) -> None:
        quote = self._normalise(record)
        self._observations += 1
        self._statuses[quote.status] += 1
        self._last_event_monotonic_ns = quote.response_received_monotonic_ns
        self._last_rtt_ms = quote.request_rtt_ms
        if quote.error is not None:
            self._last_error = quote.error
            self._recent_errors.append(quote.error)
        elif quote.status == "ok":
            self._last_error = None
        slot_id = quote.quote_slot_id or "unmapped"
        direction = quote.direction or "unknown"
        await publish(
            MarketEvent(
                source=self.name,
                key=f"{self.name}:{slot_id}:{direction}",
                kind="exact_input_quote",
                value=quote,
                summary=self._summary(quote),
                received_realtime_ns=quote.response_received_realtime_ns,
                received_monotonic_ns=quote.response_received_monotonic_ns,
                chain_position=quote.block_number,
                instrument_or_pool_id=f"{self.name}:{slot_id}:{direction}",
            ),
        )

    def _round_inputs(self) -> tuple[QuoteRoundInput, ...]:
        if self.notional_supplier is None:
            return tuple(
                QuoteRoundInput(
                    amount=value,
                    quote_slot_id=_static_slot_id(value),
                )
                for value in self.notionals
            )
        supplied = self.notional_supplier()
        normalized: list[QuoteRoundInput] = []
        for item in supplied:
            if not isinstance(item, QuoteRoundInput):
                raise TypeError("notional supplier must return QuoteRoundInput values")
            if item.amount <= 0 or not item.amount.is_finite():
                continue
            if (
                item.reference_notional_usdt is not None
                and (
                    item.reference_notional_usdt <= 0
                    or not item.reference_notional_usdt.is_finite()
                )
            ):
                continue
            slot_id = item.quote_slot_id
            if not slot_id:
                if item.reference_notional_usdt is not None:
                    slot_id = f"triangle-reference-usdt:{canonical_decimal_text(item.reference_notional_usdt)}"
                else:
                    slot_id = _static_slot_id(item.amount)
            normalized.append(
                item
                if item.quote_slot_id == slot_id
                else replace(item, quote_slot_id=slot_id)
            )
        return tuple(normalized)

    @staticmethod
    def _with_reference_notional(
        record: Mapping[str, Any],
        *,
        reference_by_notional: Mapping[str, Decimal],
    ) -> Mapping[str, Any]:
        reference = reference_by_notional.get(str(record.get("requested_notional_quote")))
        if reference is None:
            return record
        payload = dict(record)
        payload["reference_notional_usdt"] = format(reference, "f")
        payload["quote_slot_id"] = (
            f"triangle-reference-usdt:{canonical_decimal_text(reference)}"
        )
        return payload

    async def run(self, publish: Publish, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            active_breakers = [
                until
                for until in (
                    self._rate_limit_circuit_breaker_until,
                    self._terminal_route_circuit_breaker_until,
                )
                if until is not None and until > time.monotonic()
            ]
            if active_breakers:
                resume_at = max(active_breakers)
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=resume_at - time.monotonic(),
                     )
                except TimeoutError:
                    pass
                continue
            round_started = time.monotonic()
            try:
                round_inputs = self._round_inputs()
            except Exception as exc:
                self._consecutive_exceptions += 1
                now_realtime_ns = time.time_ns()
                now_monotonic_ns = time.monotonic_ns()
                await self._publish_record(
                    publish,
                    {
                        "provider": self.provider.name,
                        "status": "request_error",
                        "error": f"notional supplier {type(exc).__name__}: {exc}",
                        "response_received_realtime_ns": now_realtime_ns,
                        "response_received_monotonic_ns": now_monotonic_ns,
                    },
                )
                round_inputs = ()
            self._last_round_inputs = round_inputs
            if not round_inputs:
                # Wait for a current CEX reference rather than asking a DEX
                # for a meaningless fixed number of BTC/SOL/etc. units.
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=0.25)
                except TimeoutError:
                    pass
                continue
            notionals = tuple(item.amount for item in round_inputs)
            reference_by_notional = {
                format(item.amount, "f"): item.reference_notional_usdt
                for item in round_inputs
                if item.reference_notional_usdt is not None
            }
            try:
                records = await self.provider.quote_round(self._rounds, notionals)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._consecutive_exceptions += 1
                now_realtime_ns = time.time_ns()
                now_monotonic_ns = time.monotonic_ns()
                await self._publish_record(
                    publish,
                    {
                        "provider": self.provider.name,
                        "status": "request_error",
                        "error": f"{type(exc).__name__}: {exc}",
                        "response_received_realtime_ns": now_realtime_ns,
                        "response_received_monotonic_ns": now_monotonic_ns,
                    },
                )
                records = ()
            else:
                self._consecutive_exceptions = 0
            self._rounds += 1
            for record in records:
                if isinstance(record, Mapping):
                    await self._publish_record(
                        publish,
                        self._with_reference_notional(
                            record,
                            reference_by_notional=reference_by_notional,
                         ),
                     )
            rate_limited_records = [
                record
                for record in records
                if isinstance(record, Mapping) and _is_rate_limited(record)
            ]
            if rate_limited_records:
                self._rate_limit_events += len(rate_limited_records)
                self._rate_limit_streak += 1
                self._last_rate_limit_error = _safe_error(rate_limited_records[-1].get("error"))
                # A shared gate applies the pause to every sibling market on
                # that provider.  This is crucial: a 429 for one pair is a
                # quota signal for the endpoint, not an invitation to probe
                # the next pair at the same rate.
                if self.shared_quota_pacer is not None:
                    await self.shared_quota_pacer.defer(
                        cooldown_seconds=min(120.0, 5.0 * (2 ** min(self._rate_limit_streak, 5))),
                    )
                if (
                    self.rate_limit_circuit_breaker_events is not None
                    and self._rate_limit_events >= self.rate_limit_circuit_breaker_events
                ):
                    self._rate_limit_circuit_breaker_until = (
                        time.monotonic() + self.rate_limit_circuit_breaker_seconds
                     )
            else:
                self._rate_limit_streak = 0
            terminal_route_records = [
                record
                for record in records
                if isinstance(record, Mapping) and _is_terminal_route_miss(record)
            ]
            if terminal_route_records:
                self._terminal_route_events += len(terminal_route_records)
                self._last_terminal_route_error = _safe_error(
                    terminal_route_records[-1].get("error"),
                )
                if (
                    self.terminal_route_circuit_breaker_events is not None
                    and self._terminal_route_events >= self.terminal_route_circuit_breaker_events
                ):
                    self._terminal_route_circuit_breaker_until = (
                        time.monotonic() + self.terminal_route_circuit_breaker_seconds
                     )
            elapsed = time.monotonic() - round_started
            delay = max(0.0, self.minimum_round_interval_seconds - elapsed)
            # A provider exception or malformed empty response must not turn
            # into a hot retry loop.  This is separate from the public API
            # pace and only reduces load during a fault.
            if self._consecutive_exceptions:
                delay = max(delay, min(15.0, 0.25 * (2 ** min(self._consecutive_exceptions, 6))))
            elif self._rate_limit_streak:
                delay = max(delay, min(120.0, 5.0 * (2 ** min(self._rate_limit_streak, 5))))
            elif not records:
                delay = max(delay, 0.25)
            if delay > 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except TimeoutError:
                    pass
