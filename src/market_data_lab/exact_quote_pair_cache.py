"""Bounded in-memory reuse of compatible exact-input DEX quote pairs.

The live data plane deliberately retains compact normalized quotes rather than
vendor payloads.  A provider can publish several routes or fee tiers for one
notional, so a single ``latest[provider, notional, direction]`` slot is not
enough to recover a valid ``stable -> base -> stable`` pair.  This cache keeps
a small number of compact observations indexed by the *actual raw base
quantity*.  It never opens a connection, requests a quote, or writes quote
payloads to disk.

It is not an atomic-AMM simulator.  A returned pair only proves that two
independent public simulations are compatible at the requested quantity.  Its
state/round quality is carried explicitly for the caller to decide whether a
particular strategy may use the result.
"""

from __future__ import annotations

from collections import Counter, OrderedDict, deque
from dataclasses import dataclass
from decimal import Decimal

from market_data_lab.polling_quote_sources import ExactInputQuote


_MarketKey = tuple[str, str, str, str, str, int]
_BucketKey = tuple[_MarketKey, str, int]


def _identifier(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    return normalized or None


def _positive(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def quote_received_before(
    candidate: ExactInputQuote,
    reference: ExactInputQuote,
) -> bool:
    """Whether ``candidate`` would regress a locally received quote store.

    The local monotonic receipt clock is authoritative within one process;
    UTC is a deterministic tiebreaker only.  That prevents a delayed old
    response from becoming fresh merely because it arrived through a later
    event-handling path.
    """

    if candidate.source_epoch != reference.source_epoch:
        return candidate.source_epoch < reference.source_epoch
    if candidate.response_received_monotonic_ns != reference.response_received_monotonic_ns:
        return (
            candidate.response_received_monotonic_ns
            < reference.response_received_monotonic_ns
        )
    return candidate.response_received_realtime_ns < reference.response_received_realtime_ns


@dataclass(frozen=True, slots=True)
class ExactQuotePair:
    """A compatible entry and reverse quote already seen on the shared bus."""

    entry: ExactInputQuote
    exit: ExactInputQuote
    pairing_quality: str
    same_source_epoch: bool
    same_source_round: bool
    same_block_number: bool | None


class ExactQuotePairCache:
    """Keep a small LRU of exact quote curves keyed by native base quantity."""

    def __init__(
        self,
        *,
        max_buckets: int = 2_048,
        max_records_per_bucket: int = 4,
    ) -> None:
        if max_buckets <= 0:
            raise ValueError("max_buckets must be positive")
        if max_records_per_bucket <= 0:
            raise ValueError("max_records_per_bucket must be positive")
        self.max_buckets = max_buckets
        self.max_records_per_bucket = max_records_per_bucket
        self._buckets: dict[_BucketKey, deque[ExactInputQuote]] = {}
        self._lru: OrderedDict[_BucketKey, None] = OrderedDict()
        self._bucket_keys_by_market_direction: dict[
            tuple[_MarketKey, str], set[_BucketKey]
        ] = {}
        self._counts: Counter[str] = Counter()

    @staticmethod
    def _metadata(
        quote: ExactInputQuote,
    ) -> tuple[_MarketKey, str, int] | None:
        if (
            quote.status != "ok"
            or quote.direction not in {"buy_base", "sell_base"}
            or not _positive(quote.base_amount)
            or not _positive(quote.quote_amount)
        ):
            return None
        provider = _identifier(quote.provider)
        chain = _identifier(quote.chain)
        pair = _identifier(quote.pair)
        if quote.direction == "buy_base":
            base = _identifier(quote.output_symbol)
            quote_asset = _identifier(quote.input_symbol)
            raw_base = quote.output_amount_raw
        else:
            base = _identifier(quote.input_symbol)
            quote_asset = _identifier(quote.output_symbol)
            raw_base = quote.input_amount_raw
        if (
            provider is None
            or chain is None
            or pair is None
            or base is None
            or quote_asset is None
            or not isinstance(raw_base, int)
            or raw_base <= 0
        ):
            return None
        return (
            provider,
            chain,
            pair,
            base,
            quote_asset,
            quote.source_epoch,
        ), quote.direction, raw_base

    def _touch(self, key: _BucketKey) -> None:
        self._lru[key] = None
        self._lru.move_to_end(key)

    def _drop_bucket(self, key: _BucketKey) -> None:
        self._buckets.pop(key, None)
        self._lru.pop(key, None)
        market_direction = key[0], key[1]
        siblings = self._bucket_keys_by_market_direction.get(market_direction)
        if siblings is None:
            return
        siblings.discard(key)
        if not siblings:
            self._bucket_keys_by_market_direction.pop(market_direction, None)

    def put(self, quote: ExactInputQuote) -> bool:
        """Store one valid compact quote, evicting LRU buckets if necessary."""

        metadata = self._metadata(quote)
        if metadata is None:
            self._counts["rejected_invalid_quote"] += 1
            return False
        market, direction, raw_base = metadata
        key: _BucketKey = market, direction, raw_base
        records = self._buckets.get(key)
        if records is None:
            while len(self._buckets) >= self.max_buckets:
                oldest, _ = self._lru.popitem(last=False)
                self._drop_bucket(oldest)
                self._counts["buckets_evicted_lru"] += 1
            records = deque(maxlen=self.max_records_per_bucket)
            self._buckets[key] = records
            self._bucket_keys_by_market_direction.setdefault((market, direction), set()).add(key)
        elif records and records[-1] == quote:
            self._touch(key)
            self._counts["duplicate_records_ignored"] += 1
            return False
        elif records:
            newest = max(
                records,
                key=lambda item: (
                    item.response_received_monotonic_ns,
                    item.response_received_realtime_ns,
                ),
            )
            if quote_received_before(quote, newest):
                self._touch(key)
                self._counts["out_of_order_records_ignored"] += 1
                return False
        if len(records) == records.maxlen:
            self._counts["records_evicted_per_bucket"] += 1
        records.append(quote)
        self._touch(key)
        self._counts["records_stored"] += 1
        return True

    @staticmethod
    def _age_ns(quote: ExactInputQuote, now_monotonic_ns: int) -> int:
        """Age a locally received quote on the process monotonic clock.

        The public response also carries a UTC receipt timestamp for evidence,
        but wall-clock corrections must not make an old cache entry look new.
        The analyzer separately guards suspend/reboot-like discontinuities with
        the realtime receipt timestamp before it evaluates a route.
        """

        return max(0, now_monotonic_ns - quote.response_received_monotonic_ns)

    @staticmethod
    def _pair_quality(
        entry: ExactInputQuote,
        exit: ExactInputQuote,
    ) -> tuple[str, bool, bool | None] | None:
        if entry.source_epoch != exit.source_epoch:
            return None
        if entry.base_amount != exit.base_amount:
            return None
        if entry.block_number is not None and exit.block_number is not None:
            if entry.block_number != exit.block_number:
                return None
            same_block: bool | None = True
        else:
            same_block = None
        same_round = (
            entry.round_id is not None
            and exit.round_id is not None
            and entry.round_id == exit.round_id
        )
        if same_round and same_block is True:
            quality = "same_round_same_block"
        elif same_round:
            quality = "same_round_unpinned_state"
        elif same_block is True:
            quality = "cross_round_same_block"
        elif entry.round_id is None or exit.round_id is None:
            quality = "round_unavailable_unpinned_state"
        else:
            quality = "cross_round_unpinned_state"
        return quality, same_round, same_block

    @staticmethod
    def _quality_rank(pair: ExactQuotePair) -> tuple[int, int, Decimal, int]:
        # A pinned common block is stronger than a local source round.  Within
        # equivalent quality, retain the best currently executable exit quote.
        quality_rank = {
            "same_round_same_block": 0,
            "cross_round_same_block": 1,
            "same_round_unpinned_state": 2,
            "cross_round_unpinned_state": 3,
            "round_unavailable_unpinned_state": 4,
        }[pair.pairing_quality]
        assert pair.exit.quote_amount is not None
        return (
            quality_rank,
            0 if pair.same_source_round else 1,
            -pair.exit.quote_amount,
            -pair.exit.response_received_realtime_ns,
        )

    def best_reverse_for(
        self,
        entry: ExactInputQuote,
        *,
        now_monotonic_ns: int,
        max_age_ns: int,
    ) -> tuple[ExactQuotePair | None, str]:
        """Find the freshest compatible ``sell_base`` quote for a buy quote."""

        if max_age_ns <= 0:
            raise ValueError("max_age_ns must be positive")
        metadata = self._metadata(entry)
        if metadata is None or entry.direction != "buy_base":
            self._counts["lookup_invalid_entry"] += 1
            return None, "not_applicable"
        if self._age_ns(entry, now_monotonic_ns) > max_age_ns:
            self._counts["lookup_entry_stale"] += 1
            return None, "entry_stale"
        market, _, raw_base = metadata
        reverse_key: _BucketKey = market, "sell_base", raw_base
        reverse_records = self._buckets.get(reverse_key)
        if reverse_records is None:
            same_identity_other_epoch = any(
                candidate_market[:-1] == market[:-1]
                and candidate_market[-1] != market[-1]
                and direction == "sell_base"
                and candidate_raw_base == raw_base
                for candidate_market, direction, candidate_raw_base in self._buckets
            )
            has_other_raw = bool(
                self._bucket_keys_by_market_direction.get((market, "sell_base"))
            )
            reason = (
                "source_epoch_mismatch"
                if same_identity_other_epoch
                else ("raw_base_quantity_mismatch" if has_other_raw else "missing")
            )
            self._counts[f"lookup_{reason}"] += 1
            return None, reason
        pairs: list[ExactQuotePair] = []
        stale_records = 0
        block_mismatches = 0
        for exit in reversed(reverse_records):
            if self._age_ns(exit, now_monotonic_ns) > max_age_ns:
                stale_records += 1
                continue
            quality = self._pair_quality(entry, exit)
            if quality is None:
                block_mismatches += 1
                continue
            pairing_quality, same_round, same_block = quality
            pairs.append(
                ExactQuotePair(
                    entry=entry,
                    exit=exit,
                    pairing_quality=pairing_quality,
                    same_source_epoch=True,
                    same_source_round=same_round,
                    same_block_number=same_block,
                ),
            )
        if not pairs:
            if block_mismatches:
                reason = "block_number_mismatch"
            elif stale_records:
                reason = "all_reverse_quotes_stale"
            else:
                reason = "missing"
            self._counts[f"lookup_{reason}"] += 1
            return None, reason
        result = min(pairs, key=self._quality_rank)
        self._counts["lookup_pair_matched"] += 1
        self._counts[f"lookup_pair_{result.pairing_quality}"] += 1
        return result, result.pairing_quality

    def snapshot(self) -> dict[str, object]:
        """Return bounded cache telemetry without exposing quote payloads."""

        return {
            "mode": "bounded_in_memory_exact_quote_pair_reuse",
            "max_buckets": self.max_buckets,
            "max_records_per_bucket": self.max_records_per_bucket,
            "stored_buckets": len(self._buckets),
            "stored_records": sum(len(records) for records in self._buckets.values()),
            "counts": dict(sorted(self._counts.items())),
        }
