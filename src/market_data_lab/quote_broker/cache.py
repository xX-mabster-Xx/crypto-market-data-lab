"""Quote cache with TTL-based invalidation.

Section 9.4: Cache keys include provider, chain, assets, amount, mode,
route, fee/slippage policy, state version, TTL class.
Section 9.2: Pair reuse validation per QTE-02 checks.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Sequence

from .contracts import CacheKey, QuoteResult, QuoteRequest


@dataclass
class _CacheEntry:
    result: QuoteResult
    expires_at_ns: int
    last_access_ns: int


@dataclass
class QuoteCache:
    """TTL-based quote cache with version invalidation.

    QTE-01: Fresh exact quote in cache takes priority over local simulation.
    QTE-03: Local deterministic quote invalidated on dependency change.
    """

    max_items: int = 256
    max_bytes: int = 64 * 1024 * 1024  # 64 MiB
    ttl_ms: int = 1500  # TIME-01: default research TTL

    _cache: dict[str, _CacheEntry] = field(default_factory=dict)
    _lru_order: list[str] = field(default_factory=list)
    _current_bytes: int = 0
    _hit_count: int = 0
    _miss_count: int = 0
    _eviction_count: int = 0

    @property
    def hits(self) -> int:
        return self._hit_count

    @property
    def misses(self) -> int:
        return self._miss_count

    @property
    def evictions(self) -> int:
        return self._eviction_count

    @property
    def hit_ratio(self) -> float:
        total = self._hit_count + self._miss_count
        return self._hit_count / total if total > 0 else 0.0

    def get(self, key: CacheKey) -> QuoteResult | None:
        """Retrieve a cached quote if fresh.

        QTE-02: Validate AssetId, chain, amount, state version, freshness,
        exact/approximate semantics, and all fees.
        """
        key_str = key.key_string
        entry = self._cache.get(key_str)
        if entry is None:
            self._miss_count += 1
            return None

        now_ns = int(time.monotonic_ns())
        if now_ns > entry.expires_at_ns:
            # TTL expired — evict (TIME-03)
            del self._cache[key_str]
            if key_str in self._lru_order:
                self._lru_order.remove(key_str)
            self._eviction_count += 1
            self._miss_count += 1
            return None

        # Check data quality: stale state version
        if not entry.result.is_fresh(now_ns, self.ttl_ms):
            # TIME-03: TTL expiry invalidates candidate
            del self._cache[key_str]
            if key_str in self._lru_order:
                self._lru_order.remove(key_str)
            self._eviction_count += 1
            self._miss_count += 1
            return None

        # Move to LRU position
        if key_str in self._lru_order:
            self._lru_order.remove(key_str)
        self._lru_order.append(key_str)
        entry.last_access_ns = now_ns
        self._hit_count += 1
        return entry.result

    def put(self, key: CacheKey, result: QuoteResult, ttl_ms: int | None = None) -> None:
        """Store a quote result with TTL."""
        key_str = key.key_string

        # If already cached, update
        if key_str in self._cache:
            if key_str in self._lru_order:
                self._lru_order.remove(key_str)
            self._lru_order.append(key_str)
        else:
            self._lru_order.append(key_str)

        # Enforce size caps
        self._evict_if_needed()

        ttl = (ttl_ms or self.ttl_ms) * 1_000_000
        now_ns = int(time.monotonic_ns())
        self._cache[key_str] = _CacheEntry(
            result=result,
            expires_at_ns=now_ns + ttl,
            last_access_ns=now_ns,
        )

    def invalidate(self, key: CacheKey) -> bool:
        """Invalidate a cache entry (Section 9.4: state version change)."""
        key_str = key.key_string
        if key_str in self._cache:
            del self._cache[key_str]
            if key_str in self._lru_order:
                self._lru_order.remove(key_str)
            return True
        return False

    def invalidate_by_pattern(
        self,
        provider: str | None = None,
        chain: str | None = None,
        asset_id: str | None = None,
    ) -> int:
        """Invalidate entries matching pattern.

        Per QTE-03: state change invalidates dependent quotes.
        """
        to_remove = []
        for key_str in self._cache:
            key = self._parse_key(key_str)
            if provider and key.provider != provider:
                continue
            if chain and key.chain != chain:
                continue
            if asset_id and key.input_asset_id != asset_id and key.output_asset_id != asset_id:
                continue
            to_remove.append(key_str)

        for key_str in to_remove:
            del self._cache[key_str]
            if key_str in self._lru_order:
                self._lru_order.remove(key_str)
            self._eviction_count += 1

        return len(to_remove)

    def _parse_key(self, key_str: str) -> CacheKey:
        parts = key_str.split("|")
        return CacheKey(
            provider=parts[0],
            chain=parts[1],
            input_asset_id=parts[2],
            output_asset_id=parts[3],
            amount_raw=int(parts[4]),
            mode=parts[5],  # type: ignore
            route_restrictions=tuple(parts[6].split(",") if parts[6] else []),
            fee_policy=parts[7],
            slippage_policy=parts[8],
            pinned_block=parts[9] if parts[9] != "None" else None,
            state_version=int(parts[10]),
        )

    def _evict_if_needed(self) -> None:
        """Evict LRU entries if over capacity."""
        while (len(self._cache) > self.max_items or self._current_bytes > self.max_bytes):
            if not self._lru_order:
                break
            key_str = self._lru_order.pop(0)
            del self._cache[key_str]
            self._eviction_count += 1

    def try_pair_reuse(
        self,
        forward_key: CacheKey,
        reverse_key: CacheKey,
        tolerance_raw: int = 0,
    ) -> tuple[QuoteResult | None, QuoteResult | None, bool]:
        """Check if a cached forward/reverse pair can be reused.

        QTE-02: Validate:
        1. AssetId, chain, provider policy, raw amounts
        2. Both sides fresh at decision time
        3. State versions match
        4. Exact/approximate/indicative semantics and all fees
        5. Spot-size matches hedge or explicit residual
        """
        forward = self.get(forward_key)
        reverse = self.get(reverse_key)

        if forward is None or reverse is None:
            return None, None, False

        # Validate all QTE-02 checks
        if not forward.is_compatible_with(reverse, tolerance_raw):
            return forward, reverse, False

        return forward, reverse, True

    def stats(self) -> dict[str, int | float]:
        """Return cache statistics."""
        return {
            "items": len(self._cache),
            "hits": self._hit_count,
            "misses": self._miss_count,
            "evictions": self._eviction_count,
            "hit_ratio": self.hit_ratio,
        }
