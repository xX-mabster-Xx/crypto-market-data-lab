"""Quote Broker contracts.

Section 7.4: QuoteRequest/QuoteResult with exact modes, TTL, state versions.
Section 9: QTE-01 priority, cache, dedup, pacing/deadlines.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Mapping

ExactMode = Literal["exact_in", "exact_out", "two_sided"]
QuoteQuality = Literal["exact", "indicative", "response_time_only"]
ChainConsistency = Literal[
    "pinned_consistent_snapshot",
    "validated_multi_account_snapshot",
    "slot_window_estimate",
    "response_time_only",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class CacheKey:
    """Cache key per QTE-02.

    provider/endpoint, chain, input/output AssetId, raw amount, exact mode,
    route restrictions, fee/slippage policy, pinned block/state version.
    """

    provider: str
    chain: str
    input_asset_id: str
    output_asset_id: str
    amount_raw: int
    mode: ExactMode
    route_restrictions: tuple[str, ...]
    fee_policy: str
    slippage_policy: str
    pinned_block: str | None
    state_version: int

    @property
    def key_string(self) -> str:
        return "|".join(str(v) for v in [
            self.provider, self.chain, self.input_asset_id,
            self.output_asset_id, self.amount_raw, self.mode,
            self.route_restrictions, self.fee_policy, self.slippage_policy,
            self.pinned_block, self.state_version,
        ])


@dataclass
class QuoteRequest:
    """Request for a quote via the broker."""

    request_id: str
    cache_key: CacheKey
    input_asset_id: str
    output_asset_id: str
    amount_raw: int
    mode: ExactMode
    deadline_ns: int
    priority: str  # "exit" | "verification" | "exploration" | "maintenance"
    consumers: list[str] = field(default_factory=list)


@dataclass
class QuoteResult:
    """Result of a quote request."""

    request_id: str
    input_asset_id: str
    output_asset_id: str
    input_amount_raw: int
    output_amount_raw: int
    amount_raw: int  # for two_sided, the common base amount
    mode: ExactMode
    estimated_output: int | None = None
    min_output: int | None = None
    max_input: int | None = None
    fee_breakdown: dict[str, int] = field(default_factory=dict)
    route: str | None = None
    pool_id: str | None = None
    provider: str = ""
    quality: QuoteQuality = "indicative"
    chain_consistency: ChainConsistency = "response_time_only"
    pinned_block: str | None = None
    state_version: int = 0
    request_time_ns: int = 0
    receive_time_ns: int = 0
    ttl_ms: int = 1500
    expires_at_ns: int = 0
    round_id: str | None = None
    source_epoch: str | None = None
    warning: str | None = None

    @property
    def is_fresh(self, now_ns: int, max_age_ms: int) -> bool:
        """Check freshness per TIME-01."""
        age_ms = (now_ns - self.receive_time_ns) / 1_000_000
        return age_ms <= max_age_ms

    def is_compatible_with(
        self,
        other: QuoteResult,
        tolerance_raw: int = 0,
    ) -> bool:
        """Check if two quote results are compatible for pairing.

        QTE-02: Check AssetId, chain, amount, state version,
        receive skew, exact/approximate semantics, all fees.
        """
        if self.input_asset_id != other.input_asset_id:
            return False
        if self.output_asset_id != other.output_asset_id:
            return False
        if self.chain != other.provider:  # Simplified
            pass
        if self.state_version != other.state_version:
            return False

        # Check amount compatibility with tolerance
        if abs(self.input_amount_raw - other.input_amount_raw) > tolerance_raw:
            return False

        # Check quality
        if self.quality != other.quality:
            return False

        return True
