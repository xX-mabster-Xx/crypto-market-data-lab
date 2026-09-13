from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence


@dataclass(frozen=True, slots=True)
class SearchConfig:
    dirty_group_coalesce_ms: int = 25
    shortlist_per_group_and_horizon: int = 16
    detailed_plans_per_group: int = 4
    global_pending_plan_cap: int = 128
    notional_grid_numeraire: tuple[int, ...] = (25, 50, 100, 250, 500, 1000)
    horizons_hours: tuple[int, ...] = (1, 4, 8, 24)
    include_funding_event_horizons: bool = True
    exploration_enabled: bool = True


@dataclass(frozen=True, slots=True)
class QuoteConfig:
    max_remote_requests_per_verification: int = 6
    max_pending_requests: int = 256
    priority_shares: Mapping[str, float] = field(default_factory=lambda: {
        "exit": 0.30, "verification": 0.50, "exploration": 0.10, "maintenance": 0.10
    })


@dataclass(frozen=True, slots=True)
class DataQualityConfig:
    max_execution_book_age_ms: int = 500
    max_remote_quote_age_ms: int = 1500
    max_leg_receive_skew_ms: int = 1000
    source_overrides_require_reason: bool = True
    unknown_contract_model: str = "exclude_from_verified"
    unknown_funding_semantics: str = "exclude_from_carry_verified"
    stablecoin_parity_assumption: bool = False


@dataclass(frozen=True, slots=True)
class PortfolioConfig:
    context: str = "hypothetical"
    allow_unconfirmed_borrow: bool = False
    residue_policy: str = "bounded_residual"


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    raw_ticks: bool = False
    ram_history_seconds: float = 180.0
    ram_history_bytes_cap: int = 134217728
    journal_segment_bytes: int = 8388608
    journal_and_evidence_bytes_cap: int = 1073741824
    ordinary_evidence_ttl_days: int = 7
    rich_status_flush_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    """W1/W2 benchmark profiles."""

    name: str = "W1"
    logical_cpus: int = 4
    ram_gib: int = 8
    instruments: int = 200
    pools: int = 200
    updates_per_second: int = 5000
    burst_updates_per_second: int = 20000
    burst_duration_seconds: int = 10

    @property
    def target_p95_screen_ms(self) -> float:
        return 50.0

    @property
    def target_p99_screen_ms(self) -> float:
        return 200.0

    @property
    def target_event_loop_lag_p99_ms(self) -> float:
        return 50.0

    @property
    def target_burst_recovery_seconds(self) -> float:
        return 30.0


@dataclass(frozen=True, slots=True)
class InitialConfig:
    """Complete initial configuration."""

    mode: str = "research"
    execution_enabled: bool = False
    search: SearchConfig = field(default_factory=SearchConfig)
    quotes: QuoteConfig = field(default_factory=QuoteConfig)
    data_quality: DataQualityConfig = field(default_factory=DataQualityConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
