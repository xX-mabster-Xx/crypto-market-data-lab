from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Mapping, Sequence

DataDomain = Literal["market", "lifecycle", "evidence", "journal", "status"]
EvidenceLevel = Literal["minimal", "standard", "extended"]
ReplayMode = Literal["decision_replay", "path_replay"]


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Retention configuration for data domains."""

    ram_history_seconds: float = 180.0
    ram_history_bytes_cap: int = 134217728
    journal_segment_bytes: int = 8388608
    journal_total_bytes_cap: int = 1073741824
    ordinary_evidence_ttl_seconds: int = 604800
    rich_status_flush_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class StorageBudget:
    """Current storage budget status."""

    journal_used_bytes: int = 0
    journal_total_bytes: int = 0
    evidence_used_bytes: int = 0
    evidence_total_bytes: int = 0
    ram_used_bytes: int = 0
    ram_total_bytes: int = 0

    @property
    def journal_full(self) -> bool:
        return self.journal_used_bytes >= self.journal_total_bytes

    @property
    def ram_full(self) -> bool:
        return self.ram_used_bytes >= self.ram_total_bytes


@dataclass(frozen=True, slots=True)
class BundleManifest:
    schema_version: int = 2
    bundle_id: str = ""
    evidence_level: EvidenceLevel = "standard"
    code_hash: str | None = None
    model_version: str | None = None
    config_hash: str | None = None
    provider_spec_revision: str | None = None
    determinism_seed: str | None = None
    content_hash: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "bundle_id": self.bundle_id,
            "evidence_level": self.evidence_level,
            "code_hash": self.code_hash,
            "model_version": self.model_version,
            "config_hash": self.config_hash,
            "provider_spec_revision": self.provider_spec_revision,
            "determinism_seed": self.determinism_seed,
            "content_hash": self.content_hash,
        }


@dataclass
class JournalSegment:
    segment_id: str
    entries: list[dict] = field(default_factory=list)
    byte_size: int = 0


@dataclass
class JournalEntry:
    entry_id: str
    timestamp_ns: int
    domain: DataDomain
    event_type: str
    payload: dict

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_id": self.entry_id,
            "timestamp_ns": self.timestamp_ns,
            "domain": self.domain,
            "event_type": self.event_type,
            "payload": self.payload,
        }
