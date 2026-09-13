"""Storage manager for evidence bundles and journal.

Section 15: STORE-01 to STORE-04.
Compact journal with segment rotation.
Evidence artifacts with retention.
STORE-01: Hot RAM, STORE-02: Compact journal, STORE-03: Evidence artifacts.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from ..storage.journal import CompactJournal
from ..storage.contracts import JournalEntry, DataDomain
from ..storage.retention import RetentionManager, RetentionPolicy
from ..storage.replay import ReplayManager
from .evidence import EvidenceBundle


@dataclass
class StorageManager:
    """Manages all storage domains per STORE-01 to STORE-04.

    STORE-01: Hot RAM (bounded)
    STORE-02: Compact journal (append-only JSONL)
    STORE-03: Evidence artifacts (exact quotes, book levels, pool inputs)
    """

    journal_dir: Path
    evidence_dir: Path
    retention_policy: RetentionPolicy = field(default_factory=RetentionPolicy)

    def __post_init__(self) -> None:
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self._journal = CompactJournal(
            journal_dir=self.journal_dir,
            max_segment_bytes=self.retention_policy.journal_segment_bytes,
            max_total_bytes=self.retention_policy.journal_and_evidence_bytes_cap,
        )
        self._retention = RetentionManager(policy=self.retention_policy, evidence_dir=self.evidence_dir)
        self._replay = ReplayManager()

    def store_evidence_bundle(self, bundle: EvidenceBundle) -> Path:
        """Store an evidence bundle as an artifact (STORE-03).

        Bundle + experiment contain schema version, code/content hash,
        model version, config hash, provider spec revision, determinism seed.
        """
        bundle_dir = self.evidence_dir / bundle.bundle_id
        bundle_dir.mkdir(parents=True, exist_ok=True)

        # Write manifest (STORE-04)
        manifest = {
            "bundle_id": bundle.bundle_id,
            "schema_version": bundle.schema_version,
            "evidence_level": "standard",
            "code_hash": bundle.code_hash,
            "model_version": bundle.model_version,
            "config_hash": bundle.config_hash,
            "provider_spec_revision": bundle.provider_spec_revision,
            "determinism_seed": bundle.determinism_seed,
            "content_hash": bundle.content_hash,
            "created_at_s": int(time.time()),
            "constraints": bundle.constraints,
            "sampling_policy": {
                "includes_negative_controls": False,
                "includes_random_controls": False,
            },
        }

        manifest_path = bundle_dir / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        # Write full bundle data
        bundle_path = bundle_dir / "bundle.json"
        bundle.save_to_file(str(bundle_path))

        # Write journal entry
        entry = JournalEntry(
            entry_id=f"evidence-{bundle.bundle_id}",
            timestamp_ns=int(time.monotonic_ns()),
            domain="evidence",
            event_type="bundle_stored",
            payload={
                "bundle_id": bundle.bundle_id,
                "strategy_id": bundle.strategy_id,
                "candidate_id": bundle.candidate_id,
            },
        )
        self._journal.append(entry)

        return bundle_dir

    def get_evidence_bundle(self, bundle_id: str) -> EvidenceBundle | None:
        """Load an evidence bundle by ID."""
        bundle_dir = self.evidence_dir / bundle_id
        bundle_path = bundle_dir / "bundle.json"
        if not bundle_path.exists():
            return None
        return EvidenceBundle.load_from_file(str(bundle_path))

    def store_decision(
        self,
        candidate_id: str,
        decision: dict,
        effective_time_ns: int,
    ) -> None:
        """Store a decision in the journal (STORE-02)."""
        entry = JournalEntry(
            entry_id=f"decision-{candidate_id}",
            timestamp_ns=effective_time_ns,
            domain="lifecycle",
            event_type="decision",
            payload=decision,
        )
        self._journal.append(entry)

    def record_funding_event(
        self,
        funding_event_id: str,
        payload: dict,
        effective_time_ns: int,
    ) -> None:
        """Record a funding event in the journal (STORE-02, Section 5)."""
        entry = JournalEntry(
            entry_id=f"funding-{funding_event_id}",
            timestamp_ns=effective_time_ns,
            domain="lifecycle",
            event_type="funding_event",
            payload=payload,
        )
        self._journal.append(entry)

    def add_replay_bundle(self, bundle_id: str, bundle_data: dict) -> None:
        """Add bundle for decision_replay (Section 15.3)."""
        self._replay.add_bundle(bundle_id, bundle_data)

    def replay_decision(self, bundle_id: str):
        """Replay a decision from saved EvidenceBundle."""
        return self._replay.decision_replay(bundle_id)

    def cleanup_expired(self, protected_ids: Sequence[str] = ()) -> int:
        """Clean up expired evidence and old journal segments."""
        removed_evidence = self._retention.cleanup_evidence_directory(protected_ids)
        removed_segments = self._journal.cleanup_old_segments()
        return removed_evidence + removed_segments

    def journal_budget(self) -> dict:
        """Return journal storage budget status."""
        return self._journal.budget.as_dict()
