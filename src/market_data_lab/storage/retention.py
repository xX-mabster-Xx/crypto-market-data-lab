from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Sequence

from .contracts import DataDomain, RetentionPolicy


@dataclass
class RetentionManager:
    """Enforce retention policies across data domains."""

    policy: RetentionPolicy
    evidence_dir: Path | None = None

    def should_evict_ram(self, timestamp_ns: int) -> bool:
        cutoff = time.monotonic_ns() - int(self.policy.ram_history_seconds * 1_000_000_000)
        return timestamp_ns < cutoff

    def should_evict_evidence(self, created_timestamp_s: int) -> bool:
        cutoff = time.time() - self.policy.ordinary_evidence_ttl_seconds
        return created_timestamp_s < cutoff

    def cleanup_evidence_directory(self, protected_ids: Sequence[str] = ()) -> int:
        """Remove expired evidence. Returns removed count."""
        if self.evidence_dir is None or not self.evidence_dir.exists():
            return 0

        protected = set(protected_ids)
        removed = 0

        for bundle_dir in self.evidence_dir.iterdir():
            if not bundle_dir.is_dir():
                continue
            if bundle_dir.name in protected:
                continue

            manifest_path = bundle_dir / "manifest.json"
            if not manifest_path.exists():
                continue

            try:
                import json
                with open(manifest_path, "r", encoding="utf-8") as fh:
                    manifest = json.load(fh)
                created_at = manifest.get("created_at_s", 0)
                if self.should_evict_evidence(created_at):
                    import shutil
                    shutil.rmtree(bundle_dir)
                    removed += 1
            except OSError:
                continue

        return removed
