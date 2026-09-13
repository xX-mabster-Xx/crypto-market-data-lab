from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from .contracts import ReplayMode


@dataclass
class ReplayResult:
    mode: ReplayMode
    success: bool
    bundle_id: str
    messages: list[str]


class ReplayManager:
    """Manage decision_replay and path_replay modes."""

    def __init__(
        self,
        evidence_bundles: Mapping[str, Mapping] | None = None,
        path_events: Sequence[dict] | None = None,
    ) -> None:
        self._bundles = dict(evidence_bundles) if evidence_bundles else {}
        self._path_events = list(path_events) if path_events else []

    def decision_replay(self, bundle_id: str) -> ReplayResult:
        """Replay a decision from a saved EvidenceBundle."""
        bundle = self._bundles.get(bundle_id)
        if bundle is None:
            return ReplayResult(
                mode="decision_replay",
                success=False,
                bundle_id=bundle_id,
                messages=[f"bundle not found: {bundle_id}"],
            )

        messages = [
            f"replaying bundle: {bundle_id}",
            f"schema_version: {bundle.get('schema_version', 'unknown')}",
        ]

        if "request" in bundle:
            messages.append("request data present")
        if "result" in bundle:
            messages.append("result data present")

        return ReplayResult(
            mode="decision_replay",
            success=True,
            bundle_id=bundle_id,
            messages=messages,
        )

    def path_replay(self, path_id: str) -> ReplayResult:
        """Replay a path from recorded market events."""
        events = [e for e in self._path_events if e.get("path_id") == path_id]
        if not events:
            return ReplayResult(
                mode="path_replay",
                success=False,
                bundle_id=path_id,
                messages=[f"no events found for path: {path_id}"],
            )

        return ReplayResult(
            mode="path_replay",
            success=True,
            bundle_id=path_id,
            messages=[f"replaying {len(events)} events"],
        )

    def add_bundle(self, bundle_id: str, bundle: dict) -> None:
        self._bundles[bundle_id] = bundle

    def add_path_event(self, event: dict) -> None:
        self._path_events.append(event)
