from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Mapping, Sequence

from .contracts import JournalEntry, JournalSegment, StorageBudget


@dataclass
class CompactJournal:
    """Append-only journal with bounded segments and rotation."""

    journal_dir: Path
    max_segment_bytes: int = 8 * 1024 * 1024
    max_total_bytes: int = 1024 * 1024 * 1024

    def __post_init__(self) -> None:
        self.journal_dir.mkdir(parents=True, exist_ok=True)
        self._current_segment: JournalSegment | None = None
        self._sequence = 0
        self._segments: list[str] = []

    @property
    def total_bytes(self) -> int:
        return sum(
            (self.journal_dir / seg).stat().st_size
            for seg in self._segments
            if (self.journal_dir / seg).exists()
        )

    @property
    def budget(self) -> StorageBudget:
        return StorageBudget(
            journal_used_bytes=self.total_bytes,
            journal_total_bytes=self.max_total_bytes,
        )

    def append(self, entry: JournalEntry) -> str | None:
        """Append entry to journal. Returns segment ID if written."""
        if self.budget.journal_full or not self._can_append(entry):
            return None

        if self._current_segment is None or self._current_segment.byte_size >= self.max_segment_bytes:
            self._rotate_segment()

        segment = self._current_segment
        line = json.dumps(entry.as_dict()) + "\n"
        line_bytes = len(line.encode("utf-8"))

        file_path = self.journal_dir / segment.segment_id
        with open(file_path, "a", encoding="utf-8") as fh:
            fh.write(line)

        segment.entries.append(entry.as_dict())
        segment.byte_size += line_bytes
        return segment.segment_id

    def _rotate_segment(self) -> None:
        self._sequence += 1
        segment_id = f"journal-{self._sequence:06d}.jsonl"
        self._current_segment = JournalSegment(segment_id=segment_id)
        self._segments.append(segment_id)

    def _can_append(self, entry: JournalEntry) -> bool:
        line = json.dumps(entry.as_dict()) + "\n"
        projected_size = self.total_bytes + len(line.encode("utf-8"))
        return projected_size <= self.max_total_bytes

    def read_all(self) -> Iterator[dict]:
        for segment_id in self._segments:
            file_path = self.journal_dir / segment_id
            if not file_path.exists():
                continue
            with open(file_path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        yield json.loads(line)

    def cleanup_old_segments(self, keep_last_n: int = 10) -> int:
        """Remove old segments beyond keep_last_n. Returns removed count."""
        removed = 0
        while len(self._segments) > keep_last_n:
            old_segment = self._segments.pop(0)
            old_path = self.journal_dir / old_segment
            if old_path.exists():
                old_path.unlink()
                removed += 1
        return removed
