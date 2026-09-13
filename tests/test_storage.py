from __future__ import annotations

import tempfile
import time
import unittest
from decimal import Decimal
from pathlib import Path

from market_data_lab.storage import (
    BundleManifest,
    CompactJournal,
    DataDomain,
    EvidenceLevel,
    JournalEntry,
    ReplayManager,
    RetentionManager,
    RetentionPolicy,
    StorageBudget,
)


class StorageBudgetTest(unittest.TestCase):
    def test_budget_not_full(self) -> None:
        budget = StorageBudget(journal_used_bytes=100, journal_total_bytes=1000)
        self.assertFalse(budget.journal_full)

    def test_budget_full(self) -> None:
        budget = StorageBudget(journal_used_bytes=1000, journal_total_bytes=1000)
        self.assertTrue(budget.journal_full)


class BundleManifestTest(unittest.TestCase):
    def test_as_dict(self) -> None:
        manifest = BundleManifest(
            bundle_id="test-bundle",
            schema_version=2,
            evidence_level="standard",
            code_hash="abc123",
        )
        result = manifest.as_dict()
        self.assertEqual(result["bundle_id"], "test-bundle")
        self.assertEqual(result["schema_version"], 2)


class JournalEntryTest(unittest.TestCase):
    def test_as_dict(self) -> None:
        entry = JournalEntry(
            entry_id="e1",
            timestamp_ns=1000,
            domain="market",
            event_type="quote_update",
            payload={"price": 100},
        )
        result = entry.as_dict()
        self.assertEqual(result["entry_id"], "e1")
        self.assertEqual(result["domain"], "market")


class CompactJournalTest(unittest.TestCase):
    def test_append_and_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = CompactJournal(
                journal_dir=Path(tmpdir),
                max_segment_bytes=1024,
                max_total_bytes=10240,
            )
            entry = JournalEntry(
                entry_id="e1",
                timestamp_ns=1000,
                domain="market",
                event_type="test",
                payload={"data": 1},
            )
            result = journal.append(entry)
            self.assertIsNotNone(result)

            entries = list(journal.read_all())
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0]["entry_id"], "e1")

    def test_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = CompactJournal(
                journal_dir=Path(tmpdir),
                max_segment_bytes=200,
                max_total_bytes=10000,
            )
            for i in range(20):
                entry = JournalEntry(
                    entry_id=f"e{i}",
                    timestamp_ns=i * 1000,
                    domain="market",
                    event_type="test",
                    payload={"data": i},
                )
                journal.append(entry)

            self.assertGreater(len(journal._segments), 1)

    def test_budget_full_rejects_append(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = CompactJournal(
                journal_dir=Path(tmpdir),
                max_segment_bytes=1024,
                max_total_bytes=50,
            )
            # Fill up the journal
            big_payload = {"data": "x" * 100}
            for i in range(100):
                entry = JournalEntry(
                    entry_id=f"e{i}",
                    timestamp_ns=i * 1000,
                    domain="market",
                    event_type="test",
                    payload=big_payload,
                )
                result = journal.append(entry)
                if result is None:
                    break

            entry = JournalEntry(
                entry_id="e1",
                timestamp_ns=1000,
                domain="market",
                event_type="test",
                payload={"data": 1},
            )
            result = journal.append(entry)
            self.assertIsNone(result)

    def test_cleanup_old_segments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            journal = CompactJournal(
                journal_dir=Path(tmpdir),
                max_segment_bytes=100,
                max_total_bytes=100000,
            )
            for i in range(10):
                entry = JournalEntry(
                    entry_id=f"e{i}",
                    timestamp_ns=i * 1000,
                    domain="market",
                    event_type="test",
                    payload={"data": i},
                )
                journal.append(entry)

            removed = journal.cleanup_old_segments(keep_last_n=3)
            self.assertGreater(removed, 0)


class RetentionManagerTest(unittest.TestCase):
    def test_should_evict_ram(self) -> None:
        policy = RetentionPolicy(ram_history_seconds=60.0)
        manager = RetentionManager(policy=policy)

        old_timestamp = time.monotonic_ns() - 120_000_000_000
        self.assertTrue(manager.should_evict_ram(old_timestamp))

        recent_timestamp = time.monotonic_ns()
        self.assertFalse(manager.should_evict_ram(recent_timestamp))

    def test_should_evict_evidence(self) -> None:
        policy = RetentionPolicy(ordinary_evidence_ttl_seconds=86400)
        manager = RetentionManager(policy=policy)

        old_timestamp = time.time() - 172800
        self.assertTrue(manager.should_evict_evidence(old_timestamp))

        recent_timestamp = time.time()
        self.assertFalse(manager.should_evict_evidence(recent_timestamp))


class ReplayManagerTest(unittest.TestCase):
    def test_decision_replay_success(self) -> None:
        manager = ReplayManager(
            evidence_bundles={"bundle-1": {"schema_version": 2, "request": {}, "result": {}}},
        )
        result = manager.decision_replay("bundle-1")
        self.assertTrue(result.success)
        self.assertEqual(result.mode, "decision_replay")

    def test_decision_replay_not_found(self) -> None:
        manager = ReplayManager()
        result = manager.decision_replay("nonexistent")
        self.assertFalse(result.success)

    def test_path_replay_success(self) -> None:
        manager = ReplayManager(
            path_events=[
                {"path_id": "path-1", "event": "e1"},
                {"path_id": "path-1", "event": "e2"},
            ],
        )
        result = manager.path_replay("path-1")
        self.assertTrue(result.success)
        self.assertIn("2 events", result.messages[0])

    def test_path_replay_not_found(self) -> None:
        manager = ReplayManager()
        result = manager.path_replay("nonexistent")
        self.assertFalse(result.success)

    def test_add_bundle(self) -> None:
        manager = ReplayManager()
        manager.add_bundle("bundle-1", {"schema_version": 2})
        result = manager.decision_replay("bundle-1")
        self.assertTrue(result.success)

    def test_add_path_event(self) -> None:
        manager = ReplayManager()
        manager.add_path_event({"path_id": "path-1", "event": "e1"})
        result = manager.path_replay("path-1")
        self.assertTrue(result.success)


if __name__ == "__main__":
    unittest.main()
