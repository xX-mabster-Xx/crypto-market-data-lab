"""Versioned event/state contracts for the read-only strategy engine.

This module owns no collectors and performs no I/O.  It provides the M1
semantics that strategy consumers need before they can call a group of latest
objects a coherent evidence bundle:

* every event belongs to a source epoch and one local boot;
* a reconnect invalidates untouched state from the prior source epoch;
* an older sequence/receipt cannot overwrite a newer state;
* expiry changes the state version instead of silently leaving a verified
  result active; and
* dependency notifications coalesce, while a key dirtied after a group was
  taken is retained for the next pass.

The store is intentionally in-memory.  Ledger/funding events that must survive
a crash require the separate durable channel described by the technical spec;
they must not be reduced to latest-wins state here.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any, Literal


IntegrityStatus = Literal["valid", "invalid"]
PutStatus = Literal[
    "accepted",
    "duplicate_event",
    "duplicate_source_sequence",
    "stale_source_epoch",
    "out_of_order_source_sequence",
    "out_of_order_receipt",
    "boot_id_mismatch",
]


def _require_text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class EventEnvelope:
    """Normalized version/provenance wrapper around one typed state payload."""

    event_id: str
    event_type: str
    schema_version: int
    source_id: str
    source_epoch: int
    instrument_or_pool_id: str
    received_realtime_ns: int
    received_monotonic_ns: int
    boot_id: str
    payload: Any
    spec_version: str | None = None
    exchange_event_time_ns: int | None = None
    exchange_event_time_semantics: str | None = None
    source_sequence: int | None = None
    block_number: int | None = None
    block_hash: str | None = None
    slot: int | None = None
    write_version: int | None = None
    quality_flags: tuple[str, ...] = ()
    provenance: str | None = None
    diagnostic: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "event_id",
            "event_type",
            "source_id",
            "instrument_or_pool_id",
            "boot_id",
        ):
            _require_text(getattr(self, name), name)
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")
        if self.source_epoch < 0:
            raise ValueError("source_epoch must be non-negative")
        if self.received_realtime_ns <= 0 or self.received_monotonic_ns <= 0:
            raise ValueError("receive timestamps must be positive")
        for name in ("source_sequence", "block_number", "slot", "write_version"):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative when present")
        if any(not isinstance(flag, str) or not flag for flag in self.quality_flags):
            raise ValueError("quality_flags must contain non-empty strings")
        if self.diagnostic is not None and len(self.diagnostic) > 512:
            raise ValueError("diagnostic must be at most 512 characters")

    @property
    def state_key(self) -> str:
        return self.instrument_or_pool_id


@dataclass(frozen=True, slots=True)
class VersionedStateRecord:
    state_key: str
    state_version: int
    event: EventEnvelope
    integrity: IntegrityStatus
    invalid_reason: str | None
    valid_until_monotonic_ns: int | None

    def unusable_reason(self, *, now_monotonic_ns: int, boot_id: str) -> str | None:
        if self.event.boot_id != boot_id:
            return "boot_id_mismatch"
        if self.integrity != "valid":
            return self.invalid_reason or "state_invalid"
        if (
            self.valid_until_monotonic_ns is not None
            and now_monotonic_ns >= self.valid_until_monotonic_ns
        ):
            return "state_stale"
        return None

    def age_ms(self, *, now_monotonic_ns: int, boot_id: str) -> float | None:
        if self.event.boot_id != boot_id:
            return None
        return max(
            0.0,
            (now_monotonic_ns - self.event.received_monotonic_ns) / 1_000_000,
        )


@dataclass(frozen=True, slots=True)
class PutResult:
    accepted: bool
    status: PutStatus
    record: VersionedStateRecord | None
    invalidated_keys: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StateView:
    """Immutable multi-key read with an explicit version vector."""

    requested_keys: tuple[str, ...]
    records: Mapping[str, VersionedStateRecord]
    version_vector: tuple[tuple[str, int], ...]
    missing_keys: tuple[str, ...]
    unusable_reasons: Mapping[str, str]
    consistent: bool


class VersionedMarketState:
    """Latest immutable state with source-epoch and TTL invalidation."""

    def __init__(self, *, boot_id: str) -> None:
        _require_text(boot_id, "boot_id")
        self.boot_id = boot_id
        self._records: dict[str, VersionedStateRecord] = {}
        self._source_epochs: dict[str, int] = {}
        self._keys_by_source: dict[str, set[str]] = defaultdict(set)
        self._next_version = 1
        self._counts: Counter[str] = Counter()

    def _version(self) -> int:
        value = self._next_version
        self._next_version += 1
        return value

    def advance_source_epoch(self, source_id: str, source_epoch: int) -> tuple[str, ...]:
        """Invalidate prior-epoch state before a reconnected source publishes."""

        _require_text(source_id, "source_id")
        if source_epoch < 0:
            raise ValueError("source_epoch must be non-negative")
        current = self._source_epochs.get(source_id)
        if current is not None and source_epoch < current:
            raise ValueError("source epoch cannot move backwards")
        if current == source_epoch:
            return ()
        self._source_epochs[source_id] = source_epoch
        invalidated: list[str] = []
        for key in sorted(self._keys_by_source.get(source_id, ())):
            record = self._records.get(key)
            if (
                record is None
                or record.event.source_id != source_id
                or record.event.source_epoch >= source_epoch
            ):
                continue
            self._records[key] = replace(
                record,
                state_version=self._version(),
                integrity="invalid",
                invalid_reason="source_epoch_advanced",
            )
            invalidated.append(key)
        self._counts["source_epoch_advances"] += 1
        self._counts["states_invalidated_by_source_epoch"] += len(invalidated)
        return tuple(invalidated)

    def put(
        self,
        event: EventEnvelope,
        *,
        ttl_ns: int | None,
        integrity: IntegrityStatus = "valid",
        invalid_reason: str | None = None,
    ) -> PutResult:
        if ttl_ns is not None and ttl_ns <= 0:
            raise ValueError("ttl_ns must be positive when present")
        if integrity not in {"valid", "invalid"}:
            raise ValueError("integrity must be valid or invalid")
        if integrity == "invalid" and not invalid_reason:
            raise ValueError("invalid state needs an invalid_reason")
        if event.boot_id != self.boot_id:
            self._counts["boot_id_mismatch"] += 1
            return PutResult(False, "boot_id_mismatch", None)

        known_epoch = self._source_epochs.get(event.source_id)
        if known_epoch is not None and event.source_epoch < known_epoch:
            self._counts["stale_source_epoch"] += 1
            return PutResult(False, "stale_source_epoch", None)
        invalidated = (
            self.advance_source_epoch(event.source_id, event.source_epoch)
            if known_epoch is None or event.source_epoch > known_epoch
            else ()
        )

        current = self._records.get(event.state_key)
        if current is not None:
            previous = current.event
            if event.event_id == previous.event_id:
                self._counts["duplicate_event"] += 1
                return PutResult(False, "duplicate_event", current, invalidated)
            if (
                event.source_id == previous.source_id
                and event.source_epoch == previous.source_epoch
            ):
                if event.source_sequence is not None and previous.source_sequence is not None:
                    if event.source_sequence == previous.source_sequence:
                        self._counts["duplicate_source_sequence"] += 1
                        return PutResult(
                            False,
                            "duplicate_source_sequence",
                            current,
                            invalidated,
                        )
                    if event.source_sequence < previous.source_sequence:
                        self._counts["out_of_order_source_sequence"] += 1
                        return PutResult(
                            False,
                            "out_of_order_source_sequence",
                            current,
                            invalidated,
                        )
                elif (
                    event.received_monotonic_ns < previous.received_monotonic_ns
                    or (
                        event.received_monotonic_ns == previous.received_monotonic_ns
                        and event.received_realtime_ns <= previous.received_realtime_ns
                    )
                ):
                    self._counts["out_of_order_receipt"] += 1
                    return PutResult(False, "out_of_order_receipt", current, invalidated)

        record = VersionedStateRecord(
            state_key=event.state_key,
            state_version=self._version(),
            event=event,
            integrity=integrity,
            invalid_reason=invalid_reason,
            valid_until_monotonic_ns=(
                event.received_monotonic_ns + ttl_ns if ttl_ns is not None else None
            ),
        )
        if current is not None and current.event.source_id != event.source_id:
            self._keys_by_source[current.event.source_id].discard(event.state_key)
        self._records[event.state_key] = record
        self._keys_by_source[event.source_id].add(event.state_key)
        self._counts["accepted"] += 1
        if integrity == "invalid":
            self._counts["invalid_states_accepted"] += 1
        return PutResult(True, "accepted", record, invalidated)

    def expire(self, *, now_monotonic_ns: int) -> tuple[str, ...]:
        """Version and invalidate all states whose monotonic TTL elapsed."""

        expired: list[str] = []
        for key, record in tuple(self._records.items()):
            if (
                record.integrity == "valid"
                and record.valid_until_monotonic_ns is not None
                and now_monotonic_ns >= record.valid_until_monotonic_ns
            ):
                self._records[key] = replace(
                    record,
                    state_version=self._version(),
                    integrity="invalid",
                    invalid_reason="state_stale",
                )
                expired.append(key)
        self._counts["states_expired"] += len(expired)
        return tuple(sorted(expired))

    def retire(self, state_key: str) -> bool:
        """Physically remove a state key and clean up all index structures."""

        record = self._records.pop(state_key, None)
        if record is None:
            return False
        source_id = record.event.source_id
        keys = self._keys_by_source.get(source_id)
        if keys is not None:
            keys.discard(state_key)
            if not keys:
                self._keys_by_source.pop(source_id, None)
        self._counts["retired"] += 1
        return True

    def latest(self, state_key: str) -> VersionedStateRecord | None:
        return self._records.get(state_key)

    def view(self, state_keys: Iterable[str], *, now_monotonic_ns: int) -> StateView:
        requested = tuple(dict.fromkeys(state_keys))
        records: dict[str, VersionedStateRecord] = {}
        missing: list[str] = []
        unusable: dict[str, str] = {}
        for key in requested:
            record = self._records.get(key)
            if record is None:
                missing.append(key)
                continue
            records[key] = record
            reason = record.unusable_reason(
                now_monotonic_ns=now_monotonic_ns,
                boot_id=self.boot_id,
            )
            if reason is not None:
                unusable[key] = reason
        vector = tuple(sorted((key, record.state_version) for key, record in records.items()))
        return StateView(
            requested_keys=requested,
            records=MappingProxyType(records),
            version_vector=vector,
            missing_keys=tuple(missing),
            unusable_reasons=MappingProxyType(unusable),
            consistent=not missing and not unusable,
        )

    def snapshot(self) -> dict[str, object]:
        valid = sum(record.integrity == "valid" for record in self._records.values())
        return {
            "schema_version": 1,
            "boot_id": self.boot_id,
            "states": len(self._records),
            "valid_states_before_ttl_check": valid,
            "invalid_states": len(self._records) - valid,
            "sources": len(self._source_epochs),
            "source_epochs": dict(sorted(self._source_epochs.items())),
            "next_state_version": self._next_version,
            "counts": dict(sorted(self._counts.items())),
        }


class DependencyIndex:
    """Many-to-many dirty-group index with bounded coalescing drains."""

    def __init__(self) -> None:
        self._groups_by_dependency: dict[str, set[str]] = defaultdict(set)
        self._dependencies_by_group: dict[str, set[str]] = {}
        self._dirty_groups: set[str] = set()

    def register(self, group_id: str, dependencies: Iterable[str]) -> None:
        _require_text(group_id, "group_id")
        normalized = {item for item in dependencies if isinstance(item, str) and item}
        if not normalized:
            raise ValueError("a dependency group must contain at least one key")
        self.unregister(group_id)
        self._dependencies_by_group[group_id] = normalized
        for dependency in normalized:
            self._groups_by_dependency[dependency].add(group_id)

    def unregister(self, group_id: str) -> None:
        previous = self._dependencies_by_group.pop(group_id, None)
        if previous is None:
            return
        for dependency in previous:
            groups = self._groups_by_dependency.get(dependency)
            if groups is None:
                continue
            groups.discard(group_id)
            if not groups:
                self._groups_by_dependency.pop(dependency, None)
        self._dirty_groups.discard(group_id)

    def mark_dirty(self, dependency: str) -> tuple[str, ...]:
        groups = tuple(sorted(self._groups_by_dependency.get(dependency, ())))
        self._dirty_groups.update(groups)
        return groups

    def mark_groups_dirty(self, group_ids: Iterable[str]) -> None:
        self._dirty_groups.update(
            group_id for group_id in group_ids if group_id in self._dependencies_by_group
        )

    def take_dirty(self, *, limit: int) -> tuple[str, ...]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        selected = tuple(sorted(self._dirty_groups)[:limit])
        self._dirty_groups.difference_update(selected)
        return selected

    def dependencies_for(self, group_id: str) -> tuple[str, ...]:
        return tuple(sorted(self._dependencies_by_group.get(group_id, ())))

    def snapshot(self) -> dict[str, int]:
        return {
            "groups": len(self._dependencies_by_group),
            "dependency_keys": len(self._groups_by_dependency),
            "dirty_groups": len(self._dirty_groups),
        }
