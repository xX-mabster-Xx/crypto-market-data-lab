"""Bounded, event-driven foundation for the unified market scanner.

The scanner deliberately keeps market states in memory.  It persists only a
small status/health document and, in later route layers, compact candidate
lifecycles.  Sources never write a raw tick log themselves.

This module is venue-neutral: Solana, CEX WebSocket and future TON/EVM
adapters publish :class:`MarketEvent` objects into the same bounded bus.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from market_data_lab.live_common import atomic_json
from market_data_lab.versioned_market_state import EventEnvelope
from market_data_lab.versioned_market_state import SourceStateRevision
from market_data_lab.versioned_market_state import VersionedMarketState


def _current_boot_id() -> str:
    """Return the kernel boot ID, with a process-local fallback for portability."""

    try:
        value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        value = ""
    return value or f"process-{uuid.uuid4().hex}"


@dataclass(frozen=True, slots=True)
class MarketEvent:
    """One normalized in-memory state change.

    ``value`` may hold a typed object such as ``BookSnapshot`` or a decoded
    pool state.  It is intentionally not serialized.  ``summary`` is a small
    JSON-safe diagnostic projection used by the status page and tests.
    """

    source: str
    key: str
    kind: str
    value: Any
    summary: Mapping[str, Any]
    received_realtime_ns: int
    received_monotonic_ns: int
    chain_position: int | None = None
    source_revision: SourceStateRevision | None = None
    event_id: str | None = None
    schema_version: int = 2
    source_epoch: int = 0
    instrument_or_pool_id: str | None = None
    boot_id: str | None = None
    exchange_event_time_ns: int | None = None
    exchange_event_time_semantics: str | None = None
    quality_flags: tuple[str, ...] = ()
    provenance: str | None = None

    def envelope(self, *, default_boot_id: str) -> EventEnvelope:
        """Project the live event into the M1 versioned envelope contract."""

        event_id = self.event_id or (
            f"legacy:{self.source}:{self.source_epoch}:{self.key}:"
            f"{self.received_monotonic_ns}"
        )
        return EventEnvelope(
            event_id=event_id,
            event_type=self.kind,
            schema_version=self.schema_version,
            source_id=self.source,
            source_epoch=self.source_epoch,
            instrument_or_pool_id=self.instrument_or_pool_id or self.key,
            received_realtime_ns=self.received_realtime_ns,
            received_monotonic_ns=self.received_monotonic_ns,
            boot_id=self.boot_id or default_boot_id,
            source_sequence=self.chain_position,
            source_revision=self.source_revision,
            payload=self.value,
            exchange_event_time_ns=self.exchange_event_time_ns,
            exchange_event_time_semantics=self.exchange_event_time_semantics,
            quality_flags=self.quality_flags,
            provenance=self.provenance or self.source,
        )


@dataclass(frozen=True, slots=True)
class SourceEpochChange:
    """Control-plane boundary published before any event in a new epoch."""

    source: str
    source_epoch: int
    reason: Literal["initial_start", "restart", "transport_reconnect"]
    realtime_ns: int
    monotonic_ns: int
    previous_source_epoch: int | None = None


class TransportReconnectRequired(RuntimeError):
    """A transport session ended and must be replaced by the source supervisor."""


@dataclass
class SourceHealth:
    """Small, bounded health record for one reconnectable source."""

    source: str
    starts: int = 0
    restarts: int = 0
    updates: int = 0
    received_events: int = 0
    accepted_events: int = 0
    rejected_events: int = 0
    first_event_monotonic_ns: int | None = None
    last_event_realtime_ns: int | None = None
    last_event_monotonic_ns: int | None = None
    last_error: str | None = None
    running: bool = False
    source_epoch: int = 0
    _interarrival_ms: deque[float] = field(default_factory=lambda: deque(maxlen=1_024), repr=False)
    _recent_errors: deque[str] = field(default_factory=lambda: deque(maxlen=8), repr=False)

    def record_error(self, error: str) -> None:
        bounded = error[:2_048]
        self.last_error = bounded
        self._recent_errors.append(bounded)

    def observe_received(self, event: MarketEvent) -> None:
        self.received_events += 1

    def observe_accepted(self, event: MarketEvent) -> None:
        if self.last_event_monotonic_ns is not None:
            self._interarrival_ms.append(
                max(0.0, (event.received_monotonic_ns - self.last_event_monotonic_ns) / 1_000_000),
            )
        if self.first_event_monotonic_ns is None:
            self.first_event_monotonic_ns = event.received_monotonic_ns
        self.accepted_events += 1
        self.updates += 1
        self.last_event_realtime_ns = event.received_realtime_ns
        self.last_event_monotonic_ns = event.received_monotonic_ns
        self.last_error = None

    def snapshot(self, *, now_monotonic_ns: int) -> dict[str, Any]:
        age_ms: float | None = None
        if self.last_event_monotonic_ns is not None:
            age_ms = max(0.0, (now_monotonic_ns - self.last_event_monotonic_ns) / 1_000_000)
        update_rate_per_second: float | None = None
        if (
            self.first_event_monotonic_ns is not None
            and self.last_event_monotonic_ns is not None
            and self.last_event_monotonic_ns > self.first_event_monotonic_ns
        ):
            elapsed_seconds = (
                self.last_event_monotonic_ns - self.first_event_monotonic_ns
            ) / 1_000_000_000
            update_rate_per_second = (self.updates - 1) / elapsed_seconds
        interarrival = sorted(self._interarrival_ms)

        def percentile(fraction: float) -> float | None:
            if not interarrival:
                return None
            index = min(len(interarrival) - 1, round((len(interarrival) - 1) * fraction))
            return round(interarrival[index], 3)

        return {
            "running": self.running,
            "source_epoch": self.source_epoch,
            "starts": self.starts,
            "restarts": self.restarts,
            "updates": self.updates,
            "received_events": self.received_events,
            "accepted_events": self.accepted_events,
            "rejected_events": self.rejected_events,
            "update_rate_per_second": (
                round(update_rate_per_second, 3) if update_rate_per_second is not None else None
            ),
            "interarrival_ms": {
                "samples": len(interarrival),
                "p50": percentile(0.50),
                "p95": percentile(0.95),
                "max": round(interarrival[-1], 3) if interarrival else None,
            },
            "last_event_realtime_ns": self.last_event_realtime_ns,
            "last_event_age_ms": round(age_ms, 3) if age_ms is not None else None,
            "last_error": self.last_error,
            "recent_errors": list(self._recent_errors),
        }


class ScannerSource(Protocol):
    """An adapter supervised by :class:`RealtimeScanner`."""

    name: str

    async def run(
        self,
        publish: Callable[[MarketEvent], Awaitable[Any]],
        stop_event: asyncio.Event,
    ) -> None: ...

    def describe(self) -> Mapping[str, Any]: ...


class RollingStateStore:
    """Latest state plus a time-bounded in-memory audit window per key."""

    def __init__(
        self,
        *,
        retention_seconds: float,
        max_events_per_key: int,
        history_minimum_interval_ms: float = 0,
        max_state_keys: int = 65536,
        boot_id: str | None = None,
    ) -> None:
        if retention_seconds <= 0 or max_events_per_key <= 0 or history_minimum_interval_ms < 0:
            raise ValueError("state-store retention, capacity, and interval must be valid")
        if max_state_keys <= 0:
            raise ValueError("max_state_keys must be positive")
        self.retention_ns = int(retention_seconds * 1_000_000_000)
        self.history_minimum_interval_ns = int(history_minimum_interval_ms * 1_000_000)
        self.max_events_per_key = max_events_per_key
        self.boot_id = boot_id or _current_boot_id()
        self._versioned = VersionedMarketState(boot_id=self.boot_id)
        self._history: dict[str, deque[MarketEvent]] = {}
        self._latest: dict[str, MarketEvent] = {}
        # Event keys and versioned state keys are normally identical, but the
        # contract permits them to differ.  Keep an explicit bounded index so
        # history-only entries can also be retired after an epoch boundary.
        self._state_key_by_event_key: dict[str, str] = {}
        self._event_keys_by_state_key: dict[str, set[str]] = {}
        self.max_state_keys = max_state_keys
        self._total_updates = 0
        self._discarded_by_retention = 0
        self._coalesced_by_interval = 0
        self._rejected_out_of_order = 0
        self._invalidated_by_source_epoch = 0
        self._capacity_evictions = 0
        self._last_sweep_monotonic_ns: int | None = None
        self._sweep_interval_ns = max(1, int(retention_seconds * 1_000_000_000 // 10))

    @property
    def total_updates(self) -> int:
        return self._total_updates

    @property
    def discarded_by_retention(self) -> int:
        return self._discarded_by_retention

    @property
    def coalesced_by_interval(self) -> int:
        return self._coalesced_by_interval

    def advance_source_epoch(self, source: str, source_epoch: int) -> tuple[str, ...]:
        invalidated = self._versioned.advance_source_epoch(source, source_epoch)
        for state_key in invalidated:
            self._retire_state_key(state_key)
        self._invalidated_by_source_epoch += len(invalidated)
        return invalidated

    def _retire_state_key(self, state_key: str, *, keep_event_key: str | None = None) -> tuple[str, ...]:
        """Remove a versioned state and every local key that indexes it."""

        removed: list[str] = []
        for event_key in sorted(self._event_keys_by_state_key.get(state_key, ())):
            if event_key == keep_event_key:
                continue
            self._unlink_event_key(event_key)
            self._latest.pop(event_key, None)
            self._history.pop(event_key, None)
            removed.append(event_key)
        if keep_event_key is None:
            self._history.pop(state_key, None)
        if state_key not in self._event_keys_by_state_key:
            self._versioned.retire(state_key)
        return tuple(removed)

    def _link_event_key(self, event_key: str, state_key: str) -> None:
        """Install both directions of the event-key/state-key index."""

        previous_state_key = self._state_key_by_event_key.get(event_key)
        if previous_state_key == state_key:
            self._event_keys_by_state_key.setdefault(state_key, set()).add(event_key)
            return
        if previous_state_key is not None:
            previous_keys = self._event_keys_by_state_key.get(previous_state_key)
            if previous_keys is not None:
                previous_keys.discard(event_key)
                if not previous_keys:
                    self._event_keys_by_state_key.pop(previous_state_key, None)
        self._state_key_by_event_key[event_key] = state_key
        self._event_keys_by_state_key.setdefault(state_key, set()).add(event_key)

    def _unlink_event_key(self, event_key: str) -> str | None:
        """Remove both directions of one event-key/state-key mapping."""

        state_key = self._state_key_by_event_key.pop(event_key, None)
        if state_key is None:
            return None
        event_keys = self._event_keys_by_state_key.get(state_key)
        if event_keys is not None:
            event_keys.discard(event_key)
            if not event_keys:
                self._event_keys_by_state_key.pop(state_key, None)
        return state_key

    def _retire_event_key(self, event_key: str) -> None:
        """Retire one local key without deleting a shared state record."""

        state_key = self._unlink_event_key(event_key)
        latest = self._latest.pop(event_key, None)
        self._history.pop(event_key, None)
        if state_key is None and latest is not None:
            state_key = latest.instrument_or_pool_id or event_key
        if state_key is not None and state_key not in self._event_keys_by_state_key:
            self._versioned.retire(state_key)

    def _evict_oldest_for_capacity(self) -> None:
        if len(self._latest) < self.max_state_keys:
            return
        oldest_key = min(
            self._latest,
            key=lambda key: (
                self._latest[key].received_monotonic_ns,
                self._latest[key].received_realtime_ns,
                key,
            ),
        )
        self._retire_event_key(oldest_key)
        self._capacity_evictions += 1

    def add(self, event: MarketEvent) -> bool:
        envelope = event.envelope(default_boot_id=self.boot_id)
        put = self._versioned.put(envelope, ttl_ns=None)
        if not put.accepted:
            self._rejected_out_of_order += 1
            return False
        previous_state_key = self._state_key_by_event_key.get(event.key)
        if previous_state_key is not None and previous_state_key != envelope.state_key:
            self._retire_state_key(previous_state_key)
        # Install the mapping before eviction.  A new event key may share an
        # existing state key; eviction must not retire the record just
        # accepted by ``VersionedMarketState.put``.
        self._link_event_key(event.key, envelope.state_key)
        self._retire_state_key(envelope.state_key, keep_event_key=event.key)
        if event.key not in self._latest:
            self._evict_oldest_for_capacity()
        for state_key in put.invalidated_keys:
            # ``put`` has already installed the new record for the current
            # event when the state key is reused.  Clear only its old history;
            # retire unrelated invalidated records physically.
            if state_key == envelope.instrument_or_pool_id:
                self._retire_state_key(state_key, keep_event_key=event.key)
                continue
            self._retire_state_key(state_key)
        self._invalidated_by_source_epoch += len(put.invalidated_keys)
        history = self._history.setdefault(
            event.key,
            deque(maxlen=self.max_events_per_key),
        )
        self._latest[event.key] = event
        self._total_updates += 1
        cutoff = event.received_monotonic_ns - self.retention_ns
        while history and history[0].received_monotonic_ns < cutoff:
            history.popleft()
            self._discarded_by_retention += 1
        if (
            self.history_minimum_interval_ns > 0
            and history
            and event.received_monotonic_ns - history[-1].received_monotonic_ns
            < self.history_minimum_interval_ns
        ):
            # The event still travels through the live bus, and `_latest`
            # advances immediately.  Only the optional retrospective window is
            # coalesced, avoiding a RAM explosion on sub-millisecond L2 feeds.
            self._coalesced_by_interval += 1
            return True
        if len(history) == history.maxlen:
            self._discarded_by_retention += 1
        history.append(event)
        return True

    def latest(self, key: str) -> MarketEvent | None:
        return self._latest.get(key)

    def recent(self, key: str) -> tuple[MarketEvent, ...]:
        return tuple(self._history.get(key, ()))

    @property
    def capacity_evictions(self) -> int:
        return self._capacity_evictions

    def sweep(self, *, now_monotonic_ns: int | None = None) -> tuple[str, ...]:
        """Retire state keys idle longer than retention and enforce hard key cap."""

        if now_monotonic_ns is None:
            now_monotonic_ns = time.monotonic_ns()
        self._last_sweep_monotonic_ns = now_monotonic_ns
        retired: list[str] = []
        cutoff_ns = now_monotonic_ns - self.retention_ns
        for key in sorted(set(self._latest) | set(self._history)):
            latest = self._latest.get(key)
            if latest is None:
                self._retire_event_key(key)
                retired.append(key)
                continue
            if latest.received_monotonic_ns < cutoff_ns:
                self._retire_event_key(key)
                retired.append(key)
        # Enforce hard key capacity by evicting oldest idle keys.
        if len(self._latest) > self.max_state_keys:
            sorted_keys = sorted(
                self._latest,
                key=lambda k: self._latest[k].received_monotonic_ns,
            )
            for key in sorted_keys[: len(self._latest) - self.max_state_keys]:
                self._retire_event_key(key)
                self._capacity_evictions += 1
                retired.append(key)
        return tuple(retired)

    def maybe_sweep(self, *, now_monotonic_ns: int | None = None) -> tuple[str, ...]:
        """Run the amortized sweep used by hot scanner loops."""

        if now_monotonic_ns is None:
            now_monotonic_ns = time.monotonic_ns()
        if (
            self._last_sweep_monotonic_ns is not None
            and now_monotonic_ns - self._last_sweep_monotonic_ns < self._sweep_interval_ns
        ):
            return ()
        return self.sweep(now_monotonic_ns=now_monotonic_ns)

    def snapshot(self, *, now_monotonic_ns: int, limit: int = 256) -> dict[str, Any]:
        states: dict[str, Any] = {}
        # This is diagnostic output, not a full market-data export.  Stable
        # lexical truncation keeps the status file bounded even for a huge
        # configured universe.
        for key in sorted(self._latest)[:limit]:
            event = self._latest[key]
            history = self._history.get(key, ())
            if event is None:
                continue
            versioned = self._versioned.latest(event.instrument_or_pool_id or key)
            age_ms = max(0.0, (now_monotonic_ns - event.received_monotonic_ns) / 1_000_000)
            states[key] = {
                "source": event.source,
                "kind": event.kind,
                "event_id": event.event_id,
                "source_epoch": event.source_epoch,
                "state_version": (
                    versioned.state_version if versioned is not None else None
                ),
                "chain_position": event.chain_position,
                "last_event_realtime_ns": event.received_realtime_ns,
                "age_ms": round(age_ms, 3),
                "retained_updates": len(history),
                "summary": dict(event.summary),
            }
        return {
            "schema_version": 2,
            "boot_id": self.boot_id,
            "keys": len(self._latest),
            "total_updates": self._total_updates,
            "discarded_by_retention": self._discarded_by_retention,
            "coalesced_by_interval": self._coalesced_by_interval,
            "rejected_out_of_order_or_duplicate": self._rejected_out_of_order,
            "invalidated_by_source_epoch": self._invalidated_by_source_epoch,
            "capacity_evictions": self._capacity_evictions,
            "max_state_keys": self.max_state_keys,
            "history_minimum_interval_ms": self.history_minimum_interval_ns / 1_000_000,
            "displayed_keys": len(states),
            "states": states,
        }


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Outcome of an event publication attempt."""

    accepted: bool
    coalesced: bool = False


class EventBusClosed(RuntimeError):
    """Raised when a publisher or consumer waits on a closed event bus."""


class CoalescingEventBus:
    """A bounded event queue which coalesces by state key with backpressure.

    Distinct pending keys are bounded by capacity.  A duplicate pending
    key replaces the queued payload and increments coalesced_updates.
    When the bus is saturated, new distinct keys apply bounded backpressure
    (the publisher awaits) rather than dropping arbitrary events.
    """

    def __init__(self, store: RollingStateStore, *, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("event bus capacity must be positive")
        self.store = store
        self.capacity = capacity
        self._pending_latest: dict[str, MarketEvent] = {}
        self._pending_order: deque[str] = deque()
        self._condition = asyncio.Condition()
        self.dropped_events = 0
        self.rejected_state_events = 0
        self.coalesced_updates = 0
        self.pending_keys = 0
        self.queue_high_watermark = 0
        self.publisher_backpressure_waits = 0
        self.waiting_publishers = 0
        self._closed = False

    @property
    def queued(self) -> int:
        return len(self._pending_latest)

    async def publish(self, event: MarketEvent) -> PublishResult:
        async with self._condition:
            if self._closed:
                raise EventBusClosed("event bus is closed")
            accepted = self.store.add(event)
            if not accepted:
                self.rejected_state_events += 1
                return PublishResult(accepted=False)
            while True:
                if self._closed:
                    raise EventBusClosed("event bus is closed")
                latest = self.store.latest(event.key)
                if latest is None:
                    # A retention/epoch operation retired the accepted state
                    # while this publisher was backpressured.
                    return PublishResult(accepted=True, coalesced=True)
                superseded = latest is not event
                if event.key in self._pending_latest:
                    pending = self._pending_latest[event.key]
                    if pending is not latest:
                        self._pending_latest[event.key] = latest
                    if superseded or pending is not latest:
                        self.coalesced_updates += 1
                    self._condition.notify_all()
                    return PublishResult(accepted=True, coalesced=True)
                if len(self._pending_latest) < self.capacity:
                    self._pending_latest[event.key] = latest
                    self._pending_order.append(event.key)
                    self.pending_keys = len(self._pending_latest)
                    self.queue_high_watermark = max(
                        self.queue_high_watermark,
                        self.pending_keys,
                    )
                    if superseded:
                        self.coalesced_updates += 1
                    self._condition.notify_all()
                    return PublishResult(accepted=True, coalesced=superseded)
                self.publisher_backpressure_waits += 1
                self.waiting_publishers += 1
                self._condition.notify_all()
                try:
                    await self._condition.wait()
                finally:
                    self.waiting_publishers -= 1

    async def next_event(self) -> MarketEvent:
        async with self._condition:
            while not self._pending_latest:
                if self._closed:
                    raise EventBusClosed("event bus is closed")
                await self._condition.wait()
            key = self._pending_order.popleft()
            event = self._pending_latest.pop(key)
            self.pending_keys = len(self._pending_latest)
            self._condition.notify_all()
            return event

    async def close(self) -> None:
        """Stop admission and wake every backpressured publisher/consumer."""

        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    def snapshot(self) -> dict[str, Any]:
        return {
            "capacity": self.capacity,
            "queued": self.queued,
            "accepted_events": self.store.total_updates,
            "rejected_state_events": self.rejected_state_events,
            "coalesced_updates": self.coalesced_updates,
            "pending_keys": self.pending_keys,
            "queue_capacity": self.capacity,
            "queue_high_watermark": self.queue_high_watermark,
            "publisher_backpressure_waits": self.publisher_backpressure_waits,
            "waiting_publishers": self.waiting_publishers,
            "closed": self._closed,
            "dropped_events": self.dropped_events,
        }


EventHandler = Callable[[MarketEvent], Awaitable[None]]
StatusProvider = Callable[[], Mapping[str, Any]]
ShutdownHandler = Callable[[], Awaitable[None]]
EpochChangeHandler = Callable[[SourceEpochChange], Awaitable[None] | None]

DEFAULT_SUPERVISOR_RETRY_INITIAL_SECONDS = 0.25
DEFAULT_SUPERVISOR_RETRY_MAX_SECONDS = 15.0


def _supervisor_retry_seconds(
    source: ScannerSource,
    attribute: str,
    *,
    default: float,
) -> float:
    """Return a source-specific retry setting without trusting malformed values."""

    value = getattr(source, attribute, default)
    if isinstance(value, bool):
        return default
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return default
    return seconds if math.isfinite(seconds) and seconds > 0 else default


@dataclass
class RealtimeScanner:
    """Supervise sources and publish only bounded diagnostics to disk."""

    sources: Sequence[ScannerSource]
    output_directory: Path
    retention_seconds: float = 180.0
    max_events_per_key: int = 4_096
    history_minimum_interval_ms: float = 0
    event_bus_capacity: int = 8_192
    max_state_keys: int = 65536
    status_flush_seconds: float = 2.0
    event_handler: EventHandler | None = None
    status_providers: Mapping[str, StatusProvider] = field(default_factory=dict)
    shutdown_handlers: Sequence[ShutdownHandler] = ()
    epoch_change_handlers: Sequence[EpochChangeHandler] = ()
    runtime_components: Mapping[str, Any] = field(default_factory=dict)
    supervisor_retry_reset_after_seconds: float = 30.0
    # Backwards-compatible alias for callers that used the pre-v2 name.
    supervisor_stable_run_reset_after_seconds: float | None = None
    _store: RollingStateStore = field(init=False, repr=False)
    _bus: CoalescingEventBus = field(init=False, repr=False)
    _health: dict[str, SourceHealth] = field(init=False, repr=False)
    _stop_event: asyncio.Event = field(init=False, repr=False)
    _started_at: str = field(init=False, repr=False)
    _started_monotonic: float = field(init=False, repr=False)
    _boot_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        names = [source.name for source in self.sources]
        if not names or len(set(names)) != len(names):
            raise ValueError("scanner source names must be non-empty and unique")
        if self.status_flush_seconds <= 0:
            raise ValueError("status_flush_seconds must be positive")
        if self.supervisor_stable_run_reset_after_seconds is not None:
            self.supervisor_retry_reset_after_seconds = self.supervisor_stable_run_reset_after_seconds
        try:
            retry_reset_seconds = float(self.supervisor_retry_reset_after_seconds)
        except (TypeError, ValueError):
            retry_reset_seconds = math.nan
        if not math.isfinite(retry_reset_seconds) or retry_reset_seconds <= 0:
            raise ValueError("supervisor_retry_reset_after_seconds must be finite and positive")
        self.supervisor_retry_reset_after_seconds = retry_reset_seconds
        if any(not isinstance(name, str) or not name.strip() for name in self.status_providers):
            raise ValueError("status provider names must be non-empty strings")
        self._boot_id = _current_boot_id()
        self._store = RollingStateStore(
            retention_seconds=self.retention_seconds,
            max_events_per_key=self.max_events_per_key,
            history_minimum_interval_ms=self.history_minimum_interval_ms,
            max_state_keys=self.max_state_keys,
            boot_id=self._boot_id,
        )
        self._bus = CoalescingEventBus(self._store, capacity=self.event_bus_capacity)
        self._health = {name: SourceHealth(name) for name in names}
        self._stop_event = asyncio.Event()
        self._started_at = ""
        self._started_monotonic = 0.0

    @property
    def store(self) -> RollingStateStore:
        return self._store

    @property
    def stop_event(self) -> asyncio.Event:
        return self._stop_event

    async def _publish(self, event: MarketEvent) -> PublishResult:
        health = self._health.get(event.source)
        if health is None:
            raise ValueError(f"event references unknown source {event.source!r}")
        health.observe_received(event)
        result = await self._bus.publish(event)
        if result.accepted:
            health.observe_accepted(event)
        else:
            health.rejected_events += 1
        return result

    async def _source_supervisor(self, source: ScannerSource) -> None:
        health = self._health[source.name]
        retry_initial_seconds = _supervisor_retry_seconds(
            source,
            "supervisor_retry_initial_seconds",
            default=DEFAULT_SUPERVISOR_RETRY_INITIAL_SECONDS,
        )
        retry_max_seconds = max(
            retry_initial_seconds,
            _supervisor_retry_seconds(
                source,
                "supervisor_retry_max_seconds",
                default=DEFAULT_SUPERVISOR_RETRY_MAX_SECONDS,
            ),
        )
        retry_delay = retry_initial_seconds
        next_epoch_reason: Literal["initial_start", "restart", "transport_reconnect"] = (
            "initial_start"
        )
        while not self._stop_event.is_set():
            health.starts += 1
            old_epoch = health.source_epoch
            health.source_epoch += 1
            source_epoch = health.source_epoch
            self._store.advance_source_epoch(source.name, source_epoch)
            change = SourceEpochChange(
                source=source.name,
                source_epoch=source_epoch,
                reason=next_epoch_reason,
                realtime_ns=time.time_ns(),
                monotonic_ns=time.monotonic_ns(),
                previous_source_epoch=old_epoch,
            )
            for handler in self.epoch_change_handlers:
                result = handler(change)
                if inspect.isawaitable(result):
                    await result
            run_started_monotonic = time.monotonic()
            event_counter = 0

            async def publish_for_epoch(event: MarketEvent) -> PublishResult:
                nonlocal event_counter
                event_counter += 1
                enriched = replace(
                    event,
                    event_id=(
                        event.event_id
                        or f"{self._boot_id}:{source.name}:{source_epoch}:{event_counter}"
                    ),
                    schema_version=max(2, event.schema_version),
                    source_epoch=source_epoch,
                    instrument_or_pool_id=event.instrument_or_pool_id or event.key,
                    boot_id=self._boot_id,
                )
                return await self._publish(enriched)

            health.running = True
            try:
                await source.run(publish_for_epoch, self._stop_event)
                if not self._stop_event.is_set():
                    raise RuntimeError("source returned without scanner shutdown")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                health.running = False
                health.restarts += 1
                health.record_error(f"{type(exc).__name__}: {exc}")
                next_epoch_reason = (
                    "transport_reconnect"
                    if isinstance(exc, TransportReconnectRequired)
                    else "restart"
                )
                # Reset backoff if the source ran stably for a while before
                # this failure; a single event followed by a crash should
                # still escalate.
                if time.monotonic() - run_started_monotonic >= self.supervisor_retry_reset_after_seconds:
                    retry_delay = retry_initial_seconds
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=retry_delay)
                except TimeoutError:
                    retry_delay = min(retry_delay * 2, retry_max_seconds)
                continue
            finally:
                health.running = False
            # Successful return (normally only on shutdown) resets backoff.
            retry_delay = retry_initial_seconds

    async def _event_consumer(self) -> None:
        while not self._stop_event.is_set():
            self._store.maybe_sweep(now_monotonic_ns=time.monotonic_ns())
            event = await self._bus.next_event()
            if self.event_handler is not None:
                try:
                    await self.event_handler(event)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A route-specific failure must not terminate state feeds.
                    health = self._health.setdefault("event_handler", SourceHealth("event_handler"))
                    health.restarts += 1
                    health.record_error(f"{type(exc).__name__}: {exc}")

    def snapshot(self, *, status: str) -> dict[str, Any]:
        now_monotonic_ns = time.monotonic_ns()
        result: dict[str, Any] = {
            "schema_version": 1,
            "status": status,
            "started_at": self._started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "duration_wall_seconds": round(time.monotonic() - self._started_monotonic, 6),
            "raw_market_data_persisted": False,
            "versioned_event_schema": 2,
            "boot_id": self._boot_id,
            "retention": {
                "in_memory_seconds": self.retention_seconds,
                "max_events_per_key": self.max_events_per_key,
                "history_minimum_interval_ms": self.history_minimum_interval_ms,
                "policy": (
                    "accepted state updates use keyed latest-state coalescing: "
                    "superseded pending updates of the same state key are replaced; "
                    "distinct-key saturation applies bounded backpressure rather than "
                    "arbitrary dropping; idle keys are retired by TTL sweep"
                ),
            },
            "event_bus": self._bus.snapshot(),
            "sources": {
                name: health.snapshot(now_monotonic_ns=now_monotonic_ns)
                for name, health in sorted(self._health.items())
            },
            "state": self._store.snapshot(now_monotonic_ns=now_monotonic_ns),
        }
        extensions: dict[str, Any] = {}
        for name, provider in sorted(self.status_providers.items()):
            try:
                snapshot = provider()
                if not isinstance(snapshot, Mapping):
                    raise TypeError("status provider must return a mapping")
                extensions[name] = dict(snapshot)
            except Exception as exc:
                # Diagnostics must never take down market-data sources.  The
                # provider itself remains responsible for not placing raw
                # market values or credentials in its compact projection.
                extensions[name] = {"status_provider_error": f"{type(exc).__name__}: {exc}"[:512]}
        if extensions:
            result["extensions"] = extensions
        if self.runtime_components:
            result["runtime_components"] = dict(self.runtime_components)
        return result

    async def _status_writer(self) -> None:
        path = self.output_directory / "status.json"
        while not self._stop_event.is_set():
            now_monotonic_ns = time.monotonic_ns()
            self._store.maybe_sweep(now_monotonic_ns=now_monotonic_ns)
            atomic_json(path, self.snapshot(status="running"))
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.status_flush_seconds)
            except TimeoutError:
                pass

    def _manifest(self, *, status: str, error: str | None = None) -> dict[str, Any]:
        result = {
            "schema_version": 1,
            "status": status,
            "started_at": self._started_at,
            "stopped_at": datetime.now(UTC).isoformat() if status != "running" else None,
            "mode": "unified_realtime_scanner",
            "raw_market_data_persisted": False,
            "sources": [dict(source.describe()) for source in self.sources],
            "error": error,
            "wallet_or_private_key_used": False,
            "transactions_submitted": False,
        }
        if self.runtime_components:
            result["runtime_components"] = dict(self.runtime_components)
        return result

    async def run(self, *, duration_seconds: float | None = None) -> dict[str, Any]:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive when supplied")
        if self.output_directory.exists():
            raise FileExistsError(f"refusing to overwrite scanner run {self.output_directory}")
        self.output_directory.mkdir(parents=True)
        self._started_at = datetime.now(UTC).isoformat()
        self._started_monotonic = time.monotonic()
        atomic_json(self.output_directory / "manifest.json", self._manifest(status="running"))

        source_tasks = [asyncio.create_task(self._source_supervisor(source)) for source in self.sources]
        status_task = asyncio.create_task(self._status_writer())
        consumer_task = asyncio.create_task(self._event_consumer())
        final_status = "completed"
        final_error: str | None = None
        try:
            if duration_seconds is None:
                await self._stop_event.wait()
            else:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=duration_seconds)
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            final_status = "stopped"
        except Exception as exc:
            final_status = "error"
            final_error = f"{type(exc).__name__}: {exc}"
        finally:
            self._stop_event.set()
            await self._bus.close()
            for handler in self.shutdown_handlers:
                try:
                    await handler()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    health = self._health.setdefault("shutdown_handler", SourceHealth("shutdown_handler"))
                    health.restarts += 1
                    health.record_error(f"{type(exc).__name__}: {exc}")
            for task in (*source_tasks, status_task, consumer_task):
                task.cancel()
            await asyncio.gather(*source_tasks, status_task, consumer_task, return_exceptions=True)
            atomic_json(self.output_directory / "status.json", self.snapshot(status=final_status))
            atomic_json(
                self.output_directory / "manifest.json",
                self._manifest(status=final_status, error=final_error),
            )
        return self._manifest(status=final_status, error=final_error)
