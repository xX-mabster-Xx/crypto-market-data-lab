"""Pool Store — manages AMM pool states with version vectors.

Section 6.1: Version vectors for pool states.
Section 8.3: AMM state includes tick arrays, liquidity, price.
Section 11: TIME-02 chain consistency levels.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal

from ..execution_cost.contracts import AMMState
from ..domain.events import StateQuality


@dataclass
class PoolEntry:
    """A versioned pool state entry."""

    pool_id: str
    state: AMMState
    version: int
    quality: StateQuality
    receive_time_ns: int
    write_version: str | None = None
    context: str | None = None
    is_pinned_block: bool = False


@dataclass
class PoolStore:
    """Manages AMM pool states with version tracking."""

    _pools: dict[str, PoolEntry] = field(default_factory=dict)
    _invalidated: set[str] = field(default_factory=set)

    def publish(
        self,
        pool_id: str,
        state: AMMState,
        quality: StateQuality = "response_time_only",
        write_version: str | None = None,
        context: str | None = None,
        is_pinned_block: bool = False,
    ) -> PoolEntry:
        """Publish a new pool state."""
        import time
        entry = PoolEntry(
            pool_id=pool_id,
            state=state,
            version=len(self._pools.get(pool_id, PoolEntry(pool_id, state, 0, "unknown", 0)).state.__dict__) + 1 if pool_id in self._pools else 1,
            quality=quality,
            receive_time_ns=int(time.monotonic_ns()),
            write_version=write_version,
            context=context,
            is_pinned_block=is_pinned_block,
        )
        self._pools[pool_id] = entry
        return entry

    def get(self, pool_id: str) -> PoolEntry | None:
        if pool_id in self._invalidated:
            return None
        return self._pools.get(pool_id)

    def get_state(self, pool_id: str) -> AMMState | None:
        entry = self.get(pool_id)
        if entry is None:
            return None
        return entry.state

    def is_valid(self, pool_id: str) -> bool:
        return pool_id not in self._invalidated

    def invalidate(self, pool_id: str, reason: str = "stale") -> None:
        self._invalidated.add(pool_id)

    def get_all_valid(self) -> list[PoolEntry]:
        return [e for pid, e in self._pools.items() if pid not in self._invalidated]
