"""Dependency Index for strategy/route selection.

Section 6.1: Dependency Index links state changes to affected calculations.
Section 10.7: dirty_groups.add(dependencies.groups_for(key))
ARCH-01: Fast stage, no remote calls, no full route enumeration.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from ..domain.instruments import Instrument, InstrumentCapability


@dataclass
class DependencyGroup:
    """A group of interdependent strategies/routes."""

    group_id: str
    strategy_ids: list[str]
    required_keys: list[str]
    priority: float = 0.0


@dataclass
class DependencyIndex:
    """Maps state changes to affected strategy groups.

    Per Section 6.1: Does NOT recalculate entire universe on each tick.
    Maintains dirty keys for incremental evaluation.
    """

    _instrument_to_groups: dict[str, list[str]] = field(default_factory=dict)
    _group_dependencies: dict[str, set[str]] = field(default_factory=dict)
    _dirty_groups: set[str] = field(default_factory=set)
    _groups: dict[str, DependencyGroup] = field(default_factory=dict)
    _last_update_version: dict[str, int] = field(default_factory=dict)

    def register_group(self, group: DependencyGroup) -> None:
        """Register a strategy group and its dependencies."""
        self._groups[group.group_id] = group
        self._group_dependencies[group.group_id] = set(group.required_keys)
        for key in group.required_keys:
            if key not in self._instrument_to_groups:
                self._instrument_to_groups[key] = []
            if group.group_id not in self._instrument_to_groups[key]:
                self._instrument_to_groups[key].append(group.group_id)

    def groups_for(self, key: str) -> list[str]:
        """Return groups affected by a change in `key`."""
        return self._instrument_to_groups.get(key, [])

    def mark_dirty(self, key: str, version: int = 1) -> list[str]:
        """Mark all groups depending on `key` as dirty.

        Returns the list of newly dirtied group IDs.
        """
        self._last_update_version[key] = version
        affected = self.groups_for(key)
        newly_dirty: list[str] = []
        for group_id in affected:
            if group_id not in self._dirty_groups:
                self._dirty_groups.add(group_id)
                newly_dirty.append(group_id)
        return newly_dirty

    def take_dirty_groups(self) -> list[DependencyGroup]:
        """Consume and return all dirty groups (Section 10.7)."""
        dirty = list(self._dirty_groups)
        self._dirty_groups.clear()
        return [self._groups[g] for g in dirty if g in self._groups]

    def is_dirty(self, group_id: str) -> bool:
        return group_id in self._dirty_groups

    @property
    def dirty_count(self) -> int:
        return len(self._dirty_groups)
