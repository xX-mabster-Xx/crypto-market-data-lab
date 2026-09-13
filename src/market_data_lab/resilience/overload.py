from dataclasses import dataclass, field

from .contracts import SourceTransport, SourceUsability  # noqa: F401


@dataclass
class OverloadHandler:
    """Handle system overload with priority-based degradation."""

    max_queue_depth: int = 10000
    max_ram_bytes: int = 4 * 1024 * 1024 * 1024
    _queue_depth: int = 0
    _ram_bytes: int = 0
    _dropped_signals: int = 0
    _dropped_analytics: int = 0

    @property
    def queue_depth(self) -> int:
        return self._queue_depth

    @property
    def ram_bytes(self) -> int:
        return self._ram_bytes

    def is_overloaded(self) -> bool:
        return self._queue_depth >= self.max_queue_depth or self._ram_bytes >= self.max_ram_bytes

    def can_accept_signal(self) -> bool:
        """Check if new signals can be accepted."""
        if self._queue_depth >= self.max_queue_depth * 0.95:
            return False
        if self._ram_bytes >= self.max_ram_bytes * 0.95:
            return False
        return True

    def can_accept_analytics(self) -> bool:
        """Analytics is lower priority than signals."""
        if self._queue_depth >= self.max_queue_depth * 0.8:
            return False
        if self._ram_bytes >= self.max_ram_bytes * 0.8:
            return False
        return True

    def record_queued(self, count: int = 1) -> None:
        self._queue_depth += count

    def record_processed(self, count: int = 1) -> None:
        self._queue_depth = max(0, self._queue_depth - count)

    def record_dropped_signal(self) -> None:
        self._dropped_signals += 1

    def record_dropped_analytics(self) -> None:
        self._dropped_analytics += 1

    def update_ram_usage(self, bytes_used: int) -> None:
        self._ram_bytes = bytes_used

    def get_degradation_actions(self) -> list[str]:
        """Return list of degradation actions to take."""
        actions: list[str] = []

        if self._queue_depth >= self.max_queue_depth * 0.8:
            actions.append("reduce_cold_discovery")
        if self._queue_depth >= self.max_queue_depth * 0.9:
            actions.append("reduce_new_candidates")
        if self._ram_bytes >= self.max_ram_bytes * 0.8:
            actions.append("flush_old_history")
        if self._ram_bytes >= self.max_ram_bytes * 0.9:
            actions.append("pause_new_admission")

        return actions
