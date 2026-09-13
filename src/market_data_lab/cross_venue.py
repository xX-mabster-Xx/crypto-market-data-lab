"""Analyze executable Bybit/OKX spreads from a simultaneous dual run."""

from __future__ import annotations

import argparse
import bisect
import json
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

from nautilus_trader.model import BookAction
from nautilus_trader.model import BookType
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import OrderBook
from nautilus_trader.model import RecordFlag
from nautilus_trader.persistence import ParquetDataCatalog

from market_data_lab.live_common import atomic_json


@dataclass(frozen=True)
class BookState:
    venue: str
    instrument_id: str
    ts_init_ns: int
    ts_event_ns: int
    sequence: int
    bids: tuple[tuple[float, float], ...]
    asks: tuple[tuple[float, float], ...]


@dataclass(frozen=True)
class ExecutionResult:
    base_quantity: float
    buy_vwap: float
    sell_vwap: float
    gross_buy_quote: float
    gross_sell_quote: float
    gross_pnl_quote: float
    net_pnl_quote: float
    gross_edge_bps: float
    net_edge_bps: float

    def to_dict(self) -> dict[str, float]:
        return {
            "base_quantity": round(self.base_quantity, 12),
            "buy_vwap": round(self.buy_vwap, 12),
            "sell_vwap": round(self.sell_vwap, 12),
            "gross_buy_quote": round(self.gross_buy_quote, 8),
            "gross_sell_quote": round(self.gross_sell_quote, 8),
            "gross_pnl_quote": round(self.gross_pnl_quote, 8),
            "net_pnl_quote": round(self.net_pnl_quote, 8),
            "gross_edge_bps": round(self.gross_edge_bps, 6),
            "net_edge_bps": round(self.net_edge_bps, 6),
        }


@dataclass
class FloatDistribution:
    capacity: int = 8_192
    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    positive: int = 0
    _samples: list[float] = field(default_factory=list, repr=False)
    _random: random.Random = field(default_factory=lambda: random.Random(0), repr=False)

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        if value > 0:
            self.positive += 1
        if len(self._samples) < self.capacity:
            self._samples.append(value)
            return
        replacement = self._random.randrange(self.count)
        if replacement < self.capacity:
            self._samples[replacement] = value

    def summary(self) -> dict[str, int | float | None]:
        samples = sorted(self._samples)

        def percentile(value: float) -> float | None:
            if not samples:
                return None
            index = round((len(samples) - 1) * value)
            return round(samples[index], 6)

        return {
            "count": self.count,
            "sample_count": len(samples),
            "positive_count": self.positive,
            "positive_ratio": round(self.positive / self.count, 6) if self.count else None,
            "min": round(self.minimum, 6) if self.minimum is not None else None,
            "mean": round(self.total / self.count, 6) if self.count else None,
            "p50": percentile(0.50),
            "p95": percentile(0.95),
            "p99": percentile(0.99),
            "max": round(self.maximum, 6) if self.maximum is not None else None,
        }


@dataclass(frozen=True)
class OpportunityStart:
    ts_init_ns: int
    direction: str
    notional_quote: float
    bybit_book_age_ms: float
    okx_book_age_ms: float
    signal: ExecutionResult


@dataclass
class EpisodeTracker:
    direction: str
    notional_quote: float
    active_start_ns: int | None = None
    active_peak_bps: float | None = None
    completed: int = 0
    censored: int = 0
    durations_ms: FloatDistribution = field(default_factory=FloatDistribution)
    peaks_bps: FloatDistribution = field(default_factory=FloatDistribution)
    starts: list[OpportunityStart] = field(default_factory=list)

    def update(
        self,
        ts_init_ns: int,
        result: ExecutionResult | None,
        bybit_book_age_ms: float = 0.0,
        okx_book_age_ms: float = 0.0,
    ) -> None:
        edge = result.net_edge_bps if result is not None else float("-inf")
        if edge > 0:
            if self.active_start_ns is None:
                self.active_start_ns = ts_init_ns
                self.active_peak_bps = edge
                self.starts.append(
                    OpportunityStart(
                        ts_init_ns=ts_init_ns,
                        direction=self.direction,
                        notional_quote=self.notional_quote,
                        bybit_book_age_ms=bybit_book_age_ms,
                        okx_book_age_ms=okx_book_age_ms,
                        signal=result,
                    ),
                )
            else:
                self.active_peak_bps = max(self.active_peak_bps or edge, edge)
            return
        if self.active_start_ns is not None:
            self._close(ts_init_ns, censored=False)

    def finish(self, ts_init_ns: int) -> None:
        if self.active_start_ns is not None:
            self._close(ts_init_ns, censored=True)

    def _close(self, ts_init_ns: int, *, censored: bool) -> None:
        assert self.active_start_ns is not None
        duration_ms = max(0.0, (ts_init_ns - self.active_start_ns) / 1_000_000)
        self.durations_ms.add(duration_ms)
        self.peaks_bps.add(self.active_peak_bps or 0.0)
        if censored:
            self.censored += 1
        else:
            self.completed += 1
        self.active_start_ns = None
        self.active_peak_bps = None

    def summary(self) -> dict[str, object]:
        return {
            "episodes": self.completed + self.censored,
            "completed_episodes": self.completed,
            "right_censored_episodes": self.censored,
            "duration_ms": self.durations_ms.summary(),
            "peak_net_edge_bps": self.peaks_bps.summary(),
        }


def _parse_positive_floats(value: str, label: str) -> list[float]:
    try:
        values = list(dict.fromkeys(float(item.strip()) for item in value.split(",")))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated numbers") from exc
    if not values or any(item <= 0 for item in values):
        raise argparse.ArgumentTypeError(f"{label} must contain positive values")
    return values


def _parse_nonnegative_floats(value: str, label: str) -> list[float]:
    try:
        values = list(dict.fromkeys(float(item.strip()) for item in value.split(",")))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{label} must be comma-separated numbers") from exc
    if not values or any(item < 0 for item in values):
        raise argparse.ArgumentTypeError(f"{label} must contain non-negative values")
    return values


def _trim_levels(levels: Iterable[Any], max_levels: int) -> tuple[tuple[float, float], ...]:
    result: list[tuple[float, float]] = []
    for level in levels:
        price = float(level.price)
        size = float(level.size())
        if price > 0 and size > 0:
            result.append((price, size))
        if len(result) >= max_levels:
            break
    return tuple(result)


def load_book_states(
    catalog_path: Path,
    instrument_id: str,
    venue: str,
    max_levels: int,
) -> tuple[list[BookState], dict[str, object]]:
    catalog = ParquetDataCatalog(str(catalog_path))
    rows = catalog.query_order_book_deltas([instrument_id])
    if not rows:
        raise ValueError(f"No order-book deltas for {instrument_id} in {catalog_path}")
    rows.sort(key=lambda row: (row.ts_init, row.sequence, row.ts_event))
    parsed_id = InstrumentId.from_str(instrument_id)
    book = OrderBook(parsed_id, BookType.L2_MBP)
    snapshot_flag = RecordFlag.F_SNAPSHOT.value
    states: list[BookState] = []
    batches = 0
    snapshots = 0
    index = 0
    while index < len(rows):
        first = rows[index]
        key = (first.ts_init, first.sequence, first.ts_event)
        end = index + 1
        while end < len(rows):
            candidate = rows[end]
            if (candidate.ts_init, candidate.sequence, candidate.ts_event) != key:
                break
            end += 1
        batch = rows[index:end]
        is_snapshot = any(
            delta.action == BookAction.CLEAR or bool(delta.flags & snapshot_flag)
            for delta in batch
        )
        if is_snapshot:
            book = OrderBook(parsed_id, BookType.L2_MBP)
            snapshots += 1
        for delta in batch:
            book.apply_delta(delta)
        bids = _trim_levels(book.bids(), max_levels)
        asks = _trim_levels(book.asks(), max_levels)
        if bids and asks and bids[0][0] < asks[0][0]:
            states.append(
                BookState(
                    venue=venue,
                    instrument_id=instrument_id,
                    ts_init_ns=first.ts_init,
                    ts_event_ns=first.ts_event,
                    sequence=first.sequence,
                    bids=bids,
                    asks=asks,
                ),
            )
        batches += 1
        index = end
    if not states:
        raise ValueError(f"No valid reconstructed book states for {instrument_id}")
    return states, {
        "delta_records": len(rows),
        "message_batches": batches,
        "valid_book_states": len(states),
        "snapshots": snapshots,
        "first_init_ns": states[0].ts_init_ns,
        "last_init_ns": states[-1].ts_init_ns,
        "max_levels_retained_per_side": max_levels,
    }


def _buy_quote_notional(
    asks: Sequence[tuple[float, float]],
    target_quote: float,
) -> tuple[float, float] | None:
    remaining_quote = target_quote
    base_quantity = 0.0
    gross_quote = 0.0
    for price, available_base in asks:
        available_quote = price * available_base
        take_quote = min(remaining_quote, available_quote)
        base_quantity += take_quote / price
        gross_quote += take_quote
        remaining_quote -= take_quote
        if remaining_quote <= max(1e-9, target_quote * 1e-12):
            return base_quantity, gross_quote
    return None


def _sell_base_quantity(
    bids: Sequence[tuple[float, float]],
    target_base: float,
) -> float | None:
    remaining_base = target_base
    gross_quote = 0.0
    for price, available_base in bids:
        take_base = min(remaining_base, available_base)
        gross_quote += take_base * price
        remaining_base -= take_base
        if remaining_base <= max(1e-12, target_base * 1e-12):
            return gross_quote
    return None


def calculate_execution(
    buy_state: BookState,
    sell_state: BookState,
    target_quote: float,
    buy_fee_bps: float,
    sell_fee_bps: float,
) -> ExecutionResult | None:
    bought = _buy_quote_notional(buy_state.asks, target_quote)
    if bought is None:
        return None
    base_quantity, gross_buy_quote = bought
    gross_sell_quote = _sell_base_quantity(sell_state.bids, base_quantity)
    if gross_sell_quote is None or base_quantity <= 0 or gross_buy_quote <= 0:
        return None
    gross_pnl = gross_sell_quote - gross_buy_quote
    net_cost = gross_buy_quote * (1 + buy_fee_bps / 10_000)
    net_proceeds = gross_sell_quote * (1 - sell_fee_bps / 10_000)
    net_pnl = net_proceeds - net_cost
    return ExecutionResult(
        base_quantity=base_quantity,
        buy_vwap=gross_buy_quote / base_quantity,
        sell_vwap=gross_sell_quote / base_quantity,
        gross_buy_quote=gross_buy_quote,
        gross_sell_quote=gross_sell_quote,
        gross_pnl_quote=gross_pnl,
        net_pnl_quote=net_pnl,
        gross_edge_bps=gross_pnl / gross_buy_quote * 10_000,
        net_edge_bps=net_pnl / net_cost * 10_000,
    )


def _state_as_of(states: Sequence[BookState], timestamps: Sequence[int], target_ns: int) -> BookState | None:
    index = bisect.bisect_right(timestamps, target_ns) - 1
    return states[index] if index >= 0 else None


def analyze_pair(
    bybit_states: list[BookState],
    okx_states: list[BookState],
    notionals: list[float],
    latencies_ms: list[float],
    bybit_fee_bps: float,
    okx_fee_bps: float,
    max_book_age_ms: float | None = None,
) -> dict[str, object]:
    overlap_start_ns = max(bybit_states[0].ts_init_ns, okx_states[0].ts_init_ns)
    overlap_end_ns = min(bybit_states[-1].ts_init_ns, okx_states[-1].ts_init_ns)
    if overlap_end_ns <= overlap_start_ns:
        raise ValueError("Bybit and OKX reconstructed books do not overlap")

    events = sorted(
        [(state.ts_init_ns, "BYBIT", state) for state in bybit_states]
        + [(state.ts_init_ns, "OKX", state) for state in okx_states],
        key=lambda item: (item[0], item[1]),
    )
    latest: dict[str, BookState] = {}
    accumulators: dict[tuple[float, str, str], FloatDistribution] = {}
    trackers: dict[tuple[float, str], EpisodeTracker] = {}
    directions = (
        "buy_bybit_sell_okx",
        "buy_okx_sell_bybit",
    )
    for notional in notionals:
        for direction in directions:
            accumulators[(notional, direction, "gross")] = FloatDistribution()
            accumulators[(notional, direction, "net")] = FloatDistribution()
            trackers[(notional, direction)] = EpisodeTracker(direction, notional)

    observations = 0
    evaluated_observations = 0
    stale_observations_skipped = 0
    compared_book_age_ms = FloatDistribution()
    for ts_init_ns, venue, state in events:
        if ts_init_ns < overlap_start_ns:
            latest[venue] = state
            continue
        if ts_init_ns > overlap_end_ns:
            break
        latest[venue] = state
        if "BYBIT" not in latest or "OKX" not in latest:
            continue
        observations += 1
        bybit_book_age_ms = (ts_init_ns - latest["BYBIT"].ts_init_ns) / 1_000_000
        okx_book_age_ms = (ts_init_ns - latest["OKX"].ts_init_ns) / 1_000_000
        maximum_book_age_ms = max(bybit_book_age_ms, okx_book_age_ms)
        compared_book_age_ms.add(maximum_book_age_ms)
        if max_book_age_ms is not None and maximum_book_age_ms > max_book_age_ms:
            stale_observations_skipped += 1
            for tracker in trackers.values():
                tracker.update(ts_init_ns, None)
            continue
        evaluated_observations += 1
        for notional in notionals:
            forward = calculate_execution(
                latest["BYBIT"],
                latest["OKX"],
                notional,
                bybit_fee_bps,
                okx_fee_bps,
            )
            reverse = calculate_execution(
                latest["OKX"],
                latest["BYBIT"],
                notional,
                okx_fee_bps,
                bybit_fee_bps,
            )
            for direction, result in zip(directions, (forward, reverse), strict=True):
                if result is not None:
                    accumulators[(notional, direction, "gross")].add(result.gross_edge_bps)
                    accumulators[(notional, direction, "net")].add(result.net_edge_bps)
                trackers[(notional, direction)].update(
                    ts_init_ns,
                    result,
                    bybit_book_age_ms,
                    okx_book_age_ms,
                )

    for tracker in trackers.values():
        tracker.finish(overlap_end_ns)

    bybit_timestamps = [state.ts_init_ns for state in bybit_states]
    okx_timestamps = [state.ts_init_ns for state in okx_states]
    results: dict[str, object] = {}
    for notional in notionals:
        notional_result: dict[str, object] = {}
        for direction in directions:
            tracker = trackers[(notional, direction)]
            latency_results: dict[str, object] = {}
            for latency_ms in latencies_ms:
                distribution = FloatDistribution()
                evaluated = 0
                skipped_outside_overlap = 0
                skipped_stale_book = 0
                for start in tracker.starts:
                    target_ns = start.ts_init_ns + round(latency_ms * 1_000_000)
                    if target_ns > overlap_end_ns:
                        skipped_outside_overlap += 1
                        continue
                    bybit_state = _state_as_of(bybit_states, bybit_timestamps, target_ns)
                    okx_state = _state_as_of(okx_states, okx_timestamps, target_ns)
                    if bybit_state is None or okx_state is None:
                        continue
                    delayed_maximum_book_age_ms = max(
                        (target_ns - bybit_state.ts_init_ns) / 1_000_000,
                        (target_ns - okx_state.ts_init_ns) / 1_000_000,
                    )
                    if (
                        max_book_age_ms is not None
                        and delayed_maximum_book_age_ms > max_book_age_ms
                    ):
                        skipped_stale_book += 1
                        continue
                    if direction == "buy_bybit_sell_okx":
                        delayed = calculate_execution(
                            bybit_state,
                            okx_state,
                            notional,
                            bybit_fee_bps,
                            okx_fee_bps,
                        )
                    else:
                        delayed = calculate_execution(
                            okx_state,
                            bybit_state,
                            notional,
                            okx_fee_bps,
                            bybit_fee_bps,
                        )
                    if delayed is not None:
                        distribution.add(delayed.net_edge_bps)
                        evaluated += 1
                latency_results[f"{latency_ms:g}"] = {
                    "latency_ms": latency_ms,
                    "episode_starts": len(tracker.starts),
                    "evaluated": evaluated,
                    "skipped_outside_overlap": skipped_outside_overlap,
                    "skipped_stale_book": skipped_stale_book,
                    "delayed_net_edge_bps": distribution.summary(),
                }
            notional_result[direction] = {
                "gross_edge_bps": accumulators[(notional, direction, "gross")].summary(),
                "net_edge_bps": accumulators[(notional, direction, "net")].summary(),
                "opportunity_episodes": tracker.summary(),
                "latency_scenarios": latency_results,
                "first_episode_starts": [
                    {
                        "ts_init_ns": start.ts_init_ns,
                        "bybit_book_age_ms": round(start.bybit_book_age_ms, 6),
                        "okx_book_age_ms": round(start.okx_book_age_ms, 6),
                        "signal": start.signal.to_dict(),
                    }
                    for start in tracker.starts[:100]
                ],
                "episode_start_rows_truncated": max(0, len(tracker.starts) - 100),
            }
        results[f"{notional:g}"] = notional_result

    return {
        "overlap_start_ns": overlap_start_ns,
        "overlap_end_ns": overlap_end_ns,
        "overlap_seconds": round((overlap_end_ns - overlap_start_ns) / 1_000_000_000, 6),
        "combined_book_update_observations": observations,
        "book_freshness": {
            "maximum_allowed_book_age_ms": max_book_age_ms,
            "evaluated_observations": evaluated_observations,
            "stale_observations_skipped": stale_observations_skipped,
            "maximum_age_of_either_book_ms": compared_book_age_ms.summary(),
        },
        "results_by_target_quote_notional": results,
    }


def analyze_group(args: argparse.Namespace) -> dict[str, object]:
    group_root = args.run_group.resolve()
    group_manifest_path = group_root / "manifest.json"
    group_manifest = json.loads(group_manifest_path.read_text(encoding="utf-8"))
    if not isinstance(group_manifest, dict):
        raise ValueError("Dual-run manifest must be a JSON object")
    if not group_manifest.get("analysis_ready") and not args.allow_incomplete:
        raise ValueError("Dual run is not marked analysis_ready; use --allow-incomplete to override")
    bases = group_manifest.get("bases")
    if not isinstance(bases, list) or not all(isinstance(base, str) for base in bases):
        raise ValueError("Dual-run manifest has no valid bases")

    report_by_base: dict[str, object] = {}
    max_levels = args.max_levels
    for base in bases:
        bybit_id = f"{base}USDT-LINEAR.BYBIT"
        okx_id = f"{base}-USDT-SWAP.OKX"
        bybit_states, bybit_load = load_book_states(
            group_root / "bybit" / "catalog",
            bybit_id,
            "BYBIT",
            max_levels,
        )
        okx_states, okx_load = load_book_states(
            group_root / "okx" / "catalog",
            okx_id,
            "OKX",
            max_levels,
        )
        report_by_base[base] = {
            "instruments": {"BYBIT": bybit_id, "OKX": okx_id},
            "loaded": {"BYBIT": bybit_load, "OKX": okx_load},
            **analyze_pair(
                bybit_states,
                okx_states,
                args.notionals,
                args.latencies_ms,
                args.bybit_taker_fee_bps,
                args.okx_taker_fee_bps,
                args.max_book_age_ms,
            ),
        }

    report: dict[str, object] = {
        "status": "ok",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_group": str(group_root),
        "run_group_id": group_manifest.get("run_group_id"),
        "method": (
            "event-driven latest-book-as-of local ts_init; signal never uses a venue state "
            "whose ts_init is later than the decision timestamp"
        ),
        "execution_model": {
            "target_quote_notionals": args.notionals,
            "latencies_ms": args.latencies_ms,
            "max_book_age_ms": args.max_book_age_ms,
            "bybit_taker_fee_bps": args.bybit_taker_fee_bps,
            "okx_taker_fee_bps": args.okx_taker_fee_bps,
            "same_base_quantity_on_both_legs": True,
            "depth": f"up to {max_levels} normalized L2 levels per side",
            "fees": "charged on buy quote cost and deducted from sell quote proceeds",
        },
        "limitations": [
            "Diagnostic book-crossing model; it does not model queue position or order acknowledgements.",
            "It assumes both IOC/marketable legs can execute at the recorded L2 state.",
            "It does not include inventory rebalancing, funding, liquidation, or transfer costs.",
            "Bybit raw matching-engine cts is not retained by NautilusTrader 2.0.0rc3.",
            "Latency scenarios reprice the first event of each positive episode at the book as-of t+delay.",
            "When max_book_age_ms is set, observations with either venue book older than the threshold are excluded.",
        ],
        "bases": report_by_base,
    }
    output_path = args.output or group_root / "analysis" / "cross_venue_report.json"
    if output_path.exists() and not args.force:
        raise FileExistsError(f"Refusing to overwrite existing report: {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report["output_path"] = str(output_path.resolve())
    atomic_json(output_path, report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-group", type=Path, required=True)
    parser.add_argument(
        "--notionals",
        type=lambda value: _parse_positive_floats(value, "notionals"),
        default=_parse_positive_floats("100,1000,10000", "notionals"),
        help="Comma-separated target quote notionals in USDT",
    )
    parser.add_argument(
        "--latencies-ms",
        type=lambda value: _parse_nonnegative_floats(value, "latencies-ms"),
        default=_parse_nonnegative_floats("0,20,50,100,150,250", "latencies-ms"),
    )
    parser.add_argument("--bybit-taker-fee-bps", type=float, default=10.0)
    parser.add_argument("--okx-taker-fee-bps", type=float, default=10.0)
    parser.add_argument("--max-levels", type=int, default=50)
    parser.add_argument(
        "--max-book-age-ms",
        type=float,
        help="Exclude observations when either latest venue book is older than this",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-incomplete", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.bybit_taker_fee_bps < 0 or args.okx_taker_fee_bps < 0:
        raise SystemExit("taker fee bps cannot be negative")
    if args.max_levels <= 0:
        raise SystemExit("--max-levels must be positive")
    if args.max_book_age_ms is not None and args.max_book_age_ms < 0:
        raise SystemExit("--max-book-age-ms must be non-negative")
    report = analyze_group(args)
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
