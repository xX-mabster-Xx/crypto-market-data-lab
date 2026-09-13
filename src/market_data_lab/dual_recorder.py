"""Run Bybit and OKX public recorders concurrently on one local timebase."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id


@dataclass(frozen=True)
class ChildResult:
    venue: str
    returncode: int
    timed_out: bool
    launch_monotonic_ns: int
    finish_monotonic_ns: int
    log_path: Path
    manifest_path: Path

    def to_dict(self, root: Path) -> dict[str, object]:
        return {
            "venue": self.venue,
            "returncode": self.returncode,
            "timed_out": self.timed_out,
            "launch_monotonic_ns": self.launch_monotonic_ns,
            "finish_monotonic_ns": self.finish_monotonic_ns,
            "wall_seconds": round(
                (self.finish_monotonic_ns - self.launch_monotonic_ns) / 1_000_000_000,
                6,
            ),
            "log_path": str(self.log_path.relative_to(root)),
            "manifest_path": str(self.manifest_path.relative_to(root)),
        }


def _parse_bases(value: str) -> list[str]:
    bases = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    if not bases or any(not base.isalnum() for base in bases):
        raise argparse.ArgumentTypeError("bases must be comma-separated alphanumeric symbols")
    return bases


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _child_failure(
    result: ChildResult,
    manifest: dict[str, Any] | None,
) -> dict[str, object] | None:
    manifest_status = manifest.get("status") if manifest is not None else "missing"
    if result.returncode == 0 and manifest_status == "ok":
        return None
    return {
        "returncode": result.returncode,
        "timed_out": result.timed_out,
        "manifest_status": manifest_status,
    }


def extract_book_window(manifest: dict[str, Any], instrument_id: str) -> tuple[int, int] | None:
    """Return the actual normalized book-arrival window for one instrument."""
    stats = manifest.get("actor_stats")
    if not isinstance(stats, dict):
        return None
    streams = stats.get("streams")
    if not isinstance(streams, dict):
        return None
    books = streams.get("order_book_deltas")
    if not isinstance(books, dict):
        return None
    item = books.get(instrument_id)
    if not isinstance(item, dict):
        return None
    first = item.get("first_init_ns")
    last = item.get("last_init_ns")
    if not isinstance(first, int) or not isinstance(last, int) or last < first:
        return None
    return first, last


def calculate_overlaps(
    bybit_manifest: dict[str, Any],
    okx_manifest: dict[str, Any],
    bases: list[str],
    requested_duration_seconds: float,
) -> dict[str, dict[str, object]]:
    overlaps: dict[str, dict[str, object]] = {}
    for base in bases:
        bybit_id = f"{base}USDT-LINEAR.BYBIT"
        okx_id = f"{base}-USDT-SWAP.OKX"
        bybit_window = extract_book_window(bybit_manifest, bybit_id)
        okx_window = extract_book_window(okx_manifest, okx_id)
        if bybit_window is None or okx_window is None:
            overlaps[base] = {
                "status": "missing_stream",
                "bybit_instrument_id": bybit_id,
                "okx_instrument_id": okx_id,
            }
            continue
        start_ns = max(bybit_window[0], okx_window[0])
        end_ns = min(bybit_window[1], okx_window[1])
        duration_seconds = max(0.0, (end_ns - start_ns) / 1_000_000_000)
        ratio = (
            duration_seconds / requested_duration_seconds
            if requested_duration_seconds > 0
            else 0.0
        )
        overlaps[base] = {
            "status": "ok" if duration_seconds > 0 else "no_overlap",
            "bybit_instrument_id": bybit_id,
            "okx_instrument_id": okx_id,
            "bybit_first_init_ns": bybit_window[0],
            "bybit_last_init_ns": bybit_window[1],
            "okx_first_init_ns": okx_window[0],
            "okx_last_init_ns": okx_window[1],
            "overlap_start_ns": start_ns,
            "overlap_end_ns": end_ns,
            "overlap_seconds": round(duration_seconds, 6),
            "requested_duration_ratio": round(ratio, 6),
        }
    return overlaps


async def _terminate_process_group(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(process.wait(), timeout=5.0)
    except TimeoutError:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()


async def _run_child(
    venue: str,
    command: list[str],
    log_path: Path,
    manifest_path: Path,
    timeout_seconds: float,
) -> ChildResult:
    launch_ns = time.monotonic_ns()
    with log_path.open("wb") as output:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=output,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
        timed_out = False
        try:
            returncode = await asyncio.wait_for(process.wait(), timeout=timeout_seconds)
        except TimeoutError:
            timed_out = True
            await _terminate_process_group(process)
            returncode = process.returncode if process.returncode is not None else -signal.SIGKILL
    return ChildResult(
        venue=venue,
        returncode=returncode,
        timed_out=timed_out,
        launch_monotonic_ns=launch_ns,
        finish_monotonic_ns=time.monotonic_ns(),
        log_path=log_path,
        manifest_path=manifest_path,
    )


def _child_command(
    module: str,
    symbols: str,
    duration_seconds: float,
    book_depth: int,
    group_root: Path,
    run_id: str,
    *,
    proxy_url: str | None,
    enable_open_interest: bool,
    enable_clock_sync: bool,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        "-m",
        module,
        "--symbols",
        symbols,
        "--duration-seconds",
        str(duration_seconds),
        "--book-depth",
        str(book_depth),
        "--output-root",
        str(group_root),
        "--run-id",
        run_id,
    ]
    if not enable_open_interest:
        command.append("--disable-open-interest")
    if not enable_clock_sync:
        command.append("--disable-clock-sync")
    if proxy_url is not None:
        command.extend(("--proxy-url", proxy_url))
    return command


async def collect_dual(args: argparse.Namespace) -> dict[str, object]:
    group_id = args.run_group_id or default_run_id()
    validate_run_id(group_id)
    group_root = args.output_root / group_id
    if group_root.exists():
        raise FileExistsError(f"Refusing to overwrite existing run group: {group_root}")
    group_root.mkdir(parents=True)

    bybit_symbols = ",".join(f"{base}USDT" for base in args.bases)
    okx_symbols = ",".join(f"{base}-USDT-SWAP" for base in args.bases)
    timeout_seconds = args.process_timeout_seconds or args.duration_seconds + 120.0
    bybit_command = _child_command(
        "market_data_lab.live_bybit",
        bybit_symbols,
        args.duration_seconds,
        args.book_depth,
        group_root,
        "bybit",
        proxy_url=args.proxy_url,
        enable_open_interest=args.enable_open_interest,
        enable_clock_sync=args.enable_clock_sync,
    )
    bybit_command.extend(("--transport-backend", args.bybit_transport_backend))
    okx_command = _child_command(
        "market_data_lab.live_okx",
        okx_symbols,
        args.duration_seconds,
        args.book_depth,
        group_root,
        "okx",
        proxy_url=args.proxy_url,
        enable_open_interest=args.enable_open_interest,
        enable_clock_sync=args.enable_clock_sync,
    )

    started_at = datetime.now(UTC)
    parent_start_ns = time.monotonic_ns()
    bybit_task = asyncio.create_task(
        _run_child(
            "BYBIT",
            bybit_command,
            group_root / "bybit.log",
            group_root / "bybit" / "manifest.json",
            timeout_seconds,
        ),
    )
    okx_task = asyncio.create_task(
        _run_child(
            "OKX",
            okx_command,
            group_root / "okx.log",
            group_root / "okx" / "manifest.json",
            timeout_seconds,
        ),
    )
    bybit_result, okx_result = await asyncio.gather(bybit_task, okx_task)
    parent_finish_ns = time.monotonic_ns()
    stopped_at = datetime.now(UTC)

    bybit_manifest = _load_json(bybit_result.manifest_path)
    okx_manifest = _load_json(okx_result.manifest_path)
    overlaps = (
        calculate_overlaps(
            bybit_manifest,
            okx_manifest,
            args.bases,
            args.duration_seconds,
        )
        if bybit_manifest is not None and okx_manifest is not None
        else {}
    )
    child_failures: dict[str, dict[str, object]] = {}
    for result, child_manifest in (
        (bybit_result, bybit_manifest),
        (okx_result, okx_manifest),
    ):
        failure = _child_failure(result, child_manifest)
        if failure is not None:
            child_failures[result.venue] = failure
    insufficient_overlap = {
        base: item
        for base, item in overlaps.items()
        if item.get("status") != "ok"
        or float(item.get("requested_duration_ratio", 0.0)) < args.min_overlap_ratio
    }
    status = "ok" if not child_failures and not insufficient_overlap else "error"
    manifest: dict[str, object] = {
        "status": status,
        "run_group_id": group_id,
        "bases": args.bases,
        "duration_requested_seconds": args.duration_seconds,
        "book_depth_requested": args.book_depth,
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_wall_seconds": round(
            (parent_finish_ns - parent_start_ns) / 1_000_000_000,
            6,
        ),
        "network_route": {
            "mode": "explicit_proxy" if args.proxy_url is not None else "direct",
            "proxy_value_persisted": False,
            "bybit_websocket_transport_backend": args.bybit_transport_backend,
        },
        "rest_polling": {
            "open_interest": args.enable_open_interest,
            "clock_sync": args.enable_clock_sync,
        },
        "children": {
            "BYBIT": bybit_result.to_dict(group_root),
            "OKX": okx_result.to_dict(group_root),
        },
        "launch_skew_ms": round(
            abs(bybit_result.launch_monotonic_ns - okx_result.launch_monotonic_ns) / 1_000_000,
            6,
        ),
        "overlaps": overlaps,
        "minimum_overlap_ratio": args.min_overlap_ratio,
        "child_failures": child_failures,
        "insufficient_overlap": insufficient_overlap,
        "analysis_ready": status == "ok",
        "timestamp_policy": {
            "primary": "ts_init (same-host local arrival scale)",
            "audit": "callback_monotonic_ns in each child arrivals.jsonl",
            "event_time_warning": (
                "Bybit normalized order books retain message ts, not raw matching-engine cts"
            ),
        },
    }
    atomic_json(group_root / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bases", type=_parse_bases, default=_parse_bases("SOL"))
    parser.add_argument("--duration-seconds", type=float, default=600.0)
    parser.add_argument("--book-depth", type=int, choices=(1, 50), default=50)
    parser.add_argument("--run-group-id")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/dual"))
    parser.add_argument("--min-overlap-ratio", type=float, default=0.70)
    parser.add_argument("--process-timeout-seconds", type=float)
    parser.add_argument("--enable-open-interest", action="store_true")
    parser.add_argument("--enable-clock-sync", action="store_true")
    parser.add_argument("--proxy-url", help="Explicit per-client proxy; direct by default")
    parser.add_argument(
        "--bybit-transport-backend",
        choices=("sockudo", "tungstenite"),
        default="tungstenite",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.duration_seconds <= 0:
        raise SystemExit("--duration-seconds must be positive")
    if not 0 < args.min_overlap_ratio <= 1:
        raise SystemExit("--min-overlap-ratio must be in (0, 1]")
    if args.process_timeout_seconds is not None and args.process_timeout_seconds <= 0:
        raise SystemExit("--process-timeout-seconds must be positive")
    manifest = asyncio.run(collect_dual(args))
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    if manifest["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
