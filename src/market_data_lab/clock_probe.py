"""Probe local clock alignment against public Bybit and OKX time endpoints."""

from __future__ import annotations

import argparse
import asyncio
import json
import urllib.request
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from market_data_lab.clock_sync import ClockOffsetEstimator
from market_data_lab.clock_sync import LocalClockContinuity
from market_data_lab.clock_sync import calculate_clock_sample
from market_data_lab.clock_sync import read_local_clocks
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route


VENUES = {
    "BYBIT": {
        "url": "https://api.bybit.com/v5/market/time",
        "endpoint": "GET /v5/market/time",
        "resolution_ns": 1,
    },
    "OKX": {
        "url": "https://www.okx.com/api/v5/public/time",
        "endpoint": "GET /api/v5/public/time",
        "resolution_ns": 1_000_000,
    },
}


def parse_server_time_ns(venue: str, payload: Any) -> int:
    if venue == "BYBIT":
        if not isinstance(payload, dict) or int(payload.get("retCode", -1)) != 0:
            raise ValueError(f"Bybit server-time error response: {payload!r}")
        result = payload.get("result")
        if not isinstance(result, dict) or not result.get("timeNano"):
            raise ValueError(f"Bybit server-time response has no timeNano: {payload!r}")
        value = int(result["timeNano"])
    elif venue == "OKX":
        if not isinstance(payload, dict) or str(payload.get("code")) != "0":
            raise ValueError(f"OKX server-time error response: {payload!r}")
        rows = payload.get("data")
        if not isinstance(rows, list) or not rows or not rows[0].get("ts"):
            raise ValueError(f"OKX server-time response has no ts: {payload!r}")
        value = int(rows[0]["ts"]) * 1_000_000
    else:
        raise ValueError(f"Unsupported venue: {venue}")

    if value <= 0:
        raise ValueError(f"Invalid {venue} server time: {value}")
    return value


def _fetch_json_sync(url: str, proxy_url: str | None, timeout_seconds: float) -> Any:
    if proxy_url and not proxy_url.startswith(("http://", "https://")):
        raise ValueError("clock probe supports direct, HTTP, or HTTPS proxy URLs")
    handler = (
        urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        if proxy_url
        else urllib.request.ProxyHandler({})
    )
    opener = urllib.request.build_opener(handler)
    request = urllib.request.Request(url, headers={"User-Agent": "crypto-market-data-lab/0.1"})
    with opener.open(request, timeout=timeout_seconds) as response:
        return json.loads(response.read())


async def probe_clocks(
    venues: Sequence[str],
    *,
    samples: int,
    interval_seconds: float,
    timeout_seconds: float,
    proxy_url: str | None,
    fetch_json: Callable[[str, str | None, float], Any] = _fetch_json_sync,
) -> dict[str, object]:
    """Collect bounded HTTP clock diagnostics for one or more venues."""
    network_route = configure_process_network_route(proxy_url)
    estimators = {venue: ClockOffsetEstimator() for venue in venues}
    errors = {venue: 0 for venue in venues}
    last_errors: dict[str, str | None] = {venue: None for venue in venues}
    local_clock = LocalClockContinuity()
    started_at = datetime.now(UTC)

    async def sample_venue(venue: str) -> None:
        config = VENUES[venue]
        request = read_local_clocks()
        local_clock.add(request)
        try:
            payload = await asyncio.to_thread(
                fetch_json,
                str(config["url"]),
                proxy_url,
                timeout_seconds,
            )
            receive = read_local_clocks()
            local_clock.add(receive)
            sample = calculate_clock_sample(
                request,
                receive,
                server_time_ns=parse_server_time_ns(venue, payload),
                server_time_resolution_ns=int(config["resolution_ns"]),
                venue=venue,
                endpoint=str(config["endpoint"]),
            )
            estimators[venue].add(sample)
        except Exception as exc:
            local_clock.add(read_local_clocks())
            errors[venue] += 1
            last_errors[venue] = f"{type(exc).__name__}: {exc}"

    for index in range(samples):
        await asyncio.gather(*(sample_venue(venue) for venue in venues))
        if index + 1 < samples and interval_seconds > 0:
            await asyncio.sleep(interval_seconds)

    local_clock.add(read_local_clocks())
    stopped_at = datetime.now(UTC)
    successful = [estimators[venue].count for venue in venues]
    if all(count == samples for count in successful):
        status = "ok"
    elif any(successful):
        status = "partial"
    else:
        status = "error"

    return {
        "status": status,
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_wall_seconds": round((stopped_at - started_at).total_seconds(), 6),
        "requested_samples_per_venue": samples,
        "interval_seconds": interval_seconds,
        "timeout_seconds": timeout_seconds,
        "api_credentials_used": False,
        "network_route": network_route,
        "venues": {
            venue: {
                "endpoint": VENUES[venue]["endpoint"],
                "server_time_resolution_ns": VENUES[venue]["resolution_ns"],
                "errors": errors[venue],
                "last_error": last_errors[venue],
                **estimators[venue].to_dict(),
            }
            for venue in venues
        },
        "local_clock_continuity": local_clock.to_dict(),
        "warning": (
            "HTTP midpoint offsets are diagnostics with route-asymmetry uncertainty; "
            "they are not NTP corrections and are not applied to market-data timestamps"
        ),
    }


def _parse_venues(value: str) -> list[str]:
    venues = list(dict.fromkeys(item.strip().upper() for item in value.split(",") if item.strip()))
    invalid = [venue for venue in venues if venue not in VENUES]
    if not venues or invalid:
        raise argparse.ArgumentTypeError(
            f"venues must be comma-separated values from {', '.join(VENUES)}",
        )
    return venues


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--venues", type=_parse_venues, default=_parse_venues("BYBIT,OKX"))
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--proxy-url", help="Explicit HTTP/HTTPS proxy; direct by default")
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.samples <= 0:
        raise SystemExit("--samples must be positive")
    if args.interval_seconds < 0:
        raise SystemExit("--interval-seconds cannot be negative")
    if args.timeout_seconds <= 0:
        raise SystemExit("--timeout-seconds must be positive")
    report = asyncio.run(
        probe_clocks(
            args.venues,
            samples=args.samples,
            interval_seconds=args.interval_seconds,
            timeout_seconds=args.timeout_seconds,
            proxy_url=args.proxy_url,
        ),
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        atomic_json(args.output, report)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if report["status"] == "error":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
