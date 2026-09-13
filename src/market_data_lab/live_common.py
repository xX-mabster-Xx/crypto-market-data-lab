"""Shared persistence and validation helpers for live CEX recorders."""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from nautilus_trader.model import BookType
from nautilus_trader.model import FundingRateUpdate
from nautilus_trader.model import IndexPriceUpdate
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import MarkPriceUpdate
from nautilus_trader.model import OrderBook
from nautilus_trader.model import OrderBookDelta
from nautilus_trader.model import QuoteTick
from nautilus_trader.model import RecordFlag
from nautilus_trader.model import TradeTick
from nautilus_trader.persistence import ParquetDataCatalog


STREAM_TYPES = (
    "order_book_deltas",
    "quotes",
    "trades",
    "mark_prices",
    "index_prices",
    "funding_rate_update",
)
SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9._-]+$")
PROXY_ENVIRONMENT_VARIABLES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


class ArrivalSidecar:
    """Persist one lightweight arrival audit row per normalized callback.

    ``ts_init`` remains the primary replay timestamp.  The monotonic callback
    timestamp provides an independent, system-wide ordering audit when two
    venue recorders run as separate processes on the same Linux host.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._output: Any | None = None
        self.records = 0

    def open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._output = self.path.open("a", encoding="utf-8", buffering=1)

    def write(
        self,
        *,
        data_type: str,
        instrument_id: object,
        ts_event_ns: int,
        ts_init_ns: int,
        records: int = 1,
        sequence: int | None = None,
    ) -> None:
        if self._output is None:
            return
        payload = {
            "data_type": data_type,
            "instrument_id": str(instrument_id),
            "ts_event_ns": ts_event_ns,
            "ts_init_ns": ts_init_ns,
            "callback_realtime_ns": time.time_ns(),
            "callback_monotonic_ns": time.monotonic_ns(),
            "records": records,
            "sequence": sequence,
        }
        self._output.write(json.dumps(payload, ensure_ascii=False) + "\n")
        self.records += 1

    def close(self) -> None:
        if self._output is not None:
            self._output.close()
            self._output = None

    def summary(self) -> dict[str, object]:
        return {
            "path": str(self.path.resolve()),
            "records": self.records,
            "timestamp": "CLOCK_MONOTONIC sampled at normalized Python callback",
        }


def atomic_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def default_run_id() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}-{uuid.uuid4().hex[:6]}"


def validate_run_id(run_id: str) -> None:
    if not SAFE_RUN_ID.fullmatch(run_id):
        raise ValueError("run-id may contain only letters, digits, dot, underscore, and hyphen")


def configure_process_network_route(proxy_url: str | None) -> dict[str, object]:
    """Make direct mode independent from ambient proxy environment variables.

    The recorder is a dedicated process. Removing these variables affects only
    it and its child libraries; it does not alter the user's shell or system
    proxy configuration. Explicit ``--proxy-url`` values are passed directly to
    venue clients and leave the environment untouched.
    """
    if proxy_url is not None:
        return {"mode": "explicit_proxy", "ambient_proxy_variables_ignored": []}

    removed = [name for name in PROXY_ENVIRONMENT_VARIABLES if name in os.environ]
    for name in removed:
        os.environ.pop(name, None)
    return {"mode": "direct", "ambient_proxy_variables_ignored": removed}


def _replay_book(records: list[OrderBookDelta], instrument_id: str) -> OrderBook:
    """Replay L2 records, replacing state when a venue sends a fresh snapshot."""
    parsed_id = InstrumentId.from_str(instrument_id)
    book = OrderBook(parsed_id, BookType.L2_MBP)
    in_snapshot = False
    snapshot_flag = RecordFlag.F_SNAPSHOT.value
    last_flag = RecordFlag.F_LAST.value

    for delta in records:
        is_snapshot = bool(delta.flags & snapshot_flag)
        if is_snapshot and not in_snapshot:
            # OKX snapshot deltas contain ADDs but no explicit CLEAR. Starting a
            # new book here also works for venues whose snapshot starts with CLEAR.
            book = OrderBook(parsed_id, BookType.L2_MBP)
        book.apply_delta(delta)
        in_snapshot = is_snapshot and not bool(delta.flags & last_flag)
    return book


def convert_streams(
    catalog_root: Path,
    run_id: str,
    actor_stats: dict[str, Any],
    max_validation_records: int,
) -> dict[str, Any]:
    """Convert completed Feather fragments and validate Parquet round trips.

    NautilusTrader 2.0.0rc3 converts each auto-flushed Feather file separately.
    Adjacent files can contain the same ``ts_init`` at their boundary, which the
    catalog rejects as overlapping intervals. Decode the completed run first and
    write one Parquet interval per type and instrument to preserve every record.
    """
    catalog = ParquetDataCatalog(str(catalog_root))
    stream_root = catalog_root / "live" / run_id
    conversion: dict[str, Any] = {}

    records_by_type: dict[str, dict[str, list[Any]]] = {
        data_type: defaultdict(list) for data_type in STREAM_TYPES
    }
    type_names = {
        OrderBookDelta: "order_book_deltas",
        QuoteTick: "quotes",
        TradeTick: "trades",
        MarkPriceUpdate: "mark_prices",
        IndexPriceUpdate: "index_prices",
        FundingRateUpdate: "funding_rate_update",
    }
    for record in catalog.read_live_run(run_id):
        data_type = type_names.get(type(record))
        if data_type is not None:
            records_by_type[data_type][str(record.instrument_id)].append(record)

    writers = {
        "order_book_deltas": catalog.write_order_book_deltas,
        "quotes": catalog.write_quote_ticks,
        "trades": catalog.write_trade_ticks,
        "mark_prices": catalog.write_mark_price_updates,
        "index_prices": catalog.write_index_price_updates,
    }

    for data_type in STREAM_TYPES:
        feather_files = list((stream_root / data_type).rglob("*.feather"))
        if not feather_files:
            conversion[data_type] = {"status": "no_data", "feather_files": 0}
            continue

        grouped_records = records_by_type[data_type]
        if data_type == "funding_rate_update":
            # FundingRateUpdate currently has no dedicated Python catalog write
            # method. It is sparse, so native per-fragment conversion is safe.
            catalog.convert_stream_to_data(run_id, data_type, "live")
        else:
            writer = writers[data_type]
            for records in grouped_records.values():
                writer(records)

        parquet_files = list((catalog_root / "data" / data_type).rglob("*.parquet"))
        conversion[data_type] = {
            "status": "converted",
            "feather_files": len(feather_files),
            "feather_bytes": sum(path.stat().st_size for path in feather_files),
            "decoded_records": sum(len(records) for records in grouped_records.values()),
            "identifiers": len(grouped_records),
            "parquet_files": len(parquet_files),
            "parquet_bytes": sum(path.stat().st_size for path in parquet_files),
        }

    query_methods = {
        "order_book_deltas": catalog.query_order_book_deltas,
        "quotes": catalog.query_quote_ticks,
        "trades": catalog.query_trade_ticks,
        "mark_prices": catalog.query_mark_price_updates,
        "index_prices": catalog.query_index_price_updates,
    }
    validations: dict[str, Any] = {}
    stream_stats = actor_stats.get("streams", {})
    for data_type, query in query_methods.items():
        for instrument_id, stats in stream_stats.get(data_type, {}).items():
            expected = int(stats["records"])
            key = f"{data_type}:{instrument_id}"
            if expected > max_validation_records:
                validations[key] = {"status": "skipped_large", "expected": expected}
                continue
            records = query([instrument_id])
            item: dict[str, Any] = {
                "status": "ok" if len(records) == expected else "count_mismatch",
                "expected": expected,
                "actual": len(records),
            }
            if data_type == "order_book_deltas" and records:
                book = _replay_book(records, instrument_id)
                try:
                    book.check_integrity()
                    item["book_integrity"] = "ok"
                except Exception as exc:
                    item["book_integrity"] = f"error: {exc}"
                bid = book.best_bid_price()
                ask = book.best_ask_price()
                item["bid"] = str(bid) if bid is not None else None
                item["ask"] = str(ask) if ask is not None else None
                item["bid_levels"] = len(book.bids())
                item["ask_levels"] = len(book.asks())
            validations[key] = item

    for instrument_id, stats in stream_stats.get("funding_rate_update", {}).items():
        expected = int(stats["records"])
        actual = len(records_by_type["funding_rate_update"].get(instrument_id, []))
        parquet_files = catalog.list_parquet_files("funding_rate_update", instrument_id)
        validations[f"funding_rate_update:{instrument_id}"] = {
            "status": "ok" if actual == expected and parquet_files else "count_mismatch",
            "expected": expected,
            "actual": actual,
            "parquet_files": len(parquet_files),
        }

    return {"conversion": conversion, "validations": validations}
