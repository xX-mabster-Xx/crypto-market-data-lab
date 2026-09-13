# Portions of the venue decoder are adapted from NautilusTrader's LGPL-3.0 tutorial.
# SPDX-License-Identifier: LGPL-3.0-only

"""Import a public Bybit L2 archive into a NautilusTrader catalog.

The venue decoding follows the public Bybit archive schema and the event-boundary
semantics demonstrated by NautilusTrader's official order-book tutorial:
https://github.com/nautechsystems/nautilus_trader/blob/master/docs/tutorials/orderbook_data.py

No account credentials are used, and this module contains no execution client.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from functools import reduce
from os import PathLike
from pathlib import Path
from zipfile import ZipFile, is_zipfile

import pandas as pd
from nautilus_trader.model import (
    BookAction,
    BookOrder,
    BookType,
    CryptoPerpetual,
    Currency,
    InstrumentId,
    OrderBook,
    OrderBookDelta,
    OrderSide,
    Price,
    Quantity,
    RecordFlag,
    Symbol,
    Venue,
)
from nautilus_trader.persistence import ParquetDataCatalog


@dataclass(frozen=True)
class Increment:
    precision: int
    value: Decimal


@dataclass(frozen=True)
class ImportSummary:
    archive: str
    archive_sha256: str
    source_url: str | None
    catalog: str
    symbol: str
    instrument_id: str
    source_rows: int
    stored_deltas: int
    unique_sequences: int
    timestamp_regressions: int
    sequence_regressions: int
    first_event_ns: int
    last_event_ns: int
    price_precision: int
    price_increment: str
    size_precision: int
    size_increment: str
    final_bid: str
    final_ask: str
    final_spread: str
    final_bid_levels: int
    final_ask_levels: int
    imported_at: str


def _event_rows(lines: Iterable[bytes]) -> Iterator[list[dict[str, object]]]:
    """Yield one normalized group per exchange WebSocket event."""
    for line in lines:
        message = json.loads(line)
        data = message["data"]
        timestamp = pd.to_datetime(int(message["ts"]) * 1_000_000, unit="ns", utc=True)
        is_snapshot = message["type"] == "snapshot"
        sides = [("BUY", data.get("b") or []), ("SELL", data.get("a") or [])]
        event: list[dict[str, object]] = []

        if is_snapshot:
            populated = next(((side, levels) for side, levels in sides if levels), ("BUY", []))
            side, levels = populated
            event.append(
                {
                    "timestamp": timestamp,
                    "instrument_id": f"{data['s']}-LINEAR.BYBIT",
                    "action": "CLEAR",
                    "side": side,
                    "price": levels[0][0] if levels else "0",
                    "size": "0",
                    "order_id": 0,
                    "flags": RecordFlag.F_SNAPSHOT.value,
                    "sequence": data["seq"],
                },
            )

        for side, levels in sides:
            for price, size in levels:
                if is_snapshot:
                    action = "ADD"
                elif Decimal(size) == 0:
                    action = "DELETE"
                else:
                    action = "UPDATE"
                event.append(
                    {
                        "timestamp": timestamp,
                        "instrument_id": f"{data['s']}-LINEAR.BYBIT",
                        "action": action,
                        "side": side,
                        "price": price,
                        "size": size,
                        "order_id": 0,
                        "flags": RecordFlag.F_SNAPSHOT.value if is_snapshot else 0,
                        "sequence": data["seq"],
                    },
                )

        if event:
            yield event


def load_archive(file_path: str | PathLike[str], limit: int | None) -> pd.DataFrame:
    """Read complete Bybit events up to ``limit`` normalized rows."""
    if not is_zipfile(file_path):
        raise ValueError(f"Expected a Bybit ZIP archive: {file_path}")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    rows: list[dict[str, object]] = []
    with ZipFile(file_path) as archive:
        members = archive.namelist()
        if len(members) != 1:
            raise ValueError(f"Expected exactly one file in archive, found {len(members)}")
        with archive.open(members[0]) as source:
            for event in _event_rows(source):
                if limit is not None and len(rows) + len(event) > limit:
                    break
                rows.extend(event)

    if not rows:
        raise ValueError("Archive produced no complete events")
    frame = pd.DataFrame(rows).set_index("timestamp")
    return frame.astype({"order_id": int, "flags": int, "sequence": int})


def _decimal_places(value: object) -> int:
    decimal = Decimal(str(value))
    if decimal == 0:
        return 0
    return max(0, -decimal.normalize().as_tuple().exponent)


def infer_increment(values: Sequence[object], *, use_differences: bool) -> Increment:
    """Infer decimal precision and the smallest common grid unit in a sample."""
    decimals = [Decimal(str(value)) for value in values if Decimal(str(value)) != 0]
    if not decimals:
        raise ValueError("Cannot infer an increment from all-zero values")

    precision = max(_decimal_places(value) for value in decimals)
    scale = 10**precision
    scaled = sorted({int(value * scale) for value in decimals})
    if use_differences:
        candidates = [right - left for left, right in zip(scaled, scaled[1:]) if right > left]
    else:
        candidates = [abs(value) for value in scaled if value]
    if not candidates:
        unit = 1
    else:
        unit = reduce(math.gcd, candidates)
    return Increment(precision=precision, value=Decimal(unit) / Decimal(scale))


def make_instrument(frame: pd.DataFrame) -> CryptoPerpetual:
    instrument_ids = frame["instrument_id"].unique()
    if len(instrument_ids) != 1:
        raise ValueError(f"Expected one instrument, found {instrument_ids!r}")

    instrument_id = str(instrument_ids[0])
    symbol = instrument_id.removesuffix("-LINEAR.BYBIT")
    if not symbol.endswith("USDT"):
        raise ValueError(f"Only linear USDT perpetuals are supported, got {symbol}")
    base = symbol.removesuffix("USDT")

    non_clear = frame[frame["action"] != "CLEAR"]
    price_grid = infer_increment(non_clear["price"].tolist(), use_differences=True)
    nonzero_sizes = non_clear[non_clear["size"].map(Decimal) != 0]["size"].tolist()
    size_grid = infer_increment(nonzero_sizes, use_differences=False)

    return CryptoPerpetual(
        instrument_id=InstrumentId(Symbol(f"{symbol}-LINEAR"), Venue("BYBIT")),
        raw_symbol=Symbol(symbol),
        base_currency=Currency.from_str(base),
        quote_currency=Currency.from_str("USDT"),
        settlement_currency=Currency.from_str("USDT"),
        is_inverse=False,
        price_precision=price_grid.precision,
        size_precision=size_grid.precision,
        price_increment=Price.from_decimal_dp(price_grid.value, price_grid.precision),
        size_increment=Quantity.from_decimal_dp(size_grid.value, size_grid.precision),
        ts_event=0,
        ts_init=0,
    )


def deltas_from_frame(frame: pd.DataFrame, instrument: CryptoPerpetual) -> list[OrderBookDelta]:
    """Convert normalized rows while preserving snapshots and event boundaries."""
    expected = str(instrument.id)
    if not frame["instrument_id"].eq(expected).all():
        raise ValueError(f"Expected only {expected} order-book data")

    rows = list(frame.itertuples())
    result: list[OrderBookDelta] = []
    for index, row in enumerate(rows):
        ts = int(row.Index.value)
        next_row = rows[index + 1] if index + 1 < len(rows) else None
        flags = int(row.flags)
        next_starts_snapshot = next_row is not None and next_row.action == "CLEAR"
        snapshot_continues = (
            next_row is not None
            and not next_starts_snapshot
            and bool(flags & RecordFlag.F_SNAPSHOT.value)
            and bool(int(next_row.flags) & RecordFlag.F_SNAPSHOT.value)
        )
        event_continues = (
            next_row is not None
            and not next_starts_snapshot
            and next_row.Index == row.Index
            and next_row.sequence == row.sequence
        )
        if not snapshot_continues and not event_continues:
            flags |= RecordFlag.F_LAST.value

        result.append(
            OrderBookDelta(
                instrument_id=instrument.id,
                action=BookAction.from_str(row.action),
                order=BookOrder(
                    side=OrderSide.from_str(row.side),
                    price=Price.from_decimal_dp(Decimal(str(row.price)), instrument.price_precision),
                    size=Quantity.from_decimal_dp(Decimal(str(row.size)), instrument.size_precision),
                    order_id=int(row.order_id),
                ),
                flags=flags,
                sequence=int(row.sequence),
                ts_event=ts,
                ts_init=ts,
            ),
        )
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def import_archive(
    archive_path: Path,
    catalog_path: Path,
    limit: int | None,
    source_url: str | None,
) -> ImportSummary:
    if catalog_path.exists():
        if not catalog_path.is_dir() or any(catalog_path.iterdir()):
            raise FileExistsError(
                f"Catalog path is not an empty directory; refusing to overwrite it: {catalog_path}",
            )
    else:
        catalog_path.mkdir(parents=True)

    frame = load_archive(archive_path, limit)
    timestamp_regressions = int((frame.index.to_series().diff().dropna() < pd.Timedelta(0)).sum())
    event_sequences = frame.groupby([frame.index, "sequence"], sort=False).size().index
    sequence_values = [int(sequence) for _, sequence in event_sequences]
    sequence_regressions = sum(
        current < previous for previous, current in zip(sequence_values, sequence_values[1:])
    )
    if timestamp_regressions:
        raise RuntimeError(f"Source contains {timestamp_regressions} timestamp regressions")
    if sequence_regressions:
        raise RuntimeError(f"Source contains {sequence_regressions} sequence regressions")

    instrument = make_instrument(frame)
    deltas = deltas_from_frame(frame, instrument)
    deltas.sort(key=lambda delta: delta.ts_init)

    catalog = ParquetDataCatalog(str(catalog_path))
    catalog.write_instruments([instrument])
    catalog.write_order_book_deltas(deltas)

    stored = catalog.query_order_book_deltas(identifiers=[str(instrument.id)])
    if len(stored) != len(deltas):
        raise RuntimeError(f"Catalog count mismatch: wrote {len(deltas)}, read {len(stored)}")

    book = OrderBook(instrument.id, BookType.L2_MBP)
    for delta in stored:
        book.apply_delta(delta)
    book.check_integrity()

    bid = book.best_bid_price()
    ask = book.best_ask_price()
    if bid is None or ask is None:
        raise RuntimeError("Replay ended without a valid two-sided order book")
    spread = ask.as_decimal() - bid.as_decimal()

    summary = ImportSummary(
        archive=str(archive_path.resolve()),
        archive_sha256=_sha256(archive_path),
        source_url=source_url,
        catalog=str(catalog_path.resolve()),
        symbol=str(instrument.raw_symbol),
        instrument_id=str(instrument.id),
        source_rows=len(frame),
        stored_deltas=len(stored),
        unique_sequences=int(frame["sequence"].nunique()),
        timestamp_regressions=timestamp_regressions,
        sequence_regressions=sequence_regressions,
        first_event_ns=int(frame.index[0].value),
        last_event_ns=int(frame.index[-1].value),
        price_precision=instrument.price_precision,
        price_increment=str(instrument.price_increment),
        size_precision=instrument.size_precision,
        size_increment=str(instrument.size_increment),
        final_bid=str(bid),
        final_ask=str(ask),
        final_spread=str(spread),
        final_bid_levels=len(book.bids()),
        final_ask_levels=len(book.asks()),
        imported_at=datetime.now(UTC).isoformat(),
    )
    manifest_path = catalog_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(asdict(summary), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True, help="Bybit ob200/ob500 ZIP")
    parser.add_argument("--catalog", type=Path, required=True, help="New catalog directory")
    parser.add_argument(
        "--limit",
        type=int,
        default=250_000,
        help="Maximum normalized rows; complete exchange events only (default: 250000)",
    )
    parser.add_argument("--source-url", help="Optional provenance URL stored in manifest")
    return parser


def main() -> None:
    args = _parser().parse_args()
    summary = import_archive(
        archive_path=args.archive,
        catalog_path=args.catalog,
        limit=args.limit,
        source_url=args.source_url,
    )
    print(json.dumps(asdict(summary), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
