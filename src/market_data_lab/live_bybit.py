"""Record public Bybit perpetual market data without account credentials.

This process registers only a market-data client. It has no execution client and
cannot place orders.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import nautilus_trader
from nautilus_trader.adapters.bybit import BYBIT
from nautilus_trader.adapters.bybit import BybitDataClientConfig
from nautilus_trader.adapters.bybit import BybitDataClientFactory
from nautilus_trader.adapters.bybit import BybitEnvironment
from nautilus_trader.adapters.bybit import BybitHttpClient
from nautilus_trader.adapters.bybit import BybitProductType
from nautilus_trader.adapters.bybit import BybitRawHttpClient
from nautilus_trader.adapters.bybit import BybitTickersParams
from nautilus_trader.common import DataActor
from nautilus_trader.common import Environment
from nautilus_trader.common import ImportableActorConfig
from nautilus_trader.common import TimeEvent
from nautilus_trader.config import DataActorConfig
from nautilus_trader.live import LiveNode
from nautilus_trader.model import ActorId
from nautilus_trader.model import BookAction
from nautilus_trader.model import BookOrder
from nautilus_trader.model import BookType
from nautilus_trader.model import ClientId
from nautilus_trader.model import FundingRateUpdate
from nautilus_trader.model import IndexPriceUpdate
from nautilus_trader.model import InstrumentId
from nautilus_trader.model import MarkPriceUpdate
from nautilus_trader.model import OrderBookDelta
from nautilus_trader.model import OrderBookDeltas
from nautilus_trader.model import Price
from nautilus_trader.model import Quantity
from nautilus_trader.model import QuoteTick
from nautilus_trader.model import TradeTick
from nautilus_trader.model import TraderId
from nautilus_trader.network import TransportBackend
from nautilus_trader.persistence import ParquetDataCatalog
from nautilus_trader.persistence import StreamingFeatherWriter

from market_data_lab.clock_sync import ClockOffsetEstimator
from market_data_lab.clock_sync import LocalClockContinuity
from market_data_lab.clock_sync import calculate_clock_sample
from market_data_lab.clock_sync import read_local_clocks
from market_data_lab.live_common import STREAM_TYPES
from market_data_lab.live_common import ArrivalSidecar
from market_data_lab.live_common import atomic_json
from market_data_lab.live_common import configure_process_network_route
from market_data_lab.live_common import convert_streams
from market_data_lab.live_common import default_run_id
from market_data_lab.live_common import validate_run_id
from market_data_lab.live_stats import BookSequenceMetric
from market_data_lab.live_stats import StreamMetric


STOP_TIMER = "live-recorder-stop"
BYBIT_TRANSPORT_BACKENDS = {
    "sockudo": TransportBackend.SOCKUDO,
    "tungstenite": TransportBackend.TUNGSTENITE,
}


def _normalize_clear_delta(
    delta: OrderBookDelta,
    price_precision: int,
    size_precision: int,
) -> OrderBookDelta:
    """Give a snapshot CLEAR the same schema metadata as the following levels."""
    if (
        delta.action != BookAction.CLEAR
        or (
            delta.order.price.precision == price_precision
            and delta.order.size.precision == size_precision
        )
    ):
        return delta

    zero_price = "0" if price_precision == 0 else f"0.{price_precision * '0'}"
    zero_size = "0" if size_precision == 0 else f"0.{size_precision * '0'}"
    return OrderBookDelta(
        instrument_id=delta.instrument_id,
        action=delta.action,
        order=BookOrder(
            side=delta.order.side,
            price=Price.from_str(zero_price),
            size=Quantity.from_str(zero_size),
            order_id=delta.order.order_id,
        ),
        flags=delta.flags,
        sequence=delta.sequence,
        ts_event=delta.ts_event,
        ts_init=delta.ts_init,
    )


class BybitRecorderConfig(DataActorConfig):
    def __init__(
        self,
        instrument_ids: Sequence[str],
        stream_path: str,
        stats_path: str,
        arrival_path: str | None = None,
        duration_seconds: float = 60.0,
        book_depth: int = 50,
        actor_id: ActorId | str | None = None,
        log_events: bool = False,
        log_commands: bool = False,
    ) -> None:
        self.actor_id = ActorId.from_str(actor_id) if isinstance(actor_id, str) else actor_id
        self.log_events = log_events
        self.log_commands = log_commands
        self.instrument_ids = list(instrument_ids)
        self.stream_path = stream_path
        self.stats_path = stats_path
        self.arrival_path = arrival_path
        self.duration_seconds = duration_seconds
        self.book_depth = book_depth


class BybitRecorder(DataActor):
    """Subscribe, persist with Nautilus's writer, and measure stream quality."""

    def __init__(self, config: BybitRecorderConfig) -> None:
        super().__init__(config)
        self._instrument_ids = [InstrumentId.from_str(value) for value in config.instrument_ids]
        self._stream_path = Path(config.stream_path)
        self._stats_path = Path(config.stats_path)
        self._arrival_sidecar = (
            ArrivalSidecar(Path(config.arrival_path)) if config.arrival_path is not None else None
        )
        self._duration_seconds = config.duration_seconds
        self._book_depth = config.book_depth
        self._writer: StreamingFeatherWriter | None = None
        self._writer_mode = "uninitialized"
        self._metrics: dict[str, dict[str, StreamMetric]] = defaultdict(dict)
        self._book_sequences: dict[str, BookSequenceMetric] = {}
        self._socket_events: list[dict[str, object]] = []
        self._started_ns: int | None = None
        self._stopped_ns: int | None = None
        self._finalized = False

    def on_start(self) -> None:
        self._started_ns = self.clock.timestamp_ns()
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.open()
        self._writer = StreamingFeatherWriter(
            path=str(self._stream_path),
            cache=self.cache,
            clock=self.clock,
            include_types=list(STREAM_TYPES),
            flush_interval_ms=1_000,
            rotation_mode=3,
            replace=False,
        )
        # A writer created from a Python actor is not attached to the live node's
        # Rust message bus by ``subscribe()`` in NautilusTrader 2.0.0rc3.  Route
        # the actor callbacks explicitly so persistence and quality metrics see
        # exactly the same messages.
        self._writer_mode = "manual_callbacks"

        client_id = ClientId.from_str(BYBIT)
        self.subscribe_socket_state()
        for instrument_id in self._instrument_ids:
            self.subscribe_book_deltas(
                instrument_id,
                BookType.L2_MBP,
                depth=self._book_depth,
                client_id=client_id,
                managed=True,
            )
            self.subscribe_quotes(instrument_id, client_id=client_id)
            self.subscribe_trades(instrument_id, client_id=client_id)
            self.subscribe_mark_prices(instrument_id, client_id=client_id)
            self.subscribe_index_prices(instrument_id, client_id=client_id)
            self.subscribe_funding_rates(instrument_id, client_id=client_id)

        if self._duration_seconds > 0:
            self.clock.set_time_alert_ns(
                STOP_TIMER,
                self.clock.timestamp_ns() + int(self._duration_seconds * 1_000_000_000),
            )

    def on_time_event(self, event: TimeEvent) -> None:
        if event.name == STOP_TIMER:
            self.shutdown_system(f"Bybit recorder completed {self._duration_seconds:g}s run")

    def _metric(self, data_type: str, instrument_id: InstrumentId) -> StreamMetric:
        key = str(instrument_id)
        metric = self._metrics[data_type].get(key)
        if metric is None:
            metric = StreamMetric()
            self._metrics[data_type][key] = metric
        return metric

    def _write_manually(self, value: object) -> None:
        if self._writer is not None:
            self._writer.write(value)

    def on_book_deltas(self, deltas: OrderBookDeltas) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="order_book_deltas",
                instrument_id=deltas.instrument_id,
                ts_event_ns=deltas.ts_event,
                ts_init_ns=deltas.ts_init,
                records=len(deltas.deltas),
                sequence=deltas.sequence,
            )
        self._metric("order_book_deltas", deltas.instrument_id).add(
            ts_event=deltas.ts_event,
            ts_init=deltas.ts_init,
            records=len(deltas.deltas),
        )
        key = str(deltas.instrument_id)
        sequence_metric = self._book_sequences.setdefault(key, BookSequenceMetric())
        snapshot = any(delta.action == BookAction.CLEAR for delta in deltas.deltas)
        update_ids = [delta.order.order_id for delta in deltas.deltas if delta.order.order_id]
        sequence_metric.add(
            update_id=update_ids[0] if update_ids else None,
            cross_sequence=deltas.sequence,
            snapshot=snapshot,
        )
        instrument = self.cache.instrument(deltas.instrument_id)
        if instrument is not None:
            price_precision = instrument.price_precision
            size_precision = instrument.size_precision
        else:  # defensive fallback; instruments normally load before subscriptions
            non_clear = [delta for delta in deltas.deltas if delta.action != BookAction.CLEAR]
            price_precision = max((delta.order.price.precision for delta in non_clear), default=0)
            size_precision = max((delta.order.size.precision for delta in non_clear), default=0)

        for delta in deltas.deltas:
            self._write_manually(
                _normalize_clear_delta(delta, price_precision, size_precision),
            )

    def on_quote(self, quote: QuoteTick) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="quotes",
                instrument_id=quote.instrument_id,
                ts_event_ns=quote.ts_event,
                ts_init_ns=quote.ts_init,
            )
        self._metric("quotes", quote.instrument_id).add(
            ts_event=quote.ts_event,
            ts_init=quote.ts_init,
        )
        self._write_manually(quote)

    def on_trade(self, trade: TradeTick) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="trades",
                instrument_id=trade.instrument_id,
                ts_event_ns=trade.ts_event,
                ts_init_ns=trade.ts_init,
            )
        self._metric("trades", trade.instrument_id).add(
            ts_event=trade.ts_event,
            ts_init=trade.ts_init,
        )
        self._write_manually(trade)

    def on_mark_price(self, update: MarkPriceUpdate) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="mark_prices",
                instrument_id=update.instrument_id,
                ts_event_ns=update.ts_event,
                ts_init_ns=update.ts_init,
            )
        self._metric("mark_prices", update.instrument_id).add(
            ts_event=update.ts_event,
            ts_init=update.ts_init,
        )
        self._write_manually(update)

    def on_index_price(self, update: IndexPriceUpdate) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="index_prices",
                instrument_id=update.instrument_id,
                ts_event_ns=update.ts_event,
                ts_init_ns=update.ts_init,
            )
        self._metric("index_prices", update.instrument_id).add(
            ts_event=update.ts_event,
            ts_init=update.ts_init,
        )
        self._write_manually(update)

    def on_funding_rate(self, update: FundingRateUpdate) -> None:
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.write(
                data_type="funding_rate_update",
                instrument_id=update.instrument_id,
                ts_event_ns=update.ts_event,
                ts_init_ns=update.ts_init,
            )
        self._metric("funding_rate_update", update.instrument_id).add(
            ts_event=update.ts_event,
            ts_init=update.ts_init,
        )
        self._write_manually(update)

    def on_socket_state(self, event: Any) -> None:
        if len(self._socket_events) < 200:
            self._socket_events.append(
                {
                    "endpoint": event.endpoint,
                    "state": str(event.state),
                    "ts_event": event.ts_event,
                    "ts_init": event.ts_init,
                },
            )

    def _unsubscribe(self) -> None:
        client_id = ClientId.from_str(BYBIT)
        for instrument_id in self._instrument_ids:
            self.unsubscribe_book_deltas(instrument_id, client_id=client_id)
            self.unsubscribe_quotes(instrument_id, client_id=client_id)
            self.unsubscribe_trades(instrument_id, client_id=client_id)
            self.unsubscribe_mark_prices(instrument_id, client_id=client_id)
            self.unsubscribe_index_prices(instrument_id, client_id=client_id)
            self.unsubscribe_funding_rates(instrument_id, client_id=client_id)
        self.unsubscribe_socket_state()

    def _book_summaries(self) -> dict[str, dict[str, object]]:
        summaries: dict[str, dict[str, object]] = {}
        for instrument_id in self._instrument_ids:
            book = self.cache.order_book(instrument_id)
            if book is None:
                summaries[str(instrument_id)] = {"available": False}
                continue
            try:
                book.check_integrity()
                integrity = "ok"
            except Exception as exc:  # pragma: no cover - requires corrupt live book
                integrity = f"error: {exc}"
            bid = book.best_bid_price()
            ask = book.best_ask_price()
            summaries[str(instrument_id)] = {
                "available": True,
                "integrity": integrity,
                "bid": str(bid) if bid is not None else None,
                "ask": str(ask) if ask is not None else None,
                "bid_levels": len(book.bids()),
                "ask_levels": len(book.asks()),
                "updates": book.update_count,
            }
        return summaries

    def _finalize(self) -> None:
        if self._finalized:
            return
        self._finalized = True
        self._stopped_ns = self.clock.timestamp_ns()
        books = self._book_summaries()
        if self._writer is not None:
            self._writer.close()
        if self._arrival_sidecar is not None:
            self._arrival_sidecar.close()

        payload = {
            "writer_mode": self._writer_mode,
            "started_ns": self._started_ns,
            "stopped_ns": self._stopped_ns,
            "streams": {
                data_type: {instrument: metric.to_dict() for instrument, metric in metrics.items()}
                for data_type, metrics in self._metrics.items()
            },
            "book_sequences": {
                instrument: metric.to_dict() for instrument, metric in self._book_sequences.items()
            },
            "books": books,
            "socket_events": self._socket_events,
            "arrival_sidecar": (
                self._arrival_sidecar.summary() if self._arrival_sidecar is not None else None
            ),
        }
        atomic_json(self._stats_path, payload)

    def on_stop(self) -> None:
        self._unsubscribe()
        self._finalize()

    def on_fault(self) -> None:
        self._finalize()

    def on_dispose(self) -> None:
        self._finalize()


def parse_server_time_ns(response: Any) -> int:
    """Extract Bybit's nanosecond server timestamp from the public response."""
    result = getattr(response, "result", response)
    if isinstance(result, dict) and "result" in result:
        result = result["result"]

    if isinstance(result, dict):
        value = result.get("timeNano") or result.get("time_nano")
    else:
        value = getattr(result, "time_nano", None)
    if value is None:
        raise ValueError(f"Bybit server-time response has no timeNano: {response!r}")
    server_time_ns = int(value)
    if server_time_ns <= 0:
        raise ValueError(f"Invalid Bybit server time: {server_time_ns}")
    return server_time_ns


async def poll_public_rest(
    symbols: Sequence[str],
    open_interest_path: Path,
    clock_path: Path,
    interval_seconds: float,
    stop_event: asyncio.Event,
    proxy_url: str | None,
    disable_open_interest: bool,
    disable_clock_sync: bool,
) -> dict[str, object]:
    """Poll public OI and diagnose Bybit/local clock alignment."""
    oi_client = (
        None
        if disable_open_interest
        else BybitHttpClient(timeout_secs=10, max_retries=0, proxy_url=proxy_url)
    )
    clock_client = (
        None
        if disable_clock_sync
        else BybitRawHttpClient(timeout_secs=10, max_retries=0, proxy_url=proxy_url)
    )
    oi_samples = 0
    oi_errors = 0
    oi_last_error: str | None = None
    oi_first_receive_ns: int | None = None
    oi_last_receive_ns: int | None = None
    clock_errors = 0
    clock_last_error: str | None = None
    clock_estimator = ClockOffsetEstimator()
    local_clock = LocalClockContinuity()
    open_interest_path.parent.mkdir(parents=True, exist_ok=True)

    oi_output = None if disable_open_interest else open_interest_path.open(
        "a",
        encoding="utf-8",
        buffering=1,
    )
    clock_output = None if disable_clock_sync else clock_path.open(
        "a",
        encoding="utf-8",
        buffering=1,
    )
    try:
        while not stop_event.is_set():
            local_clock.add(read_local_clocks())
            if clock_client is not None and clock_output is not None:
                request = read_local_clocks()
                local_clock.add(request)
                try:
                    response = await clock_client.get_server_time()
                    receive = read_local_clocks()
                    local_clock.add(receive)
                    sample = calculate_clock_sample(
                        request,
                        receive,
                        server_time_ns=parse_server_time_ns(response),
                        server_time_resolution_ns=1,
                        venue="BYBIT",
                        endpoint="GET /v5/market/time",
                    )
                    clock_output.write(json.dumps(sample, ensure_ascii=False) + "\n")
                    clock_estimator.add(sample)
                except Exception as exc:
                    clock_errors += 1
                    clock_last_error = f"{type(exc).__name__}: {exc}"

            if oi_client is not None and oi_output is not None:
                for symbol in symbols:
                    request = read_local_clocks()
                    local_clock.add(request)
                    try:
                        result = await oi_client.request_tickers(
                            BybitTickersParams(BybitProductType.LINEAR, symbol=symbol),
                        )
                        receive = read_local_clocks()
                        local_clock.add(receive)
                        if not result:
                            raise RuntimeError(f"Empty ticker response for {symbol}")
                        ticker = result[0]
                        record = {
                            "symbol": ticker.symbol,
                            "ts_request_ns": request.realtime_ns,
                            "ts_receive_ns": receive.realtime_ns,
                            "monotonic_request_ns": request.monotonic_ns,
                            "monotonic_receive_ns": receive.monotonic_ns,
                            "rtt_ms": round(
                                (receive.monotonic_ns - request.monotonic_ns) / 1_000_000,
                                6,
                            ),
                            "open_interest": ticker.open_interest,
                            "funding_rate": ticker.funding_rate,
                            "next_funding_time": ticker.next_funding_time,
                            "mark_price": ticker.mark_price,
                            "index_price": ticker.index_price,
                            "last_price": ticker.last_price,
                            "volume_24h": ticker.volume24h,
                            "turnover_24h": ticker.turnover24h,
                        }
                        oi_output.write(json.dumps(record, ensure_ascii=False) + "\n")
                        oi_samples += 1
                        oi_first_receive_ns = oi_first_receive_ns or receive.realtime_ns
                        oi_last_receive_ns = receive.realtime_ns
                    except Exception as exc:  # keep market-data recording alive if REST fails
                        oi_errors += 1
                        oi_last_error = f"{type(exc).__name__}: {exc}"

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass
    finally:
        local_clock.add(read_local_clocks())
        if oi_client is not None:
            oi_client.cancel_all_requests()
        if clock_client is not None:
            clock_client.cancel_all_requests()
        if oi_output is not None:
            oi_output.close()
        if clock_output is not None:
            clock_output.close()

    return {
        "open_interest": {
            "enabled": not disable_open_interest,
            "samples": oi_samples,
            "errors": oi_errors,
            "last_error": oi_last_error,
            "first_receive_ns": oi_first_receive_ns,
            "last_receive_ns": oi_last_receive_ns,
            "path": None if disable_open_interest else str(open_interest_path.resolve()),
        },
        "clock_sync": {
            "enabled": not disable_clock_sync,
            "endpoint": "GET /v5/market/time",
            "method": (
                "exchange timeNano minus local monotonic-RTT midpoint; "
                "offset estimate uses a bounded minimum-delay filter"
            ),
            "errors": clock_errors,
            "last_error": clock_last_error,
            **clock_estimator.to_dict(),
            "path": None if disable_clock_sync else str(clock_path.resolve()),
        },
        "local_clock_continuity": local_clock.to_dict(),
    }


def _parse_symbols(value: str) -> list[str]:
    symbols = [symbol.strip().upper() for symbol in value.split(",") if symbol.strip()]
    if not symbols or any(not symbol.endswith("USDT") for symbol in symbols):
        raise argparse.ArgumentTypeError("symbols must be comma-separated USDT pairs")
    return symbols


async def collect(args: argparse.Namespace) -> dict[str, Any]:
    run_id = args.run_id or default_run_id()
    validate_run_id(run_id)
    network_route = configure_process_network_route(args.proxy_url)
    network_route["websocket_transport_backend"] = args.transport_backend

    output_dir = args.output_root / run_id
    if output_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing run: {output_dir}")
    catalog_root = output_dir / "catalog"
    stream_path = catalog_root / "live" / run_id
    stream_path.mkdir(parents=True)
    stats_path = output_dir / "stream_stats.json"
    oi_path = output_dir / "open_interest.jsonl"
    clock_path = output_dir / "clock_offsets.jsonl"
    arrival_path = output_dir / "arrivals.jsonl"
    instrument_ids = [f"{symbol}-LINEAR.BYBIT" for symbol in args.symbols]

    node = (
        LiveNode.builder(
            f"BYBIT-RECORDER-{run_id}",
            TraderId.from_str("DATA-RESEARCH-001"),
            Environment.LIVE,
        )
        .add_data_client(
            None,
            BybitDataClientFactory(),
            BybitDataClientConfig(
                product_types=[BybitProductType.LINEAR],
                environment=BybitEnvironment.MAINNET,
                proxy_url=args.proxy_url,
                http_timeout_secs=15,
                max_retries=1,
                transport_backend=BYBIT_TRANSPORT_BACKENDS[args.transport_backend],
            ),
        )
        .build()
    )
    node.add_actor_from_config(
        ImportableActorConfig(
            actor_path="market_data_lab.live_bybit:BybitRecorder",
            config_path="market_data_lab.live_bybit:BybitRecorderConfig",
            config={
                "actor_id": "BYBIT-RECORDER-001",
                "instrument_ids": instrument_ids,
                "stream_path": str(stream_path),
                "stats_path": str(stats_path),
                "arrival_path": str(arrival_path),
                "duration_seconds": args.duration_seconds,
                "book_depth": args.book_depth,
                "log_events": False,
                "log_commands": False,
            },
        ),
    )

    started_at = datetime.now(UTC)
    stop_rest = asyncio.Event()
    rest_task = asyncio.create_task(
        poll_public_rest(
            args.symbols,
            oi_path,
            clock_path,
            args.oi_interval_seconds,
            stop_rest,
            args.proxy_url,
            args.disable_open_interest,
            args.disable_clock_sync,
        ),
    )

    run_error: str | None = None
    try:
        await node.run_async()
    except Exception as exc:
        run_error = f"{type(exc).__name__}: {exc}"
    finally:
        stop_rest.set()
        rest_summary = await rest_task

    stopped_at = datetime.now(UTC)
    actor_stats = json.loads(stats_path.read_text(encoding="utf-8")) if stats_path.exists() else {}
    instruments = [
        instrument
        for instrument_id in map(InstrumentId.from_str, instrument_ids)
        if (instrument := node.cache.instrument(instrument_id)) is not None
    ]

    conversion_error: str | None = None
    converted: dict[str, Any] = {"conversion": {}, "validations": {}}
    try:
        catalog = ParquetDataCatalog(str(catalog_root))
        if instruments:
            catalog.write_instruments(instruments)
        converted = convert_streams(
            catalog_root,
            run_id,
            actor_stats,
            args.max_validation_records,
        )
    except Exception as exc:
        conversion_error = f"{type(exc).__name__}: {exc}"
    finally:
        node.dispose()

    validation_failures = {
        key: value
        for key, value in converted["validations"].items()
        if value["status"] not in {"ok", "skipped_large"}
        or value.get("book_integrity", "ok") != "ok"
    }
    manifest = {
        "status": "error" if run_error or conversion_error or validation_failures else "ok",
        "run_error": run_error,
        "conversion_error": conversion_error,
        "validation_failures": validation_failures,
        "run_id": run_id,
        "venue": "BYBIT",
        "symbols": args.symbols,
        "instrument_ids": instrument_ids,
        "book_depth": args.book_depth,
        "timestamp_semantics": {
            "ts_init": "local adapter initialization/receipt time on this host",
            "callback_monotonic": (
                "CLOCK_MONOTONIC sampled in the Python actor callback; retained in "
                "arrivals.jsonl as an independent same-host ordering audit"
            ),
            "order_book_ts_event": (
                "Bybit WebSocket message ts (system-generated data timestamp); "
                "NautilusTrader 2.0.0rc3 does not map order-book cts into ts_event"
            ),
            "order_book_cts": (
                "Bybit matching-engine timestamp exists in the raw feed but is not retained "
                "by the current normalized adapter"
            ),
        },
        "duration_requested_seconds": args.duration_seconds,
        "started_at": started_at.isoformat(),
        "stopped_at": stopped_at.isoformat(),
        "duration_wall_seconds": round((stopped_at - started_at).total_seconds(), 6),
        "nautilus_trader_version": nautilus_trader.__version__,
        "execution_client_registered": False,
        "api_credentials_used": False,
        "network_route": network_route,
        "output_dir": str(output_dir.resolve()),
        "actor_stats": actor_stats,
        **rest_summary,
        **converted,
    }
    atomic_json(output_dir / "manifest.json", manifest)
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        type=_parse_symbols,
        default=_parse_symbols("BTCUSDT,ETHUSDT,SOLUSDT"),
        help="Comma-separated linear perpetual symbols",
    )
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--book-depth", type=int, choices=(1, 50, 200, 1000), default=50)
    parser.add_argument(
        "--oi-interval-seconds",
        type=float,
        default=5.0,
        help="Polling interval shared by public OI and clock diagnostics",
    )
    parser.add_argument("--disable-open-interest", action="store_true")
    parser.add_argument("--disable-clock-sync", action="store_true")
    parser.add_argument("--proxy-url", help="Per-client proxy URL; direct connection by default")
    parser.add_argument(
        "--transport-backend",
        choices=tuple(BYBIT_TRANSPORT_BACKENDS),
        default="tungstenite",
        help="WebSocket implementation (tungstenite is the reliable Bybit default)",
    )
    parser.add_argument("--run-id")
    parser.add_argument("--output-root", type=Path, default=Path("data/live/bybit"))
    parser.add_argument("--max-validation-records", type=int, default=2_000_000)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.duration_seconds < 0:
        raise SystemExit("--duration-seconds must be non-negative")
    if args.oi_interval_seconds <= 0:
        raise SystemExit("--oi-interval-seconds must be positive")
    manifest = asyncio.run(collect(args))
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    if manifest["status"] != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
