"""Bounded in-memory public CEX order-book streams.

This module is intentionally a *market-data* layer: it opens public
WebSockets, maintains only a short local history and never receives account
credentials or creates orders.  A DEX quote is expensive compared with a CEX
book update, so consumers can use :meth:`next_update` to re-evaluate a fresh
DEX quote immediately when the CEX changes.

The adapters use public, documented snapshot/depth channels:

* Binance partial depth 20 at 100 ms;
* Bybit spot and linear-perpetual orderbook depth 50 (snapshot + deltas);
* Bitget V3 spot ``books50`` snapshots (up to 50 levels at 20 ms);
* OKX ``books5`` snapshots at 100 ms for public/VIP0 access;
* MEXC's existing public partial-depth adapter.

They do not promise that a top-of-book update is executable liquidity.  The
cycle calculator still walks the retained depth and rejects stale or
insufficient books.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from websockets.asyncio.client import connect as websocket_connect

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import MexcPartialDepthStream
from market_data_lab.dex_quotes import TimedResponse


BINANCE_PARTIAL_DEPTH_WS_ORIGIN = "wss://stream.binance.com:9443"
BYBIT_SPOT_ORDERBOOK_WS_ENDPOINT = "wss://stream.bybit.com/v5/public/spot"
BYBIT_LINEAR_ORDERBOOK_WS_ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
OKX_PUBLIC_WS_ENDPOINT = "wss://ws.okx.com:8443/ws/v5/public"
BITGET_PUBLIC_V3_WS_ENDPOINT = "wss://ws.bitget.com/v3/ws/public"


@dataclass(frozen=True)
class BybitLinearTicker:
    """Latest public linear-perpetual ticker state for one Bybit symbol.

    The order-book stream owns this tiny in-memory side channel so a consumer
    can attach funding/mark/index context to the exact same public WebSocket
    connection without writing an additional raw feed to disk.
    """

    symbol: str
    funding_rate: Decimal | None
    next_funding_time_ms: int | None
    mark_price: Decimal | None
    index_price: Decimal | None
    received_realtime_ns: int
    received_monotonic_ns: int
    exchange_system_time_ms: int | None


class PublicBookStream(Protocol):
    """The small common surface required by the continuous cycle monitor."""

    symbols: tuple[str, ...]

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def next_update(self) -> BookSnapshot: ...

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None: ...


class ShardedPublicBookStream:
    """Merge several bounded venue streams behind the common consumer API.

    Some public venues cap the number of subscriptions carried by one socket.
    Sharding is an internal transport detail: consumers still see one venue
    feed, one bounded update queue and one nearest-snapshot lookup.
    """

    def __init__(self, shards: Sequence[PublicBookStream], *, queue_capacity: int = 8_192) -> None:
        if not shards or queue_capacity <= 0:
            raise ValueError("sharded stream needs at least one shard and a positive queue capacity")
        self.shards = tuple(shards)
        self.symbols = tuple(symbol for shard in self.shards for symbol in shard.symbols)
        if len(set(self.symbols)) != len(self.symbols):
            raise ValueError("symbols must not overlap across stream shards")
        self._by_symbol = {
            symbol.upper(): shard
            for shard in self.shards
            for symbol in shard.symbols
        }
        self._updates: asyncio.Queue[BookSnapshot] = asyncio.Queue(maxsize=queue_capacity)
        self._active: list[PublicBookStream] = []
        self._forwarders: list[asyncio.Task[None]] = []
        self._dropped_updates = 0

    async def start(self) -> None:
        starts = await asyncio.gather(*(shard.start() for shard in self.shards), return_exceptions=True)
        self._active = [
            shard
            for shard, result in zip(self.shards, starts, strict=True)
            if not isinstance(result, BaseException)
        ]
        if not self._active:
            details = "; ".join(
                f"{type(result).__name__}: {result}"
                for result in starts
                if isinstance(result, BaseException)
            )
            raise RuntimeError(f"all public book shards failed to start: {details}"[:1024])
        self._forwarders = [asyncio.create_task(self._forward(shard)) for shard in self._active]

    async def _forward(self, shard: PublicBookStream) -> None:
        while True:
            update = await shard.next_update()
            if self._updates.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    self._updates.get_nowait()
                self._dropped_updates += 1
            self._updates.put_nowait(update)

    async def next_update(self) -> BookSnapshot:
        # If the receiver task has exited (e.g. transport failure), propagate
        # the error to the caller instead of blocking forever on the queue.
        # This lets the outer supervisor own reconnection and epoch transitions.
        if self._receiver is not None and self._receiver.done():
            exc = self._receiver.exception()
            if exc is not None:
                if isinstance(exc, asyncio.CancelledError):
                    raise exc
                raise RuntimeError(
                    f"{self.venue} websocket transport failed: {exc}",
                ) from exc
            raise RuntimeError(f"{self.venue} websocket receiver exited unexpectedly")
        return await self._updates.get()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
        shard = self._by_symbol.get(symbol.upper())
        return shard.nearest_snapshot(symbol, target_realtime_ns) if shard is not None else None

    @property
    def available_symbols(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    symbol
                    for shard in self._active
                    for symbol in getattr(shard, "available_symbols", ())
                },
            ),
        )

    @property
    def error(self) -> str | None:
        errors = [str(getattr(shard, "error", "")) for shard in self._active if getattr(shard, "error", None)]
        return "; ".join(errors)[:1024] if self._active and len(errors) == len(self._active) else None

    @property
    def reconnects(self) -> int:
        return sum(int(getattr(shard, "reconnects", 0)) for shard in self._active)

    @property
    def dropped_updates(self) -> int:
        return self._dropped_updates + sum(
            int(getattr(shard, "dropped_updates", 0)) for shard in self._active
        )

    @property
    def malformed_messages(self) -> int:
        return sum(int(getattr(shard, "malformed_messages", 0)) for shard in self._active)

    async def close(self) -> None:
        for task in self._forwarders:
            task.cancel()
        if self._forwarders:
            await asyncio.gather(*self._forwarders, return_exceptions=True)
        self._forwarders.clear()
        await asyncio.gather(*(shard.close() for shard in self.shards), return_exceptions=True)
        self._active.clear()


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _optional_decimal(value: Any) -> Decimal | None:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _level_tuple(
    rows: Any,
    *,
    reverse: bool,
    limit: int,
) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(rows, list):
        raise ValueError("book side is not a list")
    parsed: dict[Decimal, Decimal] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            raise ValueError("malformed book level")
        price = Decimal(str(row[0]))
        size = Decimal(str(row[1]))
        if price > 0 and size > 0:
            parsed[price] = size
    levels = sorted(parsed.items(), key=lambda item: item[0], reverse=reverse)
    if not levels:
        raise ValueError("empty book side")
    return tuple(levels[:limit])


def _timed_stream_response(received_realtime_ns: int, received_monotonic_ns: int) -> TimedResponse:
    """Represent a push event on the same timing scale as HTTP snapshots."""

    return TimedResponse(
        payload=None,
        error=None,
        sent_realtime_ns=received_realtime_ns,
        received_realtime_ns=received_realtime_ns,
        sent_monotonic_ns=received_monotonic_ns,
        received_monotonic_ns=received_monotonic_ns,
    )


def _book_snapshot(
    *,
    symbol: str,
    bids: Any,
    asks: Any,
    depth: int,
    received_realtime_ns: int,
    received_monotonic_ns: int,
    source: str,
    category: str = "spot",
    exchange_system_time_ms: Any = None,
    matching_engine_time_ms: Any = None,
    update_id: Any = None,
    cross_sequence: Any = None,
) -> BookSnapshot | None:
    try:
        parsed_bids = _level_tuple(bids, reverse=True, limit=depth)
        parsed_asks = _level_tuple(asks, reverse=False, limit=depth)
        if parsed_bids[0][0] >= parsed_asks[0][0]:
            raise ValueError("crossed book")
    except (InvalidOperation, TypeError, ValueError):
        return None
    return BookSnapshot(
        symbol=symbol,
        category=category,
        status="ok",
        error=None,
        bids=parsed_bids,
        asks=parsed_asks,
        exchange_system_time_ms=_optional_int(exchange_system_time_ms),
        matching_engine_time_ms=_optional_int(matching_engine_time_ms),
        update_id=_optional_int(update_id),
        cross_sequence=_optional_int(cross_sequence),
        response=_timed_stream_response(received_realtime_ns, received_monotonic_ns),
        source=source,
    )


class _EventedBookStream:
    """Reconnectable WebSocket stream with bounded book history and queue."""

    venue = "UNKNOWN"
    source = "websocket"
    endpoint = ""
    # Most venues understand standard WebSocket ping control frames.  Bitget
    # documents a text ``ping`` heartbeat instead, so its adapter overrides
    # these two values below.
    websocket_ping_interval: float | None = 20
    application_heartbeat_interval_seconds: float | None = None
    application_heartbeat_payload: str | bytes | None = None

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        depth: int,
        timeout_seconds: float,
        proxy_url: str | None,
        connect_websocket: Callable[..., Any] = websocket_connect,
        history_capacity_per_symbol: int = 256,
        queue_capacity: int = 4_096,
    ) -> None:
        unique = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        if not unique:
            raise ValueError("at least one CEX symbol is required")
        if depth <= 0 or history_capacity_per_symbol <= 0 or queue_capacity <= 0:
            raise ValueError("stream depths and capacities must be positive")
        self.symbols = unique
        self.depth = depth
        self.timeout_seconds = timeout_seconds
        self.proxy_url = proxy_url
        self.connect_websocket = connect_websocket
        self.history_capacity_per_symbol = history_capacity_per_symbol
        self._latest: dict[str, BookSnapshot] = {}
        self._history: dict[str, deque[BookSnapshot]] = {
            symbol: deque(maxlen=history_capacity_per_symbol) for symbol in self.symbols
        }
        self._updates: asyncio.Queue[BookSnapshot] = asyncio.Queue(maxsize=queue_capacity)
        self._first_update = asyncio.Event()
        self._receiver: asyncio.Task[None] | None = None
        self._websocket: Any = None
        self._stopping = False
        self._error: str | None = None
        self._reconnects = 0
        self._dropped_updates = 0
        self._malformed_messages = 0

    async def start(self) -> None:
        if self._receiver is not None:
            raise RuntimeError("stream is already started")
        self._receiver = asyncio.create_task(self._supervise())
        try:
            await asyncio.wait_for(self._first_update.wait(), timeout=self.timeout_seconds)
        except Exception:
            await self.close()
            detail = self._error or "no valid public book update"
            raise RuntimeError(f"{self.venue} websocket startup failed: {detail}") from None

    async def _supervise(self) -> None:
        """Run a single WebSocket session, surfacing transport failures to the
        outer supervisor.

        The old implementation retried connections in an inner loop.  That
        hid transport failures from the scanner's supervisor, which owns
        reconnect semantics and source-epoch transitions.  Instead of
        reconnecting here, we let the ``_receiver`` task fail so the outer
        supervisor sees the exception, bumps ``source_epoch``, and restarts
        the whole source with fresh epoch propagation.
        """
        websocket: Any = None
        heartbeat: asyncio.Task[None] | None = None
        try:
            websocket = await self.connect_websocket(
                self.endpoint,
                open_timeout=self.timeout_seconds,
                close_timeout=1,
                ping_interval=self.websocket_ping_interval,
                ping_timeout=20,
                proxy=self.proxy_url,
            )
            self._websocket = websocket
            await self._subscribe(websocket)
            if (
                self.application_heartbeat_interval_seconds is not None
                and self.application_heartbeat_payload is not None
            ):
                heartbeat = asyncio.create_task(self._send_application_heartbeats(websocket))
            while not self._stopping:
                raw = await websocket.recv()
                if raw is None:
                    raise ConnectionError("websocket closed by peer")
                self._handle_raw(raw)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await heartbeat
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await websocket.close()
            if self._websocket is websocket:
                self._websocket = None

    async def _send_application_heartbeats(self, websocket: Any) -> None:
        """Send a venue-required application heartbeat while a socket is live."""

        assert self.application_heartbeat_interval_seconds is not None
        assert self.application_heartbeat_payload is not None
        while not self._stopping:
            await asyncio.sleep(self.application_heartbeat_interval_seconds)
            if not self._stopping:
                await websocket.send(self.application_heartbeat_payload)

    async def _subscribe(self, websocket: Any) -> None:
        raise NotImplementedError

    def _handle_raw(self, raw: Any) -> None:
        raise NotImplementedError

    def _publish(self, snapshot: BookSnapshot) -> None:
        if snapshot.symbol not in self._history:
            return
        self._latest[snapshot.symbol] = snapshot
        self._history[snapshot.symbol].append(snapshot)
        if self._updates.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self._updates.get_nowait()
            self._dropped_updates += 1
        self._updates.put_nowait(snapshot)
        self._error = None
        self._first_update.set()

    def _decode(self, raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, str):
            self._malformed_messages += 1
            return None
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            self._malformed_messages += 1
            return None
        return payload if isinstance(payload, dict) else None

    async def next_update(self) -> BookSnapshot:
        return await self._updates.get()

    def nearest_snapshot(self, symbol: str, target_realtime_ns: int) -> BookSnapshot | None:
        history = self._history.get(symbol.upper())
        if not history:
            return None
        return min(
            history,
            key=lambda item: abs(item.response.received_realtime_ns - target_realtime_ns),
        )

    def latest_snapshot(self, symbol: str, *, max_age_ms: Decimal) -> BookSnapshot | None:
        snapshot = self._latest.get(symbol.upper())
        if snapshot is None:
            return None
        age_ns = time.time_ns() - snapshot.response.received_realtime_ns
        if age_ns > int(max_age_ms * Decimal(1_000_000)):
            return replace(
                snapshot,
                status="stale",
                error=f"{self.venue} websocket book age {age_ns / 1_000_000:.3f}ms exceeds {max_age_ms}",
            )
        return snapshot

    @property
    def available_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._latest))

    @property
    def error(self) -> str | None:
        return self._error

    @property
    def reconnects(self) -> int:
        return self._reconnects

    @property
    def dropped_updates(self) -> int:
        return self._dropped_updates

    @property
    def malformed_messages(self) -> int:
        return self._malformed_messages

    async def close(self) -> None:
        self._stopping = True
        if self._receiver is not None:
            self._receiver.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._receiver
            self._receiver = None
        if self._websocket is not None:
            with contextlib.suppress(Exception):
                await self._websocket.close()
            self._websocket = None


class BybitOrderBookStream(_EventedBookStream):
    """Maintain Bybit's documented spot depth-50 snapshot/delta book."""

    venue = "BYBIT"
    source = "websocket_orderbook_50"
    endpoint = BYBIT_SPOT_ORDERBOOK_WS_ENDPOINT
    book_category = "spot"

    def __init__(self, symbols: Sequence[str], **kwargs: Any) -> None:
        super().__init__(symbols, depth=50, **kwargs)
        self._bids: dict[str, dict[Decimal, Decimal]] = {symbol: {} for symbol in self.symbols}
        self._asks: dict[str, dict[Decimal, Decimal]] = {symbol: {} for symbol in self.symbols}

    async def _subscribe(self, websocket: Any) -> None:
        # Bybit spot accepts at most ten args per subscription request.
        topics = [f"orderbook.50.{symbol}" for symbol in self.symbols]
        for start in range(0, len(topics), 10):
            await websocket.send(
                json.dumps({"op": "subscribe", "args": topics[start : start + 10]}, separators=(",", ":")),
            )

    @staticmethod
    def _apply_delta(target: dict[Decimal, Decimal], rows: Any) -> None:
        if not isinstance(rows, list):
            raise ValueError("Bybit delta side is not a list")
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                raise ValueError("malformed Bybit delta level")
            price = Decimal(str(row[0]))
            size = Decimal(str(row[1]))
            if size <= 0:
                target.pop(price, None)
            elif price > 0:
                target[price] = size

    def _handle_raw(self, raw: Any) -> None:
        payload = self._decode(raw)
        if payload is None or not str(payload.get("topic", "")).startswith("orderbook."):
            return
        data = payload.get("data")
        if not isinstance(data, dict):
            self._malformed_messages += 1
            return
        symbol = str(data.get("s") or str(payload.get("topic")).split(".")[-1]).upper()
        if symbol not in self._bids:
            return
        try:
            bids = data.get("b")
            asks = data.get("a")
            if payload.get("type") == "snapshot":
                self._bids[symbol] = dict(_level_tuple(bids, reverse=True, limit=50))
                self._asks[symbol] = dict(_level_tuple(asks, reverse=False, limit=50))
            elif payload.get("type") == "delta":
                self._apply_delta(self._bids[symbol], bids)
                self._apply_delta(self._asks[symbol], asks)
            else:
                return
            received_realtime_ns = time.time_ns()
            received_monotonic_ns = time.monotonic_ns()
            snapshot = _book_snapshot(
                symbol=symbol,
                bids=[[str(price), str(size)] for price, size in self._bids[symbol].items()],
                asks=[[str(price), str(size)] for price, size in self._asks[symbol].items()],
                depth=50,
                received_realtime_ns=received_realtime_ns,
                received_monotonic_ns=received_monotonic_ns,
                source=self.source,
                category=self.book_category,
                exchange_system_time_ms=payload.get("ts"),
                matching_engine_time_ms=data.get("cts"),
                update_id=data.get("u"),
                cross_sequence=payload.get("seq"),
            )
        except (InvalidOperation, TypeError, ValueError):
            self._malformed_messages += 1
            return
        if snapshot is not None:
            self._publish(snapshot)
        else:
            self._malformed_messages += 1


class BybitLinearOrderBookStream(BybitOrderBookStream):
    """Bybit linear L50 book plus ticker/funding context, all in memory.

    Linear order-book topics use the same snapshot/delta shape as Bybit spot.
    Ticker messages are delta-compressed, therefore the stream retains the
    last full ticker dictionary per symbol before exposing typed values.
    """

    source = "websocket_orderbook_50_linear"
    endpoint = BYBIT_LINEAR_ORDERBOOK_WS_ENDPOINT
    book_category = "linear"

    def __init__(self, symbols: Sequence[str], **kwargs: Any) -> None:
        super().__init__(symbols, **kwargs)
        self._ticker_payloads: dict[str, dict[str, Any]] = {symbol: {} for symbol in self.symbols}
        self._tickers: dict[str, BybitLinearTicker] = {}

    async def _subscribe(self, websocket: Any) -> None:
        await super()._subscribe(websocket)
        topics = [f"tickers.{symbol}" for symbol in self.symbols]
        for start in range(0, len(topics), 10):
            await websocket.send(
                json.dumps({"op": "subscribe", "args": topics[start : start + 10]}, separators=(",", ":")),
            )

    def _handle_raw(self, raw: Any) -> None:
        payload = self._decode(raw)
        if payload is None:
            return
        topic = str(payload.get("topic", ""))
        if not topic.startswith("tickers."):
            # Reuse the well-tested snapshot/delta book handler.  It decodes
            # the payload once more, which is negligible relative to WebSocket
            # delivery and keeps its error accounting unchanged.
            super()._handle_raw(raw)
            return
        data = payload.get("data")
        if not isinstance(data, dict):
            self._malformed_messages += 1
            return
        symbol = str(data.get("symbol") or topic.split(".", 1)[-1]).upper()
        if symbol not in self._ticker_payloads:
            return
        if payload.get("type") == "snapshot":
            state = dict(data)
        elif payload.get("type") == "delta":
            state = dict(self._ticker_payloads[symbol])
            state.update(data)
        else:
            return
        self._ticker_payloads[symbol] = state
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        self._tickers[symbol] = BybitLinearTicker(
            symbol=symbol,
            funding_rate=_optional_decimal(state.get("fundingRate")),
            next_funding_time_ms=_optional_int(state.get("nextFundingTime")),
            mark_price=_optional_decimal(state.get("markPrice")),
            index_price=_optional_decimal(state.get("indexPrice")),
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            exchange_system_time_ms=_optional_int(payload.get("ts")),
        )

    def perp_ticker(self, symbol: str) -> BybitLinearTicker | None:
        """Return the latest funding/mark/index state without persistence."""

        return self._tickers.get(symbol.upper())


class BinancePartialDepthStream(_EventedBookStream):
    """Consume Binance's top-20 partial-depth snapshots at 100 ms."""

    venue = "BINANCE"
    source = "websocket_partial_depth_20_100ms"

    def __init__(self, symbols: Sequence[str], **kwargs: Any) -> None:
        super().__init__(symbols, depth=20, **kwargs)
        streams = "/".join(f"{symbol.lower()}@depth20@100ms" for symbol in self.symbols)
        self.endpoint = f"{BINANCE_PARTIAL_DEPTH_WS_ORIGIN}/stream?streams={streams}"

    async def _subscribe(self, websocket: Any) -> None:
        # Combined stream subscriptions are encoded in the connection URL.
        return None

    def _handle_raw(self, raw: Any) -> None:
        payload = self._decode(raw)
        if payload is None:
            return
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            self._malformed_messages += 1
            return
        # Partial-depth payloads on the combined endpoint do not always
        # repeat ``s``.  The outer stream name is authoritative in that case,
        # e.g. ``solusdt@depth20@100ms``.
        stream_name = str(payload.get("stream", ""))
        symbol = str(data.get("s") or stream_name.split("@", 1)[0]).upper()
        if symbol not in self._history:
            return
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        snapshot = _book_snapshot(
            symbol=symbol,
            bids=data.get("bids", data.get("b")),
            asks=data.get("asks", data.get("a")),
            depth=20,
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            source=self.source,
            exchange_system_time_ms=data.get("E"),
            matching_engine_time_ms=data.get("T"),
            update_id=data.get("lastUpdateId"),
        )
        if snapshot is not None:
            self._publish(snapshot)
        else:
            self._malformed_messages += 1


class OkxBooks5Stream(_EventedBookStream):
    """Consume public OKX ``books5`` snapshots without a VIP requirement."""

    venue = "OKX"
    source = "websocket_books5"
    endpoint = OKX_PUBLIC_WS_ENDPOINT

    def __init__(self, symbols: Sequence[str], **kwargs: Any) -> None:
        super().__init__(symbols, depth=5, **kwargs)

    async def _subscribe(self, websocket: Any) -> None:
        await websocket.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "args": [{"channel": "books5", "instId": symbol} for symbol in self.symbols],
                },
                separators=(",", ":"),
            ),
        )

    def _handle_raw(self, raw: Any) -> None:
        payload = self._decode(raw)
        if payload is None:
            return
        arg = payload.get("arg")
        rows = payload.get("data")
        if not isinstance(arg, dict) or arg.get("channel") != "books5" or not isinstance(rows, list):
            return
        symbol = str(arg.get("instId", "")).upper()
        if symbol not in self._history or not rows or not isinstance(rows[0], dict):
            return
        book = rows[0]
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        snapshot = _book_snapshot(
            symbol=symbol,
            bids=book.get("bids"),
            asks=book.get("asks"),
            depth=5,
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            source=self.source,
            exchange_system_time_ms=book.get("ts"),
            update_id=book.get("seqId"),
            cross_sequence=book.get("prevSeqId"),
        )
        if snapshot is not None:
            self._publish(snapshot)
        else:
            self._malformed_messages += 1


class BitgetBooks50Stream(_EventedBookStream):
    """Consume Bitget UTA V3 spot ``books50`` standalone snapshots.

    The channel pushes a complete top-50 state rather than a delta, so every
    valid message can be normalized directly.  A single socket is kept below
    Bitget's documented recommended 50-channel ceiling by the factory.
    """

    venue = "BITGET"
    source = "websocket_books50_v3"
    endpoint = BITGET_PUBLIC_V3_WS_ENDPOINT
    websocket_ping_interval = None
    application_heartbeat_interval_seconds = 25.0
    application_heartbeat_payload = "ping"

    def __init__(self, symbols: Sequence[str], **kwargs: Any) -> None:
        super().__init__(symbols, depth=50, **kwargs)

    async def _subscribe(self, websocket: Any) -> None:
        await websocket.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "args": [
                        {"instType": "spot", "topic": "books50", "symbol": symbol}
                        for symbol in self.symbols
                    ],
                },
                separators=(",", ":"),
            ),
        )

    def _handle_raw(self, raw: Any) -> None:
        # Bitget replies to the text heartbeat with a text ``pong`` frame.
        if raw == "pong":
            return
        payload = self._decode(raw)
        if payload is None:
            return
        if payload.get("event") == "error" or (
            payload.get("code") not in (None, "0", "00000", 0)
        ):
            raise RuntimeError(
                f"Bitget subscription error {payload.get('code')}: {payload.get('msg')}",
            )
        arg = payload.get("arg")
        rows = payload.get("data")
        if (
            not isinstance(arg, dict)
            or str(arg.get("topic", "")).lower() != "books50"
            or not isinstance(rows, list)
        ):
            return
        symbol = str(arg.get("symbol", "")).upper()
        if symbol not in self._history or not rows or not isinstance(rows[0], dict):
            return
        book = rows[0]
        received_realtime_ns = time.time_ns()
        received_monotonic_ns = time.monotonic_ns()
        snapshot = _book_snapshot(
            symbol=symbol,
            bids=book.get("b"),
            asks=book.get("a"),
            depth=50,
            received_realtime_ns=received_realtime_ns,
            received_monotonic_ns=received_monotonic_ns,
            source=self.source,
            exchange_system_time_ms=book.get("ts", payload.get("ts")),
            update_id=book.get("seq"),
            cross_sequence=book.get("pseq"),
        )
        if snapshot is not None:
            self._publish(snapshot)
        else:
            self._malformed_messages += 1


def build_public_book_stream(
    venue: str,
    symbols: Sequence[str],
    *,
    timeout_seconds: float,
    proxy_url: str | None,
    history_capacity_per_symbol: int = 256,
    category: str = "spot",
) -> PublicBookStream:
    """Create one persistent public book feed for a CEX venue.

    MEXC accepts the same interface through its existing binary partial-depth
    stream.  It is allowed to start with a non-empty subset so that one absent
    long-tail symbol does not prevent all the other public symbols from
    streaming; the continuous monitor reports missing symbols separately.
    """

    normalized = venue.upper()
    normalized_category = category.lower()
    common = {
        "timeout_seconds": timeout_seconds,
        "proxy_url": proxy_url,
        "history_capacity_per_symbol": history_capacity_per_symbol,
    }
    if normalized_category == "linear":
        if normalized != "BYBIT":
            raise ValueError("only Bybit linear books are supported by this public stream factory")
        return BybitLinearOrderBookStream(symbols, **common)
    if normalized_category != "spot":
        raise ValueError(f"unsupported CEX book category: {category}")
    if normalized == "MEXC":
        unique = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        max_symbols_per_socket = 20
        shards = [
            MexcPartialDepthStream(
                unique[start : start + max_symbols_per_socket],
                levels=20,
                require_all_initial_books=False,
                **common,
            )
            for start in range(0, len(unique), max_symbols_per_socket)
        ]
        return shards[0] if len(shards) == 1 else ShardedPublicBookStream(shards)
    if normalized == "BYBIT":
        return BybitOrderBookStream(symbols, **common)
    if normalized == "BINANCE":
        return BinancePartialDepthStream(symbols, **common)
    if normalized == "OKX":
        return OkxBooks5Stream(symbols, **common)
    if normalized == "BITGET":
        unique = tuple(dict.fromkeys(symbol.upper() for symbol in symbols))
        # The documented soft limit is fewer than 50 channels per connection.
        # Each books50 subscription is one channel, hence transport sharding
        # preserves that recommendation as coverage expands.
        max_symbols_per_socket = 40
        shards = [
            BitgetBooks50Stream(unique[start : start + max_symbols_per_socket], **common)
            for start in range(0, len(unique), max_symbols_per_socket)
        ]
        return shards[0] if len(shards) == 1 else ShardedPublicBookStream(shards)
    raise ValueError(f"unsupported CEX websocket venue: {venue}")


def stream_health(stream: PublicBookStream) -> dict[str, Any]:
    """Return safe, bounded diagnostics without dumping book state to disk."""

    return {
        "available_symbols": list(getattr(stream, "available_symbols", ())),
        "error": getattr(stream, "error", None),
        "reconnects": int(getattr(stream, "reconnects", 0)),
        "dropped_updates": int(getattr(stream, "dropped_updates", 0)),
        "malformed_messages": int(getattr(stream, "malformed_messages", 0)),
    }
