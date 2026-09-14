from __future__ import annotations

import asyncio
import unittest
from decimal import Decimal

from market_data_lab.cex_dex_cycles import BookSnapshot
from market_data_lab.cex_dex_cycles import CycleMarket
from market_data_lab.cex_dex_cycles import MARKETS
from market_data_lab.cex_dex_cycles import MexcPartialDepthStream
from market_data_lab.cex_dex_cycles import _best_dex_records
from market_data_lab.cex_dex_cycles import build_cycle_providers
from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.cex_dex_cycles import calculate_cycle
from market_data_lab.cex_dex_cycles import choose_nearest_book
from market_data_lab.cex_dex_cycles import market_for_cex
from market_data_lab.cex_dex_cycles import parse_binance_book
from market_data_lab.cex_dex_cycles import parse_bybit_book
from market_data_lab.cex_dex_cycles import parse_mexc_book
from market_data_lab.cex_dex_cycles import parse_mexc_partial_depth_message
from market_data_lab.cex_dex_cycles import parse_okx_book
from market_data_lab.dex_quotes import TimedResponse


def _response(payload: object, received_ns: int = 2_000_000_000) -> TimedResponse:
    return TimedResponse(
        payload=payload,
        error=None,
        sent_realtime_ns=received_ns - 10_000_000,
        received_realtime_ns=received_ns,
        sent_monotonic_ns=1_000_000,
        received_monotonic_ns=11_000_000,
    )


def _protobuf_varint(value: int) -> bytes:
    encoded = bytearray()
    while True:
        chunk = value & 0x7F
        value >>= 7
        encoded.append(chunk | (0x80 if value else 0))
        if not value:
            return bytes(encoded)


def _protobuf_bytes(field_number: int, value: bytes) -> bytes:
    return _protobuf_varint((field_number << 3) | 2) + _protobuf_varint(len(value)) + value


def _protobuf_int(field_number: int, value: int) -> bytes:
    return _protobuf_varint(field_number << 3) + _protobuf_varint(value)


def _mexc_partial_depth_message(symbol: str = "TESTUSDT", levels: int = 5) -> bytes:
    ask = _protobuf_bytes(1, b"0.6") + _protobuf_bytes(2, b"2")
    bid = _protobuf_bytes(1, b"0.5") + _protobuf_bytes(2, b"3")
    depth = (
        _protobuf_bytes(1, ask)
        + _protobuf_bytes(2, bid)
        + _protobuf_bytes(4, b"77")
        + _protobuf_int(5, 122)
    )
    return (
        _protobuf_bytes(1, f"spot@public.limit.depth.v3.api.pb@{symbol}@{levels}".encode())
        + _protobuf_bytes(303, depth)
        + _protobuf_bytes(3, symbol.encode())
        + _protobuf_int(6, 123)
    )


class _FakeMexcWebsocket:
    def __init__(self, messages: list[bytes | BaseException]) -> None:
        self._messages = list(messages)
        self._closed = asyncio.Event()
        self.sent: list[str] = []

    async def send(self, payload: str) -> None:
        self.sent.append(payload)

    async def recv(self) -> bytes:
        if self._messages:
            message = self._messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message
        await self._closed.wait()
        raise ConnectionError("fake websocket closed")

    async def close(self) -> None:
        self._closed.set()


def _book(received_ns: int) -> BookSnapshot:
    return BookSnapshot(
        symbol="TESTUSDC",
        category="spot",
        status="ok",
        error=None,
        bids=((Decimal("101"), Decimal("2")),),
        asks=((Decimal("102"), Decimal("2")),),
        exchange_system_time_ms=1,
        matching_engine_time_ms=1,
        update_id=2,
        cross_sequence=3,
        response=_response({}, received_ns),
    )


MARKET = CycleMarket(
    name="TEST",
    provider="DEX",
    chain="base",
    dex_pair="WTEST/USDC",
    cex_symbol="TESTUSDC",
    cex_base_symbol="TEST",
    quote_symbol="USDC",
    asset_equivalence="test",
)


class BybitBookTest(unittest.TestCase):
    def test_parse_bybit_book_sorts_and_retains_exchange_context(self) -> None:
        snapshot = parse_bybit_book(
            _response(
                {
                    "retCode": 0,
                    "retMsg": "OK",
                    "result": {
                        "s": "TESTUSDC",
                        "b": [["100", "2"], ["101", "1"]],
                        "a": [["103", "2"], ["102", "1"]],
                        "ts": 123,
                        "cts": 122,
                        "u": 7,
                        "seq": 8,
                    },
                },
            ),
            "TESTUSDC",
            "spot",
        )

        self.assertEqual(snapshot.status, "ok")
        self.assertEqual(snapshot.bids[0][0], Decimal("101"))
        self.assertEqual(snapshot.asks[0][0], Decimal("102"))
        self.assertEqual(snapshot.matching_engine_time_ms, 122)
        self.assertEqual(snapshot.cross_sequence, 8)

    def test_parse_mexc_book_sorts_depth_and_retains_update_id(self) -> None:
        snapshot = parse_mexc_book(
            _response(
                {
                    "lastUpdateId": 77,
                    "timestamp": 123,
                    "bids": [["0.4", "2"], ["0.5", "1"]],
                    "asks": [["0.7", "2"], ["0.6", "1"]],
                },
            ),
            "TESTUSDT",
        )

        self.assertEqual(snapshot.status, "ok")
        self.assertEqual(snapshot.bids[0][0], Decimal("0.5"))
        self.assertEqual(snapshot.asks[0][0], Decimal("0.6"))
        self.assertEqual(snapshot.update_id, 77)
        self.assertEqual(snapshot.exchange_system_time_ms, 123)

    def test_parse_mexc_websocket_partial_depth_protobuf(self) -> None:
        ask = _protobuf_bytes(1, b"0.6") + _protobuf_bytes(2, b"2")
        bid = _protobuf_bytes(1, b"0.5") + _protobuf_bytes(2, b"3")
        depth = (
            _protobuf_bytes(1, ask)
            + _protobuf_bytes(2, bid)
            + _protobuf_bytes(4, b"77")
            + _protobuf_int(5, 122)
        )
        message = (
            _protobuf_bytes(1, b"spot@public.limit.depth.v3.api.pb@TESTUSDT@20")
            + _protobuf_bytes(303, depth)
            + _protobuf_bytes(3, b"TESTUSDT")
            + _protobuf_int(6, 123)
        )

        snapshot = parse_mexc_partial_depth_message(
            message,
            received_realtime_ns=2_000_000_000,
            received_monotonic_ns=3_000_000_000,
        )

        self.assertEqual(snapshot.status, "ok")
        self.assertEqual(snapshot.source, "websocket_partial_depth")
        self.assertEqual(snapshot.bids, ((Decimal("0.5"), Decimal("3")),))
        self.assertEqual(snapshot.asks, ((Decimal("0.6"), Decimal("2")),))
        self.assertEqual(snapshot.exchange_system_time_ms, 123)
        self.assertEqual(snapshot.matching_engine_time_ms, 122)
        self.assertEqual(snapshot.update_id, 77)

    def test_parse_binance_and_okx_books(self) -> None:
        binance = parse_binance_book(
            _response(
                {
                    "lastUpdateId": 88,
                    "bids": [["0.5", "1"]],
                    "asks": [["0.6", "1"]],
                },
            ),
            "TESTUSDT",
        )
        okx = parse_okx_book(
            _response(
                {
                    "code": "0",
                    "msg": "",
                    "data": [
                        {
                            "bids": [["0.5", "1", "0", "1"]],
                            "asks": [["0.6", "1", "0", "1"]],
                            "ts": "456",
                            "seqId": 99,
                        },
                    ],
                },
            ),
            "TEST-USDT",
        )

        self.assertEqual(binance.status, "ok")
        self.assertEqual(binance.update_id, 88)
        self.assertEqual(okx.status, "ok")
        self.assertEqual(okx.cross_sequence, 99)
        self.assertEqual(okx.exchange_system_time_ms, 456)

    def test_okx_market_symbol_uses_hyphen(self) -> None:
        self.assertEqual(market_for_cex(MARKET, "OKX").cex_symbol, "TEST-USDC")

    def test_nearest_snapshot_is_selected_around_dex_response(self) -> None:
        before = _book(1_000_000_000)
        after = _book(1_300_000_000)
        self.assertIs(choose_nearest_book(1_240_000_000, (before, after)), after)

    def test_mexc_websocket_history_selects_nearest_event(self) -> None:
        stream = MexcPartialDepthStream(
            ("TESTUSDT",),
            levels=5,
            timeout_seconds=1,
            proxy_url=None,
            history_capacity_per_symbol=2,
        )
        earlier = _book(1_000_000_000)
        later = _book(1_300_000_000)
        stream._history["TESTUSDT"].extend((earlier, later))

        self.assertIs(stream.nearest_snapshot("TESTUSDT", 1_240_000_000), later)
        self.assertIs(stream.nearest_snapshot("TESTUSDT", 1_040_000_000), earlier)
        self.assertIsNone(stream.nearest_snapshot("MISSING", 1_040_000_000))


class MexcStreamReconnectTest(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_receive_error_is_owned_by_outer_supervisor(self) -> None:
        first = _FakeMexcWebsocket([ConnectionError("transient receive failure")])
        second = _FakeMexcWebsocket([_mexc_partial_depth_message()])
        sockets = iter((first, second))

        async def connect(*_args: object, **_kwargs: object) -> _FakeMexcWebsocket:
            return next(sockets)

        stream = MexcPartialDepthStream(
            ("TESTUSDT",),
            levels=5,
            timeout_seconds=1,
            proxy_url=None,
            connect_websocket=connect,
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "transport failed"):
                await stream.start()
            self.assertEqual(stream.reconnects, 0)
            self.assertIn("transient receive failure", stream.error or "")
            self.assertTrue(first.sent)
            self.assertFalse(second.sent)
        finally:
            await stream.close()


class CycleCalculationTest(unittest.TestCase):
    def test_t01_quote_fee_uses_depth_cost_without_double_charging_dex_fee(self) -> None:
        book = BookSnapshot(
            symbol="TESTUSDC",
            category="spot",
            status="ok",
            error=None,
            bids=((Decimal("0.99"), Decimal("200")),),
            asks=(
                (Decimal("1"), Decimal("50")),
                (Decimal("1.02"), Decimal("50")),
            ),
            exchange_system_time_ms=1,
            matching_engine_time_ms=1,
            update_id=2,
            cross_sequence=3,
            response=_response({}, 2_050_000_000),
        )
        cycle = calculate_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "sell_base",
                "requested_notional_quote": "101",
                "base_amount": "100",
                "quote_amount": "104",
                "average_price_quote_per_base": "1.04",
                "response_received_realtime_ns": 2_000_000_000,
                "request_rtt_ms": 5,
            },
            book=book,
            cex_taker_fee_bps=Decimal("10"),
            cex_buy_fee_currency="quote",
            network_cost_floor_quote=Decimal("0.2"),
            max_response_skew_ms=Decimal("100"),
        )

        self.assertEqual(cycle["cex_book_base_quantity"], "100")
        self.assertEqual(cycle["cex_execution_estimate"]["levels_consumed"], 2)
        self.assertEqual(cycle["cex_execution_estimate"]["costs"][0]["native_amount"], "0.10100")
        self.assertEqual(Decimal(cycle["net_pnl_before_network_quote"]), Decimal("2.899"))
        self.assertEqual(
            Decimal(cycle["net_pnl_after_minimum_network_quote"]),
            Decimal("2.699"),
        )

    def test_t02_base_fee_walks_gross_quantity_in_cycle(self) -> None:
        book = BookSnapshot(
            symbol="TESTUSDC",
            category="spot",
            status="ok",
            error=None,
            bids=((Decimal("0.99"), Decimal("200")),),
            asks=(
                (Decimal("1"), Decimal("100")),
                (Decimal("2"), Decimal("1")),
            ),
            exchange_system_time_ms=1,
            matching_engine_time_ms=1,
            update_id=2,
            cross_sequence=3,
            response=_response({}, 2_050_000_000),
        )
        cycle = calculate_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "sell_base",
                "requested_notional_quote": "100",
                "base_amount": "100",
                "quote_amount": "103",
                "average_price_quote_per_base": "1.03",
                "response_received_realtime_ns": 2_000_000_000,
                "request_rtt_ms": 5,
            },
            book=book,
            cex_taker_fee_bps=Decimal("10"),
            cex_buy_fee_currency="base",
            network_cost_floor_quote=Decimal("0"),
            max_response_skew_ms=Decimal("100"),
        )

        execution = cycle["cex_execution_estimate"]
        expected_gross_base = Decimal("100") / Decimal("0.999")
        self.assertEqual(Decimal(execution["requested_book_base_quantity"]), expected_gross_base)
        self.assertEqual(Decimal(execution["net_base_movement"]), Decimal("100"))
        self.assertEqual(execution["levels_consumed"], 2)
        self.assertGreater(Decimal(cycle["gross_cost_quote"]), Decimal("100"))

    def test_buy_dex_sell_cex_walks_exact_base_and_applies_fee_and_cost(self) -> None:
        cycle = calculate_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "buy_base",
                "requested_notional_quote": "100",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": 2_000_000_000,
                "request_rtt_ms": 5,
            },
            book=_book(2_050_000_000),
            cex_taker_fee_bps=Decimal("10"),
            network_cost_floor_quote=Decimal("0.1"),
            max_response_skew_ms=Decimal("100"),
        )

        self.assertEqual(cycle["cycle_direction"], "buy_dex_sell_cex")
        self.assertEqual(cycle["gross_pnl_quote"], "1")
        self.assertEqual(cycle["net_pnl_before_network_quote"], "0.899")
        self.assertEqual(cycle["net_pnl_after_minimum_network_quote"], "0.799")
        self.assertTrue(cycle["positive_after_minimum_network"])

    def test_cycle_records_selected_cex_venue(self) -> None:
        cycle = calculate_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "buy_base",
                "requested_notional_quote": "100",
                "base_amount": "1",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
                "response_received_realtime_ns": 2_000_000_000,
                "request_rtt_ms": 5,
            },
            book=_book(2_050_000_000),
            cex_taker_fee_bps=Decimal("0"),
            network_cost_floor_quote=Decimal("0.1"),
            max_response_skew_ms=Decimal("100"),
            cex_venue="MEXC",
        )

        self.assertEqual(cycle["cex_venue"], "MEXC")

    def test_buy_cex_sell_dex_sizes_spot_fee_without_losing_base(self) -> None:
        cycle = calculate_cycle(
            market=MARKET,
            dex_record={
                "round_id": 1,
                "direction": "sell_base",
                "requested_notional_quote": "100",
                "base_amount": "1",
                "quote_amount": "103",
                "average_price_quote_per_base": "103",
                "response_received_realtime_ns": 2_000_000_000,
                "request_rtt_ms": 5,
            },
            book=_book(2_050_000_000),
            cex_taker_fee_bps=Decimal("10"),
            network_cost_floor_quote=Decimal("0.1"),
            max_response_skew_ms=Decimal("100"),
        )

        self.assertEqual(cycle["cycle_direction"], "buy_cex_sell_dex")
        self.assertAlmostEqual(float(cycle["net_pnl_before_network_quote"]), 0.8978979)
        self.assertTrue(cycle["positive_after_minimum_network"])

    def test_side_specific_account_fee_is_applied_to_the_actual_cex_side(self) -> None:
        common = {
            "round_id": 1,
            "requested_notional_quote": "100",
            "base_amount": "1",
            "response_received_realtime_ns": 2_000_000_000,
            "request_rtt_ms": 5,
        }
        sell_on_cex = calculate_cycle(
            market=MARKET,
            dex_record={
                **common,
                "direction": "buy_base",
                "quote_amount": "100",
                "average_price_quote_per_base": "100",
            },
            book=_book(2_050_000_000),
            cex_taker_fee_bps=Decimal("0"),
            cex_buy_taker_fee_bps=Decimal("20"),
            cex_sell_taker_fee_bps=Decimal("10"),
            cex_fee_source="account_fee_api",
            cex_fee_account_verified=True,
            network_cost_floor_quote=Decimal("0.1"),
            max_response_skew_ms=Decimal("100"),
        )
        buy_on_cex = calculate_cycle(
            market=MARKET,
            dex_record={
                **common,
                "direction": "sell_base",
                "quote_amount": "103",
                "average_price_quote_per_base": "103",
            },
            book=_book(2_050_000_000),
            cex_taker_fee_bps=Decimal("0"),
            cex_buy_taker_fee_bps=Decimal("20"),
            cex_sell_taker_fee_bps=Decimal("10"),
            cex_fee_source="account_fee_api",
            cex_fee_account_verified=True,
            network_cost_floor_quote=Decimal("0.1"),
            max_response_skew_ms=Decimal("100"),
        )

        self.assertEqual(sell_on_cex["cex_fee_side_used"], "SELL")
        self.assertEqual(sell_on_cex["cex_taker_fee_bps"], "10")
        self.assertEqual(sell_on_cex["net_pnl_before_network_quote"], "0.899")
        self.assertEqual(buy_on_cex["cex_fee_side_used"], "BUY")
        self.assertEqual(buy_on_cex["cex_taker_fee_bps"], "20")
        self.assertAlmostEqual(float(buy_on_cex["net_pnl_before_network_quote"]), 0.79559118)
        self.assertTrue(buy_on_cex["candidate_eligible_with_account_verified_fee"])

    def test_best_pool_is_selected_per_direction(self) -> None:
        common = {
            "status": "ok",
            "requested_notional_quote": "100",
            "round_id": 0,
        }
        selected = _best_dex_records(
            [
                {**common, "direction": "buy_base", "base_amount": "1", "quote_amount": "100"},
                {**common, "direction": "buy_base", "base_amount": "1.1", "quote_amount": "100"},
                {**common, "direction": "sell_base", "base_amount": "1", "quote_amount": "99"},
                {**common, "direction": "sell_base", "base_amount": "1", "quote_amount": "99.5"},
            ],
        )

        self.assertEqual(selected[("100", "buy_base")]["base_amount"], "1.1")
        self.assertEqual(selected[("100", "sell_base")]["quote_amount"], "99.5")


class LongTailRegistryTest(unittest.TestCase):
    def test_non_solana_providers_receive_request_level_pacers(self) -> None:
        evm_pacer = AsyncRequestPacer(0.25)
        stonfi_pacer = AsyncRequestPacer(0.5)
        omniston_pacer = AsyncRequestPacer(0.75)
        providers = build_cycle_providers(
            ["ETH_BASE_UNISWAP", "NOT_TON_STONFI", "DOGS_TON_OMNISTON"],
            base_rpc_url="https://base.example",
            polygon_rpc_url="https://polygon.example",
            fee_tiers=[500],
            proxy_url=None,
            timeout_seconds=1,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            evm_request_pacer=evm_pacer,
            stonfi_request_pacer=stonfi_pacer,
            omniston_request_pacer=omniston_pacer,
            fetch_json=lambda *args: {},
        )

        self.assertIs(providers["UNISWAP_BASE"].request_pacer, evm_pacer)
        self.assertIs(providers["STONFI_NOT"].request_pacer, stonfi_pacer)
        self.assertIs(providers["OMNISTON_DOGS"].request_pacer, omniston_pacer)

    def test_solana_and_ton_markets_build_canonical_asset_providers(self) -> None:
        providers = build_cycle_providers(
            ["MEW_SOLANA_RAYDIUM", "NOT_TON_STONFI"],
            base_rpc_url="https://base.example",
            polygon_rpc_url="https://polygon.example",
            fee_tiers=[500],
            proxy_url=None,
            timeout_seconds=1,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            fetch_json=lambda *args: {},
        )

        self.assertEqual(MARKETS["MEW_SOLANA_RAYDIUM"].cex_symbol, "MEWUSDC")
        self.assertEqual(providers["RAYDIUM_MEW"].base.symbol, "MEW")
        self.assertEqual(providers["RAYDIUM_MEW"].base.decimals, 5)
        self.assertEqual(MARKETS["NOT_TON_STONFI"].cex_symbol, "NOTUSDT")
        self.assertEqual(providers["STONFI_NOT"].base.symbol, "NOT")
        self.assertEqual(providers["STONFI_NOT"].base.decimals, 9)

    def test_jupiter_market_uses_canonical_mint_and_shared_pacer(self) -> None:
        providers = build_cycle_providers(
            ["MEW_SOLANA_JUPITER", "TRUMP_SOLANA_JUPITER"],
            base_rpc_url="https://base.example",
            polygon_rpc_url="https://polygon.example",
            fee_tiers=[500],
            proxy_url=None,
            timeout_seconds=1,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            jupiter_min_request_interval_seconds=0,
            fetch_json=lambda *args: {},
        )

        self.assertEqual(MARKETS["MEW_SOLANA_JUPITER"].cex_symbol, "MEWUSDC")
        self.assertEqual(providers["JUPITER_MEW"].base.decimals, 5)
        self.assertIs(
            providers["JUPITER_MEW"].request_pacer,
            providers["JUPITER_TRUMP"].request_pacer,
        )

    def test_solana_usdt_markets_use_canonical_tether_mint_and_cex_tickers(self) -> None:
        providers = build_cycle_providers(
            [
                "JUP_SOLANA_RAYDIUM_USDT",
                "PUMP_SOLANA_RAYDIUM_USDT",
                "BONK_SOLANA_RAYDIUM_USDT",
                "BTC_SOLANA_JUPITER_USDT",
            ],
            base_rpc_url="https://base.example",
            polygon_rpc_url="https://polygon.example",
            fee_tiers=[500],
            proxy_url=None,
            timeout_seconds=1,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            raydium_min_request_interval_seconds=0,
            jupiter_min_request_interval_seconds=0,
            fetch_json=lambda *args: {},
        )

        self.assertEqual(MARKETS["JUP_SOLANA_RAYDIUM_USDT"].cex_symbol, "JUPUSDT")
        self.assertEqual(providers["RAYDIUM_USDT_JUP"].base.symbol, "JUP")
        self.assertEqual(providers["RAYDIUM_USDT_JUP"].quote.symbol, "USDT")
        self.assertIs(
            providers["RAYDIUM_USDT_JUP"].request_pacer,
            providers["RAYDIUM_USDT_PUMP"].request_pacer,
        )
        self.assertEqual(
            providers["RAYDIUM_USDT_JUP"].config()["minimum_request_interval_seconds"],
            0,
        )
        self.assertEqual(MARKETS["BONK_SOLANA_RAYDIUM_USDT"].cex_symbol, "BONKUSDT")
        self.assertEqual(providers["RAYDIUM_USDT_BONK"].base.decimals, 5)
        self.assertEqual(MARKETS["BTC_SOLANA_JUPITER_USDT"].cex_symbol, "BTCUSDT")
        self.assertEqual(providers["JUPITER_USDT_CBBTC"].base.symbol, "cbBTC")
        self.assertEqual(providers["JUPITER_USDT_CBBTC"].quote.address, "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB")

    def test_omniston_market_uses_v1beta8_aggregator_provider(self) -> None:
        providers = build_cycle_providers(
            ["DOGS_TON_OMNISTON"],
            base_rpc_url="https://base.example",
            polygon_rpc_url="https://polygon.example",
            fee_tiers=[500],
            proxy_url=None,
            timeout_seconds=1,
            raydium_slippage_bps=50,
            stonfi_slippage_tolerance=Decimal("0.005"),
            omniston_quote_selection_window_seconds=0,
            fetch_json=lambda *args: {},
        )

        provider = providers["OMNISTON_DOGS"]
        self.assertEqual(MARKETS["DOGS_TON_OMNISTON"].cex_symbol, "DOGSUSDT")
        self.assertEqual(provider.base.symbol, "DOGS")
        self.assertEqual(provider.config()["protocol"], "Omniston v1beta8 Meta-Aggregator")
        self.assertFalse(provider.config()["wallet_or_taker_supplied"])


if __name__ == "__main__":
    unittest.main()


class EventedBookStreamTransportFailureTest(unittest.IsolatedAsyncioTestCase):
    """BUG-004: transport failures must propagate to the outer supervisor."""

    async def test_transport_failure_propagates_instead_of_silent_reconnect(self) -> None:
        from market_data_lab.cex_book_streams import BybitOrderBookStream

        async def connect(*_args, **_kwargs):
            raise ConnectionError("test: transport failure")

        stream = BybitOrderBookStream(
            ("BTCUSDT",),
            timeout_seconds=1,
            proxy_url=None,
            connect_websocket=connect,
        )
        try:
            await stream.start()
            self.fail("expected RuntimeError from transport failure")
        except RuntimeError as exc:
            self.assertIn("transport", str(exc).lower())
            self.assertIsNotNone(stream.error)
        finally:
            await stream.close()

    async def test_next_update_raises_after_receiver_task_exits(self) -> None:
        from market_data_lab.cex_book_streams import BybitOrderBookStream

        class _FailingWebsocket:
            sent = False

            async def send(self, data: str) -> None:
                self.sent = True

            async def recv(self) -> str:
                raise ConnectionError("test: recv failure")

            async def close(self) -> None:
                pass

        async def connect(*_args, **_kwargs):
            return _FailingWebsocket()

        stream = BybitOrderBookStream(
            ("BTCUSDT",),
            timeout_seconds=1,
            proxy_url=None,
            connect_websocket=connect,
        )
        try:
            await stream.start()
            self.fail("expected RuntimeError from transport failure during start")
        except RuntimeError:
            pass
        finally:
            await stream.close()


if __name__ == "__main__":
    unittest.main()
