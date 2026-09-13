from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from market_data_lab.account_fee_audit import _parse_symbols_by_venue
from market_data_lab.account_fee_audit import _signed_query
from market_data_lab.account_fee_audit import audit_spot_fee_rates_best_effort
from market_data_lab.account_fee_audit import fetch_mexc_spot_fees
from market_data_lab.account_fee_audit import fetch_okx_spot_fees
from market_data_lab.account_fee_audit import load_spot_fee_audit
from market_data_lab.account_fee_audit import okx_readonly_headers
from market_data_lab.account_fee_audit import parse_binance_spot_fee
from market_data_lab.account_fee_audit import parse_bybit_spot_fee
from market_data_lab.account_fee_audit import parse_mexc_spot_fee
from market_data_lab.account_fee_audit import parse_okx_spot_fee
from market_data_lab.account_fee_audit import parse_okx_spot_instrument_group
from market_data_lab.account_fee_audit import resolve_spot_fee_rate
from market_data_lab.account_fee_audit import symbols_for_market_universe


class AccountFeeAuditTest(unittest.TestCase):
    def test_bybit_and_mexc_parsers_normalize_to_cost_bps(self) -> None:
        bybit = parse_bybit_spot_fee(
            {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "BTCUSDT", "makerFeeRate": "0.0008", "takerFeeRate": "0.001"},
                    ],
                },
            },
            symbol="BTCUSDT",
        )
        mexc = parse_mexc_spot_fee(
            {
                "code": 0,
                "data": {"makerCommission": "0", "takerCommission": "0.0005"},
            },
            symbol="BTCUSDT",
        )

        self.assertEqual(bybit.taker_buy_bps, Decimal("10"))
        self.assertEqual(bybit.maker_sell_bps, Decimal("8"))
        self.assertEqual(mexc.maker_buy_bps, Decimal("0"))
        self.assertEqual(mexc.taker_sell_bps, Decimal("5"))

    def test_binance_is_side_specific_and_conservative_about_bnb_discount(self) -> None:
        fee = parse_binance_spot_fee(
            {
                "symbol": "BTCUSDT",
                "standardCommission": {
                    "maker": "0.0008",
                    "taker": "0.001",
                    "buyer": "0.0001",
                    "seller": "0.0002",
                },
                "specialCommission": {"maker": "0", "taker": "0", "buyer": "0", "seller": "0"},
                "taxCommission": {
                    "maker": "0.00001",
                    "taker": "0.00001",
                    "buyer": "0.00001",
                    "seller": "0.00001",
                },
                "discount": {
                    "enabledForAccount": True,
                    "enabledForSymbol": True,
                    "discountAsset": "BNB",
                    "discount": "0.75",
                },
            },
            symbol="BTCUSDT",
        )

        self.assertEqual(fee.taker_buy_bps, Decimal("11.2"))
        self.assertEqual(fee.taker_sell_bps, Decimal("12.2"))
        self.assertIn("conservative_no_optional_bnb_fee_discount", fee.assumptions)
        self.assertIn("BNB_discount_is_available_in_schedule_but_not_assumed_at_fill_time", fee.assumptions)

    def test_okx_negative_api_rate_becomes_positive_cost(self) -> None:
        fee = parse_okx_spot_fee(
            {
                "code": "0",
                "data": [{"maker": "-0.0008", "taker": "-0.001"}],
            },
            symbol="BTC-USDT",
        )

        self.assertEqual(fee.maker_buy_bps, Decimal("8"))
        self.assertEqual(fee.taker_sell_bps, Decimal("10"))

    def test_okx_current_fee_group_is_selected_for_the_symbol(self) -> None:
        group_id = parse_okx_spot_instrument_group(
            {
                "code": "0",
                "data": [{"instId": "BTC-USDC", "groupId": "2"}],
            },
            symbol="BTC-USDC",
        )
        fee = parse_okx_spot_fee(
            {
                "code": "0",
                "data": [
                    {
                        "feeGroup": [
                            {"groupId": "1", "maker": "-0.0008", "taker": "-0.001"},
                            {"groupId": "2", "maker": "-0.0003", "taker": "-0.0005"},
                        ],
                    },
                ],
            },
            symbol="BTC-USDC",
            group_id=group_id,
        )

        self.assertEqual(fee.maker_buy_bps, Decimal("3"))
        self.assertEqual(fee.taker_sell_bps, Decimal("5"))
        self.assertEqual(fee.source, "okx_v5_account_trade_fee_spot_fee_group")

    def test_okx_fetch_uses_instrument_group_then_fee_group(self) -> None:
        seen: list[str] = []

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            self.assertEqual(method, "GET")
            self.assertIn("OK-ACCESS-SIGN", headers)
            seen.append(url)
            if "/account/instruments?" in url:
                return {"code": "0", "data": [{"instId": "BTC-USDT", "groupId": "1"}]}
            self.assertIn("/account/trade-fee?instType=SPOT&groupId=1", url)
            return {
                "code": "0",
                "data": [
                    {"feeGroup": [{"groupId": "1", "maker": "-0.0008", "taker": "-0.001"}]},
                ],
            }

        rates = asyncio.run(
            fetch_okx_spot_fees(
                ("BTC-USDT",),
                api_key="key",
                api_secret="secret",
                passphrase="pass",
                proxy_url=None,
                timeout_seconds=1,
                fetch_json=fetch,
            ),
        )

        self.assertEqual(rates["BTC-USDT"].taker_buy_bps, Decimal("10"))
        self.assertEqual(len(seen), 2)

    def test_query_and_okx_signature_are_deterministic(self) -> None:
        query = _signed_query(
            (("symbol", "BTCUSDT"), ("timestamp", "1700000000000")),
            secret="secret",
        )
        self.assertEqual(
            query,
            "symbol=BTCUSDT&timestamp=1700000000000&signature="
            "6244d11c958f45ac56733152cb3cb1831d23a2b3709b3a88b8b42a072aceb410",
        )
        headers = okx_readonly_headers(
            api_key="key",
            api_secret="secret",
            passphrase="pass",
            request_path="/api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT",
            timestamp="2026-09-02T00:00:00.000Z",
        )
        expected = base64.b64encode(
            hmac.new(
                b"secret",
                b"2026-09-02T00:00:00.000ZGET/api/v5/account/trade-fee?instType=SPOT&instId=BTC-USDT",
                hashlib.sha256,
            ).digest(),
        ).decode()
        self.assertEqual(headers["OK-ACCESS-SIGN"], expected)

    def test_mexc_fetch_only_uses_read_fee_endpoint(self) -> None:
        seen: list[tuple[str, str, dict[str, str]]] = []

        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            seen.append((url, method, headers))
            return {"code": 0, "data": {"makerCommission": "0", "takerCommission": "0.0005"}}

        rates = asyncio.run(
            fetch_mexc_spot_fees(
                ("BTCUSDT",),
                api_key="key",
                api_secret="secret",
                proxy_url=None,
                timeout_seconds=1,
                fetch_json=fetch,
            ),
        )

        self.assertEqual(rates["BTCUSDT"].taker_buy_bps, Decimal("5"))
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0][1], "GET")
        self.assertEqual(seen[0][2], {"X-MEXC-APIKEY": "key"})
        self.assertIn("/api/v3/tradeFee?symbol=BTCUSDT", seen[0][0])

    def test_symbol_mapping_parser(self) -> None:
        parsed = _parse_symbols_by_venue("BYBIT=BTCUSDT|ETHUSDT;OKX=BTC-USDT")
        self.assertEqual(parsed["BYBIT"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(parsed["OKX"], ["BTC-USDT"])

    def test_market_universe_builds_venue_specific_symbols(self) -> None:
        single_asset = symbols_for_market_universe(
            "continuous-maximum",
            ("BYBIT", "OKX"),
        )
        triangle = symbols_for_market_universe("triangle-default", ("BYBIT", "OKX"))
        combined = symbols_for_market_universe("all-current", ("BYBIT", "OKX"))

        self.assertIn("BTCUSDC", single_asset["BYBIT"])
        self.assertIn("BTC-USDC", single_asset["OKX"])
        self.assertIn("BTCUSDT", triangle["BYBIT"])
        self.assertIn("BTC-USDT", triangle["OKX"])
        self.assertGreaterEqual(set(combined["BYBIT"]), set(single_asset["BYBIT"]))
        self.assertGreaterEqual(set(combined["OKX"]), set(triangle["OKX"]))

    def test_broad_audit_keeps_valid_rates_when_one_symbol_fails(self) -> None:
        def fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            self.assertEqual(method, "GET")
            self.assertEqual(headers["X-BAPI-API-KEY"], "key")
            if "MISSINGUSDT" in url:
                return {"retCode": 10001, "retMsg": "symbol not found"}
            return {
                "retCode": 0,
                "result": {
                    "list": [
                        {"symbol": "BTCUSDT", "makerFeeRate": "0.0001", "takerFeeRate": "0.0002"},
                    ],
                },
            }

        with patch.dict(
            os.environ,
            {"BYBIT_READONLY_API_KEY": "key", "BYBIT_READONLY_API_SECRET": "secret"},
            clear=False,
        ):
            outcome = asyncio.run(
                audit_spot_fee_rates_best_effort(
                    {"BYBIT": ("BTCUSDT", "MISSINGUSDT")},
                    proxy_url=None,
                    timeout_seconds=1,
                    minimum_request_interval_seconds=0,
                    fetch_json=fetch,
                ),
            )

        self.assertEqual(outcome.rates["BYBIT"]["BTCUSDT"].taker_buy_bps, Decimal("2"))
        self.assertIn("MISSINGUSDT", outcome.symbol_errors["BYBIT"])
        self.assertNotIn("secret", outcome.symbol_errors["BYBIT"]["MISSINGUSDT"])

    def test_load_completed_secret_free_audit_report(self) -> None:
        report = {
            "schema_version": 1,
            "status": "completed",
            "venues": {
                "BYBIT": {
                    "rates": [
                        {
                            "venue": "BYBIT",
                            "symbol": "BTCUSDT",
                            "maker_buy_bps": "2",
                            "maker_sell_bps": "2",
                            "taker_buy_bps": "5.5",
                            "taker_sell_bps": "5.5",
                            "account_verified": True,
                            "source": "bybit_v5_account_fee_rate_spot",
                            "assumptions": [],
                        },
                    ],
                },
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fees.json"
            path.write_text(json.dumps(report), encoding="utf-8")
            rates = load_spot_fee_audit(path)

        loaded = rates[("BYBIT", "BTCUSDT")]
        self.assertTrue(loaded.account_verified)
        self.assertEqual(loaded.taker_buy_bps, Decimal("5.5"))

    def test_missing_symbol_is_explicitly_a_non_verified_fallback(self) -> None:
        fallback = resolve_spot_fee_rate(
            venue="BYBIT",
            symbol="ETHUSDT",
            fallback_taker_bps=Decimal("10"),
            account_fee_rates={},
        )

        self.assertFalse(fallback.account_verified)
        self.assertEqual(fallback.taker_buy_bps, Decimal("10"))
        self.assertEqual(fallback.source, "configured_public_baseline_not_account_verified")


if __name__ == "__main__":
    unittest.main()
