from __future__ import annotations

import asyncio
import unittest
from typing import Any

from market_data_lab.jupiter_gated_verifier import JupiterGatedVerifier


class JupiterGatedVerifierTest(unittest.TestCase):
    def test_quote_only_omits_taker_and_does_not_retain_transaction(self) -> None:
        captured: dict[str, Any] = {}

        def fake_fetch(
            url: str,
            method: str,
            body: bytes | None,
            headers: dict[str, str],
            proxy_url: str | None,
            timeout_seconds: float,
        ) -> object:
            captured.update(
                url=url,
                method=method,
                body=body,
                headers=headers,
                proxy_url=proxy_url,
                timeout_seconds=timeout_seconds,
            )
            return {
                "inAmount": "100",
                "outAmount": "234",
                "router": "Metis",
                "routePlan": [{"swapInfo": {"label": "Raydium CLMM"}}],
                "transaction": "serialized-transaction-must-not-be-retained",
            }

        verifier = JupiterGatedVerifier(
            api_key="local-test-key",
            minimum_request_interval_seconds=0.001,
            fetch_json=fake_fetch,
        )
        result = asyncio.run(
            verifier.verify_exact_input(
                input_mint="input-mint",
                output_mint="output-mint",
                input_amount_raw=100,
            ),
        )

        self.assertEqual(captured["method"], "GET")
        self.assertIsNone(captured["body"])
        self.assertEqual(captured["headers"], {"x-api-key": "local-test-key"})
        self.assertIn("inputMint=input-mint", captured["url"])
        self.assertNotIn("taker=", captured["url"])
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["out_amount_raw"], "234")
        self.assertTrue(result["transaction_returned_nonempty"])
        self.assertNotIn("serialized-transaction-must-not-be-retained", str(result))
        self.assertNotIn("local-test-key", str(verifier.safe_descriptor()))

    def test_error_response_is_compact(self) -> None:
        def fake_fetch(*_args: Any, **_kwargs: Any) -> object:
            return {"error": "no route"}

        verifier = JupiterGatedVerifier(
            minimum_request_interval_seconds=0.001,
            fetch_json=fake_fetch,
        )
        result = asyncio.run(
            verifier.verify_exact_input(
                input_mint="input-mint",
                output_mint="output-mint",
                input_amount_raw=1,
            ),
        )
        self.assertEqual(result["status"], "quote_unavailable")
        self.assertEqual(verifier.snapshot()["errors"], {"invalid_quote": 1})


if __name__ == "__main__":
    unittest.main()
