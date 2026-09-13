"""Low-frequency, quote-only Jupiter cross-checks for local route screens.

Jupiter is intentionally *not* a hot feed in the unified scanner.  Local pool
state and direct CEX WebSocket books drive the high-frequency path.  This
module may be called only after that path finds a timing-valid positive screen,
at a separately shared request budget.  It omits ``taker`` so it cannot ask
Jupiter to construct a usable transaction for a wallet.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
import urllib.parse

from market_data_lab.dex_quotes import AsyncRequestPacer
from market_data_lab.dex_quotes import JsonFetcher
from market_data_lab.dex_quotes import _fetch_json_sync
from market_data_lab.dex_quotes import _redact_url
from market_data_lab.dex_quotes import _timed_fetch


JUPITER_ORDER_ENDPOINT = "https://api.jup.ag/swap/v2/order"


def _safe_text(value: object, *, limit: int = 128) -> str | None:
    if not isinstance(value, str):
        return None
    compact = value.strip()
    return compact[:limit] if compact else None


@dataclass
class JupiterGatedVerifier:
    """Shared low-rate quote-only verifier with compact, non-secret results."""

    api_key: str | None = field(default=None, repr=False)
    minimum_request_interval_seconds: float = 1.05
    proxy_url: str | None = None
    timeout_seconds: float = 10.0
    fetch_json: JsonFetcher = _fetch_json_sync
    _pacer: AsyncRequestPacer = field(init=False, repr=False)
    requests: int = field(default=0, init=False)
    successes: int = field(default=0, init=False)
    errors: Counter[str] = field(default_factory=Counter, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.minimum_request_interval_seconds <= 0 or self.timeout_seconds <= 0:
            raise ValueError("Jupiter verifier interval and timeout must be positive")
        self._pacer = AsyncRequestPacer(self.minimum_request_interval_seconds)

    def safe_descriptor(self) -> dict[str, object]:
        return {
            "enabled": True,
            "mode": "gated_quote_only_cross_check_not_hot_feed",
            "endpoint_origin": _redact_url(JUPITER_ORDER_ENDPOINT),
            "api_key_configured": self.api_key is not None,
            "minimum_request_interval_seconds": self.minimum_request_interval_seconds,
            "taker_omitted": True,
            "transaction_requested": False,
            "transactions_submitted": False,
        }

    def snapshot(self) -> dict[str, object]:
        return {
            **self.safe_descriptor(),
            "requests": self.requests,
            "successes": self.successes,
            "errors": dict(sorted(self.errors.items())),
        }

    async def verify_exact_input(
        self,
        *,
        input_mint: str,
        output_mint: str,
        input_amount_raw: int,
    ) -> dict[str, object]:
        """Return a small route cross-check; never retain a transaction body."""

        if not input_mint or not output_mint or input_mint == output_mint:
            raise ValueError("Jupiter verification requires two distinct non-empty mints")
        if input_amount_raw <= 0:
            raise ValueError("Jupiter verification input amount must be positive")
        await self._pacer.wait()
        self.requests += 1
        query = urllib.parse.urlencode(
            {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(input_amount_raw),
                # No `taker`, payer, receiver, referral or execution option.
                # The endpoint is therefore used as a quote-only observation.
            },
        )
        headers = {"x-api-key": self.api_key} if self.api_key is not None else {}
        response = await _timed_fetch(
            self.fetch_json,
            url=f"{JUPITER_ORDER_ENDPOINT}?{query}",
            method="GET",
            body=None,
            headers=headers,
            proxy_url=self.proxy_url,
            timeout_seconds=self.timeout_seconds,
        )
        common: dict[str, object] = {
            "source": "jupiter_swap_v2_order_quote_only",
            "request_rtt_ms": round(response.rtt_ms, 3),
            "taker_omitted": True,
            "transaction_requested": False,
        }
        if response.error is not None:
            self.errors["request_error"] += 1
            return {**common, "status": "request_error", "error": _redact_url(response.error)[:256]}
        payload = response.payload
        if not isinstance(payload, Mapping):
            self.errors["malformed_response"] += 1
            return {**common, "status": "quote_unavailable", "error": "Jupiter response is not an object"}
        try:
            observed_input = int(str(payload.get("inAmount")))
            output = int(str(payload.get("outAmount")))
            if observed_input != input_amount_raw or output <= 0:
                raise ValueError("mismatched input amount or non-positive output")
        except (TypeError, ValueError) as exc:
            self.errors["invalid_quote"] += 1
            error = _safe_text(payload.get("errorMessage") or payload.get("error")) or str(exc)
            return {**common, "status": "quote_unavailable", "error": error[:256]}

        # A route plan can be useful to classify an otherwise promising
        # discrepancy, but account keys and a serialized transaction are not.
        labels: list[str] = []
        raw_plan = payload.get("routePlan")
        if isinstance(raw_plan, list):
            for leg in raw_plan[:8]:
                if not isinstance(leg, Mapping):
                    continue
                swap_info = leg.get("swapInfo")
                if isinstance(swap_info, Mapping):
                    label = _safe_text(swap_info.get("label"), limit=64)
                    if label is not None:
                        labels.append(label)
        self.successes += 1
        return {
            **common,
            "status": "ok",
            "out_amount_raw": str(output),
            "router": _safe_text(payload.get("router")),
            "mode": _safe_text(payload.get("mode")),
            "fee_bps": payload.get("feeBps") if isinstance(payload.get("feeBps"), (int, float, str)) else None,
            "fee_mint": _safe_text(payload.get("feeMint")),
            "price_impact_pct": _safe_text(payload.get("priceImpactPct")),
            "route_labels": labels,
            # Never copy payload["transaction"] into any in-memory candidate
            # or on-disk diagnostic.  Quote-only calls normally return null,
            # but an unexpected response is merely recorded as a boolean.
            "transaction_returned_nonempty": bool(payload.get("transaction")),
        }
