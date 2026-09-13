"""Conservative capability gate for public perpetual contract arithmetic.

The strategy evaluator receives public top-of-book quantities.  Multiplying a
price by such a quantity is only valid when the feed has identified a linear
base-quantity perpetual.  This small module centralises that decision so an
inverse, quanto, prelaunch, or absent contract classification cannot
accidentally inherit the linear PnL formula.

It deliberately does not implement inverse or multiplier arithmetic.  Those
need their own settlement, multiplier, fee-currency, and golden-fixture work.
"""

from __future__ import annotations

from dataclasses import dataclass


_LINEAR_BASE_QUANTITY_TYPES = frozenset(
    {
        "linear",
        "linear_perp",
        "linear_perpetual",
        "linearperpetual",
    }
)


def _normalise_contract_type(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = "_".join(value.strip().lower().replace("-", "_").split())
    return normalized or None


@dataclass(frozen=True, slots=True)
class PerpContractModel:
    """What the shared evaluator is permitted to do with one perp quote."""

    model_id: str | None
    contract_type: str | None
    supported: bool
    quantity_semantics: str | None
    reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "model_id": self.model_id,
            "contract_type": self.contract_type,
            "supported": self.supported,
            "quantity_semantics": self.quantity_semantics,
            "reason": self.reason,
        }


def resolve_perp_contract_model(contract_type: str | None) -> PerpContractModel:
    """Resolve the only contract shape implemented by the current evaluator.

    ``linear_perpetual`` adapters expose sizes in base-asset units, so the
    common ``quantity * price`` cash-flow formula is valid for that typed
    input.  Every other type returns an explicit unsupported result rather
    than a best-effort conversion.
    """

    normalized = _normalise_contract_type(contract_type)
    if normalized is None:
        return PerpContractModel(
            model_id=None,
            contract_type=None,
            supported=False,
            quantity_semantics=None,
            reason="contract_type_unavailable",
        )
    if normalized in _LINEAR_BASE_QUANTITY_TYPES:
        return PerpContractModel(
            model_id="linear_base_quantity_perpetual_v1",
            contract_type=normalized,
            supported=True,
            quantity_semantics="base_asset_quantity",
            reason=None,
        )
    return PerpContractModel(
        model_id=None,
        contract_type=normalized,
        supported=False,
        quantity_semantics=None,
        reason="unsupported_contract_model",
    )
