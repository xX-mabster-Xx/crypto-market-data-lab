"""Small, explicit quality vocabulary for read-only strategy results.

``execution_ready`` is intentionally always false in this project, but that
single flag cannot tell an operator whether a row is only a price observation,
has a current close model, or merely lacks account context.  This module keeps
those independent statements typed and stable without turning them into an
execution policy.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class CandidateQuality:
    """Orthogonal evidence dimensions for one public-data model."""

    market_evidence: str
    economics: str
    resource_context: str
    funding_quality: str
    validation: str
    search_completeness: str
    execution_enabled: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def derive_candidate_quality(
    *,
    analysis_kind: str,
    timing_valid: bool,
    pnl_model_complete: bool,
    full_exit_model: bool,
    funding_quality: str,
    has_exact_dex_quote: bool,
    top_of_book_only: bool,
    dex_post_trade_pool_state_simulated: bool | None,
) -> CandidateQuality:
    """Classify what a result proves, without upgrading its eligibility.

    The vocabulary intentionally describes the current implementation rather
    than an aspirational execution path.  In particular, two public DEX
    simulations are still ``quote_checked`` research evidence, never a
    sequential pool-state fill simulation.
    """

    if dex_post_trade_pool_state_simulated is True:
        market_evidence = "sequential_simulation"
    elif has_exact_dex_quote and top_of_book_only:
        market_evidence = "exact_quote_and_bbo"
    elif has_exact_dex_quote:
        market_evidence = "exact_quote"
    elif top_of_book_only:
        market_evidence = "bbo"
    else:
        market_evidence = "depth_checked"

    if not pnl_model_complete:
        economics = "entry_basis_only"
    elif funding_quality == "projected":
        economics = "position_scenario"
    elif analysis_kind == "spot_spot_inventory_cycle":
        economics = "spot_conversion"
    elif full_exit_model:
        # Static current-price close math is not a paper position lifecycle.
        economics = "current_flat_model"
    else:
        economics = "incomplete"

    if funding_quality not in {"none_required", "unknown", "projected"}:
        raise ValueError("funding_quality must be none_required, unknown, or projected")

    if timing_valid and has_exact_dex_quote:
        validation = "quote_checked"
    elif timing_valid:
        validation = "screened"
    else:
        validation = "stale_or_unsynchronised"

    return CandidateQuality(
        market_evidence=market_evidence,
        economics=economics,
        resource_context="hypothetical",
        funding_quality=funding_quality,
        validation=validation,
        search_completeness="bounded_shortlist",
        execution_enabled=False,
    )
