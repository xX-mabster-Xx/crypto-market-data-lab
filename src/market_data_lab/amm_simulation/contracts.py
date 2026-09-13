from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .codec import raw_from_json


SwapMode = Literal["exact_in", "exact_out"]
PathStatus = Literal[
    "complete",
    "unsupported",
    "invalid_request",
    "insufficient_balance",
    "insufficient_liquidity",
    "deadline_exceeded",
    "state_unavailable",
    "arithmetic_error",
    "limit_exceeded",
]


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")


def _raw(value: int, name: str, *, minimum: int = 0) -> None:
    if type(value) is not int or value < minimum:  # noqa: E721
        raise ValueError(f"{name} must be an integer >= {minimum}")


@dataclass(frozen=True, slots=True)
class AssetRef:
    asset_id: str
    chain_namespace: str
    chain_id: str
    address_or_native_id: str
    decimals: int
    token_program: str
    token_extensions_fingerprint: str
    spec_version: int

    def __post_init__(self) -> None:
        for name in (
            "asset_id",
            "chain_namespace",
            "chain_id",
            "address_or_native_id",
            "token_program",
            "token_extensions_fingerprint",
        ):
            _text(getattr(self, name), name)
        _raw(self.decimals, "decimals")
        _raw(self.spec_version, "spec_version", minimum=1)


@dataclass(frozen=True, slots=True)
class PoolRef:
    chain_namespace: str
    chain_id: str
    program_id: str
    pool_address: str
    protocol: str
    protocol_revision: str
    asset_0_id: str
    asset_1_id: str
    pool_spec_version: int

    @property
    def pool_id(self) -> str:
        return f"{self.chain_namespace}:{self.chain_id}:{self.pool_address}"

    @property
    def economic_projection(self) -> dict[str, str | int]:
        return {
            "chain_namespace": self.chain_namespace,
            "chain_id": self.chain_id,
            "program_id": self.program_id,
            "pool_address": self.pool_address,
            "protocol": self.protocol,
            "protocol_revision": self.protocol_revision,
            "asset_0_id": self.asset_0_id,
            "asset_1_id": self.asset_1_id,
            "pool_spec_version": self.pool_spec_version,
        }

    def __post_init__(self) -> None:
        for name in (
            "chain_namespace",
            "chain_id",
            "program_id",
            "pool_address",
            "protocol",
            "protocol_revision",
            "asset_0_id",
            "asset_1_id",
        ):
            _text(getattr(self, name), name)
        _raw(self.pool_spec_version, "pool_spec_version", minimum=1)


@dataclass(frozen=True, slots=True)
class AccountVersion:
    address: str
    owner_program_id: str
    data_hash: str
    local_revision: int
    context_slot: int
    write_version: int | None = None
    received_realtime_ns: int | None = None
    received_monotonic_ns: int | None = None
    validated_realtime_ns: int | None = None
    validated_monotonic_ns: int | None = None
    read_batch_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("address", "owner_program_id", "data_hash"):
            _text(getattr(self, name), name)
        _raw(self.local_revision, "local_revision", minimum=1)
        _raw(self.context_slot, "context_slot")


@dataclass(frozen=True, slots=True)
class CpmmPoolBody:
    pool_ref: PoolRef
    reserve_0_raw: int
    reserve_1_raw: int
    fee_numerator: int
    fee_denominator: int

    @property
    def economic_projection(self) -> dict[str, str]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "reserve_0_raw": str(self.reserve_0_raw),
            "reserve_1_raw": str(self.reserve_1_raw),
            "fee_numerator": str(self.fee_numerator),
            "fee_denominator": str(self.fee_denominator),
        }

    def __post_init__(self) -> None:
        if not (self.reserve_0_raw > 0 and self.reserve_1_raw > 0):
            raise ValueError("CPMM reserves must be positive")
        if not (self.fee_numerator >= 0 and self.fee_denominator > self.fee_numerator):
            raise ValueError("CPMM fee numerator must be in [0, denominator)")


@dataclass(frozen=True, slots=True)
class RaydiumCpmmPoolBody:
    pool_ref: PoolRef
    vault_a_raw: int
    vault_b_raw: int
    protocol_fees_a_raw: int
    protocol_fees_b_raw: int
    fund_fees_a_raw: int
    fund_fees_b_raw: int
    creator_fees_a_raw: int
    creator_fees_b_raw: int
    trade_fee_rate: int
    creator_fee_rate: int
    protocol_fee_rate: int
    fund_fee_rate: int
    fee_on: int

    @property
    def economic_projection(self) -> dict[str, str | int]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "protocol_revision": self.pool_ref.protocol_revision,
            "vault_a_raw": str(self.vault_a_raw),
            "vault_b_raw": str(self.vault_b_raw),
            "protocol_fees_a_raw": str(self.protocol_fees_a_raw),
            "protocol_fees_b_raw": str(self.protocol_fees_b_raw),
            "fund_fees_a_raw": str(self.fund_fees_a_raw),
            "fund_fees_b_raw": str(self.fund_fees_b_raw),
            "creator_fees_a_raw": str(self.creator_fees_a_raw),
            "creator_fees_b_raw": str(self.creator_fees_b_raw),
            "trade_fee_rate": str(self.trade_fee_rate),
            "creator_fee_rate": str(self.creator_fee_rate),
            "protocol_fee_rate": str(self.protocol_fee_rate),
            "fund_fee_rate": str(self.fund_fee_rate),
            "fee_on": str(self.fee_on),
        }

    def effective_reserves(self) -> tuple[int, int]:
        return (
            self.vault_a_raw - self.protocol_fees_a_raw - self.fund_fees_a_raw - self.creator_fees_a_raw,
            self.vault_b_raw - self.protocol_fees_b_raw - self.fund_fees_b_raw - self.creator_fees_b_raw,
        )

    def __post_init__(self) -> None:
        if not (self.vault_a_raw > 0 and self.vault_b_raw > 0):
            raise ValueError("Raydium CPMM vault balances must be positive")
        if self.fee_on not in {0, 1, 2}:
            raise ValueError("Raydium CPMM fee_on must be 0, 1, or 2")
        _raw(self.trade_fee_rate, "trade_fee_rate")
        _raw(self.creator_fee_rate, "creator_fee_rate")
        _raw(self.protocol_fee_rate, "protocol_fee_rate")
        _raw(self.fund_fee_rate, "fund_fee_rate")
        if self.fund_fee_rate > self.trade_fee_rate:
            raise ValueError("Raydium CPMM fund fee rate must not exceed trade fee rate")





@dataclass(frozen=True, slots=True)
class RaydiumClmmPoolBody:
    """Immutable Raydium CLMM (concentrated liquidity) body for swap simulation."""

    pool_ref: PoolRef
    sqrt_price_x64: int
    liquidity_raw: int
    tick_current_index: int
    tick_spacing: int
    fee_rate: int
    protocol_fee_rate: int
    tick_arrays: tuple[ClmmTickArrayBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "protocol_revision": self.pool_ref.protocol_revision,
            "sqrt_price_x64": str(self.sqrt_price_x64),
            "liquidity_raw": str(self.liquidity_raw),
            "tick_current_index": self.tick_current_index,
            "tick_spacing": self.tick_spacing,
            "fee_rate": str(self.fee_rate),
            "protocol_fee_rate": str(self.protocol_fee_rate),
            "tick_arrays": [array.economic_projection for array in self.tick_arrays],
        }

    def effective_reserves(self) -> tuple[int, int]:
        """Notional effective reserves from liquidity and price, for diagnostics."""
        if self.sqrt_price_x64 == 0:
            raise ValueError("CLMM sqrt price must be positive")
        token_a = (self.liquidity_raw * self.sqrt_price_x64) >> 64
        token_b = (self.liquidity_raw << 64) // self.sqrt_price_x64
        return token_a, token_b

    def __post_init__(self) -> None:
        _raw(self.sqrt_price_x64, "sqrt_price_x64", minimum=1)
        _raw(self.liquidity_raw, "liquidity_raw", minimum=1)
        _raw(self.tick_spacing, "tick_spacing", minimum=1)
        _raw(self.fee_rate, "fee_rate")
        _raw(self.protocol_fee_rate, "protocol_fee_rate")


@dataclass(frozen=True, slots=True)
class MeteoraDlmmPoolBody:
    """Immutable Meteora DLMM (dynamic liquidity market maker) body.

    LB pair with bin arrays and bin step.  Only the supported subset
    (classic LB pair without token extensions) is accepted.
    """

    pool_ref: PoolRef
    active_id: int
    bin_step: int
    reserve_x_raw: int
    reserve_y_raw: int
    fee_bps: int
    protocol_fee_bps: int
    bin_arrays: tuple[DlmmBinArrayBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "protocol_revision": self.pool_ref.protocol_revision,
            "active_id": self.active_id,
            "bin_step": self.bin_step,
            "reserve_x_raw": str(self.reserve_x_raw),
            "reserve_y_raw": str(self.reserve_y_raw),
            "fee_bps": str(self.fee_bps),
            "protocol_fee_bps": str(self.protocol_fee_bps),
            "bin_arrays": [array.economic_projection for array in self.bin_arrays],
        }

    def effective_reserves(self) -> tuple[int, int]:
        return (self.reserve_x_raw, self.reserve_y_raw)

    def __post_init__(self) -> None:
        _raw(self.reserve_x_raw, "reserve_x_raw", minimum=1)
        _raw(self.reserve_y_raw, "reserve_y_raw", minimum=1)
        _raw(self.fee_bps, "fee_bps")
        _raw(self.protocol_fee_bps, "protocol_fee_bps")


@dataclass(frozen=True, slots=True)
class DlmmBinArrayBody:
    """Immutable DLMM bin array body."""

    start_bin_id: int
    bins: tuple[DlmmBinBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "start_bin_id": self.start_bin_id,
            "bins": [bin.economic_projection for bin in self.bins],
        }

    def __post_init__(self) -> None:
        _raw(self.start_bin_id, "start_bin_id", minimum=0)


@dataclass(frozen=True, slots=True)
class DlmmBinBody:
    """Immutable DLMM bin body with liquidity and fee info."""

    bin_id: int
    reserve_x_raw: int
    reserve_y_raw: int
    liquidity_raw: int
    fee_x_raw: int
    fee_y_raw: int

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "bin_id": self.bin_id,
            "reserve_x_raw": str(self.reserve_x_raw),
            "reserve_y_raw": str(self.reserve_y_raw),
            "liquidity_raw": str(self.liquidity_raw),
            "fee_x_raw": str(self.fee_x_raw),
            "fee_y_raw": str(self.fee_y_raw),
        }

    def __post_init__(self) -> None:
        _raw(self.bin_id, "bin_id")
        _raw(self.reserve_x_raw, "reserve_x_raw")
        _raw(self.reserve_y_raw, "reserve_y_raw")
        _raw(self.liquidity_raw, "liquidity_raw")
        _raw(self.fee_x_raw, "fee_x_raw")
        _raw(self.fee_y_raw, "fee_y_raw")


@dataclass(frozen=True, slots=True)
class ClmmTickArrayBody:
    """Immutable CLMM tick array body."""

    start_tick_index: int
    ticks: tuple[ClmmTickBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "start_tick_index": self.start_tick_index,
            "ticks": [tick.economic_projection for tick in self.ticks],
        }

    def __post_init__(self) -> None:
        _raw(self.start_tick_index, "start_tick_index")


@dataclass(frozen=True, slots=True)
class ClmmTickBody:
    """Immutable CLMM tick body."""

    initialized: bool
    liquidity_net: int
    liquidity_gross: int

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "initialized": self.initialized,
            "liquidity_net": self.liquidity_net,
            "liquidity_gross": self.liquidity_gross,
        }

    def __post_init__(self) -> None:
        if not isinstance(self.initialized, bool):
            raise ValueError("CLMM tick initialized must be a bool")


@dataclass(frozen=True, slots=True)
class RaydiumAmmV4PoolBody:
    """Immutable Raydium AMM v4 body (restricted swap-only subset).

    Only the subset with direct vault accounting and no OpenBook
    orderbook dependencies is supported.  Pools using needTakePnl with
    orderbook integration return UnsupportedVariant at runtime.
    """

    pool_ref: PoolRef
    vault_a_raw: int
    vault_b_raw: int
    fee_raw_a: int
    fee_raw_b: int
    fee_rate: int
    need_take_pnl: bool
    open_orders: str | None
    status: int

    @property
    def economic_projection(self) -> dict[str, str | int | bool | None]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "protocol_revision": self.pool_ref.protocol_revision,
            "vault_a_raw": str(self.vault_a_raw),
            "vault_b_raw": str(self.vault_b_raw),
            "fee_raw_a": str(self.fee_raw_a),
            "fee_raw_b": str(self.fee_raw_b),
            "fee_rate": str(self.fee_rate),
            "need_take_pnl": self.need_take_pnl,
            "open_orders": self.open_orders,
            "status": self.status,
        }

    def effective_reserves(self) -> tuple[int, int]:
        return (
            self.vault_a_raw - self.fee_raw_a,
            self.vault_b_raw - self.fee_raw_b,
        )

    def __post_init__(self) -> None:
        if not (self.vault_a_raw > 0 and self.vault_b_raw > 0):
            raise ValueError("AMM v4 vault balances must be positive")
        _raw(self.fee_raw_a, "fee_raw_a")
        _raw(self.fee_raw_b, "fee_raw_b")
        _raw(self.fee_rate, "fee_rate")
        _raw(self.status, "status")



@dataclass(frozen=True, slots=True)
class AmmSnapshot:
    schema_version: int
    snapshot_id: str
    worker_generation: int
    source_epoch: int
    boot_id: str
    model_version: str
    pool_refs: tuple[PoolRef, ...]
    dependency_vector: tuple[AccountVersion, ...]
    pools: tuple[PoolBody, ...]
    context_slot: int
    chain_consistency: str
    state_valid_until_monotonic_ns: int
    sdk_versions: tuple[tuple[str, str], ...] = ()

    @property
    def snapshot_hash(self) -> str:
        from .codec import canonical_hash

        return canonical_hash(self.economic_projection, domain="amm_snapshot")

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "worker_generation": self.worker_generation,
            "source_epoch": self.source_epoch,
            "boot_id": self.boot_id,
            "model_version": self.model_version,
            "sdk_versions": [list(item) for item in self.sdk_versions],
            "pool_refs": [pool.economic_projection for pool in self.pool_refs],
            "dependency_vector": [
                {
                    "address": item.address,
                    "owner_program_id": item.owner_program_id,
                    "data_hash": item.data_hash,
                    "local_revision": item.local_revision,
                    "context_slot": item.context_slot,
                    "write_version": item.write_version,
                }
                for item in self.dependency_vector
            ],
            "pools": [pool.economic_projection for pool in self.pools],
            "context_slot": self.context_slot,
            "chain_consistency": self.chain_consistency,
        }

    def pool_by_ref(self, pool_ref: PoolRef) -> PoolBody:
        matches = [pool for pool in self.pools if pool.pool_ref == pool_ref]
        if len(matches) != 1:
            raise KeyError(pool_ref.pool_id)
        return matches[0]

    def __post_init__(self) -> None:
        _raw(self.schema_version, "schema_version", minimum=1)
        for name in ("snapshot_id", "boot_id", "model_version"):
            _text(getattr(self, name), name)
        _raw(self.worker_generation, "worker_generation", minimum=1)
        _raw(self.source_epoch, "source_epoch")
        _raw(self.context_slot, "context_slot")
        _raw(self.state_valid_until_monotonic_ns, "state_valid_until_monotonic_ns", minimum=1)
        if self.chain_consistency not in {
            "validated_multi_account_snapshot",
            "slot_window_estimate",
            "unknown",
        }:
            raise ValueError("unsupported chain consistency value")
        if len({pool.pool_ref.pool_id for pool in self.pools}) != len(self.pools):
            raise ValueError("snapshot pool identities must be unique")


@dataclass(frozen=True, slots=True)
class VirtualStateRef:
    root_snapshot_id: str
    root_snapshot_hash: str
    branch_id: str
    step_index: int
    parent_state_hash: str | None
    state_hash: str

    def __post_init__(self) -> None:
        _text(self.root_snapshot_id, "root_snapshot_id")
        _text(self.root_snapshot_hash, "root_snapshot_hash")
        _text(self.branch_id, "branch_id")
        _raw(self.step_index, "step_index")


@dataclass(frozen=True, slots=True)
class SwapLeg:
    leg_id: str
    pool_ref: PoolRef
    input_asset_id: str
    output_asset_id: str
    mode: SwapMode
    amount_source: Literal["literal", "previous_output"]
    amount_raw: int | None
    previous_leg_id: str | None
    minimum_net_output_raw: int | None = None
    maximum_gross_input_raw: int | None = None

    def __post_init__(self) -> None:
        _text(self.leg_id, "leg_id")
        _text(self.input_asset_id, "input_asset_id")
        _text(self.output_asset_id, "output_asset_id")
        if self.mode not in {"exact_in", "exact_out"}:
            raise ValueError("mode must be exact_in or exact_out")
        if self.amount_source == "literal":
            if self.amount_raw is None or self.amount_raw <= 0:
                raise ValueError("literal amount_raw must be positive")
            if self.previous_leg_id is not None:
                raise ValueError("previous_leg_id is not allowed for literal amount")
        elif self.amount_source == "previous_output":
            if self.amount_raw is not None or not self.previous_leg_id:
                raise ValueError("previous_output requires previous_leg_id only")
        else:
            raise ValueError("unsupported amount source")
        for name, value in (
            ("minimum_net_output_raw", self.minimum_net_output_raw),
            ("maximum_gross_input_raw", self.maximum_gross_input_raw),
        ):
            if value is not None and value < 0:
                raise ValueError(f"{name} must be non-negative")


@dataclass(frozen=True, slots=True)
class AmmSimulationLimits:
    max_path_legs: int = 4
    max_state_bytes: int = 1_048_576
    max_computation_steps: int = 1_000_000
    max_inflight_requests: int = 32
    max_pending_paths: int = 256
    max_snapshot_dependencies: int = 64
    max_evidence_bundle_bytes: int = 8_388_608
    max_queue_bytes: int = 16_777_216

    def __post_init__(self) -> None:
        for name in (
            "max_path_legs",
            "max_state_bytes",
            "max_computation_steps",
            "max_inflight_requests",
            "max_pending_paths",
            "max_snapshot_dependencies",
            "max_evidence_bundle_bytes",
            "max_queue_bytes",
        ):
            _raw(getattr(self, name), name, minimum=1)


@dataclass(frozen=True, slots=True)
class AmmPathRequest:
    schema_version: int
    request_id: str
    reason: str
    priority: str
    snapshot: AmmSnapshot
    legs: tuple[SwapLeg, ...]
    initial_balances: tuple[tuple[str, int], ...]
    scenario_id: str
    scenario_kind: str = "frozen_market"
    required_consistency: str = "validated_multi_account_snapshot"
    limits: AmmSimulationLimits = AmmSimulationLimits()
    execution_policy: Literal["require_complete", "best_effort"] = "require_complete"
    deadline_monotonic_ns: int | None = None

    def __post_init__(self) -> None:
        _raw(self.schema_version, "schema_version", minimum=1)
        for name in ("request_id", "reason", "priority", "scenario_id"):
            _text(getattr(self, name), name)
        if self.execution_policy not in {"require_complete", "best_effort"}:
            raise ValueError("unsupported execution policy")
        if self.deadline_monotonic_ns is not None:
            _raw(self.deadline_monotonic_ns, "deadline_monotonic_ns", minimum=1)
        if len(self.legs) > self.limits.max_path_legs:
            raise ValueError("path exceeds max_path_legs")
        if not self.initial_balances:
            raise ValueError("initial_balances must contain the funded scenario assets")
        for asset_id, amount in self.initial_balances:
            _text(asset_id, "asset_id")
            _raw(amount, "initial balance")
        if len({asset_id for asset_id, _ in self.initial_balances}) != len(self.initial_balances):
            raise ValueError("initial balances must have unique assets")


@dataclass(frozen=True, slots=True)
class SwapLegResult:
    leg_id: str
    status: str
    complete: bool
    requested_amount_raw: int
    actual_gross_input_raw: int
    input_received_by_pool_raw: int
    input_used_for_curve_raw: int
    gross_pool_output_raw: int
    actual_net_output_raw: int
    unconsumed_input_raw: int
    fee_amount_raw: int
    reserve_0_after_raw: int
    reserve_1_after_raw: int
    state_before_hash: str
    state_after_hash: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class AmmPathResult:
    request_id: str
    snapshot_id: str
    snapshot_hash: str
    status: PathStatus
    complete: bool
    failed_leg_id: str | None
    leg_results: tuple[SwapLegResult, ...]
    final_balances: tuple[tuple[str, int], ...]
    reason: str | None = None
    state_after_scope: str = "swap_execution_projection"
    exactness: str = "protocol_integer"
    firmness: str = "simulated"
    execution_ready: bool = False
    evidence_hash: str | None = None
    # Provenance is repeated on the result so consumers do not need to infer
    # worker identity from a mutable/live source after simulation completed.
    worker_generation: int | None = None
    source_epoch: int | None = None
    boot_id: str | None = None
    context_slot: int | None = None

    def __post_init__(self) -> None:
        _text(self.request_id, "request_id")
        _text(self.snapshot_id, "snapshot_id")
        _text(self.snapshot_hash, "snapshot_hash")
        if self.worker_generation is not None:
            _raw(self.worker_generation, "worker_generation", minimum=1)
        if self.source_epoch is not None:
            _raw(self.source_epoch, "source_epoch")
        if self.boot_id is not None:
            _text(self.boot_id, "boot_id")
        if self.context_slot is not None:
            _raw(self.context_slot, "context_slot")
        if self.execution_ready:
            raise ValueError("AMM simulation results are never execution-ready")
        if self.complete != (self.status == "complete"):
            raise ValueError("AMM path status and completion flag disagree")


@dataclass(frozen=True, slots=True)
class SnapshotRequest:
    schema_version: int
    request_id: str
    pool_ids: tuple[str, ...]
    required_consistency: str
    max_dependencies: int | None = None

    def __post_init__(self) -> None:
        _raw(self.schema_version, "schema_version", minimum=1)
        _text(self.request_id, "request_id")
        if not self.pool_ids:
            raise ValueError("snapshot request requires at least one pool id")
        if len(set(self.pool_ids)) != len(self.pool_ids):
            raise ValueError("snapshot request pool ids must be unique")
        for pool_id in self.pool_ids:
            _text(pool_id, "pool_id")
        if self.required_consistency not in {
            "validated_multi_account_snapshot",
            "slot_window_estimate",
            "unknown",
        }:
            raise ValueError("unsupported required consistency")
        if self.max_dependencies is not None:
            _raw(self.max_dependencies, "max_dependencies", minimum=1)


@dataclass(frozen=True, slots=True)
class SnapshotCaptureResult:
    request_id: str
    status: str
    reason: str | None
    snapshot: AmmSnapshot | None = None
    missing_dependency_descriptors: tuple[str, ...] = ()
    remote_calls_used: int = 0

    def __post_init__(self) -> None:
        _text(self.request_id, "request_id")
        _text(self.status, "status")
        _raw(self.remote_calls_used, "remote_calls_used")
        if self.status == "ok" and self.snapshot is None:
            raise ValueError("an ok snapshot capture must carry a snapshot")
        for descriptor in self.missing_dependency_descriptors:
            _text(descriptor, "missing dependency descriptor")


@dataclass(frozen=True, slots=True)
class SimulationEvidenceRef:
    evidence_hash: str
    schema_version: int
    snapshot_hash: str
    request_id: str
    retention_class: str = "shadow_replay"

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "evidence_hash": self.evidence_hash,
            "schema_version": self.schema_version,
            "snapshot_hash": self.snapshot_hash,
            "request_id": self.request_id,
            "retention_class": self.retention_class,
        }

    def __post_init__(self) -> None:
        for name in ("evidence_hash", "snapshot_hash", "request_id", "retention_class"):
            _text(getattr(self, name), name)
        _raw(self.schema_version, "schema_version", minimum=1)


@dataclass(frozen=True, slots=True)
class SequentialUnwindResult:
    """Economic projection of one exact AMM buy-then-sell of the net base.

    The AMM quantities are raw integers observed from a sequential simulation
    over an immutable snapshot.  The human-scaled stable balances are provided
    by the simulator, which knows the stable token decimals.  This value is
    research evidence; it never opens an order or wallet.
    """

    snapshot_id: str
    snapshot_hash: str
    worker_generation: int | None
    evidence_hash: str | None
    scenario_kind: str
    provider: str
    pool_id: str
    base_asset_id: str
    stable_asset_id: str
    buy_leg_id: str
    sell_leg_id: str
    buy_input_raw: int
    buy_output_raw: int
    sell_input_raw: int
    sell_output_raw: int
    initial_stable: object
    final_stable: object
    execution_ready: bool = False
    source_epoch: int | None = None
    boot_id: str | None = None
    context_slot: int | None = None

    def __post_init__(self) -> None:
        _text(self.snapshot_id, "snapshot_id")
        _text(self.snapshot_hash, "snapshot_hash")
        if self.worker_generation is not None:
            _raw(self.worker_generation, "worker_generation", minimum=1)
        if self.source_epoch is not None:
            _raw(self.source_epoch, "source_epoch")
        if self.boot_id is not None:
            _text(self.boot_id, "boot_id")
        if self.context_slot is not None:
            _raw(self.context_slot, "context_slot")
        if self.execution_ready:
            raise ValueError("sequential AMM research results are never execution-ready")
        _text(self.scenario_kind, "scenario_kind")
        _text(self.provider, "provider")
        _text(self.pool_id, "pool_id")
        _text(self.base_asset_id, "base_asset_id")
        _text(self.stable_asset_id, "stable_asset_id")
        _text(self.buy_leg_id, "buy_leg_id")
        _text(self.sell_leg_id, "sell_leg_id")
        if self.buy_input_raw <= 0 or self.buy_output_raw <= 0:
            raise ValueError("sequential buy leg raw amounts must be positive")
        if self.sell_input_raw <= 0 or self.sell_output_raw <= 0:
            raise ValueError("sequential sell leg raw amounts must be positive")
        if self.sell_input_raw > self.buy_output_raw:
            raise ValueError("sequential sell input cannot exceed the buy net output")


@dataclass(frozen=True, slots=True)
class WhirlpoolTickBody:
    """One immutable initialized/zeroed tick in an Orca tick array."""

    initialized: bool
    liquidity_net_raw: int
    liquidity_gross_raw: int

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "initialized": self.initialized,
            "liquidity_net_raw": str(self.liquidity_net_raw),
            "liquidity_gross_raw": str(self.liquidity_gross_raw),
        }

    def __post_init__(self) -> None:
        if not isinstance(self.initialized, bool):
            raise ValueError("tick initialized must be a bool")
        if type(self.liquidity_net_raw) is not int:
            raise ValueError("tick liquidity_net_raw must be an integer")
        _raw(self.liquidity_gross_raw, "liquidity_gross_raw")


@dataclass(frozen=True, slots=True)
class WhirlpoolTickArrayBody:
    start_tick_index: int
    ticks: tuple[WhirlpoolTickBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "start_tick_index": self.start_tick_index,
            "ticks": [tick.economic_projection for tick in self.ticks],
        }

    def __post_init__(self) -> None:
        if type(self.start_tick_index) is not int:
            raise ValueError("tick array start_tick_index must be an integer")
        if not self.ticks:
            raise ValueError("tick array must contain at least one tick")
        for tick in self.ticks:
            if not isinstance(tick, WhirlpoolTickBody):
                raise ValueError("tick array entries must be WhirlpoolTickBody")


@dataclass(frozen=True, slots=True)
class OrcaWhirlpoolPoolBody:
    """Immutable classic Orca Whirlpool body sufficient for supported swaps.

    `state_after_scope = swap_execution_projection`: fee-growth and protocol-owed
    counters are updated to feed subsequent supported swaps.  Full byte-identical
    chain state (LP accounting, oracle, adaptive-fee tier) is outside this
    projection.  Only classic non-adaptive fee tiers are supported.
    """

    pool_ref: PoolRef
    sqrt_price_x64: int
    liquidity_raw: int
    tick_current_index: int
    tick_spacing: int
    fee_rate: int
    protocol_fee_rate: int
    fee_growth_global_a: int
    fee_growth_global_b: int
    protocol_fee_owed_a: int
    protocol_fee_owed_b: int
    tick_arrays: tuple[WhirlpoolTickArrayBody, ...]

    @property
    def economic_projection(self) -> dict[str, object]:
        return {
            "pool_id": self.pool_ref.pool_id,
            "protocol": self.pool_ref.protocol,
            "protocol_revision": self.pool_ref.protocol_revision,
            "sqrt_price_x64": str(self.sqrt_price_x64),
            "liquidity_raw": str(self.liquidity_raw),
            "tick_current_index": self.tick_current_index,
            "tick_spacing": self.tick_spacing,
            "fee_rate": str(self.fee_rate),
            "protocol_fee_rate": str(self.protocol_fee_rate),
            "fee_growth_global_a": str(self.fee_growth_global_a),
            "fee_growth_global_b": str(self.fee_growth_global_b),
            "protocol_fee_owed_a": str(self.protocol_fee_owed_a),
            "protocol_fee_owed_b": str(self.protocol_fee_owed_b),
            "tick_arrays": [array.economic_projection for array in self.tick_arrays],
        }

    def effective_reserves(self) -> tuple[int, int]:
        """Notional effective reserves implied by the current price/liquidity.

        These are aggregate liquidity projections used purely for diagnostics
        and state hashing within the swap-execution projection; the legal
        economic source of truth for subsequent swaps is the price/liquidity
        pair itself.
        """
        token_a = (self.liquidity_raw * self.sqrt_price_x64) >> 64
        if self.sqrt_price_x64 == 0:
            raise ValueError("Whirlpool sqrt price must be positive")
        token_b = (self.liquidity_raw << 64) // self.sqrt_price_x64
        return token_a, token_b

    def __post_init__(self) -> None:
        _raw(self.sqrt_price_x64, "sqrt_price_x64", minimum=1)
        _raw(self.liquidity_raw, "liquidity_raw")
        _raw(self.tick_spacing, "tick_spacing", minimum=1)
        _raw(self.fee_rate, "fee_rate")
        _raw(self.protocol_fee_rate, "protocol_fee_rate")
        _raw(self.fee_growth_global_a, "fee_growth_global_a")
        _raw(self.fee_growth_global_b, "fee_growth_global_b")
        _raw(self.protocol_fee_owed_a, "protocol_fee_owed_a")
        _raw(self.protocol_fee_owed_b, "protocol_fee_owed_b")
        if type(self.tick_current_index) is not int:
            raise ValueError("tick_current_index must be an integer")


PoolBody = CpmmPoolBody | RaydiumCpmmPoolBody | OrcaWhirlpoolPoolBody | RaydiumClmmPoolBody | MeteoraDlmmPoolBody | RaydiumAmmV4PoolBody
SupportedPoolBody = PoolBody
