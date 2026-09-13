from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .codec import canonical_hash, decode_json, encode_json, raw_from_json
from .contracts import (
    AmmPathRequest,
    AmmPathResult,
    AmmSimulationLimits,
    AmmSnapshot,
    ClmmTickArrayBody,
    ClmmTickBody,
    CpmmPoolBody,
    DlmmBinArrayBody,
    DlmmBinBody,
    MeteoraDlmmPoolBody,
    OrcaWhirlpoolPoolBody,
    PoolBody,
    PoolRef,
    RaydiumAmmV4PoolBody,
    RaydiumClmmPoolBody,
    RaydiumCpmmPoolBody,
    SwapLeg,
    SwapLegResult,
    VirtualStateRef,
    WhirlpoolTickArrayBody,
    WhirlpoolTickBody,
)
from .engine import PathSimulator


EVIDENCE_SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class SimulationEvidenceBundle:
    schema_version: int
    request: dict[str, object]
    snapshot: dict[str, object]
    expected_result: dict[str, object]
    evidence_hash: str

    @property
    def bundle(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request": self.request,
            "snapshot": self.snapshot,
            "expected_result": self.expected_result,
        }

    @property
    def replay_request(self) -> AmmPathRequest:
        return _decode_request(self.request, self.snapshot)

    def validate_integrity(self) -> None:
        """Reject in-memory edits to any canonical evidence component."""

        expected = canonical_hash(self.bundle, domain="amm_simulation_evidence")
        if expected != self.evidence_hash:
            raise ValueError("evidence hash mismatch")


def build_evidence_bundle(request: AmmPathRequest, result: AmmPathResult) -> SimulationEvidenceBundle:
    if result.request_id != request.request_id:
        raise ValueError("evidence result request_id does not match request")
    if result.snapshot_id != request.snapshot.snapshot_id:
        raise ValueError("evidence result snapshot_id does not match snapshot")
    if result.snapshot_hash != request.snapshot.snapshot_hash:
        raise ValueError("evidence result snapshot_hash does not match snapshot")
    if (
        result.worker_generation != request.snapshot.worker_generation
        or result.source_epoch != request.snapshot.source_epoch
        or result.boot_id != request.snapshot.boot_id
        or result.context_slot != request.snapshot.context_slot
    ):
        raise ValueError("evidence result provenance does not match snapshot")
    snapshot_projection = dict(request.snapshot.economic_projection)
    # Freshness is provenance required for replay/live admission, but is not
    # part of the economic snapshot hash (which must stay stable across TTL
    # metadata changes).
    snapshot_projection["state_valid_until_monotonic_ns"] = str(
        request.snapshot.state_valid_until_monotonic_ns,
    )
    bundle = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "request": _request_projection(request),
        "snapshot": snapshot_projection,
        "expected_result": _result_projection(result),
    }
    evidence_hash = canonical_hash(bundle, domain="amm_simulation_evidence")
    _ensure_wire_size(
        {**bundle, "evidence_hash": evidence_hash},
        max_bytes=request.limits.max_evidence_bundle_bytes,
    )
    return SimulationEvidenceBundle(
        schema_version=EVIDENCE_SCHEMA_VERSION,
        request=bundle["request"],
        snapshot=bundle["snapshot"],
        expected_result=bundle["expected_result"],
        evidence_hash=evidence_hash,
    )


def replay_evidence_bundle(bundle: SimulationEvidenceBundle, *, monotonic_ns: object = None) -> AmmPathResult:
    del monotonic_ns
    bundle.validate_integrity()
    _ensure_wire_size(
        {**bundle.bundle, "evidence_hash": bundle.evidence_hash},
        max_bytes=_bundle_size_limit(bundle.request),
    )
    request = bundle.replay_request
    simulator = PathSimulator(monotonic_ns=lambda: 0)
    result = simulator.simulate(request, replay=True)
    if _result_projection(result) != bundle.expected_result:
        raise ValueError("replayed result does not match evidence")
    return result


def load_evidence_bundle(path: str | Path) -> SimulationEvidenceBundle:
    payload = decode_json(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("unsupported evidence schema")
    expected_hash = payload.pop("evidence_hash", None)
    if not isinstance(expected_hash, str) or not expected_hash:
        raise ValueError("evidence bundle is missing evidence_hash")
    bundle = SimulationEvidenceBundle(
        schema_version=EVIDENCE_SCHEMA_VERSION,
        request=payload.get("request", {}),
        snapshot=payload.get("snapshot", {}),
        expected_result=payload.get("expected_result", {}),
        evidence_hash=canonical_hash(payload, domain="amm_simulation_evidence"),
    )
    if expected_hash != bundle.evidence_hash:
        raise ValueError("evidence hash mismatch")
    _ensure_wire_size(
        {**payload, "evidence_hash": expected_hash},
        max_bytes=_bundle_size_limit(bundle.request),
    )
    return bundle


def save_evidence_bundle(bundle: SimulationEvidenceBundle, path: str | Path) -> None:
    bundle.validate_integrity()
    wire = {**bundle.bundle, "evidence_hash": bundle.evidence_hash}
    _ensure_wire_size(wire, max_bytes=_bundle_size_limit(bundle.request))
    Path(path).write_text(encode_json(wire) + "\n", encoding="utf-8")


def _bundle_size_limit(request_projection: dict[str, object]) -> int:
    limits = request_projection.get("limits")
    if not isinstance(limits, dict):
        # Evidence written by the pre-cap schema remains bounded by the
        # default hard limit when loaded for compatibility.
        return 8_388_608
    value = limits.get("max_evidence_bundle_bytes", 8_388_608)
    if type(value) is not int or value <= 0:  # noqa: E721
        raise ValueError("evidence request has an invalid size limit")
    return value


def _ensure_wire_size(wire: dict[str, object], *, max_bytes: int) -> None:
    encoded_size = len(encode_json(wire).encode("utf-8")) + 1
    if encoded_size > max_bytes:
        raise ValueError(
            f"evidence bundle exceeds max_evidence_bundle_bytes: {encoded_size} > {max_bytes}",
        )


def _request_projection(request: AmmPathRequest) -> dict[str, object]:
    return {
        "schema_version": request.schema_version,
        "request_id": request.request_id,
        "reason": request.reason,
        "priority": request.priority,
        "scenario_id": request.scenario_id,
        "scenario_kind": request.scenario_kind,
        "required_consistency": request.required_consistency,
        "execution_policy": request.execution_policy,
        "limits": {
            "max_path_legs": request.limits.max_path_legs,
            "max_state_bytes": request.limits.max_state_bytes,
            "max_computation_steps": request.limits.max_computation_steps,
            "max_inflight_requests": request.limits.max_inflight_requests,
            "max_pending_paths": request.limits.max_pending_paths,
            "max_snapshot_dependencies": request.limits.max_snapshot_dependencies,
            "max_evidence_bundle_bytes": request.limits.max_evidence_bundle_bytes,
            "max_queue_bytes": request.limits.max_queue_bytes,
        },
        "deadline_monotonic_ns": request.deadline_monotonic_ns,
        "initial_balances": [[asset, str(amount)] for asset, amount in request.initial_balances],
        "legs": [
            {
                "leg_id": leg.leg_id,
                "pool_id": leg.pool_ref.pool_id,
                "input_asset_id": leg.input_asset_id,
                "output_asset_id": leg.output_asset_id,
                "mode": leg.mode,
                "amount_source": leg.amount_source,
                "amount_raw": None if leg.amount_raw is None else str(leg.amount_raw),
                "previous_leg_id": leg.previous_leg_id,
                "minimum_net_output_raw": None if leg.minimum_net_output_raw is None else str(leg.minimum_net_output_raw),
                "maximum_gross_input_raw": None if leg.maximum_gross_input_raw is None else str(leg.maximum_gross_input_raw),
            }
            for leg in request.legs
        ],
    }


def _result_projection(result: AmmPathResult) -> dict[str, object]:
    return {
        "request_id": result.request_id,
        "snapshot_id": result.snapshot_id,
        "snapshot_hash": result.snapshot_hash,
        "status": result.status,
        "reason": result.reason,
        "complete": result.complete,
        "failed_leg_id": result.failed_leg_id,
        "leg_results": [
            {
                "leg_id": item.leg_id,
                "status": item.status,
                "complete": item.complete,
                "requested_amount_raw": str(item.requested_amount_raw),
                "actual_gross_input_raw": str(item.actual_gross_input_raw),
                "input_received_by_pool_raw": str(item.input_received_by_pool_raw),
                "input_used_for_curve_raw": str(item.input_used_for_curve_raw),
                "gross_pool_output_raw": str(item.gross_pool_output_raw),
                "actual_net_output_raw": str(item.actual_net_output_raw),
                "unconsumed_input_raw": str(item.unconsumed_input_raw),
                "fee_amount_raw": str(item.fee_amount_raw),
                "reserve_0_after_raw": str(item.reserve_0_after_raw),
                "reserve_1_after_raw": str(item.reserve_1_after_raw),
                "state_before_hash": item.state_before_hash,
                "state_after_hash": item.state_after_hash,
                "reason": item.reason,
            }
            for item in result.leg_results
        ],
        "final_balances": [[asset, str(amount)] for asset, amount in result.final_balances],
        "state_after_scope": result.state_after_scope,
        "exactness": result.exactness,
        "firmness": result.firmness,
        "execution_ready": result.execution_ready,
        # The top-level bundle hash covers this projection.  Embedding an
        # outer hash here would create recursive/non-deterministic evidence;
        # result.evidence_hash is therefore intentionally canonicalized away.
        "evidence_hash": None,
        "worker_generation": result.worker_generation,
        "source_epoch": result.source_epoch,
        "boot_id": result.boot_id,
        "context_slot": result.context_slot,
    }


def _decode_request(payload: dict[str, object], snapshot_payload: dict[str, object]) -> AmmPathRequest:
    snapshot = _decode_snapshot(snapshot_payload)
    pool_by_id = {pool.pool_ref.pool_id: pool for pool in snapshot.pools}
    legs = []
    for item in payload.get("legs", []):
        assert isinstance(item, dict)
        pool_id = item["pool_id"]
        assert isinstance(pool_id, str)
        pool = pool_by_id[pool_id]
        amount_raw = item.get("amount_raw")
        legs.append(
            _swap_leg(
                leg_id=item["leg_id"],
                pool_ref=pool.pool_ref,
                input_asset_id=item["input_asset_id"],
                output_asset_id=item["output_asset_id"],
                mode=item["mode"],
                amount_source=item["amount_source"],
                amount_raw=None if amount_raw is None else int(amount_raw),
                previous_leg_id=item.get("previous_leg_id"),
                minimum_net_output_raw=item.get("minimum_net_output_raw"),
                maximum_gross_input_raw=item.get("maximum_gross_input_raw"),
            ),
        )
    balances = tuple(
        (item[0], int(item[1]))
        for item in payload.get("initial_balances", [])
    )
    limits_payload = payload.get("limits", {})
    if not isinstance(limits_payload, dict):
        raise ValueError("evidence request limits must be an object")
    limits = AmmSimulationLimits(
        **{
            field: limits_payload[field]
            for field in (
                "max_path_legs",
                "max_state_bytes",
                "max_computation_steps",
                "max_inflight_requests",
                "max_pending_paths",
                "max_snapshot_dependencies",
                "max_evidence_bundle_bytes",
                "max_queue_bytes",
            )
            if field in limits_payload
        },
    )
    deadline = payload.get("deadline_monotonic_ns")
    if isinstance(deadline, str):
        deadline = int(deadline)
    return AmmPathRequest(
        schema_version=payload["schema_version"],
        request_id=payload["request_id"],
        reason=payload["reason"],
        priority=payload["priority"],
        snapshot=snapshot,
        legs=tuple(legs),
        initial_balances=balances,
        scenario_id=payload["scenario_id"],
        scenario_kind=payload["scenario_kind"],
        required_consistency=payload["required_consistency"],
        execution_policy=payload["execution_policy"],
        limits=limits,
        deadline_monotonic_ns=deadline,
    )


def _swap_leg(
    *,
    leg_id: str,
    pool_ref: object,
    input_asset_id: str,
    output_asset_id: str,
    mode: str,
    amount_source: str,
    amount_raw: int | None,
    previous_leg_id: str | None,
    minimum_net_output_raw: str | int | None,
    maximum_gross_input_raw: str | int | None,
):
    from .contracts import PoolRef, SwapLeg

    assert isinstance(pool_ref, PoolRef)
    return SwapLeg(
        leg_id=leg_id,
        pool_ref=pool_ref,
        input_asset_id=input_asset_id,
        output_asset_id=output_asset_id,
        mode=mode,
        amount_source=amount_source,
        amount_raw=amount_raw,
        previous_leg_id=previous_leg_id,
        minimum_net_output_raw=(
            None if minimum_net_output_raw is None else int(minimum_net_output_raw)
        ),
        maximum_gross_input_raw=(
            None if maximum_gross_input_raw is None else int(maximum_gross_input_raw)
        ),
    )


def _decode_snapshot(payload: dict[str, object]):
    from .contracts import (
        AccountVersion,
        AmmSnapshot,
        CpmmPoolBody,
        OrcaWhirlpoolPoolBody,
        PoolRef,
        RaydiumClmmPoolBody,
        RaydiumCpmmPoolBody,
        RaydiumAmmV4PoolBody,
        MeteoraDlmmPoolBody,
        WhirlpoolTickArrayBody,
        WhirlpoolTickBody,
        ClmmTickArrayBody,
        ClmmTickBody,
        DlmmBinArrayBody,
        DlmmBinBody,
    )

    pool_refs = {}
    for item in payload.get("pool_refs", []):
        assert isinstance(item, dict)
        pool_ref = PoolRef(
            chain_namespace=item["chain_namespace"],
            chain_id=item["chain_id"],
            program_id=item["program_id"],
            pool_address=item["pool_address"],
            protocol=item["protocol"],
            protocol_revision=item["protocol_revision"],
            asset_0_id=item["asset_0_id"],
            asset_1_id=item["asset_1_id"],
            pool_spec_version=item["pool_spec_version"],
        )
        pool_refs[pool_ref.pool_id] = pool_ref
    dependencies = tuple(
        AccountVersion(
            address=item["address"],
            owner_program_id=item["owner_program_id"],
            data_hash=item["data_hash"],
            local_revision=item["local_revision"],
            context_slot=item["context_slot"],
            write_version=item.get("write_version"),
        )
        for item in payload.get("dependency_vector", [])
    )
    pools = []
    for item in payload.get("pools", []):
        pool_ref = pool_refs[item["pool_id"]]
        protocol = item.get("protocol", "")
        if protocol == "orca_whirlpool":
            pools.append(
                _decode_orca_pool(pool_ref, item),
            )
        elif protocol == "raydium_clmm":
            pools.append(
                _decode_clmm_pool(pool_ref, item),
            )
        elif protocol == "meteora_dlmm":
            pools.append(
                _decode_dlmm_pool(pool_ref, item),
            )
        elif protocol == "raydium_amm_v4":
            pools.append(
                _decode_amm_v4_pool(pool_ref, item),
            )
        elif "vault_a_raw" in item and "need_take_pnl" not in item:
            pools.append(
                RaydiumCpmmPoolBody(
                    pool_ref=pool_ref,
                    vault_a_raw=raw_from_json(item["vault_a_raw"]),
                    vault_b_raw=raw_from_json(item["vault_b_raw"]),
                    protocol_fees_a_raw=raw_from_json(item["protocol_fees_a_raw"]),
                    protocol_fees_b_raw=raw_from_json(item["protocol_fees_b_raw"]),
                    fund_fees_a_raw=raw_from_json(item["fund_fees_a_raw"]),
                    fund_fees_b_raw=raw_from_json(item["fund_fees_b_raw"]),
                    creator_fees_a_raw=raw_from_json(item["creator_fees_a_raw"]),
                    creator_fees_b_raw=raw_from_json(item["creator_fees_b_raw"]),
                    trade_fee_rate=raw_from_json(item["trade_fee_rate"]),
                    creator_fee_rate=raw_from_json(item["creator_fee_rate"]),
                    protocol_fee_rate=raw_from_json(item["protocol_fee_rate"]),
                    fund_fee_rate=raw_from_json(item["fund_fee_rate"]),
                    fee_on=raw_from_json(item["fee_on"]),
                ),
            )
        else:
            pools.append(
                CpmmPoolBody(
                    pool_ref=pool_ref,
                    reserve_0_raw=raw_from_json(item["reserve_0_raw"]),
                    reserve_1_raw=raw_from_json(item["reserve_1_raw"]),
                    fee_numerator=raw_from_json(item["fee_numerator"]),
                    fee_denominator=raw_from_json(item["fee_denominator"]),
                ),
            )
    pools = tuple(pools)
    state_valid_until = payload.get("state_valid_until_monotonic_ns")
    if state_valid_until is None:
        # Legacy evidence had no TTL field; replay runs at a frozen clock and
        # therefore needs a valid positive sentinel rather than live expiry.
        state_valid_until = str(2**63 - 1)
    elif isinstance(state_valid_until, int):
        state_valid_until = str(state_valid_until)
    return AmmSnapshot(
        schema_version=payload["schema_version"],
        snapshot_id=payload["snapshot_id"],
        worker_generation=payload["worker_generation"],
        source_epoch=payload["source_epoch"],
        boot_id=payload["boot_id"],
        model_version=payload["model_version"],
        pool_refs=tuple(pool_refs.values()),
        dependency_vector=dependencies,
        pools=pools,
        context_slot=payload["context_slot"],
        chain_consistency=payload["chain_consistency"],
        state_valid_until_monotonic_ns=raw_from_json(state_valid_until),
        sdk_versions=tuple((item[0], item[1]) for item in payload.get("sdk_versions", [])),
    )


def _decode_clmm_pool(pool_ref: object, item: dict[str, object]):
    return RaydiumClmmPoolBody(
        pool_ref=pool_ref,
        sqrt_price_x64=raw_from_json(item["sqrt_price_x64"]),
        liquidity_raw=raw_from_json(item["liquidity_raw"]),
        tick_current_index=item["tick_current_index"],
        tick_spacing=item["tick_spacing"],
        fee_rate=raw_from_json(item["fee_rate"]),
        protocol_fee_rate=raw_from_json(item["protocol_fee_rate"]),
        tick_arrays=tuple(
            ClmmTickArrayBody(
                start_tick_index=array["start_tick_index"],
                ticks=tuple(
                    ClmmTickBody(
                        initialized=tick["initialized"],
                        liquidity_net=_signed_int_from_json(tick["liquidity_net"]),
                        liquidity_gross=_signed_int_from_json(tick["liquidity_gross"]),
                    )
                    for tick in array["ticks"]
                ),
            )
            for array in item["tick_arrays"]
        ),
    )


def _decode_dlmm_pool(pool_ref: object, item: dict[str, object]):
    return MeteoraDlmmPoolBody(
        pool_ref=pool_ref,
        active_id=item["active_id"],
        bin_step=item["bin_step"],
        reserve_x_raw=raw_from_json(item["reserve_x_raw"]),
        reserve_y_raw=raw_from_json(item["reserve_y_raw"]),
        fee_bps=raw_from_json(item["fee_bps"]),
        protocol_fee_bps=raw_from_json(item["protocol_fee_bps"]),
        bin_arrays=tuple(
            DlmmBinArrayBody(
                start_bin_id=array["start_bin_id"],
                bins=tuple(
                    DlmmBinBody(
                        bin_id=bin_["bin_id"],
                        reserve_x_raw=raw_from_json(bin_["reserve_x_raw"]),
                        reserve_y_raw=raw_from_json(bin_["reserve_y_raw"]),
                        liquidity_raw=raw_from_json(bin_.get("liquidity_raw", 0)),
                        fee_x_raw=raw_from_json(bin_.get("fee_x_raw", 0)),
                        fee_y_raw=raw_from_json(bin_.get("fee_y_raw", 0)),
                    )
                    for bin_ in array["bins"]
                ),
            )
            for array in item["bin_arrays"]
        ),
    )


def _decode_amm_v4_pool(pool_ref: object, item: dict[str, object]):
    return RaydiumAmmV4PoolBody(
        pool_ref=pool_ref,
        vault_a_raw=raw_from_json(item["vault_a_raw"]),
        vault_b_raw=raw_from_json(item["vault_b_raw"]),
        fee_raw_a=raw_from_json(item["fee_raw_a"]),
        fee_raw_b=raw_from_json(item["fee_raw_b"]),
        fee_rate=raw_from_json(item.get("fee_rate", "2500")),
        need_take_pnl=bool(item["need_take_pnl"]),
        open_orders=item.get("open_orders"),
        status=item["status"],
    )


def _decode_orca_pool(pool_ref: object, item: dict[str, object]):

    return OrcaWhirlpoolPoolBody(
        pool_ref=pool_ref,
        sqrt_price_x64=raw_from_json(item["sqrt_price_x64"]),
        liquidity_raw=raw_from_json(item["liquidity_raw"]),
        tick_current_index=item["tick_current_index"],
        tick_spacing=item["tick_spacing"],
        fee_rate=raw_from_json(item["fee_rate"]),
        protocol_fee_rate=raw_from_json(item["protocol_fee_rate"]),
        fee_growth_global_a=raw_from_json(item["fee_growth_global_a"]),
        fee_growth_global_b=raw_from_json(item["fee_growth_global_b"]),
        protocol_fee_owed_a=raw_from_json(item["protocol_fee_owed_a"]),
        protocol_fee_owed_b=raw_from_json(item["protocol_fee_owed_b"]),
        tick_arrays=tuple(
            WhirlpoolTickArrayBody(
                start_tick_index=array["start_tick_index"],
                ticks=tuple(
                    WhirlpoolTickBody(
                        initialized=tick["initialized"],
                        liquidity_net_raw=_signed_int_from_json(tick["liquidity_net_raw"]),
                        liquidity_gross_raw=_signed_int_from_json(tick["liquidity_gross_raw"]),
                    )
                    for tick in array["ticks"]
                ),
            )
            for array in item["tick_arrays"]
        ),
    )


def _signed_int_from_json(value: object) -> int:
    if isinstance(value, str) and value.lstrip("-").isdigit():
        return int(value)
    if type(value) is int:
        return value
    raise ValueError("value must be a signed decimal string or integer")
