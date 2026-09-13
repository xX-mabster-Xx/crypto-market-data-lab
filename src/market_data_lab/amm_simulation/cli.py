"""Offline replay CLI for saved AMM simulation evidence bundles."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .replay import load_evidence_bundle, replay_evidence_bundle


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay-amm-simulation",
        description="Replay a saved post-trade AMM simulation evidence bundle offline",
    )
    parser.add_argument("bundle", type=Path, help="path to a saved evidence JSON bundle")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="print only the path status and leg count instead of the full result",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    bundle = load_evidence_bundle(args.bundle)
    result = replay_evidence_bundle(bundle)
    if args.summary:
        print(
            json.dumps(
                {
                    "request_id": result.request_id,
                    "status": result.status,
                    "complete": result.complete,
                    "legs": len(result.leg_results),
                },
                sort_keys=True,
            ),
        )
    else:
        print(
            json.dumps(
                {
                    "request_id": result.request_id,
                    "snapshot_id": result.snapshot_id,
                    "snapshot_hash": result.snapshot_hash,
                    "status": result.status,
                    "reason": result.reason,
                    "complete": result.complete,
                    "leg_results": [
                        {
                            "leg_id": leg.leg_id,
                            "status": leg.status,
                            "actual_gross_input_raw": str(leg.actual_gross_input_raw),
                            "actual_net_output_raw": str(leg.actual_net_output_raw),
                            "fee_amount_raw": str(leg.fee_amount_raw),
                            "state_after_hash": leg.state_after_hash,
                        }
                        for leg in result.leg_results
                    ],
                    "final_balances": [
                        [asset, str(amount)] for asset, amount in result.final_balances
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
    return 0 if result.complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
