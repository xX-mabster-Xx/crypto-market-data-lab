from __future__ import annotations

import hashlib
import json
import re
from typing import Any


_RAW_STRING = re.compile(r"^(0|[1-9][0-9]*)$")


def encode_json(value: object) -> str:
    """Encode a transport JSON document with canonical object ordering."""

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def decode_json(value: str) -> Any:
    """Decode JSON while rejecting duplicate object keys."""

    return json.loads(value, object_pairs_hook=_unique_object)


def raw_from_json(value: object, field: str = "value") -> int:
    """Parse a strict canonical non-negative decimal string."""

    if not isinstance(value, str) or _RAW_STRING.fullmatch(value) is None:
        raise ValueError(f"{field} must be a canonical non-negative decimal string")
    return int(value)


def canonical_hash(value: object, *, domain: str) -> str:
    """Compute the canonical SHA-256 hash for economic evidence."""

    if not domain:
        raise ValueError("domain must be non-empty")
    payload = encode_json({"domain": domain, "value": value}).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result
