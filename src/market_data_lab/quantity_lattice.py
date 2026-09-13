"""Exact common quantity lattices for compatible linear perp legs.

Sequentially rounding a quantity down to each venue's lot step is not safe:
rounding ``0.04`` first to ``0.03`` and then to ``0.02`` produces ``0.02``,
which is no longer valid on the first venue.  This module computes the shared
Decimal lattice once, then rounds only to a quantity valid on every supplied
leg.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from math import gcd
from typing import Iterable


_MAX_SUPPORTED_DECIMAL_PLACES = 18


def _positive_finite(value: Decimal | None) -> bool:
    return value is not None and value.is_finite() and value > 0


def common_quantity_step(steps: Iterable[Decimal | None]) -> Decimal | None:
    """Return the least positive Decimal step shared by all known legs.

    ``None`` means that a leg has no published quantity restriction and does
    not add a lattice constraint.  An explicit invalid step is rejected with
    ``ValueError`` instead of being silently treated as unrestricted.
    """

    known_steps: list[Decimal] = []
    for step in steps:
        if step is None:
            continue
        if not _positive_finite(step):
            raise ValueError("quantity step must be finite and positive when supplied")
        places = max(0, -step.as_tuple().exponent)
        if places > _MAX_SUPPORTED_DECIMAL_PLACES:
            raise ValueError("quantity step precision exceeds supported lattice limit")
        known_steps.append(step)
    if not known_steps:
        return None

    places = max(max(0, -step.as_tuple().exponent) for step in known_steps)
    units: list[int] = []
    for step in known_steps:
        scaled = step.scaleb(places)
        integral = scaled.to_integral_value()
        if scaled != integral or integral <= 0:
            raise ValueError("quantity step cannot be represented on common decimal lattice")
        units.append(int(integral))

    common_units = units[0]
    for unit in units[1:]:
        common_units = common_units // gcd(common_units, unit) * unit
    return Decimal(common_units).scaleb(-places)


def round_down_to_common_quantity_lattice(
    quantity: Decimal,
    steps: Iterable[Decimal | None],
) -> Decimal | None:
    """Round down only to an amount admissible on every known lattice.

    Returns ``None`` when the requested amount is invalid, too small for the
    shared lattice, or an input step is not safely representable.
    """

    if not _positive_finite(quantity):
        return None
    try:
        common_step = common_quantity_step(steps)
    except ValueError:
        return None
    if common_step is None:
        return quantity
    result = (quantity / common_step).to_integral_value(rounding=ROUND_DOWN) * common_step
    if not _positive_finite(result):
        return None
    return result
