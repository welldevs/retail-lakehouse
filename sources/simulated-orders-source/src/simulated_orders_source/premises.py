"""Typed access to the declared premises table.

Every premise of this Source is SYNTHETIC and declared: no source ingested by this platform
measures sales, baskets, purchase cadence or product availability. This module exists so that
this statement is enforced, not merely written down.

TWO RULES, ENFORCED HERE
------------------------
1. THERE IS NO DEFAULT. Reading a missing key raises an error instead of returning a number
   chosen by the code. A default hidden in the generator would be an undeclared premise —
   exactly what the table exists to prevent.
2. RANGES ARE CHECKED. `*_min` greater than `*_max` would produce inverted sampling that no
   downstream test would catch, because the result would still look plausible.
"""

from __future__ import annotations

# (min, max) pairs that must be ordered. An inverted range does not make sampling fail; it
# produces a plausible number — the most expensive class of defect in this repo.
RANGE_PAIRS = (
    ("basket_lines_min", "basket_lines_max"),
    ("order_hour_min", "order_hour_max"),
    ("slot_lead_hours_min", "slot_lead_hours_max"),
    ("minutes_to_payment_min", "minutes_to_payment_max"),
    ("minutes_to_cancel_min", "minutes_to_cancel_max"),
    ("minutes_to_picking_min", "minutes_to_picking_max"),
    ("minutes_to_dispatch_min", "minutes_to_dispatch_max"),
    ("minutes_to_delivered_min", "minutes_to_delivered_max"),
    ("minutes_to_return_min", "minutes_to_return_max"),
)

# Every key whose value is a fraction from 0 to 1. A rate above 1 would be accepted by the
# sampling and would produce, for example, 100% cancelled orders without any error.
PROPORTIONS = (
    "daily_order_rate",
    "substitution_rate",
    "removal_rate",
    "payment_failure_rate",
    "cancellation_rate",
    "delivery_failure_rate",
    "return_rate",
)


class PremiseError(Exception):
    """Missing, non-numeric or inconsistent premise."""


class Premises:
    """Typed, checked reading of the premises table."""

    def __init__(self, values: dict):
        self._values = dict(values)
        self._check()

    def _check(self) -> None:
        for key in PROPORTIONS:
            value = self.number(key)
            if not 0.0 <= value <= 1.0:
                raise PremiseError(
                    f"premise {key!r} = {value}: it is a proportion and must be between 0 and 1"
                )
        for low, high in RANGE_PAIRS:
            if self.number(low) > self.number(high):
                raise PremiseError(
                    f"inverted range: {low}={self.number(low)} > {high}={self.number(high)}. "
                    f"Sampling would not fail; it would produce a plausible, wrong number."
                )
        mode = self.integer("basket_lines_mode")
        if not self.integer("basket_lines_min") <= mode <= self.integer("basket_lines_max"):
            raise PremiseError(
                f"basket_lines_mode={mode} outside "
                f"[{self.integer('basket_lines_min')}, {self.integer('basket_lines_max')}]"
            )
        if self.integer("basket_lines_min") < 1:
            raise PremiseError("basket_lines_min must be at least 1")
        if self.integer("quantity_max") < 1:
            raise PremiseError("quantity_max must be at least 1")

    def number(self, key: str) -> float:
        if key not in self._values:
            raise PremiseError(
                f"missing premise: {key!r}. There is no default for any premise, on "
                f"purpose: a default hidden in the generator would be an undeclared premise."
            )
        try:
            return float(self._values[key])
        except (TypeError, ValueError) as exc:
            raise PremiseError(f"premise {key!r} is not numeric: {self._values[key]!r}") from exc

    def integer(self, key: str) -> int:
        value = self.number(key)
        if value != int(value):
            raise PremiseError(f"premise {key!r} must be an integer: {value}")
        return int(value)

    def as_dict(self) -> dict:
        return dict(self._values)
