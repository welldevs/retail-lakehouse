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
    ("minutes_to_payment_min", "minutes_to_payment_max"),
    ("minutes_to_cancel_min", "minutes_to_cancel_max"),
    ("minutes_dispatch_before_slot_min", "minutes_dispatch_before_slot_max"),
    ("minutes_picked_before_dispatch_min", "minutes_picked_before_dispatch_max"),
    ("minutes_to_return_min", "minutes_to_return_max"),
)

PROPORTIONS = (
    "daily_order_rate",
    "substitution_rate_fresh",
    "substitution_rate_other",
    "removal_rate_fresh",
    "removal_rate_other",
    "payment_failure_rate",
    "cancellation_rate",
    "delivery_failure_rate",
    "return_rate",
    "order_skip_rate",
    "delivery_on_time_rate",
    "delivery_early_rate",
)

CADENCE_DAYS = (7, 14, 21, 28)
WEEKDAYS = tuple(range(1, 8))
ORDER_HOURS = tuple(range(24))
LEAD_DAYS = (1, 2, 3)

# Weight vectors that are SHARES and must add up to 1. A vector of relative weights (hours of
# the day, slot hours) only needs to be non-negative with a positive total.
SHARE_VECTORS = (
    tuple(f"cadence_weight_{days}d" for days in CADENCE_DAYS),
    tuple(f"order_weekday_weight_{day}" for day in WEEKDAYS),
    tuple(f"slot_lead_day_weight_{day}" for day in LEAD_DAYS),
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
        for vector in SHARE_VECTORS:
            total = sum(self.number(key) for key in vector)
            if any(self.number(key) < 0 for key in vector) or abs(total - 1.0) > 1e-9:
                raise PremiseError(
                    f"{vector[0]}..{vector[-1]} must be non-negative and sum to 1, got {total}"
                )
        for prefix, hours in (("order_hour_weight", ORDER_HOURS),
                              ("slot_hour_weight", self.slot_hours())):
            if any(self.number(f"{prefix}_{h:02d}") < 0 for h in hours) or not sum(
                self.number(f"{prefix}_{h:02d}") for h in hours
            ) > 0:
                raise PremiseError(f"{prefix}_*: weights must be non-negative with a positive total")
        if self.number("delivery_on_time_rate") + self.number("delivery_early_rate") > 1:
            raise PremiseError("delivery_on_time_rate + delivery_early_rate exceeds 1")
        if not 1 <= self.integer("delivery_closed_isoweekday") <= 7:
            raise PremiseError("delivery_closed_isoweekday must be an ISO weekday, 1..7")

    def slot_hours(self) -> tuple:
        """LOCAL hours a delivery slot may START: from opening until the slot still ends by closing."""
        opening = self.integer("delivery_open_hour")
        closing = self.integer("delivery_close_hour")
        length = self.integer("slot_hours")
        if not 0 <= opening < closing <= 24 or length < 1 or opening + length > closing:
            raise PremiseError(
                f"delivery window {opening}h-{closing}h cannot hold a {length}h slot"
            )
        return tuple(range(opening, closing - length + 1))

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
