"""Generates the event log of one (warehouse, day).

THE GOLDEN RULE
---------------
Zero randomness in customer, product and price. The ORDER is invented; who buys, what is
bought and what it costs come from the reference, which came from Silver, which came from RAW.
No product, price, customer or postal code is fabricated here.

WHAT IS DRAWN, AND WHAT THAT MEANS
----------------------------------
    customer        -> its own cadence (every 7/14/21/28 days), weekday and phase, from a
                       hash of (seed, customer_id): the SAME schedule on every day generated
    day + warehouse -> its own sub-seed
                    -> which scheduled customers actually order (1 - skip, x seasonal x regional)
                    -> which COHORT each of them is (age band x autonomous community)
                    -> how many lines the basket has (declared triangular), topped up to the
                       minimum order amount
                    -> which DEMAND GROUP (weighted by THAT COHORT's profile)
                    -> which product WITHIN the group (weighted by `popularity_weight`)
                    -> quantity per line (decreasing, up to the product's `quantity_max`)
                    -> placement hour (LOCAL time profile), delivery day and slot (operating
                       calendar), and picking/dispatch scheduled backwards from the slot
                    -> which lines are substituted or removed (fresh vs other rates)
                    -> which terminal branch the order follows (declared rates)

WHAT CHANGED IN CR-017, AND WHY. The dashboard showed what independent uniform draws look like
on a real catalog: the same number of orders every day, deliveries before 07:00 and on Sunday,
77% delivered on the day of the order, perfume as the top non-food category, two whole
Iberian hams per line. Each draw now follows a stated calendar or weight. The product weight
and the units ceiling are computed by the platform (`product_choice.py`) — the price
elasticity is FITTED so the basket's EUR/kg matches what MAPA measures households pay — and
are read here, never recomputed.

THE BOUNDARY, which matters more than the calibration: MAPA measures residents' household
consumption. It does NOT measure online-store orders, baskets or purchase cadence. Cadence,
weekday, hour and delivery shapes remain `synthetic` premises, declared one by one in
`order_premises_seed.csv`; the rules a real operator publishes (no delivery on Sunday,
07:00-22:00, next-day at the earliest, 50 EUR minimum) are written there with their origin.

REPRODUCIBILITY: SUB-SEED PER (WAREHOUSE, DAY), SCHEDULE PER CUSTOMER
--------------------------------------------------------------------
Each `(wh, order_date)` derives its own `random.Random` from `sha256(seed|wh|order_date)`, and
each customer's cadence derives from `sha256(seed|cadence|customer_id)` — a pure function of
the customer, not of the day. So:

    ADDING A DAY TO THE WINDOW IS STILL ADDITIVE. Already generated partitions stay
    byte-for-byte identical, because no day depends on another day's draw.

`hashlib.sha256` is used, not `hash()`, because CPython's `str` hash is randomized per
process: deriving the sub-seed from it would make the output depend on PYTHONHASHSEED.

NO CLOCK. Every `occurred_at` derives from the partition date plus declared offsets. No call
to `datetime.now()` in this module. Times are drawn in LOCAL time (Europe/Madrid, stdlib
`zoneinfo`) and stamped in UTC; the partition day is the UTC day of `order_placed`, so local
hours that fall on the previous UTC day are not drawn.
"""

from __future__ import annotations

import bisect
import hashlib
import random
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from . import SOURCE_NAME
from .events import (
    CANCEL_REASONS,
    CANCELLED_BY,
    DECLINE_REASONS,
    DELIVERY_FAILURE_REASONS,
    LINE_FULFILLED,
    LINE_REMOVED,
    LINE_SUBSTITUTED,
    ORDER_CANCELLED,
    ORDER_DELIVERED,
    ORDER_DELIVERY_FAILED,
    ORDER_DISPATCHED,
    ORDER_LINE_REMOVED,
    ORDER_LINE_SUBSTITUTED,
    ORDER_PAYMENT_AUTHORIZED,
    ORDER_PAYMENT_FAILED,
    ORDER_PICKED,
    ORDER_PICKING_STARTED,
    ORDER_PLACED,
    ORDER_RETURNED,
    PAYMENT_METHODS,
    apply_transition,
    envelope,
)

CENTS = Decimal("0.01")
LOCAL = ZoneInfo("Europe/Madrid")
CADENCE_DAYS = (7, 14, 21, 28)


class GenerationError(Exception):
    """The reference does not support the requested generation."""


def day_seed(seed: int, wh: str, order_date: str) -> int:
    """Deterministic sub-seed of one (warehouse, day).

    sha256, not `hash()`: CPython's str hash is randomized per process.
    """
    blob = f"{seed}|{wh}|{order_date}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(blob).digest()[:8], "big")


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENTS, rounding=ROUND_HALF_UP)


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _sample_indices(rng: random.Random, population: int, wanted: int) -> list[int]:
    """`wanted` distinct indices from range(population), in the order they came out.

    Hand-written instead of `rng.sample`: the internal algorithm of `sample` is not part of
    the stdlib contract and may change between Python versions, which would break
    reproducibility between the local machine and the container. `randrange` is stable.

    The `set` here is only looked up, never iterated — output order comes from the sequence
    of draws, so PYTHONHASHSEED cannot reach it.
    """
    if wanted > population:
        raise GenerationError(
            f"requested {wanted} distinct indices from a population of {population}"
        )
    chosen: list[int] = []
    seen: set = set()
    while len(chosen) < wanted:
        candidate = rng.randrange(population)
        if candidate in seen:
            continue
        seen.add(candidate)
        chosen.append(candidate)
    return chosen


def _quantity_weights(quantity_max: int) -> list[float]:
    """Decreasing weights for 1..quantity_max, as a cumulative list.

    w(k) = 1/2^(k-1): buying one unit dominates, and each extra unit is half as likely. The
    CEILING is the product's own (`quantity_max` in the catalog, from declared price tiers):
    six bottles of water, never two whole hams.
    """
    raw = [1.0 / (2 ** (k - 1)) for k in range(quantity_max)]
    total = sum(raw)
    cumulative, running = [], 0.0
    for weight in raw:
        running += weight / total
        cumulative.append(running)
    return cumulative


def _pick_weighted(rng: random.Random, cumulative: list[float]) -> int:
    draw = rng.random()
    for index, limit in enumerate(cumulative):
        if draw <= limit:
            return index
    return len(cumulative) - 1


def _pick_relative(rng: random.Random, keys: tuple, weights: list[float]):
    """One key, drawn proportionally to non-negative relative weights."""
    total = sum(weights)
    draw = rng.random() * total
    running = 0.0
    for key, weight in zip(keys, weights):
        running += weight
        if draw < running:
            return key
    return keys[-1]


def _unit_floats(blob: str, count: int) -> list[float]:
    """`count` uniform numbers in [0, 1) from one sha256 — no rng state, no PYTHONHASHSEED."""
    digest = hashlib.sha256(blob.encode("utf-8")).digest()
    return [int.from_bytes(digest[8 * i:8 * i + 8], "big") / 2 ** 64 for i in range(count)]


def customer_schedule(seed: int, customer_id: str, premises) -> tuple[int, int, int]:
    """(cadence in days, ISO weekday, phase in weeks) of one customer — a function of the
    customer alone, so every generated day agrees on it."""
    u_cadence, u_weekday, u_phase = _unit_floats(f"{seed}|cadence|{customer_id}", 3)
    running, cadence = 0.0, CADENCE_DAYS[-1]
    for days in CADENCE_DAYS:
        running += premises.number(f"cadence_weight_{days}d")
        if u_cadence < running:
            cadence = days
            break
    running, weekday = 0.0, 7
    for day in range(1, 8):
        running += premises.number(f"order_weekday_weight_{day}")
        if u_weekday < running:
            weekday = day
            break
    return cadence, weekday, int(u_phase * (cadence // 7))


def is_scheduled(schedule: tuple[int, int, int], day: date) -> bool:
    """True when the customer's cadence lands on this day. Weeks are counted from 0001-01-01,
    a Monday, so the phase means the same week on every machine."""
    cadence, weekday, phase = schedule
    if day.isoweekday() != weekday:
        return False
    week = (day.toordinal() - 1) // 7
    return (week - phase) % (cadence // 7) == 0


def _local(day: date, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, second, tzinfo=LOCAL)


def _placement_hours(day: date) -> tuple:
    """LOCAL hours whose whole hour falls on the same UTC day — the partition day."""
    return tuple(
        hour for hour in range(24)
        if _local(day, hour).astimezone(timezone.utc).date() == day
        and _local(day, hour, 59, 59).astimezone(timezone.utc).date() == day
    )


def _delivery_day(order_day: date, nth: int, closed_weekday: int) -> date:
    """The nth operating day after the order day. Closed weekdays are skipped, not counted."""
    current, counted = order_day, 0
    while counted < nth:
        current += timedelta(days=1)
        if current.isoweekday() != closed_weekday:
            counted += 1
    return current


def _order_id(wh: str, order_date: str, index: int) -> str:
    return f"ord_{wh}_{order_date.replace('-', '')}_{index:06d}"


def _basket(rng, reference, premises, wh, price_as_of, cohort):
    """Build the basket: lines with real product, real price, drawn quantity.

    TWO STEPS:

        1. DEMAND GROUP, weighted by the profile calibrated against MAPA — and, since
           `mapa_2025_v2`, by the profile OF THE COHORT of the customer placing the order.
        2. PRODUCT WITHIN THE GROUP, weighted by `popularity_weight` (CR-017; uniform before).

    The basket is then TOPPED UP to `min_order_amount_eur` with more lines drawn the same
    way: a real operator refuses smaller orders, and 18.4% of the old baskets were below it.
    Extra lines come from the same profile, so the mix does not move.
    """
    catalog = reference.catalog_of(wh, price_as_of)
    groups = reference.demand_groups_of(wh, price_as_of)
    cumulative = reference.demand_cdf_of(wh, price_as_of, cohort)

    low = premises.integer("basket_lines_min")
    high = premises.integer("basket_lines_max")
    mode = premises.integer("basket_lines_mode")
    minimum = Decimal(str(premises.number("min_order_amount_eur")))

    wanted = int(rng.triangular(low, high + 1, mode))
    wanted = max(low, min(high, wanted))
    wanted = min(wanted, len(catalog))

    # WITHOUT REPLACEMENT: a product appears at most once in the basket. A drawn product that
    # is already in the basket is redrawn (the attempt consumes rng draws, which keeps the
    # sequence deterministic). The ceiling makes a pathological profile fail fast.
    positions: list[int] = []
    taken: set = set()
    attempts = 0
    gross = Decimal("0")
    lines: list[dict] = []

    def draw_line() -> bool:
        nonlocal attempts, gross
        while attempts < max(high * 16, 64):
            attempts += 1
            group = reference.demand.pick(rng, groups, cumulative)
            candidates = reference.positions_in_demand_group(wh, price_as_of, group)
            weights = reference.popularity_cdf_of(wh, price_as_of, group)
            if not candidates:
                continue
            index = bisect.bisect_right(weights, rng.random() * weights[-1])
            picked = candidates[min(index, len(candidates) - 1)]
            if picked in taken:
                continue
            product = catalog[picked]
            taken.add(picked)
            positions.append(picked)
            quantity = _pick_weighted(rng, _quantity_weights(product["quantity_max"])) + 1
            lines.append(
                {
                    "line_no": len(lines) + 1,
                    "catalog_position": picked,
                    "source_product_id": product["source_product_id"],
                    "display_name": product["display_name"],
                    "category_id": product["category_id"],
                    "subgroup_id": product["subgroup_id"],
                    "demand_group": product["demand_group"],
                    "quantity": quantity,
                    "unit_price": product["unit_price"],
                }
            )
            gross += product["unit_price"] * quantity
            return True
        return False

    while len(lines) < wanted and draw_line():
        pass
    while gross < minimum and len(lines) < high and draw_line():
        pass
    if gross < minimum:
        raise GenerationError(
            f"basket of {len(lines)} line(s) reached {gross} EUR, below "
            f"min_order_amount_eur={minimum}, with basket_lines_max={high}"
        )
    return lines


def _substitute_for(rng, reference, wh, price_as_of, line, taken: set) -> dict | None:
    """Replacement product, from the SAME warehouse and the SAME date.

    Prefers the same subgroup, then the same category. If neither has a free candidate,
    there is NO substitution and no event is emitted — the line stays fulfilled.

    Declared consequence: the effective substitution rate is slightly below
    `substitution_rate` in small subgroups. Inventing a substitute outside that warehouse's
    assortment would be worse: it would be inventing assortment, the same prohibition as price.
    """
    catalog = reference.catalog_of(wh, price_as_of)
    for candidates in (
        reference.substitutes_in_subgroup(wh, price_as_of, line["subgroup_id"]),
        reference.substitutes_in_category(wh, price_as_of, line["category_id"]),
        # Third level: the same DEMAND GROUP. Wider than the source category and still within
        # what the calibration considers the same need — swapping hake for sea bream is a
        # substitution; swapping it for shampoo would not be.
        reference.positions_in_demand_group(wh, price_as_of, line["demand_group"]),
    ):
        free = [p for p in candidates if catalog[p]["source_product_id"] not in taken]
        if free:
            return catalog[free[rng.randrange(len(free))]]
    return None


def _lifecycle(rng, reference, premises, wh, order_date, order_id, customer, base_day):
    """Build the event sequence of ONE order.

    The state is advanced by `apply_transition` on every event: the generator is validated by
    its own vocabulary while it builds, not only by a test three layers further down.
    """
    calendar = reference.calendar_of(wh, order_date)
    price_as_of = calendar["price_as_of"]
    price_source = calendar["price_source"]

    customer_version = reference.customer_version_at(customer["first_ingestion_date"], order_date)
    if customer_version is None:
        raise GenerationError(
            f"customer {customer['customer_id']} only exists from "
            f"{customer['first_ingestion_date']}, after the order day {order_date}. "
            f"No order can be born before its customer."
        )

    # THE COHORT IS RESOLVED ON THE ORDER DAY, not at export: age changes, and a long window
    # makes a customer cross a band boundary. The computation is the difference in years,
    # which is the same convention `birth_year` was built with in the OLTP Source
    # (`reference_year - age`) — using a full date here and a year there would produce two
    # ages for the same person.
    age = int(order_date[:4]) - int(customer["birth_year"])
    cohort = reference.demand.cohort_of(wh, age)

    lines = _basket(rng, reference, premises, wh, price_as_of, cohort)
    gross = _money(sum((line["unit_price"] * line["quantity"] for line in lines), Decimal("0")))

    # PLACEMENT, in LOCAL time: evening peak, quiet overnight. Only the hours that stay on the
    # partition's UTC day are drawn (see the module docstring).
    order_day = base_day.date()
    hours = _placement_hours(order_day)
    hour = _pick_relative(rng, hours, [premises.number(f"order_hour_weight_{h:02d}") for h in hours])
    placed_at = _local(order_day, hour, rng.randrange(60), rng.randrange(60)).astimezone(timezone.utc)

    # DELIVERY SLOT, chosen on the operating calendar: never the day of the order, never on
    # the closed weekday, a one-hour window between opening and closing (local time).
    lead = _pick_relative(rng, (1, 2, 3),
                          [premises.number(f"slot_lead_day_weight_{d}") for d in (1, 2, 3)])
    delivery_day = _delivery_day(order_day, lead, premises.integer("delivery_closed_isoweekday"))
    slot_hours = premises.slot_hours()
    slot_hour = _pick_relative(
        rng, slot_hours, [premises.number(f"slot_hour_weight_{h:02d}") for h in slot_hours]
    )
    slot_start = _local(delivery_day, slot_hour).astimezone(timezone.utc)
    slot_end = slot_start + timedelta(hours=premises.integer("slot_hours"))
    opening = _local(delivery_day, premises.integer("delivery_open_hour")).astimezone(timezone.utc)

    events: list[dict] = []
    state = None
    sequence = 0

    def emit(event_type: str, occurred_at: datetime, payload: dict) -> None:
        nonlocal state, sequence
        state = apply_transition(state, event_type)
        sequence += 1
        events.append(
            envelope(
                order_id=order_id,
                wh=wh,
                sequence_no=sequence,
                event_type=event_type,
                occurred_at=_stamp(occurred_at),
                payload=payload,
                producer=SOURCE_NAME,
            )
        )

    emit(
        ORDER_PLACED,
        placed_at,
        {
            "customer_id": customer["customer_id"],
            "customer_ingestion_date": customer_version,
            # Stamped on the event for the same reason as `demand_group`: what matters is the
            # band in force AT ORDER TIME. Deriving it later, in Silver, would require
            # reimplementing the band bounds in a second language.
            "buyer_age_band": cohort.split("|", 1)[0],
            "province_code": customer["province_code"],
            "municipality_code": customer["municipality_code"],
            "postal_code": customer["postal_code"],
            "price_as_of": price_as_of,
            "price_source": price_source,
            "delivery_slot_start": _stamp(slot_start),
            "delivery_slot_end": _stamp(slot_end),
            "line_count": len(lines),
            "gross_amount": str(gross),
            "lines": [
                {
                    "line_no": line["line_no"],
                    "source_product_id": line["source_product_id"],
                    "category_id": line["category_id"],
                    "subgroup_id": line["subgroup_id"],
                    # On the event, not only in the reference: that is how Silver can group
                    # by demand group without reimplementing the mapping, and how `validate`
                    # re-checks the stamp against the reference instead of trusting it.
                    "demand_group": line["demand_group"],
                    "quantity": line["quantity"],
                    "unit_price": str(line["unit_price"]),
                }
                for line in lines
            ],
        },
    )

    # --- payment -----------------------------------------------------------------
    payment_at = placed_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_payment_min"), premises.integer("minutes_to_payment_max")
        )
    )
    if rng.random() < premises.number("payment_failure_rate"):
        emit(
            ORDER_PAYMENT_FAILED,
            payment_at,
            {"decline_reason": DECLINE_REASONS[rng.randrange(len(DECLINE_REASONS))]},
        )
        return events, state

    emit(
        ORDER_PAYMENT_AUTHORIZED,
        payment_at,
        {
            "payment_method": PAYMENT_METHODS[rng.randrange(len(PAYMENT_METHODS))],
            "authorized_amount": str(gross),
        },
    )

    # --- schedule, BACKWARDS from the slot -----------------------------------------
    # A dark store plans picking from the delivery window, not from the order time: dispatch
    # a while before the slot opens, picking ends before dispatch and takes per-line minutes.
    per_line = premises.integer("minutes_per_line_picked")
    dispatch_at = slot_start - timedelta(
        minutes=rng.randint(
            premises.integer("minutes_dispatch_before_slot_min"),
            premises.integer("minutes_dispatch_before_slot_max"),
        )
    )
    picked_at = dispatch_at - timedelta(
        minutes=rng.randint(
            premises.integer("minutes_picked_before_dispatch_min"),
            premises.integer("minutes_picked_before_dispatch_max"),
        )
    )
    picking_at = picked_at - timedelta(minutes=per_line * len(lines))
    if picking_at <= payment_at:
        raise GenerationError(
            f"{order_id}: picking would start at {picking_at}, before the payment at "
            f"{payment_at}. The delivery calendar must leave at least a night between them."
        )

    # --- cancellation -----------------------------------------------------------
    if rng.random() < premises.number("cancellation_rate"):
        # Between the confirmation and the start of picking — the only time a customer can.
        room = int((picking_at - payment_at).total_seconds() // 60) - 1
        cancel_at = payment_at + timedelta(
            minutes=rng.randint(
                min(premises.integer("minutes_to_cancel_min"), room),
                min(premises.integer("minutes_to_cancel_max"), room),
            )
        )
        emit(
            ORDER_CANCELLED,
            cancel_at,
            {
                "cancelled_by": CANCELLED_BY[rng.randrange(len(CANCELLED_BY))],
                "reason": CANCEL_REASONS[rng.randrange(len(CANCEL_REASONS))],
            },
        )
        return events, state

    # --- picking -----------------------------------------------------------------
    emit(ORDER_PICKING_STARTED, picking_at, {})

    taken = {line["source_product_id"] for line in lines}

    for line in lines:
        line["status"] = LINE_FULFILLED
        line["effective_price"] = line["unit_price"]
        line_at = picking_at + timedelta(minutes=per_line * line["line_no"])

        # Fresh lines (MAPA's own 'alimentacion fresca' scope) run out more often than
        # packaged goods; one flat rate for every category was a synthetic tell (CR-017).
        fresh = reference.demand.is_fresh(line["demand_group"])
        substitution_rate = premises.number(
            "substitution_rate_fresh" if fresh else "substitution_rate_other"
        )
        removal_rate = premises.number("removal_rate_fresh" if fresh else "removal_rate_other")

        # ONE draw per line, with two thresholds: that way both rates are exact and
        # independent of the number of rng calls, and not conditioned on each other.
        draw = rng.random()
        if draw < substitution_rate:
            substitute = _substitute_for(rng, reference, wh, price_as_of, line, taken)
            if substitute is not None:
                taken.add(substitute["source_product_id"])
                line["status"] = LINE_SUBSTITUTED
                line["effective_price"] = substitute["unit_price"]
                line["substitute_source_product_id"] = substitute["source_product_id"]
                emit(
                    ORDER_LINE_SUBSTITUTED,
                    line_at,
                    {
                        "line_no": line["line_no"],
                        "source_product_id": line["source_product_id"],
                        "substitute_source_product_id": substitute["source_product_id"],
                        "substitute_unit_price": str(substitute["unit_price"]),
                        "quantity": line["quantity"],
                    },
                )
        elif draw < substitution_rate + removal_rate:
            line["status"] = LINE_REMOVED
            emit(
                ORDER_LINE_REMOVED,
                line_at,
                {
                    "line_no": line["line_no"],
                    "source_product_id": line["source_product_id"],
                    "quantity": line["quantity"],
                    # "unavailable", never "out_of_stock": there is no stock fact in this
                    # platform, and naming it as if there were would promise nonexistent data.
                    "reason": "unavailable",
                },
            )

    kept = [line for line in lines if line["status"] != LINE_REMOVED]
    net = _money(
        sum((line["effective_price"] * line["quantity"] for line in kept), Decimal("0"))
    )
    emit(
        ORDER_PICKED,
        picked_at,
        {"picked_line_count": len(kept), "picked_amount": str(net)},
    )

    # --- dispatch and delivery---------------------------------------------------
    emit(ORDER_DISPATCHED, dispatch_at, {})

    # Mostly inside the slot; a few early (never before opening), the rest late.
    outcome = rng.random()
    slot_minutes = premises.integer("slot_hours") * 60
    if outcome < premises.number("delivery_on_time_rate"):
        delivery_at = slot_start + timedelta(minutes=rng.randrange(slot_minutes))
    elif outcome < premises.number("delivery_on_time_rate") + premises.number("delivery_early_rate"):
        early = slot_start - timedelta(
            minutes=rng.randint(1, premises.integer("delivery_early_max_minutes"))
        )
        delivery_at = max(early, opening, dispatch_at + timedelta(minutes=1))
    else:
        delivery_at = slot_end + timedelta(
            minutes=rng.randint(1, premises.integer("delivery_late_max_minutes"))
        )
    if rng.random() < premises.number("delivery_failure_rate"):
        emit(
            ORDER_DELIVERY_FAILED,
            delivery_at,
            {"reason": DELIVERY_FAILURE_REASONS[rng.randrange(len(DELIVERY_FAILURE_REASONS))]},
        )
        return events, state

    emit(
        ORDER_DELIVERED,
        delivery_at,
        {
            # The delivery slot is a commercial promise on a fixed grid, NEVER a route: the
            # Callejero has no coordinates or adjacency, so route and route time would be invented.
            "delivered_within_slot": slot_start <= delivery_at <= slot_end,
            "delivered_line_count": len(kept),
            "delivered_amount": str(net),
        },
    )

    # --- return ------------------------------------------------------------------
    if kept and rng.random() < premises.number("return_rate"):
        returned = kept[rng.randrange(len(kept))]
        return_at = delivery_at + timedelta(
            minutes=rng.randint(
                premises.integer("minutes_to_return_min"),
                premises.integer("minutes_to_return_max"),
            )
        )
        emit(
            ORDER_RETURNED,
            return_at,
            {
                "returned_line_count": 1,
                "returned_line_no": returned["line_no"],
                "returned_amount": str(
                    _money(returned["effective_price"] * returned["quantity"])
                ),
            },
        )
    return events, state


def generate(reference, premises, wh: str, order_date: str, seed: int) -> list[dict]:
    """Event log of one (warehouse, day), sorted by (occurred_at, order_id, sequence_no).

    The ordering is total and deterministic: `occurred_at` alone ties, and the tie-break by
    order_id and sequence_no guarantees that events of the same order never come out of
    order, which is the property the fold depends on.
    """
    rng = random.Random(day_seed(seed, wh, order_date))
    base_day = datetime.strptime(order_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    # ELIGIBLE: TWO CONDITIONS, and the second was born in Phase 5.
    #
    #   1. already existed on the order day — the base grows, and a future generation may
    #      add customers who did not exist on the days already generated;
    #   2. is `min_buyer_age` years old or older ON THE ORDER DAY.
    #
    # The second exists because `silver_customer` is a faithful projection of the resident
    # POPULATION (age comes from INE's provincial distribution, as the OLTP Source contract
    # declares), and population includes children: 18.01% of the base was under 18, with
    # ages starting at zero. While age did nothing this was harmless. From the moment it
    # governs demand, it no longer is — 18% of the base would fall into MAPA's '-35 years'
    # band while being children. Who MAY order is a premise of the orders domain, and lives
    # in `order_premises_seed.csv`; the customer base was not touched.
    minimum = premises.integer("min_buyer_age")
    year = int(order_date[:4])
    eligible = [
        customer
        for customer in reference.customers_of(wh)
        if reference.customer_version_at(customer["first_ingestion_date"], order_date)
        and year - int(customer["birth_year"]) >= minimum
    ]
    if not eligible:
        raise GenerationError(
            f"no customer of wh={wh!r} existed on {order_date} aged {minimum} or "
            f"older. The customer base starts on "
            f"{min(reference.customer_ingestion_dates)}."
        )

    # SEASONALITY ENTERS HERE, IN THE ORDER RATE — never in the mix. The distinction comes from
    # the evidence: the MAPA report publishes monthly spending for TOTAL food and does NOT
    # publish a monthly profile per category (the monthly charts are images). A global factor
    # on the mix would normalize away and do nothing. The delivered profile is neutral in all
    # 12 months, for lack of numerical evidence and because the window only covers August —
    # but the mechanism exists and has a test proving that a non-neutral profile changes the output.
    factor = reference.demand.seasonal_factor(base_day.month)

    # REGIONAL INTENSITY ENTERS HERE, next to seasonality, and for the same reason: it is a
    # property of HOW MUCH is bought, not of WHAT is bought. The report measures per-capita
    # consumption by community — Catalonia 620.82 kg or litre per person per year against
    # Madrid's 505.86 — and does not publish household purchase frequency, so splitting the
    # intensity between frequency and basket size would be inventing the split. The platform
    # chose frequency, declared the choice in `region_frequency_basis`, and already delivers
    # the index RENORMALIZED over the communities served: the weighted sum is 1, so the
    # window's total orders do not move and what changes is the split between warehouses.
    regional = reference.demand.frequency_index(wh)

    # CADENCE, NOT A DAILY LOTTERY (CR-017). Each customer reorders every 7/14/21/28 days on
    # their own weekday, from a hash of the customer alone. The day's draw only decides who
    # SKIPS — that is where day-to-day noise comes from. Before, each day drew a fixed
    # `round(rate x eligible)` independently, so every warehouse had the same count every
    # day and 1,130 customers ordered 4-6 times in 9 days. The cadence weights and the skip
    # rate are declared so that they reproduce `daily_order_rate`
    # (assert_order_premises_are_internally_coherent); seasonality and the regional index
    # still scale the probability of a scheduled order happening.
    day = base_day.date()
    keep = (1.0 - premises.number("order_skip_rate")) * factor * regional
    if not 0.0 < keep <= 1.0:
        raise GenerationError(
            f"(1 - order_skip_rate) x seasonal factor {factor} x regional index {regional} "
            f"= {keep}: outside (0, 1], it cannot be a probability"
        )
    chosen = [
        position
        for position, customer in enumerate(eligible)
        if is_scheduled(customer_schedule(seed, customer["customer_id"], premises), day)
        and rng.random() < keep
    ]
    if not chosen:
        raise GenerationError(
            f"no eligible customer of wh={wh!r} is scheduled on {order_date} "
            f"({len(eligible)} eligible): the cadence premises leave this weekday empty"
        )

    events: list[dict] = []
    for index, position in enumerate(chosen):
        order_id = _order_id(wh, order_date, index)
        order_events, _ = _lifecycle(
            rng, reference, premises, wh, order_date, order_id, eligible[position], base_day
        )
        events.extend(order_events)

    events.sort(key=lambda e: (e["occurred_at"], e["order_id"], e["sequence_no"]))
    return events
