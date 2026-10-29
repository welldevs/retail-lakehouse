"""Generates the event log of one (warehouse, day).

THE GOLDEN RULE
---------------
Zero randomness in customer, product and price. The ORDER is invented; who buys, what is
bought and what it costs come from the reference, which came from Silver, which came from RAW.
No product, price, customer or postal code is fabricated here.

WHAT IS DRAWN, AND WHAT THAT MEANS
----------------------------------
    day + warehouse -> its own sub-seed
                    -> how many customers order today (rate x seasonal x regional index)
                    -> which customers order today (UNIFORM, without replacement, among eligible)
                    -> which COHORT each of them is (age band x autonomous community)
                    -> how many lines the basket has (declared triangular)
                    -> which DEMAND GROUP (weighted by THAT COHORT's profile)
                    -> which product WITHIN the group (UNIFORM)
                    -> quantity per line (decreasing, derived from quantity_max)
                    -> time, delivery slot and milestones (declared ranges)
                    -> which lines are substituted or removed (declared rates)
                    -> which terminal branch the order follows (declared rates)

THE CHOICE OF PRODUCT WITHIN THE GROUP STAYS UNIFORM, on purpose: no source in this repo
measures per-SKU turnover, and weighting products would invent a distribution nobody measured
— the same prohibition Phase 1 applied to choosing the street segment ("there is no population
per street or per segment in any source this platform ingests").

WHAT CHANGED IN PHASE 4 is the level above. Before, the product was drawn uniformly over the
WHOLE CATALOG, and the declared consequence was that the category mix mirrored the SIZE OF THE
ASSORTMENT. Now there is an observational anchor that did not exist: MAPA's Informe del Consumo
Alimentario en España 2025. The demand group is drawn with a weight calibrated against Spanish
household volume, tilted by the e-commerce share; the product within the group stays uniform.

THE BOUNDARY, which matters more than the calibration: MAPA measures residents' household
consumption. It does NOT measure online-store orders, baskets or purchase cadence. That is why
`daily_order_rate`, `basket_lines_*` and `quantity_max` remain `synthetic` premises with no
calibration at all, and the non-food third of the catalog — which the report does not cover —
gets a declared share.

REPRODUCIBILITY: SUB-SEED PER (WAREHOUSE, DAY)
----------------------------------------------
Each `(wh, order_date)` derives its own `random.Random` from `sha256(seed|wh|order_date)`.
Consequence — and this is the analogue of Phase 1's "prefix property", on a different axis:

    ADDING A DAY TO THE WINDOW IS ADDITIVE. Already generated partitions stay byte-for-byte
    identical, because no day depends on another day's draw.

`hashlib.sha256` is used, not `hash()`, because CPython's `str` hash is randomized per
process: deriving the sub-seed from it would make the output depend on PYTHONHASHSEED.

NO CLOCK. Every `occurred_at` derives from the partition date plus declared offsets. No call
to `datetime.now()` in this module.
"""

from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

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
    """Decreasing weights for 1..quantity_max, derived from the premise itself.

    w(k) = 1/2^(k-1): buying one unit dominates, and each extra unit is half as likely.
    Deriving from the premise instead of tabulating avoids a second, undeclared premise.
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


def _order_id(wh: str, order_date: str, index: int) -> str:
    return f"ord_{wh}_{order_date.replace('-', '')}_{index:06d}"


def _basket(rng, reference, premises, wh, price_as_of, cohort):
    """Build the basket: lines with real product, real price, drawn quantity.

    TWO STEPS, and the separation between them is what this whole phase introduced:

        1. DEMAND GROUP, weighted by the profile calibrated against MAPA — and, since
           `mapa_2025_v2`, by the profile OF THE COHORT of the customer placing the order.
        2. PRODUCT WITHIN THE GROUP, uniform.

    Step 2 stays uniform on purpose: no source in this repo measures per-SKU turnover, and
    weighting products would invent a distribution nobody measured. Only step 1 changed —
    before, the product was drawn uniformly over the WHOLE CATALOG, and the category mix
    mirrored the SIZE OF THE ASSORTMENT. A catalog has 475 facial-care SKUs and 162 fruit and
    vegetable ones; a household does not buy in that proportion.

    PRICE TAKES NO PART IN EITHER DRAW. It only appears afterwards, copied from the
    reference. An expensive category can have low volume and high revenue, and that is a
    result of the model, not a defect: in MAPA, shellfish are 0.81% of volume and 2.88% of value.
    """
    catalog = reference.catalog_of(wh, price_as_of)
    groups = reference.demand_groups_of(wh, price_as_of)
    cumulative = reference.demand_cdf_of(wh, price_as_of, cohort)

    low = premises.integer("basket_lines_min")
    high = premises.integer("basket_lines_max")
    mode = premises.integer("basket_lines_mode")

    wanted = int(rng.triangular(low, high + 1, mode))
    wanted = max(low, min(high, wanted))
    wanted = min(wanted, len(catalog))

    quantity_cumulative = _quantity_weights(premises.integer("quantity_max"))

    # WITHOUT REPLACEMENT, as before: a product appears at most once in the basket. The
    # difference is that exclusion can now exhaust a small GROUP (MIEL has 5 products). When
    # that happens the group draw is redone — and the wasted attempt consumed one rng draw,
    # which keeps the sequence deterministic instead of silently "skipping".
    # The attempt ceiling exists so that a pathological profile fails fast instead of
    # hanging; it was never reached in the measured window.
    positions: list[int] = []
    taken: set = set()
    attempts = 0
    ceiling = max(wanted * 8, 32)
    while len(positions) < wanted and attempts < ceiling:
        attempts += 1
        group = reference.demand.pick(rng, groups, cumulative)
        candidates = reference.positions_in_demand_group(wh, price_as_of, group)
        free = [p for p in candidates if p not in taken]
        if not free:
            continue
        picked = free[rng.randrange(len(free))]
        taken.add(picked)
        positions.append(picked)

    lines = []
    for line_no, position in enumerate(positions, start=1):
        product = catalog[position]
        quantity = _pick_weighted(rng, quantity_cumulative) + 1
        lines.append(
            {
                "line_no": line_no,
                "catalog_position": position,
                "source_product_id": product["source_product_id"],
                "display_name": product["display_name"],
                "category_id": product["category_id"],
                "subgroup_id": product["subgroup_id"],
                "demand_group": product["demand_group"],
                "quantity": quantity,
                "unit_price": product["unit_price"],
            }
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

    placed_at = base_day + timedelta(
        hours=rng.randint(premises.integer("order_hour_min"), premises.integer("order_hour_max")),
        minutes=rng.randrange(60),
        seconds=rng.randrange(60),
    )
    slot_start = (
        placed_at
        + timedelta(
            hours=rng.randint(
                premises.integer("slot_lead_hours_min"), premises.integer("slot_lead_hours_max")
            )
        )
    ).replace(minute=0, second=0, microsecond=0)
    slot_end = slot_start + timedelta(hours=premises.integer("slot_hours"))

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

    # --- cancellation -----------------------------------------------------------
    if rng.random() < premises.number("cancellation_rate"):
        cancel_at = payment_at + timedelta(
            minutes=rng.randint(
                premises.integer("minutes_to_cancel_min"),
                premises.integer("minutes_to_cancel_max"),
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
    picking_at = payment_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_picking_min"), premises.integer("minutes_to_picking_max")
        )
    )
    emit(ORDER_PICKING_STARTED, picking_at, {})

    per_line = premises.integer("minutes_per_line_picked")
    substitution_rate = premises.number("substitution_rate")
    removal_rate = premises.number("removal_rate")
    taken = {line["source_product_id"] for line in lines}

    for line in lines:
        line["status"] = LINE_FULFILLED
        line["effective_price"] = line["unit_price"]
        line_at = picking_at + timedelta(minutes=per_line * line["line_no"])

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
    picked_at = picking_at + timedelta(minutes=per_line * len(lines))
    emit(
        ORDER_PICKED,
        picked_at,
        {"picked_line_count": len(kept), "picked_amount": str(net)},
    )

    # --- dispatch and delivery---------------------------------------------------
    dispatch_at = picked_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_dispatch_min"),
            premises.integer("minutes_to_dispatch_max"),
        )
    )
    emit(ORDER_DISPATCHED, dispatch_at, {})

    delivery_at = dispatch_at + timedelta(
        minutes=rng.randint(
            premises.integer("minutes_to_delivered_min"),
            premises.integer("minutes_to_delivered_max"),
        )
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

    rate = premises.number("daily_order_rate") * factor * regional
    wanted = round(len(eligible) * rate)
    if wanted <= 0:
        raise GenerationError(
            f"daily_order_rate={premises.number('daily_order_rate')} x seasonal factor "
            f"{factor} x regional index {regional} over {len(eligible)} eligible "
            f"customer(s) yields zero orders on {order_date}"
        )

    # Without replacement: a customer places at most one order per day. It is a declared
    # simplification, not a measurement — no source in this repo measures purchase cadence.
    chosen = _sample_indices(rng, len(eligible), wanted)

    events: list[dict] = []
    for index, position in enumerate(chosen):
        order_id = _order_id(wh, order_date, index)
        order_events, _ = _lifecycle(
            rng, reference, premises, wh, order_date, order_id, eligible[position], base_day
        )
        events.extend(order_events)

    events.sort(key=lambda e: (e["occurred_at"], e["order_id"], e["sequence_no"]))
    return events
