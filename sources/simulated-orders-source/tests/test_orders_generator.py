"""The generator: the golden rule, determinism and the non-trivial fold."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from simulated_orders_source import events as ev
from simulated_orders_source.events import ORDER_PLACED
from simulated_orders_source.orders_generator import (
    GenerationError,
    _quantity_weights,
    _sample_indices,
    customer_schedule,
    day_seed,
    generate,
    is_scheduled,
)
from simulated_orders_source.premises import Premises
from simulated_orders_source.reference_data import load
from simulated_orders_source.schema import group_by_order, totals_of

from .support import (
    FIXTURE_YEAR,
    ORDER_DATE,
    PREMISES,
    WH,
    catalog_rows,
    customer_rows,
    demand_payload,
    write_reference,
)

PACKAGE_DIR = __file__.rsplit("/tests/", 1)[0]


def _reference(directory, **kwargs):
    return load(write_reference(directory, **kwargs))


class SubSeedTest(unittest.TestCase):
    def test_changes_with_warehouse_and_day(self):
        base = day_seed(1, "mad1", "2026-08-24")
        self.assertNotEqual(base, day_seed(1, "bcn1", "2026-08-24"))
        self.assertNotEqual(base, day_seed(1, "mad1", "2026-08-25"))
        self.assertNotEqual(base, day_seed(2, "mad1", "2026-08-24"))

    def test_does_not_depend_on_pythonhashseed(self):
        # sha256, not hash(): CPython's str hash is randomized per process, and deriving the
        # sub-seed from it would make the whole partition depend on PYTHONHASHSEED.
        code = (
            "import sys; sys.path.insert(0, 'src');"
            "from simulated_orders_source.orders_generator import day_seed;"
            "print(day_seed(20260828, 'mad1', '2026-08-24'))"
        )
        outputs = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=PACKAGE_DIR,
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 13, 5150)
        }
        self.assertEqual(len(outputs), 1, f"sub-seed varied across processes: {outputs}")


class SamplingTest(unittest.TestCase):
    def test_indices_are_distinct_and_in_draw_order(self):
        import random

        drawn = _sample_indices(random.Random(7), 50, 10)
        self.assertEqual(len(drawn), 10)
        self.assertEqual(len(set(drawn)), 10)

    def test_requesting_more_than_exists_fails(self):
        import random

        with self.assertRaises(GenerationError):
            _sample_indices(random.Random(7), 3, 4)

    def test_quantity_weights_decrease_and_sum_to_one(self):
        cumulative = _quantity_weights(4)
        self.assertAlmostEqual(cumulative[-1], 1.0)
        steps = [cumulative[0]] + [
            cumulative[i] - cumulative[i - 1] for i in range(1, len(cumulative))
        ]
        self.assertEqual(steps, sorted(steps, reverse=True))


class GoldenRuleTest(unittest.TestCase):
    """The order is invented; customer, product and price are not."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))
        self.premises = Premises(PREMISES)
        self.events = generate(self.reference, self.premises, WH, ORDER_DATE, 42)
        self.grouped = group_by_order(self.events)

    def test_every_ordered_product_exists_in_that_warehouse_and_date_catalog(self):
        catalog = {p["source_product_id"] for p in self.reference.catalog_of(WH, ORDER_DATE)}
        for order_events in self.grouped.values():
            for line in order_events[0]["payload"]["lines"]:
                self.assertIn(line["source_product_id"], catalog)

    def test_every_price_paid_is_the_observed_price(self):
        prices = {
            p["source_product_id"]: p["unit_price"]
            for p in self.reference.catalog_of(WH, ORDER_DATE)
        }
        for order_events in self.grouped.values():
            for line in order_events[0]["payload"]["lines"]:
                self.assertEqual(
                    Decimal(line["unit_price"]), prices[line["source_product_id"]]
                )

    def test_every_customer_belongs_to_the_order_warehouse(self):
        of_warehouse = {c["customer_id"] for c in self.reference.customers_of(WH)}
        for order_events in self.grouped.values():
            self.assertIn(order_events[0]["payload"]["customer_id"], of_warehouse)

    def test_substitute_comes_from_the_same_catalog(self):
        catalog = {p["source_product_id"] for p in self.reference.catalog_of(WH, ORDER_DATE)}
        seen = 0
        for order_events in self.grouped.values():
            for event in order_events:
                if event["event_type"] == ev.ORDER_LINE_SUBSTITUTED:
                    seen += 1
                    self.assertIn(event["payload"]["substitute_source_product_id"], catalog)
        self.assertGreater(seen, 0, "no substitution generated: the test looked at nothing")

    def test_a_customer_orders_at_most_once_per_day(self):
        customers = [e[0]["payload"]["customer_id"] for e in self.grouped.values()]
        self.assertEqual(len(customers), len(set(customers)))


class NonTrivialFoldTest(unittest.TestCase):
    """If the final value were derivable from the first event, the log would be a date stamp."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))

    def test_picked_value_differs_from_placed(self):
        events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        totals = totals_of(events)
        self.assertGreater(totals["substituted_lines"] + totals["removed_lines"], 0)
        self.assertNotEqual(totals["gross_amount_placed"], totals["net_amount_picked"])

    def test_without_substitution_or_removal_the_fold_becomes_trivial(self):
        # The explicit counterpoint: with both rates at zero, the picked value becomes
        # derivable from the first event. This is the scenario the inverted dbt test watches.
        flat = dict(PREMISES, substitution_rate_fresh="0", substitution_rate_other="0",
                    removal_rate_fresh="0", removal_rate_other="0")
        events = generate(self.reference, Premises(flat), WH, ORDER_DATE, 42)
        totals = totals_of(events)
        self.assertEqual(totals["substituted_lines"], 0)
        self.assertEqual(totals["removed_lines"], 0)
        for order_events in group_by_order(events).values():
            picked = [e for e in order_events if e["event_type"] == ev.ORDER_PICKED]
            if picked:
                self.assertEqual(
                    picked[0]["payload"]["picked_amount"], order_events[0]["payload"]["gross_amount"]
                )


class LogStructureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))
        self.events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)

    def test_log_is_totally_ordered(self):
        sort_keys = [(e["occurred_at"], e["order_id"], e["sequence_no"]) for e in self.events]
        self.assertEqual(sort_keys, sorted(sort_keys))

    def test_sequence_no_is_contiguous_per_order(self):
        for order_id, order_events in group_by_order(self.events).items():
            self.assertEqual(
                [e["sequence_no"] for e in order_events],
                list(range(1, len(order_events) + 1)),
                f"{order_id} has a gap in sequence_no",
            )

    def test_every_order_starts_with_order_placed(self):
        for order_events in group_by_order(self.events).values():
            self.assertEqual(order_events[0]["event_type"], ev.ORDER_PLACED)

    def test_every_sequence_is_accepted_by_the_state_machine(self):
        for order_events in group_by_order(self.events).values():
            ev.fold(order_events)  # strict: raises if the generator emitted an impossible transition

    def test_time_does_not_go_backwards_within_an_order(self):
        for order_events in group_by_order(self.events).values():
            instants = [e["occurred_at"] for e in order_events]
            self.assertEqual(instants, sorted(instants))

    def test_no_event_stamps_the_clock(self):
        # Every occurred_at derives from the partition day. An order placed today would prove
        # that datetime.now() leaked into the generator.
        for event in self.events:
            if event["event_type"] == ev.ORDER_PLACED:
                self.assertTrue(event["occurred_at"].startswith(ORDER_DATE))


class ReproducibilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"))

    def test_same_input_produces_same_output(self):
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.assertEqual(a, b)

    def test_other_seed_produces_other_output(self):
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 43)
        self.assertNotEqual(a, b)

    def test_other_premises_table_produces_other_output(self):
        # That is why the premises sha256 goes into the manifest: without it, changing a rate
        # and regenerating would change the orders under the same order_id without a trace.
        a = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        b = generate(
            self.reference, Premises(dict(PREMISES, basket_lines_mode="5")), WH, ORDER_DATE, 42
        )
        self.assertNotEqual(a, b)

    def test_different_day_does_not_reshuffle_the_previous_day(self):
        # This Source's additive property: adding a day to the window leaves the already
        # generated partitions byte-for-byte identical, because each day derives its own sub-seed.
        calendar = [
            {"wh": WH, "order_date": d, "price_as_of": ORDER_DATE, "price_source": "observed"}
            for d in (ORDER_DATE, "2026-08-25")
        ]
        reference = _reference(os.path.join(self.tmp.name, "ref2"), calendar=calendar,
                               customers=customer_rows(total=40))
        # Customers split between Monday (the fixture day) and Tuesday, so both days order.
        two_days = Premises(dict(PREMISES, order_weekday_weight_1="0.5",
                                 order_weekday_weight_2="0.5"))
        first = generate(reference, two_days, WH, ORDER_DATE, 42)
        generate(reference, two_days, WH, "2026-08-25", 42)
        self.assertEqual(first, generate(reference, two_days, WH, ORDER_DATE, 42))


class RefusalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_order_before_customer_exists_is_refused(self):
        # Restriction 6: no order is born before its customer.
        reference = _reference(
            os.path.join(self.tmp.name, "ref"),
            customers=customer_rows(first="2026-09-01"),
            customer_ingestion_dates=["2026-09-01"],
        )
        with self.assertRaises(GenerationError) as caught:
            generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.assertIn("existed", str(caught.exception))

    def test_skip_that_zeroes_orders_is_refused(self):
        reference = _reference(os.path.join(self.tmp.name, "ref2"))
        flat = dict(PREMISES, order_skip_rate="1")
        with self.assertRaises(GenerationError):
            generate(reference, Premises(flat), WH, ORDER_DATE, 42)

    def test_basket_larger_than_catalog_does_not_blow_up(self):
        # Degrades to the whole catalog instead of raising IndexError.
        reference = _reference(
            os.path.join(self.tmp.name, "ref3"), catalog=catalog_rows(total=6)
        )
        events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        for order_events in group_by_order(events).values():
            self.assertLessEqual(len(order_events[0]["payload"]["lines"]), 6)


class EligibilityTest(unittest.TestCase):
    """Who may place an order.

    The failure mode this catches is the phase's Finding 1: `silver_customer` is a faithful
    projection of the resident POPULATION, and population includes children — 18.01% of the
    measured base was under 18, with ages starting at zero. While age did nothing this was
    harmless. Once age is tied to demand, it no longer is: 18% of the base would fall into
    MAPA's '-35 years' band while being children, and the resulting mix would look plausible.
    """

    def _reference(self, tmp, ages, total=8):
        customers = customer_rows(total=total, ages=ages)
        return load(write_reference(os.path.join(tmp, "ref"), customers=customers))

    def test_minor_never_places_an_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Half the base aged 8: if the minimum age were ignored, these customers would show
            # up as buyers in roughly half the orders.
            reference = self._reference(tmp, ages=(8, 40))
            events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
            customers = {row["customer_id"]: row for row in reference.customers_of(WH)}
            buyers = {
                e["payload"]["customer_id"] for e in events
                if e["event_type"] == ORDER_PLACED
            }
            self.assertTrue(buyers, "the fixture must generate at least one order")
            for cid in buyers:
                age = FIXTURE_YEAR - int(customers[cid]["birth_year"])
                self.assertGreaterEqual(age, 18, f"{cid} was {age} years old")

    def test_whole_base_below_minimum_age_fails(self):
        # Without this, `wanted` would drop to zero and the day would come out with no orders
        # at all — silent, and indistinguishable from a legitimately empty day.
        with tempfile.TemporaryDirectory() as tmp:
            reference = self._reference(tmp, ages=(5, 9))
            with self.assertRaises(GenerationError) as caught:
                generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
            self.assertIn("aged 18 or older", str(caught.exception))


class RegionalFrequencyTest(unittest.TestCase):
    """The regional index weighs HOW MANY order, and nothing else.

    Without this pair, an index read from the profile and never applied would go unnoticed:
    the order count would remain plausible, just equal across all warehouses — which is
    exactly the state the phase came to fix.
    """

    def _orders(self, tmp, index):
        profile = demand_payload(frequency=index)
        reference = load(write_reference(os.path.join(tmp, "ref"), demand=profile,
                                         customers=customer_rows(total=40)))
        events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        return sum(1 for e in events if e["event_type"] == ORDER_PLACED)

    def test_higher_index_produces_more_orders(self):
        with tempfile.TemporaryDirectory() as tmp:
            few = self._orders(tmp, {"13": "0.5", "09": "1.0"})
        with tempfile.TemporaryDirectory() as tmp:
            many = self._orders(tmp, {"13": "1.5", "09": "1.0"})
        self.assertLess(few, many)

    def test_neutral_index_does_not_change_the_count(self):
        # The pair of the test above: proves the mechanism only acts when the index says so,
        # and does not touch the count on its own.
        with tempfile.TemporaryDirectory() as tmp:
            neutral = self._orders(tmp, {"13": "1.0", "09": "1.0"})
        with tempfile.TemporaryDirectory() as tmp:
            reference = load(write_reference(os.path.join(tmp, "ref"),
                                             customers=customer_rows(total=40)))
            events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
            default = sum(1 for e in events if e["event_type"] == ORDER_PLACED)
        self.assertEqual(neutral, default)


class CohortOnEventTest(unittest.TestCase):
    def test_stamped_buyer_age_band_matches_customer_age(self):
        # Stamped on the event for the same reason as `demand_group`: what matters is the band
        # in force AT ORDER TIME. A wrong stamp would pick another weight vector and the
        # basket would come out consistent with another cohort — plausible, no nulls, no
        # wrong total.
        with tempfile.TemporaryDirectory() as tmp:
            reference = load(write_reference(os.path.join(tmp, "ref")))
            events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
            customers = {row["customer_id"]: row for row in reference.customers_of(WH)}
            seen = set()
            for event in events:
                if event["event_type"] != ORDER_PLACED:
                    continue
                payload = event["payload"]
                age = FIXTURE_YEAR - int(customers[payload["customer_id"]]["birth_year"])
                self.assertEqual(
                    payload["buyer_age_band"], reference.demand.band_of(age)
                )
                seen.add(payload["buyer_age_band"])
            self.assertGreater(len(seen), 1, "the fixture must cover more than one band")


class RealismTest(unittest.TestCase):
    """CR-017: the operating calendar and the basket rules a real operator publishes."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = _reference(os.path.join(self.tmp.name, "ref"),
                                    customers=customer_rows(total=40))
        self.events = generate(self.reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        self.orders = group_by_order(self.events)

    @staticmethod
    def _local(stamp):
        moment = datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return moment.astimezone(ZoneInfo("Europe/Madrid"))

    def test_there_are_orders_to_check(self):
        self.assertGreater(len(self.orders), 5)

    def test_never_delivered_on_the_day_of_the_order_nor_on_sunday(self):
        for order_events in self.orders.values():
            placed = self._local(order_events[0]["occurred_at"]).date()
            for event in order_events:
                if event["event_type"] in (ev.ORDER_DELIVERED, ev.ORDER_DELIVERY_FAILED):
                    delivered = self._local(event["occurred_at"])
                    self.assertGreater(delivered.date(), placed)
                    self.assertNotEqual(delivered.isoweekday(), 7)

    def test_slots_are_one_hour_inside_the_operating_window(self):
        for order_events in self.orders.values():
            payload = order_events[0]["payload"]
            start = self._local(payload["delivery_slot_start"])
            end = self._local(payload["delivery_slot_end"])
            self.assertEqual((end - start).total_seconds(), 3600)
            self.assertGreaterEqual(start.hour, 7)
            self.assertLessEqual(end.hour + end.minute / 60, 22)

    def test_every_basket_reaches_the_minimum_order(self):
        for order_events in self.orders.values():
            self.assertGreaterEqual(Decimal(order_events[0]["payload"]["gross_amount"]),
                                    Decimal(PREMISES["min_order_amount_eur"]))

    def test_quantity_never_exceeds_the_products_own_ceiling(self):
        ceiling = {row["source_product_id"]: row["quantity_max"] for row in catalog_rows()}
        for order_events in self.orders.values():
            for line in order_events[0]["payload"]["lines"]:
                self.assertLessEqual(line["quantity"], ceiling[line["source_product_id"]])

    def test_picking_and_dispatch_are_scheduled_before_the_slot(self):
        for order_events in self.orders.values():
            slot = order_events[0]["payload"]["delivery_slot_start"]
            for event in order_events:
                if event["event_type"] == ev.ORDER_DISPATCHED:
                    self.assertLess(event["occurred_at"], slot)

    def test_a_popular_product_is_chosen_far_more_often(self):
        rows = catalog_rows()
        for row in rows:
            if row["source_product_id"] == "p0000":
                row["popularity_weight"] = "1000.000000"
        reference = _reference(os.path.join(self.tmp.name, "pop"), catalog=rows,
                               customers=customer_rows(total=40))
        events = generate(reference, Premises(PREMISES), WH, ORDER_DATE, 42)
        baskets = [e["payload"]["lines"] for e in events if e["event_type"] == ORDER_PLACED]
        with_star = sum(any(l["source_product_id"] == "p0000" for l in b) for b in baskets)
        self.assertGreater(with_star / len(baskets), 0.5)


class CadenceTest(unittest.TestCase):
    def test_the_schedule_is_a_function_of_the_customer_alone(self):
        premises = Premises(dict(PREMISES, cadence_weight_7d="0.5", cadence_weight_14d="0.5",
                                 order_weekday_weight_1="0.5", order_weekday_weight_3="0.5"))
        first = customer_schedule(42, "cust_x", premises)
        self.assertEqual(first, customer_schedule(42, "cust_x", premises))
        self.assertIn(first[0], (7, 14))
        self.assertIn(first[1], (1, 3))

    def test_a_weekly_customer_orders_once_a_week_on_their_weekday(self):
        schedule = (7, 1, 0)  # weekly, Monday
        days = [date(2026, 8, 24) + timedelta(days=offset) for offset in range(21)]
        hits = [d for d in days if is_scheduled(schedule, d)]
        self.assertEqual([d.isoweekday() for d in hits], [1, 1, 1])

    def test_a_fortnightly_customer_skips_alternate_weeks(self):
        days = [date(2026, 8, 24) + timedelta(days=offset) for offset in range(28)]
        self.assertEqual(len([d for d in days if is_scheduled((14, 1, 0), d)]), 2)
        self.assertEqual(len([d for d in days if is_scheduled((14, 1, 1), d)]), 2)
        both = [d for d in days if is_scheduled((14, 1, 0), d) and is_scheduled((14, 1, 1), d)]
        self.assertEqual(both, [])


if __name__ == "__main__":
    unittest.main()
