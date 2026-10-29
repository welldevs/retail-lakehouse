"""The reference is distrusted: truncated, empty or other-schema input must fail."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from decimal import Decimal

from simulated_orders_source.reference_data import ReferenceError, load

from .support import (
    FIRST_INGESTION,
    ORDER_DATE,
    PRICE_AS_OF,
    WH,
    catalog_rows,
    customer_rows,
    write_reference,
)


class LoadTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = write_reference(os.path.join(self.tmp.name, "ref"))

    def test_loads_and_indexes(self):
        reference = load(self.dir)
        self.assertEqual(reference.warehouses(), [WH])
        self.assertEqual(reference.order_dates(WH), [ORDER_DATE])
        self.assertEqual(len(reference.catalog_of(WH, PRICE_AS_OF)), 30)
        self.assertEqual(len(reference.customers_of(WH)), 8)

    def test_price_becomes_decimal_not_float(self):
        # Obligation 4.4 of the Mercadona contract: float loses decimal precision for currency.
        price = load(self.dir).catalog_of(WH, PRICE_AS_OF)[0]["unit_price"]
        self.assertIsInstance(price, Decimal)

    def test_missing_directory_fails(self):
        with self.assertRaises(ReferenceError):
            load(os.path.join(self.tmp.name, "does-not-exist"))

    def test_missing_file_fails(self):
        os.unlink(os.path.join(self.dir, "catalog.json"))
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("missing", str(caught.exception))

    def test_invalid_json_fails(self):
        with open(os.path.join(self.dir, "calendar.json"), "w", encoding="utf-8") as handle:
            handle.write("{not json")
        with self.assertRaises(ReferenceError):
            load(self.dir)

    def test_empty_rows_fails(self):
        # A truncated export must not silently become "zero orders".
        write_reference(self.dir, catalog=[])
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("empty", str(caught.exception))

    def test_missing_required_field_fails(self):
        slim = [{k: v for k, v in row.items() if k != "unit_price"} for row in catalog_rows()]
        write_reference(self.dir, catalog=slim)
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("unit_price", str(caught.exception))

    def test_non_positive_price_fails(self):
        bad = catalog_rows()
        bad[0]["unit_price"] = "0.00"
        write_reference(self.dir, catalog=bad)
        with self.assertRaises(ReferenceError):
            load(self.dir)

    def test_price_source_outside_vocabulary_fails(self):
        write_reference(
            self.dir,
            calendar=[
                {
                    "wh": WH,
                    "order_date": ORDER_DATE,
                    "price_as_of": PRICE_AS_OF,
                    "price_source": "guessed",
                }
            ],
        )
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("price_source", str(caught.exception))

    def test_warehouse_without_customers_fails_on_lookup(self):
        with self.assertRaises(ReferenceError):
            load(self.dir).customers_of("svq1")

    def test_day_outside_window_fails_on_lookup(self):
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir).calendar_of(WH, "2026-01-01")
        self.assertIn("window", str(caught.exception))


class CustomerVersionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _reference(self, dates, first):
        return load(
            write_reference(
                os.path.join(self.tmp.name, "ref"),
                customers=customer_rows(first=first),
                customer_ingestion_dates=dates,
            )
        )

    def test_picks_the_latest_generation_up_to_the_order_date(self):
        reference = self._reference(["2026-08-24", "2026-08-27"], "2026-08-24")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-08-25"), "2026-08-24")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-08-27"), "2026-08-27")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-09-01"), "2026-08-27")

    def test_order_before_first_appearance_has_no_version(self):
        reference = self._reference(["2026-08-27"], "2026-08-27")
        self.assertIsNone(reference.customer_version_at("2026-08-27", "2026-08-24"))


class SubstitutionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = load(write_reference(os.path.join(self.tmp.name, "ref")))

    def test_subgroup_index_points_to_the_same_subgroup(self):
        catalog = self.reference.catalog_of(WH, PRICE_AS_OF)
        target = catalog[0]["subgroup_id"]
        for position in self.reference.substitutes_in_subgroup(WH, PRICE_AS_OF, target):
            self.assertEqual(catalog[position]["subgroup_id"], target)

    def test_nonexistent_subgroup_returns_empty_list(self):
        self.assertEqual(self.reference.substitutes_in_subgroup(WH, PRICE_AS_OF, 99999), [])


if __name__ == "__main__":
    unittest.main()
