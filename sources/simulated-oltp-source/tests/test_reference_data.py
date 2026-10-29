"""The reference is distrusted: incomplete or inconsistent must fail the generation."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from simulated_oltp_source import reference_data
from simulated_oltp_source.reference_data import ReferenceError, load

from tests import support


class LoadTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = os.path.join(self.tmp.name, "ref")

    def write(self, **kwargs) -> str:
        return support.write_reference(self.directory, **kwargs)

    def corrupt(self, name: str, mutate) -> str:
        self.write()
        path = os.path.join(self.directory, name)
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        mutate(payload)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return self.directory

    def test_reads_the_three_files_and_exposes_provenance(self):
        reference = load(self.write())
        self.assertEqual(reference.callejero_ingestion_date, "2026-08-25")
        self.assertEqual(reference.population_ingestion_date, "2026-08-26")
        self.assertEqual(reference.population_series_ingestion_date, "2026-08-26")
        self.assertEqual(reference.population_year, 2025)
        self.assertEqual(reference.age_year, 2022)
        self.assertEqual(reference.age_fk_periodo, 27)
        self.assertEqual(reference.orphan_tramos_excluded, 3)

    def test_indexes_candidates_by_municipality_preserving_file_order(self):
        reference = load(self.write())
        self.assertEqual(reference.candidates_of(support.WH_A, "01", "001"), [0, 1, 2])
        self.assertEqual(reference.candidates_of(support.WH_A, "01", "002"), [3, 4, 5])
        self.assertEqual(reference.candidates_of(support.WH_B, "02", "010"), [6])

    def test_warehouses_and_municipalities(self):
        reference = load(self.write())
        self.assertEqual(reference.warehouses(), [support.WH_A, support.WH_B])
        self.assertEqual(len(reference.municipalities(support.WH_A)), 2)

    def test_ages_by_province_come_out_as_parallel_lists(self):
        ages, proportions = load(self.write()).ages_of("01")
        self.assertEqual(ages, [30, 31, 32])
        self.assertEqual(len(proportions), 3)

    def test_nonexistent_directory_fails(self):
        with self.assertRaises(ReferenceError):
            load(os.path.join(self.tmp.name, "does-not-exist"))

    def test_missing_file_fails_with_actionable_message(self):
        self.write()
        os.unlink(os.path.join(self.directory, reference_data.AGE_DISTRIBUTION_FILE))
        with self.assertRaises(ReferenceError) as caught:
            load(self.directory)
        self.assertIn("oltp-export-reference", str(caught.exception))

    def test_invalid_json_fails(self):
        self.write()
        with open(
            os.path.join(self.directory, reference_data.POPULATION_WEIGHTS_FILE), "w"
        ) as handle:
            handle.write("{ not json")
        with self.assertRaises(ReferenceError):
            load(self.directory)

    def test_empty_rows_list_fails(self):
        directory = self.corrupt(
            reference_data.ADDRESS_CANDIDATES_FILE, lambda p: p.update(rows=[])
        )
        with self.assertRaises(ReferenceError):
            load(directory)

    def test_missing_required_field_fails(self):
        """An old-schema export cannot pass as good."""
        def remove_postal_code(payload):
            payload["rows"][0].pop("postal_code")

        with self.assertRaises(ReferenceError) as caught:
            load(self.corrupt(reference_data.ADDRESS_CANDIDATES_FILE, remove_postal_code))
        self.assertIn("postal_code", str(caught.exception))

    def test_root_that_is_not_an_object_fails(self):
        self.write()
        with open(
            os.path.join(self.directory, reference_data.AGE_DISTRIBUTION_FILE), "w"
        ) as handle:
            json.dump([1, 2, 3], handle)
        with self.assertRaises(ReferenceError):
            load(self.directory)

    def test_municipality_without_candidate_fails_on_lookup(self):
        reference = load(self.write())
        with self.assertRaises(ReferenceError):
            reference.candidates_of(support.WH_A, "99", "999")

    def test_province_without_ages_fails_on_lookup(self):
        reference = load(self.write())
        with self.assertRaises(ReferenceError):
            reference.ages_of("99")


if __name__ == "__main__":
    unittest.main()


class CustomerScopeTest(unittest.TestCase):
    """The reference declares who may be a customer, and the Source distrusts it.

    What these cases prevent has already happened: on 2026-08-31 the base had 18.01% of
    customers under 18 — 3,602 of 20,000, with ages starting from zero. An old-schema
    reference, without `min_customer_age`, would reproduce exactly that, and no total would
    break.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _ref(self, **overrides) -> str:
        return support.write_reference(os.path.join(self.tmp.name, "ref"), **overrides)

    def test_exposes_the_declared_minimum_age_and_rate(self):
        reference = load(self._ref())
        self.assertEqual(reference.min_customer_age, support.MIN_CUSTOMER_AGE)
        self.assertEqual(reference.allocation_rule, "per_warehouse_population")
        self.assertEqual(reference.penetration_pct, 2.2)
        self.assertEqual(
            reference.penetration_source, "demand_profile.channel_reference_pct"
        )

    def test_customer_target_per_warehouse(self):
        reference = load(self._ref())
        for wh, target in support.CUSTOMER_TARGETS.items():
            self.assertEqual(reference.customer_target(wh), target)

    def test_unknown_warehouse_target_fails_instead_of_guessing(self):
        reference = load(self._ref())
        with self.assertRaises(ReferenceError) as error:
            reference.customer_target("does-not-exist")
        self.assertIn("no customer target", str(error.exception))

    def test_reference_without_minimum_age_fails(self):
        """Old schema: generated before the customer base stopped being the population."""
        with self.assertRaises(ReferenceError) as error:
            load(self._ref(min_customer_age=None))
        self.assertIn("min_customer_age", str(error.exception))

    def test_age_below_declared_minimum_fails(self):
        """The header is the promise; the rows are the check.

        A halfway truncation — the header saying 18, a child surviving in the rows — would
        pass the header test and produce minors again.
        """
        with self.assertRaises(ReferenceError) as error:
            load(self._ref(ages=support.AGES_WITH_MINOR))
        self.assertIn("below the min_customer_age", str(error.exception))

    def test_reference_without_allocation_fails(self):
        with self.assertRaises(ReferenceError) as error:
            load(self._ref(allocation=None))
        self.assertIn("customer_allocation", str(error.exception))
