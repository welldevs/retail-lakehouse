"""Validation closes the loop: it proves the landed result, not just the rule on a fixture.

Each test here corrupts the partition in a different way and requires validation to fail.
A validator that only passes on a good partition proves nothing.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
import unittest

from simulated_oltp_source.canonical import write_json
from simulated_oltp_source.extract import run as extract_run
from simulated_oltp_source.partition import (
    customers_path,
    manifest_path,
    partition_path,
    success_path,
)
from simulated_oltp_source.validate import EXIT_FAILED, EXIT_OK, run as validate_run

from tests import support


class ValidateTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = support.write_reference(os.path.join(self.tmp.name, "ref"))
        self.out = os.path.join(self.tmp.name, "data")
        with contextlib.redirect_stdout(io.StringIO()):
            extract_run(support.extract_args(self.reference, self.out))
        self.partition = partition_path(self.out, "2026-08-27", support.WH_A)

    def validate(self, strict=False) -> tuple[int, str]:
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = validate_run(support.validate_args(self.partition, self.reference, strict))
        return code, buffer.getvalue()

    def rewrite_customers(self, mutate) -> None:
        """Rewrite customers.json and reconcile the manifest, so that the failure under test
        is the one the test wants — not a checksum mismatch hiding the others."""
        with open(customers_path(self.partition), encoding="utf-8") as handle:
            customers = json.load(handle)
        mutate(customers)
        sha, size = write_json(customers_path(self.partition), customers)
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["files"][0].update(sha256=sha, bytes=size, records=len(customers))
        manifest["totals"].update(
            customer_rows=len(customers),
            municipalities_used=len({c["municipality_code"] for c in customers}),
            house_number_null=sum(1 for c in customers if c["house_number"] is None),
            bytes=size,
        )
        manifest["config"]["count"] = len(customers)
        write_json(manifest_path(self.partition), manifest)


class GoodPartitionTest(ValidateTestCase):
    def test_freshly_generated_partition_passes(self):
        code, output = self.validate(strict=True)
        self.assertEqual(code, EXIT_OK, output)
        self.assertIn("geospatial coherence validated", output)

    def test_reports_coherence_for_every_customer(self):
        _, output = self.validate()
        self.assertIn("50/50 customers", output)


class GeographicCoherenceTest(ValidateTestCase):
    def test_postal_code_outside_the_fua_fails(self):
        """The negative test that proves the central guarantee really is verified."""
        self.rewrite_customers(
            lambda customers: customers[0].update(postal_code="99999")
        )
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("postal_code", output)

    def test_municipality_outside_the_service_area_fails(self):
        """Address consistent with its source row, but in a municipality the warehouse's FUA
        does not cover. Only the service area check catches this case: every field matches
        the candidate, so the field-by-field comparison passes."""
        intruder = dict(support.CANDIDATES[0], municipality_code="003")
        reference = support.write_reference(
            os.path.join(self.tmp.name, "with-intruder"),
            candidates=support.CANDIDATES + [intruder],
        )
        self.reference = reference
        self.rewrite_customers(
            lambda c: c[0].update(
                candidate_index=len(support.CANDIDATES), municipality_code="003"
            )
        )
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("outside the Functional Urban Area", output)

    def test_candidate_index_outside_the_reference_fails(self):
        self.rewrite_customers(lambda c: c[0].update(candidate_index=999999))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("candidate_index", output)

    def test_street_that_is_not_the_tramo_street_fails(self):
        self.rewrite_customers(lambda c: c[0].update(street_name="MADE-UP STREET"))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("street_name", output)

    def test_wh_diverging_from_the_partition_fails(self):
        self.rewrite_customers(lambda c: c[0].update(wh=support.WH_B))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("is not the partition's", output)

    def test_duplicated_customer_id_fails(self):
        self.rewrite_customers(lambda c: c[1].update(customer_id=c[0]["customer_id"]))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("duplicated", output)


class NumberingTest(ValidateTestCase):
    def test_number_on_street_without_numbering_fails(self):
        def fabricate(customers):
            for customer in customers:
                if customer["numbering_type"] == "0":
                    customer["house_number"] = 42
                    return
            raise AssertionError("the fixture generated no tramo without numbering")

        self.rewrite_customers(fabricate)
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("fabricated address", output)

    def test_wrong_parity_fails(self):
        def break_parity(customers):
            for customer in customers:
                if customer["numbering_type"] == "1" and customer["house_number"]:
                    customer["house_number"] += 1
                    return
            raise AssertionError("the fixture generated no odd number")

        self.rewrite_customers(break_parity)
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("contradicts numbering_type", output)

    def test_number_outside_the_real_range_fails(self):
        def overflow(customers):
            for customer in customers:
                if customer["numbering_type"] == "1" and customer["house_number"]:
                    customer["house_number"] = 10001
                    return
            raise AssertionError("the fixture generated no odd number")

        self.rewrite_customers(overflow)
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("outside the tramo's real range", output)


class IntegrityTest(ValidateTestCase):
    def test_checksum_mismatch_fails(self):
        with open(customers_path(self.partition), "a", encoding="utf-8") as handle:
            handle.write(" ")
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("checksum mismatch", output)

    def test_file_not_declared_in_manifest_fails(self):
        with open(os.path.join(self.partition, "leftover.json"), "w") as handle:
            handle.write("{}")
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("outside the manifest", output)

    def test_missing_success_contradicts_complete(self):
        os.unlink(success_path(self.partition))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("_SUCCESS", output)

    def test_missing_manifest_fails(self):
        os.unlink(manifest_path(self.partition))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("manifest not found", output)

    def test_tampered_totals_in_manifest_fail(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["totals"]["customer_rows"] = 999
        write_json(manifest_path(self.partition), manifest)
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("totals.customer_rows", output)

    def test_unknown_manifest_version_fails(self):
        with open(manifest_path(self.partition), encoding="utf-8") as handle:
            manifest = json.load(handle)
        manifest["manifest_version"] = 99
        write_json(manifest_path(self.partition), manifest)
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("manifest_version", output)

    def test_schema_fingerprint_mismatch_fails(self):
        self.rewrite_customers(lambda c: c[0].update(new_field=1))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("schema_fingerprint", output)

    def test_removed_required_field_fails(self):
        self.rewrite_customers(lambda c: c[0].pop("birth_year"))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("birth_year", output)


class ReferenceTest(ValidateTestCase):
    def test_missing_reference_fails(self):
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = validate_run(
                support.validate_args(self.partition, os.path.join(self.tmp.name, "nothing"))
            )
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("reference unusable", buffer.getvalue())

    def test_reference_from_another_callejero_snapshot_fails(self):
        """Validating against a reference other than the one that generated proves nothing."""
        other = support.write_reference(os.path.join(self.tmp.name, "other"))
        path = os.path.join(other, "address_candidates.json")
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        payload["callejero_ingestion_date"] = "2026-01-01"
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)

        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = validate_run(support.validate_args(self.partition, other))
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("another Callejero snapshot", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()


class MinimumAgeTest(ValidateTestCase):
    """The last layer, on the LANDED data.

    The other two — the cutoff in the exported distribution and the refusal of an old-schema
    reference — act before the customer exists. This one acts after: it re-reads the
    partition and redoes the age math. It is the difference between proving the rule on a
    fixture and proving the result.
    """

    def test_minor_in_the_partition_fails(self):
        self.rewrite_customers(
            lambda customers: customers[0].update(birth_year=2020)  # 6 years old in 2026
        )
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("below min_customer_age", output)
        self.assertIn("6 years old", output)

    def test_non_integer_birth_year_fails_and_is_not_counted_as_minor(self):
        """`null` in a required field cannot become 'unknown age, let it pass'.

        Nor can it be added to the minors count: a null birth_year is not a low age, it is
        an age that does not exist. Lumping both would make the report claim there is a
        child in the base when there is none, and would send the operator looking for the
        wrong thing.
        """
        self.rewrite_customers(lambda customers: customers[0].update(birth_year=None))
        code, output = self.validate()
        self.assertEqual(code, EXIT_FAILED)
        self.assertIn("without an integer birth_year", output)
        self.assertNotIn("below min_customer_age", output)

    def test_age_comes_from_ingestion_date_not_the_clock(self):
        """A customer aged 18 IN 2026 passes, and keeps passing on any run.

        If validation used `datetime.now()`, this immutable partition would start failing on
        its own as soon as the calendar turned — without anything in it having changed.
        """
        self.rewrite_customers(
            lambda customers: customers[0].update(birth_year=2026 - support.MIN_CUSTOMER_AGE)
        )
        code, output = self.validate()
        self.assertEqual(code, EXIT_OK, output)
