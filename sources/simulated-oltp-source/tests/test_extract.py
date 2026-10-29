"""End-to-end extraction: partition, manifest and byte-for-byte reproducibility."""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest

import simulated_oltp_source
from simulated_oltp_source import MANIFEST_VERSION, SOURCE_NAME
from simulated_oltp_source.canonical import digest, read_bytes
from simulated_oltp_source.extract import EXIT_FATAL, EXIT_OK, run
from simulated_oltp_source.partition import (
    CUSTOMERS_FILE,
    customers_path,
    manifest_path,
    partition_path,
    success_path,
)

from tests import support

SRC_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(simulated_oltp_source.__file__)))


def silent(handler, args) -> int:
    with contextlib.redirect_stdout(io.StringIO()):
        return handler(args)


class ExtractTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = support.write_reference(os.path.join(self.tmp.name, "ref"))
        self.out = os.path.join(self.tmp.name, "data")

    def extract(self, **overrides) -> int:
        return silent(run, support.extract_args(self.reference, self.out, **overrides))

    def partition(self, wh=support.WH_A) -> str:
        return partition_path(self.out, "2026-08-27", wh)

    def manifest(self, wh=support.WH_A) -> dict:
        with open(manifest_path(self.partition(wh)), encoding="utf-8") as handle:
            return json.load(handle)

    def customers(self, wh=support.WH_A) -> list:
        with open(customers_path(self.partition(wh)), encoding="utf-8") as handle:
            return json.load(handle)


class PartitionTest(ExtractTestCase):
    def test_writes_data_manifest_and_success(self):
        self.assertEqual(self.extract(), EXIT_OK)
        for path in (
            customers_path(self.partition()),
            manifest_path(self.partition()),
            success_path(self.partition()),
        ):
            self.assertTrue(os.path.exists(path), path)

    def test_success_holds_the_run_id(self):
        self.extract()
        with open(success_path(self.partition()), encoding="utf-8") as handle:
            self.assertEqual(handle.read().strip(), self.manifest()["run_id"])

    def test_file_path_in_manifest_is_relative_to_snapshot_root(self):
        """The platform resolves files[].path by going up two levels when there is a wh= axis."""
        self.extract()
        entry = self.manifest()["files"][0]
        self.assertEqual(
            entry["path"],
            os.path.join("ingestion_date=2026-08-27", f"wh={support.WH_A}", CUSTOMERS_FILE),
        )
        self.assertTrue(os.path.exists(os.path.join(self.out, entry["path"])))

    def test_complete_partition_is_immutable_without_overwrite(self):
        self.extract()
        self.assertEqual(self.extract(), EXIT_FATAL)

    def test_overwrite_rewrites_and_records_the_previous_run(self):
        self.extract(seed=1)
        self.assertEqual(self.extract(seed=2, overwrite=True), EXIT_OK)
        history = self.manifest()["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["seed"], 1)


class ManifestTest(ExtractTestCase):
    def setUp(self):
        super().setUp()
        self.extract()

    def test_identity_and_version(self):
        manifest = self.manifest()
        self.assertEqual(manifest["manifest_version"], MANIFEST_VERSION)
        self.assertEqual(manifest["source"]["name"], SOURCE_NAME)
        self.assertEqual(manifest["source"]["wh"], support.WH_A)
        self.assertEqual(manifest["partition"]["warehouse"], support.WH_A)
        self.assertEqual(manifest["partition"]["ingestion_date"], "2026-08-27")
        self.assertTrue(manifest["complete"])

    def test_seed_lives_in_the_manifest_not_in_each_record(self):
        """Constant for the whole partition: repeating it in 200 records would be noise."""
        manifest = self.manifest()
        self.assertEqual(manifest["config"]["seed"], 7)
        self.assertEqual(manifest["config"]["count"], 50)
        self.assertNotIn("seed", self.customers()[0])

    def test_records_provenance_of_each_silver_input(self):
        reference = self.manifest()["reference"]
        self.assertEqual(reference["callejero_ingestion_date"], "2026-08-25")
        self.assertEqual(reference["population_ingestion_date"], "2026-08-26")
        self.assertEqual(reference["population_year"], 2025)
        self.assertEqual(reference["age_year"], 2022)
        self.assertEqual(reference["age_fk_periodo"], 27)
        self.assertEqual(reference["orphan_tramos_excluded"], 3)

    def test_totals_match_the_produced_file(self):
        customers = self.customers()
        totals = self.manifest()["totals"]
        self.assertEqual(totals["customer_rows"], len(customers))
        self.assertEqual(
            totals["municipalities_used"],
            len({c["municipality_code"] for c in customers}),
        )
        self.assertEqual(
            totals["house_number_null"],
            sum(1 for c in customers if c["house_number"] is None),
        )

    def test_declared_checksum_and_size_match_the_disk(self):
        entry = self.manifest()["files"][0]
        blob = read_bytes(customers_path(self.partition()))
        self.assertEqual(entry["sha256"], digest(blob))
        self.assertEqual(entry["bytes"], len(blob))
        self.assertEqual(entry["records"], len(self.customers()))


class ReproducibilityTest(ExtractTestCase):
    def _bytes(self) -> bytes:
        return read_bytes(customers_path(self.partition()))

    def test_same_seed_produces_byte_identical_file(self):
        self.extract(seed=13)
        first = self._bytes()
        self.extract(seed=13, overwrite=True)
        self.assertEqual(first, self._bytes())

    def test_different_seeds_produce_different_files(self):
        self.extract(seed=13)
        first = self._bytes()
        self.extract(seed=14, overwrite=True)
        self.assertNotEqual(first, self._bytes())

    def test_output_does_not_depend_on_pythonhashseed(self):
        """str hashing is randomized per process. If generation iterated a set or a dict
        rebuilt out of order, the same seed would give different outputs across runs — and
        no test in the same process would catch it."""
        program = (
            "import json, sys;"
            "from simulated_oltp_source import reference_data;"
            "from simulated_oltp_source.customers_generator import generate;"
            "ref = reference_data.load(sys.argv[1]);"
            f"print(json.dumps(generate(ref, {support.WH_A!r}, 120, 5, '2026-08-27')))"
        )
        outputs = []
        for hashseed in ("0", "1", "12345"):
            env = dict(os.environ, PYTHONPATH=SRC_ROOT, PYTHONHASHSEED=hashseed)
            result = subprocess.run(
                [sys.executable, "-c", program, self.reference],
                capture_output=True, text=True, env=env, check=True,
            )
            outputs.append(result.stdout)
        self.assertEqual(len(set(outputs)), 1, "the output changed with PYTHONHASHSEED")


class FailureTest(ExtractTestCase):
    def test_missing_reference_fails_without_creating_partition(self):
        code = silent(
            run,
            support.extract_args(os.path.join(self.tmp.name, "does-not-exist"), self.out),
        )
        self.assertEqual(code, EXIT_FATAL)
        self.assertFalse(os.path.exists(customers_path(self.partition())))

    def test_warehouse_outside_the_reference_fails(self):
        self.assertEqual(self.extract(wh="nonexistent"), EXIT_FATAL)

    def test_invalid_count_fails(self):
        self.assertEqual(self.extract(count=0), EXIT_FATAL)

    def test_malformed_date_fails(self):
        self.assertEqual(self.extract(date="27-08-2026"), EXIT_FATAL)


if __name__ == "__main__":
    unittest.main()


class BaseSizeTest(ExtractTestCase):
    """How many customers the partition has, and who decided it.

    Up to Phase 5 the command line decided, and the Makefile passed 5,000 for all four
    warehouses — the same number for FUAs that differ 4.6x in population. Nothing failed,
    because density does not show up in any total.
    """

    def test_missing_count_uses_the_reference_target(self):
        self.assertEqual(self.extract(count=None), EXIT_OK)
        self.assertEqual(len(self.customers()), support.CUSTOMER_TARGETS[support.WH_A])
        self.assertEqual(self.manifest()["config"]["count_source"], "reference")

    def test_each_warehouse_gets_its_own_target(self):
        """The target is per warehouse, not a global number split up.

        Without this case, reading the FIRST warehouse's target and applying it to all would
        pass the test above — and the base would become uniform again by another route.
        """
        for wh in (support.WH_A, support.WH_B):
            self.assertEqual(self.extract(wh=wh, count=None), EXIT_OK)
            self.assertEqual(len(self.customers(wh)), support.CUSTOMER_TARGETS[wh])
        self.assertNotEqual(
            support.CUSTOMER_TARGETS[support.WH_A], support.CUSTOMER_TARGETS[support.WH_B]
        )

    def test_explicit_count_beats_the_target_and_leaves_a_trace(self):
        """The override is still possible — and it is recorded as an override.

        Without `count_source` in the manifest, a hand-generated base would be
        indistinguishable from a population-derived one six months later, and the only way
        to find out would be to redo the math.
        """
        self.assertEqual(self.extract(count=9), EXIT_OK)
        self.assertEqual(len(self.customers()), 9)
        config = self.manifest()["config"]
        self.assertEqual(config["count"], 9)
        self.assertEqual(config["count_source"], "cli")

    def test_manifest_records_the_customer_base_scope(self):
        """The four lines that make two partitions distinguishable.

        Changing the minimum age or the rate swaps the PEOPLE behind the same customer_id.
        Without these lines, the two partitions would look the same and have the same count.
        """
        self.assertEqual(self.extract(count=None), EXIT_OK)
        reference = self.manifest()["reference"]
        self.assertEqual(reference["min_customer_age"], support.MIN_CUSTOMER_AGE)
        self.assertEqual(reference["customer_target"], support.CUSTOMER_TARGETS[support.WH_A])
        self.assertEqual(reference["allocation_rule"], "per_warehouse_population")
        self.assertEqual(reference["penetration_pct"], 2.2)
        self.assertEqual(
            reference["penetration_source"], "demand_profile.channel_reference_pct"
        )

    def test_no_generated_customer_is_a_minor(self):
        """The finding that opened the phase, at the point where it originated.

        The fixture has ages 30 to 41, so the floor is loose — what this case proves is not
        that nobody is 17, it is that age comes from the DELIVERED distribution, which is
        already the customer base's. If the generator went back to reading the whole
        pyramid, it would fail.
        """
        self.assertEqual(self.extract(count=None), EXIT_OK)
        year = 2026
        ages = [year - c["birth_year"] for c in self.customers()]
        self.assertTrue(ages)
        self.assertGreaterEqual(min(ages), support.MIN_CUSTOMER_AGE)

    def test_reference_without_target_fails_instead_of_falling_back_to_a_default(self):
        reference = support.write_reference(
            os.path.join(self.tmp.name, "ref-without-target"), allocation=None
        )
        args = support.extract_args(reference, self.out, count=None)
        self.assertEqual(silent(run, args), EXIT_FATAL)
