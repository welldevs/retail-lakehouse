"""Validation: every snapshot failure mode must be detected, never silenced."""

from __future__ import annotations

import contextlib
import json
import io
import os
import tempfile
import unittest

from mercadona_catalog_source import extract, partition as part, validate
from mercadona_catalog_source.canonical import read_json, write_json
from tests.support import catalog, extract_args, make_fetcher_factory, tree, validate_args

TREE = tree((1, "Parent", [(10, "Alfa"), (11, "Beta")]))
RESPONSES = {
    "/categories/": TREE,
    "/categories/10/": catalog(10, "Alfa", ("g1", [(1, "p1", "1.00"), (2, "p2", "2.00")])),
    "/categories/11/": catalog(11, "Beta", ("g1", [(3, "p3", "3.00")])),
}


def build_partition(root, responses=None, **overrides):
    factory, _ = make_fetcher_factory(dict(responses or RESPONSES))
    with contextlib.redirect_stdout(io.StringIO()):
        extract.run(extract_args(root, **overrides), fetcher_factory=factory)
    return os.path.join(root, "ingestion_date=2026-01-01", "wh=mad1")


def run_validate(partition, strict=False):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = validate.run(validate_args(partition, strict))
    return code, buffer.getvalue()


class ValidPartitionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_intact_partition_passes(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("OK:", output)
        self.assertIn("orphans .......... 0", output)

    def test_reports_rows_and_unique_products(self):
        _, output = run_validate(self.partition)
        self.assertIn("rows ............. 3", output)
        self.assertIn("unique products .. 3", output)


class IntegrityFailureTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = build_partition(self.root)

    def test_missing_manifest(self):
        os.unlink(part.manifest_path(self.partition))
        self.assertEqual(run_validate(self.partition)[0], 1)

    def test_corrupt_manifest_is_reported_without_traceback(self):
        with open(part.manifest_path(self.partition), "wb") as handle:
            handle.write(b"{not json")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("unreadable", output)

    def test_missing_file(self):
        os.unlink(part.catalog_path(self.partition, 10))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("missing file", output)

    def test_checksum_mismatch(self):
        payload, _ = read_json(part.catalog_path(self.partition, 10))
        payload["name"] = "tampered"
        write_json(part.catalog_path(self.partition, 10), payload)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum mismatch", output)

    def test_corrupt_file_is_reported_instead_of_crashing_the_validator(self):
        with open(part.catalog_path(self.partition, 10), "wb") as handle:
            handle.write(b'{"id": 10, "categ')
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("invalid JSON", output)
        # the report runs to the end instead of dying at the first bad file
        self.assertIn("FAILED", output)

    def test_missing_category_tree(self):
        os.unlink(part.categories_path(self.partition))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("category tree missing", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_undeclared_catalog_file_is_detected(self):
        write_json(part.catalog_path(self.partition, 99), {"id": 99, "categories": []})
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("not in the manifest", output)

    def test_stray_file_at_partition_root_is_detected(self):
        with open(os.path.join(self.partition, "annotation.txt"), "w", encoding="utf-8") as handle:
            handle.write("note")
        self.assertEqual(run_validate(self.partition)[0], 1)

    def test_log_and_marker_are_not_orphans(self):
        self.assertEqual(run_validate(self.partition)[0], 0)


class SuccessMarkerTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_removed_success_contradicts_the_manifest(self):
        os.unlink(part.success_path(self.partition))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("_SUCCESS", output)

    def test_spurious_success_in_partial_partition(self):
        partial = build_partition(tempfile.mkdtemp(), limit=1)
        with open(part.success_path(partial), "w", encoding="utf-8") as handle:
            handle.write("lie\n")
        self.assertEqual(run_validate(partial)[0], 1)


class TotalsAndSchemaTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_manifest(self, mutate):
        manifest = part.read_manifest(self.partition)
        mutate(manifest)
        write_json(part.manifest_path(self.partition), manifest)

    def test_tampered_product_rows(self):
        self._tamper_manifest(lambda m: m["totals"].update(product_rows=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.product_rows", output)

    def test_tampered_bytes(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_tampered_catalog_files(self):
        self._tamper_manifest(lambda m: m["totals"].update(catalog_files=7))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.catalog_files", output)

    def test_fingerprint_mismatch(self):
        self._tamper_manifest(lambda m: m["schema_fingerprint"].update(sha256="0" * 64))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("schema_fingerprint mismatch", output)

    def test_incompatible_manifest_version(self):
        self._tamper_manifest(lambda m: m.update(manifest_version=99))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("manifest_version", output)


class PerFileRecheckTest(unittest.TestCase):
    """Every per-file declared field is re-checked, not just the aggregate totals."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_entry(self, suffix, field, value):
        manifest = part.read_manifest(self.partition)
        entry = next(f for f in manifest["files"] if f["path"].endswith(suffix))
        entry[field] = value
        write_json(part.manifest_path(self.partition), manifest)

    def test_catalog_file_records_are_rechecked(self):
        self._tamper_entry("category_id=10.json", "records", 99)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("count mismatch", output)

    def test_category_tree_records_are_rechecked(self):
        self._tamper_entry("categories.json", "records", 42)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("count mismatch", output)
        self.assertIn("categories.json", output)

    def test_bytes_of_each_file_are_rechecked(self):
        self._tamper_entry("category_id=10.json", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("size mismatch", output)

    def test_entry_pointing_outside_snapshot_root_is_blocked(self):
        self._tamper_entry("category_id=10.json", "path", "../../../../etc/passwd")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("outside the snapshot root", output)

    def test_malformed_manifest_entry_is_reported(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"].append({"no": "path"})
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("malformed", output)


class CoverageAndFailuresTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_level_2_category_without_file_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [
            f for f in manifest["files"] if not f["path"].endswith("category_id=11.json")
        ]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(part.catalog_path(self.partition, 11))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("level-2 categories without a file", output)
        self.assertIn("coverage ......... 1/2", output)

    def test_failure_recorded_in_manifest_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [
            {"stage": "catalog", "category_id": 99, "name": "x", "error": "HTTP 500"}
        ]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("recorded as failed in the manifest", output)


class RecursiveOrphanTest(unittest.TestCase):
    """The sweep must descend into subdirectories, otherwise the closed inventory is false."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_file_in_catalog_subdirectory_is_detected(self):
        target_dir = os.path.join(self.partition, "catalog", "backup")
        os.makedirs(target_dir, exist_ok=True)
        write_json(os.path.join(target_dir, "old.json"), {"id": 1})
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("not in the manifest", output)
        self.assertIn("backup", output)

    def test_unknown_directory_at_partition_root_is_detected(self):
        target_dir = os.path.join(self.partition, "_scratch")
        os.makedirs(target_dir, exist_ok=True)
        with open(os.path.join(target_dir, "notes.txt"), "w", encoding="utf-8") as handle:
            handle.write("note")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("_scratch", output)

    def test_empty_subdirectory_is_not_orphan(self):
        os.makedirs(os.path.join(self.partition, "catalog", "empty"), exist_ok=True)
        self.assertEqual(run_validate(self.partition)[0], 0)


class AnomaliesTest(unittest.TestCase):
    def test_recorded_anomaly_becomes_warning_and_does_not_fail(self):
        root = tempfile.mkdtemp()
        partition = build_partition(root, limit=1)
        # file not in canonical form: the extraction re-downloads it and records the anomaly
        target = part.catalog_path(partition, 11)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as handle:
            json.dump({"id": 11, "name": "x", "categories": []}, handle, indent=8)
        build_partition(root)
        manifest = part.read_manifest(partition)
        self.assertTrue(manifest["anomalies"])
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("WARNING: recorded anomaly", output)
        self.assertIn("anomalies ........ 1", output)


class ShapeChangeDetectionTest(unittest.TestCase):
    def test_partition_without_any_product_fails(self):
        responses = {
            "/categories/": TREE,
            "/categories/10/": {"id": 10, "name": "Alfa", "sections": []},
            "/categories/11/": {"id": 11, "name": "Beta", "sections": []},
        }
        partition = build_partition(tempfile.mkdtemp(), responses=responses)
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("no product read", output)
        self.assertIn("n/a (no rows read)", output)


class QualityTest(unittest.TestCase):
    def setUp(self):
        responses = dict(RESPONSES)
        no_price = catalog(11, "Beta", ("g1", [(3, "p3", "3.00")]))
        no_price["categories"][0]["products"][0]["price_instructions"]["unit_price"] = None
        responses["/categories/11/"] = no_price
        self.partition = build_partition(tempfile.mkdtemp(), responses=responses)

    def test_quality_is_reported_but_does_not_fail_without_strict(self):
        code, output = run_validate(self.partition)
        self.assertEqual(code, 0)
        self.assertIn("missing price .... 1", output)

    def test_strict_fails_incomplete_record(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("--strict", output)


class PartialPartitionTest(unittest.TestCase):
    def test_partial_partition_warns_but_does_not_fail_on_coverage(self):
        partition = build_partition(tempfile.mkdtemp(), limit=1)
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("WARNING", output)
        self.assertIn("partial partition", output)


if __name__ == "__main__":
    unittest.main()
