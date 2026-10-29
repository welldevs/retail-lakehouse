"""Validation: every snapshot failure mode must be detected, never silenced."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_population_source import extract, partition as part, validate
from ine_population_source.canonical import read_json, write_json
from tests.support import extract_args, make_fetcher_factory, series, table_payload, validate_args

TABLE_31304 = table_payload(series("A", "Total. Madrid. Ambos sexos.", (2025, 6779888)))
TABLE_9689 = table_payload(series("B", "Total. Barcelona. Ambos sexos.", (2025, 5716544)))
RESPONSES = {"/ES/DATOS_TABLA/31304": TABLE_31304, "/ES/DATOS_TABLA/9689": TABLE_9689}


def build_partition(root, responses=None, errors=None, **overrides):
    factory, _ = make_fetcher_factory(dict(responses or RESPONSES), errors)
    overrides.setdefault("tables", ["31304", "9689"])
    with contextlib.redirect_stdout(io.StringIO()):
        extract.run(extract_args(root, **overrides), fetcher_factory=factory)
    return os.path.join(root, "ingestion_date=2026-01-01")


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

    def test_reports_series_and_data_points(self):
        _, output = run_validate(self.partition)
        self.assertIn("series ........... 2", output)
        self.assertIn("data points ...... 2", output)


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
        os.unlink(part.table_path(self.partition, 31304))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("missing file", output)

    def test_checksum_mismatch(self):
        payload, _ = read_json(part.table_path(self.partition, 31304))
        payload[0]["Nombre"] = "tampered"
        write_json(part.table_path(self.partition, 31304), payload)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum mismatch", output)

    def test_corrupt_file_is_reported_instead_of_crashing_the_validator(self):
        with open(part.table_path(self.partition, 31304), "wb") as handle:
            handle.write(b'[{"COD": "A", "Nomb')
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("invalid JSON", output)
        self.assertIn("FAILED", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_undeclared_table_file_is_detected(self):
        write_json(part.table_path(self.partition, 99999), [{"COD": "x", "Data": []}])
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

    def test_undue_success_on_incomplete_partition(self):
        partial = build_partition(
            tempfile.mkdtemp(), errors={"/ES/DATOS_TABLA/9689": "HTTP 500"}
        )
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

    def test_tampered_series_count(self):
        self._tamper_manifest(lambda m: m["totals"].update(series_count=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.series_count", output)

    def test_tampered_bytes(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_tampered_table_files(self):
        self._tamper_manifest(lambda m: m["totals"].update(table_files=7))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.table_files", output)

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
    """Every per-file declared field is re-checked, not only the aggregate totals."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_entry(self, suffix, field, value):
        manifest = part.read_manifest(self.partition)
        entry = next(f for f in manifest["files"] if f["path"].endswith(suffix))
        entry[field] = value
        write_json(part.manifest_path(self.partition), manifest)

    def test_table_file_records_is_rechecked(self):
        self._tamper_entry("table_id=31304.json", "records", 99)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("count mismatch", output)

    def test_bytes_of_each_file_is_rechecked(self):
        self._tamper_entry("table_id=31304.json", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("size mismatch", output)

    def test_entry_pointing_outside_snapshot_root_is_blocked(self):
        self._tamper_entry("table_id=31304.json", "path", "../../../../etc/passwd")
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

    def test_configured_table_id_without_file_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [
            f for f in manifest["files"] if not f["path"].endswith("table_id=9689.json")
        ]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(part.table_path(self.partition, 9689))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("configured table_id(s) without a file", output)
        self.assertIn("coverage ......... 1/2", output)

    def test_failure_recorded_in_manifest_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [{"stage": "tables", "table_id": "99999", "error": "HTTP 500"}]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("recorded as failed in the manifest", output)


class RecursiveOrphanTest(unittest.TestCase):
    """The scan must descend into subdirectories, otherwise the closed inventory is false."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_file_in_tables_subdirectory_is_detected(self):
        target = os.path.join(self.partition, "tables", "backup")
        os.makedirs(target, exist_ok=True)
        write_json(os.path.join(target, "old.json"), [{"COD": "x"}])
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("not in the manifest", output)
        self.assertIn("backup", output)

    def test_unknown_directory_at_partition_root_is_detected(self):
        target = os.path.join(self.partition, "_scratch")
        os.makedirs(target, exist_ok=True)
        with open(os.path.join(target, "notes.txt"), "w", encoding="utf-8") as handle:
            handle.write("note")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("_scratch", output)

    def test_empty_subdirectory_is_not_an_orphan(self):
        os.makedirs(os.path.join(self.partition, "tables", "empty"), exist_ok=True)
        self.assertEqual(run_validate(self.partition)[0], 0)


class AnomaliesTest(unittest.TestCase):
    def test_recorded_anomaly_becomes_warning_and_does_not_fail(self):
        root = tempfile.mkdtemp()
        # 31304 is extracted; 9689 fails and has no file (incomplete partition)
        partition = build_partition(root, errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})
        # file not in canonical form, written outside the extraction
        target = part.table_path(partition, 9689)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as handle:
            import json

            json.dump(TABLE_9689, handle, indent=8)
        # error-free re-run: an incomplete partition can be resumed without --overwrite
        build_partition(root)
        manifest = part.read_manifest(partition)
        self.assertTrue(manifest["anomalies"])
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("WARNING: recorded anomaly", output)
        self.assertIn("anomalies ........ 1", output)


class QualityTest(unittest.TestCase):
    def setUp(self):
        responses = dict(RESPONSES)
        without_value = table_payload(
            {"COD": "B", "Nombre": "Total. Barcelona.", "Data": [{"Anyo": 2025, "Valor": None}]}
        )
        responses["/ES/DATOS_TABLA/9689"] = without_value
        self.partition = build_partition(tempfile.mkdtemp(), responses=responses)

    def test_quality_is_reported_but_does_not_fail_without_strict(self):
        code, output = run_validate(self.partition)
        self.assertEqual(code, 0)
        self.assertIn("series w/o value . 1", output)

    def test_strict_fails_incomplete_record(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("--strict", output)


if __name__ == "__main__":
    unittest.main()
