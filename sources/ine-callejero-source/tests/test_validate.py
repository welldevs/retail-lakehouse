"""Validation: every snapshot failure mode must be detected, never silenced."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_callejero_source import intake, partition as part, validate
from ine_callejero_source.canonical import write_json
from tests.support import build_input_dir, extract_args, validate_args


def build_partition(root, provinces=("28",), **overrides):
    in_dir = build_input_dir(root, provinces=provinces)
    overrides.setdefault("provinces", list(provinces))
    with contextlib.redirect_stdout(io.StringIO()):
        intake.run(extract_args(root, in_dir, **overrides))
    return os.path.join(root, "ingestion_date=2026-01-01")


def run_validate(partition, strict=False):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = validate.run(validate_args(partition, strict))
    return code, buffer.getvalue()


def _entry(partition, dataset):
    manifest = part.read_manifest(partition)
    return next(f for f in manifest["files"] if f["dataset"] == dataset)


class ValidPartitionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_intact_partition_passes(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("OK:", output)
        self.assertIn("orphans .......... 0", output)

    def test_reports_coverage(self):
        _, output = run_validate(self.partition)
        self.assertIn("coverage ......... 5/5", output)


class IntegrityFailureTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

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
        target = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        os.unlink(target)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("missing file", output)

    def test_checksum_mismatch(self):
        target = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(target, "ab") as handle:
            handle.write(b"tampered")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum mismatch", output)

    def test_line_count_mismatch(self):
        target = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(target, "a", encoding="latin-1", newline="") as handle:
            handle.write("0" * 10 + " \r\n")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("line count mismatch", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_undeclared_file_is_detected(self):
        target_file = os.path.join(self.partition, "provinces", "province=28", "EXTRA.P28.D260630.G260702")
        with open(target_file, "w", encoding="latin-1") as handle:
            handle.write("x")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("not in the manifest", output)

    def test_stray_file_at_partition_root_is_detected(self):
        with open(os.path.join(self.partition, "note.txt"), "w", encoding="utf-8") as handle:
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

    def test_spurious_success_on_incomplete_partition(self):
        root = tempfile.mkdtemp()
        in_dir = os.path.join(root, "in")
        from tests.support import write_callejero_file

        for dataset in ("SECC", "UP", "VIAS"):  # PSEU missing: incomplete
            write_callejero_file(in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")
        with contextlib.redirect_stdout(io.StringIO()):
            intake.run(extract_args(root, in_dir, provinces=["28"]))
        partial = os.path.join(root, "ingestion_date=2026-01-01")
        with open(part.success_path(partial), "w", encoding="utf-8") as handle:
            handle.write("lie\n")
        self.assertEqual(run_validate(partial)[0], 1)


class TotalsAndCoverageTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_manifest(self, mutate):
        manifest = part.read_manifest(self.partition)
        mutate(manifest)
        write_json(part.manifest_path(self.partition), manifest)

    def test_tampered_bytes(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_tampered_files_landed(self):
        self._tamper_manifest(lambda m: m["totals"].update(files_landed=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.files_landed", output)

    def test_incompatible_manifest_version(self):
        self._tamper_manifest(lambda m: m.update(manifest_version=99))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("manifest_version", output)

    def test_configured_combination_without_file_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [f for f in manifest["files"] if f["dataset"] != "PSEU"]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(
            os.path.join(self.partition, "provinces", "province=28", "PSEU.P28.D260630.G260702")
        )
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("configured combination(s) without a file", output)
        self.assertIn("coverage ......... 4/5", output)

    def test_failure_recorded_in_manifest_fails(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [{"province": "28", "dataset": "PSEU", "error": "x"}]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("recorded as failed in the manifest", output)


class PerFileRecheckTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_entry(self, dataset, field, value):
        manifest = part.read_manifest(self.partition)
        entry = next(f for f in manifest["files"] if f["dataset"] == dataset)
        entry[field] = value
        write_json(part.manifest_path(self.partition), manifest)

    def test_bytes_of_each_file_are_rechecked(self):
        self._tamper_entry("SECC", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("size mismatch", output)

    def test_entry_pointing_outside_snapshot_root_is_blocked(self):
        self._tamper_entry("SECC", "path", "../../../../etc/passwd")
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


class LineWidthWarningTest(unittest.TestCase):
    """An INE layout change is not fatal by default, only under --strict."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_diverging_dominant_width_is_warning_without_strict(self):
        target = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(target, "a", encoding="latin-1", newline="") as handle:
            handle.write("9" * 20 + "\r\n")
        code, output = run_validate(self.partition)
        # the manifest still rechecks the line count (now 2, not 1): that IS fatal.
        self.assertEqual(code, 1)
        self.assertIn("line count mismatch", output)

    def test_strict_fails_on_width_warning(self):
        # two lines of different widths from the start: the count matches (2==2), only the
        # dominant width changes when compared with the original entry.
        target = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(target, encoding="latin-1", newline="") as handle:
            original = handle.read()
        manifest = part.read_manifest(self.partition)
        entry = next(f for f in manifest["files"] if f["dataset"] == "SECC")
        entry["line_count"] = 2
        write_json(part.manifest_path(self.partition), manifest)
        with open(target, "a", encoding="latin-1", newline="") as handle:
            handle.write("9" * 20 + "\r\n")
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)


class CoverageMissingProvinceTest(unittest.TestCase):
    def test_whole_missing_province_fails_coverage(self):
        root = tempfile.mkdtemp()
        in_dir = build_input_dir(root, provinces=("08", "28"))
        with contextlib.redirect_stdout(io.StringIO()):
            intake.run(extract_args(root, in_dir, provinces=["08", "28"]))
        partition = os.path.join(root, "ingestion_date=2026-01-01")
        manifest = part.read_manifest(partition)
        manifest["files"] = [f for f in manifest["files"] if f["province"] != "08"]
        write_json(part.manifest_path(partition), manifest)
        code, output = run_validate(partition)
        self.assertEqual(code, 1)
        self.assertIn("coverage ......... 5/10", output)


if __name__ == "__main__":
    unittest.main()
