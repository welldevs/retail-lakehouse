"""End-to-end intake, no network: every closed gap has a test that fails without the fix."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_callejero_source import intake, partition as part
from ine_callejero_source.canonical import read_bytes
from tests.support import build_input_dir, extract_args, write_callejero_file


def run_intake(out, in_dir, **overrides):
    args = extract_args(out, in_dir, **overrides)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = intake.run(args)
    return code, buffer.getvalue()


class IntakeHappyPathTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_complete_intake(self):
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 0)

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(manifest["source"]["name"], "ine_callejero")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01"})
        self.assertEqual(manifest["totals"]["files_landed"], 5)  # SECC+UP+VIAS+PSEU+TRAM
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])

    def test_success_marker_created_when_complete(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_original_file_name_is_preserved(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        landed = os.path.join(
            self.partition, "provinces", "province=28", "VIAS.P28.D260630.G260702"
        )
        self.assertTrue(os.path.exists(landed))

    def test_landed_bytes_are_identical_to_input_file(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        original = os.path.join(
            self.in_dir, "call_p28_726", "call_p28_072026", "SECC.P28.D260630.G260702"
        )
        landed = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        self.assertEqual(read_bytes(original), read_bytes(landed))

    def test_tram_is_landed_like_the_others(self):
        in_dir = build_input_dir(self.root, provinces=("41",))
        code, _ = run_intake(self.root, in_dir, provinces=["41"])
        self.assertEqual(code, 0)
        province_dir = os.path.join(self.partition, "provinces", "province=41")
        landed_names = os.listdir(province_dir)
        self.assertEqual(len(landed_names), 5)
        self.assertTrue(any(name.startswith("TRAM") for name in landed_names))

    def test_multiple_provinces(self):
        in_dir = build_input_dir(self.root, provinces=("08", "28", "41", "46"))
        code, _ = run_intake(self.root, in_dir, provinces=["08", "28", "41", "46"])
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(manifest["totals"]["files_landed"], 20)  # 4 provinces x 5 datasets


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_empty_province_list_is_rejected(self):
        self.assertEqual(run_intake(self.root, self.in_dir, provinces=[])[0], 2)

    def test_malformed_province_is_rejected(self):
        self.assertEqual(run_intake(self.root, self.in_dir, provinces=["280"])[0], 2)

    def test_invalid_date_is_rejected(self):
        self.assertEqual(run_intake(self.root, self.in_dir, date="25-08-2026")[0], 2)

    def test_missing_in_dir_is_rejected(self):
        code, output = run_intake(self.root, os.path.join(self.root, "does-not-exist"))
        self.assertEqual(code, 2)
        self.assertIn("--in", output)


class MissingFileTest(unittest.TestCase):
    """Partial coverage: a province without all datasets in --in is a partial failure."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = os.path.join(self.root, "in")
        # only 4 of the 5 datasets: PSEU is missing
        for dataset in ("SECC", "UP", "VIAS", "TRAM"):
            write_callejero_file(self.in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")

    def test_partial_failure_returns_1_and_does_not_mark_success(self):
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertEqual(manifest["failures"][0]["dataset"], "PSEU")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_rerun_resumes_after_missing_file_appears(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        write_callejero_file(self.in_dir, "PSEU", "28", subdir="call_p28_726/call_p28_072026")
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["files_landed"], 5)

    def test_history_keeps_the_previous_run(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        write_callejero_file(self.in_dir, "PSEU", "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, self.in_dir, provinces=["28"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["history"]), 1)
        self.assertFalse(manifest["history"][0]["complete"])


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_rerun_of_incomplete_partition_reuses_what_already_landed(self):
        # only 3 of the 5 datasets in --in: the first run stays incomplete (resumable).
        from tests.support import write_callejero_file

        partial_in = os.path.join(self.root, "in-partial")
        for dataset in ("SECC", "UP", "VIAS"):
            write_callejero_file(partial_in, dataset, "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, partial_in, provinces=["28"])

        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])  # in_dir has all 5
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        by_dataset = {f["dataset"]: f["reused"] for f in manifest["files"]}
        self.assertTrue(by_dataset["SECC"])  # landed before, --in unchanged: reused
        self.assertFalse(by_dataset["PSEU"])  # only appeared in --in in this run

    def test_complete_partition_is_immutable_without_overwrite(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        code, output = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 2)
        self.assertIn("immutable", output)

    def test_overwrite_rewrites_complete_partition(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"], overwrite=True)
        self.assertEqual(code, 0)


class TamperedLandedFileTest(unittest.TestCase):
    """An already landed file that diverges from the current --in requires an explicit decision.

    Only testable on an INCOMPLETE partition: a complete partition is already blocked by
    assert_writable before any content comparison (immutability), so the divergence only
    matters when there is something to resume.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = os.path.join(self.root, "in")
        from tests.support import write_callejero_file

        # only 3 of the 5 datasets: partition stays incomplete, resumable without --overwrite.
        for dataset in ("SECC", "UP", "VIAS"):
            write_callejero_file(self.in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, self.in_dir, provinces=["28"])
        self.landed = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )

    def test_landed_diverging_from_current_in_fails_without_overwrite(self):
        with open(self.landed, "ab") as handle:
            handle.write(b"tampered")
        code, output = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 1)
        self.assertIn("diverge", output)

    def test_overwrite_resolves_divergence_in_favor_of_in(self):
        with open(self.landed, "ab") as handle:
            handle.write(b"tampered")
        run_intake(self.root, self.in_dir, provinces=["28"], overwrite=True)
        original = os.path.join(
            self.in_dir, "call_p28_726", "call_p28_072026", "SECC.P28.D260630.G260702"
        )
        self.assertEqual(read_bytes(self.landed), read_bytes(original))


if __name__ == "__main__":
    unittest.main()
