"""Partition identity and rules: tokens, INE file name, immutability."""

from __future__ import annotations

import os
import tempfile
import unittest

from ine_callejero_source import partition as part


class ProvinceTokenTest(unittest.TestCase):
    def test_accepts_real_codes(self):
        for value in ("08", "28", "41", "46"):
            self.assertEqual(part.validate_province(value), value)

    def test_rejects_wrong_format(self):
        for value in ("8", "280", "ab", "", None, "-1"):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_province(value)

    def test_date_must_be_iso(self):
        self.assertEqual(part.validate_date("2026-08-25"), "2026-08-25")
        for value in ("2026-8-25", "25/08/2026", "today", "", None):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_date(value)


class InputFileNameTest(unittest.TestCase):
    def test_recognizes_official_names(self):
        for name, expected in [
            ("SECC.P08.D260630.G260702", {"dataset": "SECC", "province": "08"}),
            ("VIAS.P28.D260630.G260702", {"dataset": "VIAS", "province": "28"}),
            ("UP.P41.D260630.G260702", {"dataset": "UP", "province": "41"}),
            ("PSEU.P46.D260630.G260702", {"dataset": "PSEU", "province": "46"}),
            ("TRAM.P28.D260630.G260702", {"dataset": "TRAM", "province": "28"}),
        ]:
            with self.subTest(name=name):
                match = part.INPUT_FILE.match(name)
                self.assertIsNotNone(match)
                self.assertEqual(match.group("dataset"), expected["dataset"])
                self.assertEqual(match.group("province"), expected["province"])

    def test_rejects_names_outside_the_pattern(self):
        for name in ("SECC.txt", "secc.p28.d260630.g260702", "SECC.P280.D260630.G260702", "../../etc"):
            with self.subTest(name=name):
                self.assertIsNone(part.INPUT_FILE.match(name))

    def test_dataset_path_rejects_name_outside_the_pattern(self):
        with self.assertRaises(part.PartitionError):
            part.dataset_path("/tmp/p", "28", "../../etc/passwd")


class PartitionPathTest(unittest.TestCase):
    def test_partition_has_a_single_level_no_second_axis(self):
        path = part.partition_path("/root", "2026-08-25")
        self.assertEqual(path, os.path.join("/root", "ingestion_date=2026-08-25"))

    def test_dataset_path_preserves_the_original_name(self):
        partition = part.partition_path("/root", "2026-08-25")
        path = part.dataset_path(partition, "28", "VIAS.P28.D260630.G260702")
        self.assertEqual(
            path,
            os.path.join(partition, "provinces", "province=28", "VIAS.P28.D260630.G260702"),
        )


class PartitionRulesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = part.partition_path(self.root, "2026-01-01")
        os.makedirs(self.partition, exist_ok=True)

    def _write(self, complete: bool):
        part.write_manifest(
            self.partition,
            {
                "run_id": "r1",
                "complete": complete,
                "source": {"name": "ine_callejero"},
                "totals": {"files_landed": 4},
                "files": [],
            },
        )

    def test_missing_partition_is_writable(self):
        self.assertIsNone(part.assert_writable(self.partition, False))

    def test_incomplete_partition_can_be_resumed(self):
        self._write(complete=False)
        self.assertIsNotNone(part.assert_writable(self.partition, False))

    def test_complete_partition_is_immutable_without_overwrite(self):
        self._write(complete=True)
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, False)

    def test_complete_partition_can_be_rewritten_with_overwrite(self):
        self._write(complete=True)
        self.assertIsNotNone(part.assert_writable(self.partition, True))

    def test_history_accumulates_runs(self):
        self._write(complete=False)
        previous = part.read_manifest(self.partition)
        history = part.build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["run_id"], "r1")
        self.assertEqual(history[0]["files_landed"], 4)

    def test_success_marker_follows_partition_state(self):
        part.mark_success(self.partition, True, "r1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


if __name__ == "__main__":
    unittest.main()
