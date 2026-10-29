"""Partition path, tokens and immutability."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_oltp_source import MANIFEST_VERSION
from simulated_oltp_source.partition import (
    PartitionError,
    assert_writable,
    build_history,
    mark_success,
    manifest_path,
    partition_path,
    read_manifest,
    success_path,
    validate_date,
    validate_token,
    write_manifest,
)


class PathTest(unittest.TestCase):
    def test_partition_uses_the_wh_axis(self):
        """The axis must be `wh=`: it is the only one the platform's manifest.py understands."""
        self.assertEqual(
            partition_path("root", "2026-08-27", "mad1"),
            os.path.join("root", "ingestion_date=2026-08-27", "wh=mad1"),
        )

    def test_token_with_path_separator_is_rejected(self):
        for malicious in ("../etc", "a/b", "..", "", "-starts-with-hyphen"):
            with self.subTest(token=malicious):
                with self.assertRaises(PartitionError):
                    validate_token("wh", malicious)

    def test_date_outside_iso_format_is_rejected(self):
        for malicious in ("2026-8-27", "27-08-2026", "today", ""):
            with self.subTest(date=malicious):
                with self.assertRaises(PartitionError):
                    validate_date(malicious)

    def test_valid_iso_date_passes(self):
        self.assertEqual(validate_date("2026-08-27"), "2026-08-27")


class ImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.partition = partition_path(self.tmp.name, "2026-08-27", "mad1")
        os.makedirs(self.partition, exist_ok=True)

    def complete(self, seed=7):
        write_manifest(
            self.partition,
            {"run_id": "r1", "complete": True, "files": [], "config": {"seed": seed}},
        )
        mark_success(self.partition, True, "r1")

    def test_new_partition_is_writable(self):
        self.assertIsNone(assert_writable(self.partition, overwrite=False))

    def test_complete_partition_is_immutable(self):
        self.complete()
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_overwrite_allows_rewriting(self):
        self.complete()
        previous = assert_writable(self.partition, overwrite=True)
        self.assertEqual(previous["run_id"], "r1")

    def test_deleting_the_manifest_does_not_bypass_immutability(self):
        """_SUCCESS alone still witnesses a complete partition."""
        self.complete()
        os.unlink(manifest_path(self.partition))
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_unreadable_manifest_fails_without_overwrite(self):
        with open(manifest_path(self.partition), "w") as handle:
            handle.write("{ not json")
        with self.assertRaises(PartitionError):
            assert_writable(self.partition, overwrite=False)

    def test_success_is_removed_when_partition_is_not_complete(self):
        self.complete()
        mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(success_path(self.partition)))

    def test_write_manifest_stamps_the_version(self):
        write_manifest(self.partition, {"files": []})
        self.assertEqual(read_manifest(self.partition)["manifest_version"], MANIFEST_VERSION)


class HistoryTest(unittest.TestCase):
    def test_empty_history_for_new_partition(self):
        self.assertEqual(build_history(None), [])

    def test_history_keeps_the_previous_run_seed(self):
        """Without the seed in the history, an --overwrite would swap the people behind the
        same customer_id without leaving a trace."""
        previous = {
            "run_id": "r1",
            "started_at_utc": "2026-08-27T00:00:00Z",
            "complete": True,
            "config": {"seed": 42, "count": 200},
            "totals": {"customer_rows": 200},
        }
        history = build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["seed"], 42)
        self.assertEqual(history[0]["customer_rows"], 200)

    def test_history_accumulates(self):
        previous = {"run_id": "r2", "history": [{"run_id": "r1"}], "config": {}, "totals": {}}
        self.assertEqual(len(build_history(previous)), 2)


if __name__ == "__main__":
    unittest.main()
