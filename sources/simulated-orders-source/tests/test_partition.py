"""Partition path, immutability and history."""

from __future__ import annotations

import os
import tempfile
import unittest

from simulated_orders_source import partition as part


class PathTest(unittest.TestCase):
    def test_builds_the_hive_layout(self):
        self.assertEqual(
            part.partition_path("data/orders", "2026-08-24", "mad1"),
            os.path.join("data/orders", "ingestion_date=2026-08-24", "wh=mad1"),
        )

    def test_refuses_path_traversal(self):
        for bad in ("../etc", "mad1/../..", "wh=mad1/x"):
            with self.assertRaises(part.PartitionError):
                part.partition_path("data/orders", "2026-08-24", bad)

    def test_refuses_malformed_date(self):
        for bad in ("24-08-2026", "2026-8-4", "today", ""):
            with self.assertRaises(part.PartitionError):
                part.partition_path("data/orders", bad, "mad1")

    def test_data_file_is_the_log(self):
        self.assertEqual(part.EVENTS_FILE, "order_events.jsonl")
        self.assertTrue(part.events_path("p").endswith("order_events.jsonl"))


class ImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.partition = os.path.join(self.tmp.name, "ingestion_date=2026-08-24", "wh=mad1")
        os.makedirs(self.partition)

    def _complete(self):
        part.write_manifest(self.partition, {"complete": True, "files": [], "config": {}})
        part.mark_success(self.partition, True, "run-1")

    def test_new_partition_is_writable(self):
        self.assertIsNone(part.assert_writable(self.partition, overwrite=False))

    def test_complete_partition_refuses_rewrite(self):
        self._complete()
        with self.assertRaises(part.PartitionError) as caught:
            part.assert_writable(self.partition, overwrite=False)
        self.assertIn("immutable", str(caught.exception))

    def test_explicit_overwrite_allows(self):
        self._complete()
        self.assertIsNotNone(part.assert_writable(self.partition, overwrite=True))

    def test_deleting_the_manifest_does_not_bypass_immutability(self):
        # _SUCCESS alone still attests a complete partition.
        self._complete()
        os.unlink(part.manifest_path(self.partition))
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, overwrite=False)

    def test_success_only_exists_when_complete(self):
        part.mark_success(self.partition, True, "run-1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "run-2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


class HistoryTest(unittest.TestCase):
    def test_without_previous_run_history_is_empty(self):
        self.assertEqual(part.build_history(None), [])

    def test_keeps_seed_and_premises_digest(self):
        # The two inputs that swap the orders behind the same order_id. Without both recorded,
        # an --overwrite would leave no trace.
        previous = {
            "run_id": "run-1",
            "complete": True,
            "config": {"seed": 7, "orders": 3, "premises_sha256": "abc"},
            "totals": {"event_rows": 20, "order_rows": 3},
        }
        history = part.build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["seed"], 7)
        self.assertEqual(history[0]["premises_sha256"], "abc")
        self.assertEqual(history[0]["event_rows"], 20)

    def test_accumulates_runs(self):
        previous = {"config": {}, "totals": {}, "history": [{"run_id": "run-0"}]}
        self.assertEqual(len(part.build_history(previous)), 2)


if __name__ == "__main__":
    unittest.main()
