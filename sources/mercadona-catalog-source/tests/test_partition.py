"""Partition identity and rules: tokens, immutability, fixed language and _SUCCESS."""

from __future__ import annotations

import os
import tempfile
import unittest

from mercadona_catalog_source import partition as part


class TokenValidationTest(unittest.TestCase):
    def test_rejects_path_traversal(self):
        for value in ("..", "../evil", "a/b", "a\\b", "/abs", "."):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_token("wh", value)

    def test_rejects_empty_and_wrong_types(self):
        for value in ("", None, 12, "-starts-with-hyphen"):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_token("wh", value)

    def test_accepts_real_warehouse_codes(self):
        for value in ("mad1", "mad2", "bcn1", "vlc1", "svq1", "alc1", "zgz1"):
            self.assertEqual(part.validate_token("wh", value), value)

    def test_date_must_be_iso(self):
        self.assertEqual(part.validate_date("2026-08-15"), "2026-08-15")
        for value in ("2026-8-15", "15/08/2026", "today", "", None):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_date(value)

    def test_malicious_category_id_does_not_escape_the_partition(self):
        with self.assertRaises(part.PartitionError):
            part.catalog_path("/tmp/p", "../../etc/passwd")


class PartitionRulesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = part.partition_path(self.root, "2026-01-01", "mad1")
        os.makedirs(self.partition, exist_ok=True)

    def _write(self, complete: bool, lang: str = "es"):
        part.write_manifest(
            self.partition,
            {
                "run_id": "r1",
                "complete": complete,
                "source": {"lang": lang},
                "totals": {"http_requests": 9, "product_rows": 5},
                "files": [],
            },
        )

    def test_nonexistent_partition_is_writable(self):
        self.assertIsNone(part.assert_writable(self.partition, "es", False))

    def test_incomplete_partition_can_be_resumed(self):
        self._write(complete=False)
        self.assertIsNotNone(part.assert_writable(self.partition, "es", False))

    def test_complete_partition_is_immutable_without_overwrite(self):
        self._write(complete=True)
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, "es", False)

    def test_complete_partition_can_be_rewritten_with_overwrite(self):
        self._write(complete=True)
        self.assertIsNotNone(part.assert_writable(self.partition, "es", True))

    def test_mismatched_language_is_refused(self):
        self._write(complete=False, lang="es")
        with self.assertRaises(part.PartitionError) as ctx:
            part.assert_writable(self.partition, "en", False)
        self.assertIn("lang", str(ctx.exception))

    def test_history_accumulates_runs(self):
        self._write(complete=False)
        previous = part.read_manifest(self.partition)
        history = part.build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["run_id"], "r1")
        self.assertEqual(history[0]["http_requests"], 9)

        previous["history"] = history
        previous["run_id"] = "r2"
        part.write_manifest(self.partition, previous)
        self.assertEqual(len(part.build_history(part.read_manifest(self.partition))), 2)

    def test_success_marker_follows_the_partition_state(self):
        part.mark_success(self.partition, True, "r1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


if __name__ == "__main__":
    unittest.main()
