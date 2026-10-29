"""prune-local only deletes the local copy after PROVING it is intact at the destination.

The order is the whole guarantee: the same check as verify-landing runs before any
removal. There is deliberately no --force mode — a partition that is not intact in the
object storage is not redundant, and deleting exactly that one would be the only way to
lose data.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from retail_platform.cli import EXIT_FAILED, EXIT_FATAL, EXIT_OK, main
from retail_platform.land import land

from .fake_s3 import FakeConfig, FakeS3Client
from .support import build_partition


class PruneLocalCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_partition(self.root, category_ids=(112, 113))
        patcher = mock.patch("retail_platform.cli.from_env", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def prune(self) -> int:
        return main(["prune-local", self.partition])


class TestPruneAfterLanding(PruneLocalCase):
    def setUp(self):
        super().setUp()
        land(self.config, self.partition)

    def test_deletes_the_local_copy_when_the_destination_checks_out(self):
        self.assertTrue(os.path.isdir(self.partition))
        self.assertEqual(self.prune(), EXIT_OK)
        self.assertFalse(os.path.exists(self.partition))

    def test_the_object_storage_stays_intact(self):
        before = dict(self.client.store)
        self.prune()
        self.assertEqual(self.client.store, before)

    def test_does_not_touch_neighbouring_partitions(self):
        other = build_partition(self.root, warehouse="bcn1")
        self.prune()
        self.assertTrue(os.path.isdir(other))


class TestPruneRefuses(PruneLocalCase):
    def test_refuses_when_the_partition_was_not_landed(self):
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "deleted without a destination")

    def test_refuses_when_the_object_at_the_destination_was_tampered(self):
        land(self.config, self.partition)
        key = next(k for k in self.client.store if k[1].endswith("category_id=112.json"))
        self.client.store[key] = b"{}"
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "deleted with a corrupted destination")

    def test_refuses_when_an_object_is_missing_at_the_destination(self):
        land(self.config, self.partition)
        key = next(k for k in self.client.store if k[1].endswith("category_id=113.json"))
        del self.client.store[key]
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition))

    def test_refuses_when_the_local_file_diverges_from_its_own_manifest(self):
        """verify-landing alone does NOT catch this case: it compares the object with the
        manifest, and the object is still correct. Without the local check, the removal
        would look safe without anyone having looked at the bytes being deleted."""
        land(self.config, self.partition)
        target = os.path.join(self.partition, "catalog", "category_id=112.json")
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(" ")
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "deleted a corrupted local copy")

    def test_refuses_when_a_local_file_is_gone(self):
        land(self.config, self.partition)
        os.unlink(os.path.join(self.partition, "catalog", "category_id=113.json"))
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition))

    def test_refuses_a_path_that_is_not_a_partition_without_deleting_anything(self):
        """Guards against a wrong argument turning into rm -rf on something that is not a partition."""
        target = os.path.join(self.root, "not-a-partition")
        os.makedirs(os.path.join(target, "stuff"), exist_ok=True)
        with open(os.path.join(target, "stuff", "file.txt"), "w") as handle:
            handle.write("do not delete me")
        self.assertEqual(main(["prune-local", target]), EXIT_FATAL)
        self.assertTrue(os.path.isfile(os.path.join(target, "stuff", "file.txt")))

    def test_refuses_a_nonexistent_directory(self):
        self.assertEqual(
            main(["prune-local", os.path.join(self.root, "does-not-exist")]), EXIT_FATAL
        )


if __name__ == "__main__":
    unittest.main()
