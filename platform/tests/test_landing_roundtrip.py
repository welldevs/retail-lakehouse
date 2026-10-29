"""The guarantees of land and verify, exercised without network.

These checks had already been done by hand against MinIO, and passed. The problem is that
a manual check is not re-runnable: nobody knows whether it still holds after the next
change. Here they become tests.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest

from retail_platform import SOURCE_NAME
from retail_platform.land import LandingError, land
from retail_platform.verify import verify

from .fake_s3 import FakeConfig, FakeS3Client
from .support import build_partition, build_single_axis_partition


class LandingCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_partition(self.root, category_ids=(112, 113))
        self.prefix = f"{SOURCE_NAME}/ingestion_date=2026-08-24/wh=mad1"

    def _catalog_key(self, category_id: int = 112) -> str:
        return f"{self.prefix}/catalog/category_id={category_id}.json"


class TestLand(LandingCase):
    def test_lands_declared_files_plus_the_three_undeclared(self):
        result = land(self.config, self.partition)
        # 3 declared (1 tree + 2 catalogs) + _manifest.json + _run.log + _SUCCESS
        self.assertEqual(result.uploaded, 6)
        self.assertEqual(result.skipped, 0)
        self.assertEqual(
            self.client.keys(self.config.raw_bucket),
            {
                f"{self.prefix}/categories/categories.json",
                self._catalog_key(112),
                self._catalog_key(113),
                f"{self.prefix}/_manifest.json",
                f"{self.prefix}/_run.log",
                f"{self.prefix}/_SUCCESS",
            },
        )

    def test_success_is_the_last_object_written(self):
        """An interrupted upload must never look complete."""
        land(self.config, self.partition)
        keys = list(self.client.store)
        self.assertEqual(keys[-1][1], f"{self.prefix}/_SUCCESS")

    def test_is_idempotent(self):
        land(self.config, self.partition)
        puts_after_first = self.client.puts
        result = land(self.config, self.partition)
        self.assertEqual(result.uploaded, 0)
        self.assertEqual(result.skipped, 6)
        self.assertEqual(result.bytes_uploaded, 0)
        self.assertEqual(self.client.puts, puts_after_first, "re-sent an object that was already correct")

    def test_reuploads_only_what_diverged_at_the_destination(self):
        """Self-healing: an object tampered at the destination is sent again, the others are not."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, self._catalog_key(112))
        result = land(self.config, self.partition)
        self.assertEqual(result.uploaded, 1)
        self.assertEqual(result.skipped, 5)

    def test_refuses_to_upload_a_locally_corrupted_partition(self):
        """DO NOT PROPAGATE CORRUPTION: the local sha256 is rechecked before the PUT, without
        assuming `validate` ran."""
        target = os.path.join(self.partition, "catalog", "category_id=112.json")
        with open(target, "ab") as handle:
            handle.write(b" ")
        with self.assertRaisesRegex(LandingError, "BEFORE upload"):
            land(self.config, self.partition)

    def test_aborts_before_writing_success(self):
        """A tampered partition must not leave _SUCCESS behind."""
        with open(os.path.join(self.partition, "catalog", "category_id=113.json"), "ab") as h:
            h.write(b" ")
        with self.assertRaises(LandingError):
            land(self.config, self.partition)
        self.assertNotIn(
            (self.config.raw_bucket, f"{self.prefix}/_SUCCESS"), self.client.store
        )

    def test_refuses_a_partition_whose_declared_file_is_missing(self):
        os.unlink(os.path.join(self.partition, "catalog", "category_id=112.json"))
        with self.assertRaisesRegex(LandingError, "declared file missing"):
            land(self.config, self.partition)

    def test_declared_checksum_is_accepted_by_the_server(self):
        """If the hex->base64 conversion were wrong, the double would reject every PUT — the
        same rejection the real server would make."""
        land(self.config, self.partition)
        blob = self.client.store[(self.config.raw_bucket, self._catalog_key(112))]
        with open(os.path.join(self.partition, "catalog", "category_id=112.json"), "rb") as h:
            self.assertEqual(hashlib.sha256(blob).hexdigest(),
                             hashlib.sha256(h.read()).hexdigest())


class TestVerify(LandingCase):
    def test_passes_on_a_partition_just_landed(self):
        land(self.config, self.partition)
        errors, summary = verify(self.config, self.partition)
        self.assertEqual(errors, [])
        self.assertEqual(summary["declared"], 3)
        self.assertEqual(summary["checked"], 6)
        self.assertEqual(summary["orphans"], 0)

    def test_catches_a_tampered_object(self):
        """The check that matters most: the destination diverging from the manifest."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, self._catalog_key(112))
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("sha256 mismatch" in e for e in errors), errors)

    def test_catches_a_missing_object(self):
        land(self.config, self.partition)
        self.client.drop(self.config.raw_bucket, self._catalog_key(113))
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("missing or unreadable" in e for e in errors), errors)

    def test_catches_an_orphan_object(self):
        """Closed inventory: an object in the prefix the manifest does not declare is an error."""
        land(self.config, self.partition)
        self.client.store[(self.config.raw_bucket, f"{self.prefix}/catalog/extra.json")] = b"{}"
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("outside the manifest" in e for e in errors), errors)

    def test_catches_a_manifest_that_differs_from_the_local_one(self):
        """The landed manifest must be byte-identical to the local one: without that, a
        tampered inventory at the destination would get to dictate what is 'correct' there."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, f"{self.prefix}/_manifest.json")
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("differs from the local file" in e for e in errors), errors)

    def test_catches_a_missing_success_marker(self):
        land(self.config, self.partition)
        self.client.drop(self.config.raw_bucket, f"{self.prefix}/_SUCCESS")
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("_SUCCESS" in e for e in errors), errors)

    def test_verify_rereads_instead_of_trusting_head(self):
        """verify downloads the object and recomputes: a lying HEAD would not fool it."""
        land(self.config, self.partition)
        gets_before = self.client.gets
        verify(self.config, self.partition)
        self.assertGreaterEqual(self.client.gets - gets_before, 6)


class TestLandSecondSourceWithoutAxis(unittest.TestCase):
    """Generic landing with a second source (INE), with no axis besides ingestion_date,
    landing alongside Mercadona in the same bucket without any source-specific code."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_single_axis_partition(self.root, ingestion_date="2026-09-01")
        self.prefix = "ine_population_api/ingestion_date=2026-09-01"

    def test_lands_under_its_own_prefix_alongside_mercadona(self):
        result = land(self.config, self.partition)
        # 1 declared (tables/table_id=31304.json) + _manifest.json + _run.log + _SUCCESS
        self.assertEqual(result.uploaded, 4)
        self.assertEqual(
            self.client.keys(self.config.raw_bucket),
            {
                f"{self.prefix}/tables/table_id=31304.json",
                f"{self.prefix}/_manifest.json",
                f"{self.prefix}/_run.log",
                f"{self.prefix}/_SUCCESS",
            },
        )

    def test_verify_passes_on_a_partition_just_landed(self):
        land(self.config, self.partition)
        errors, summary = verify(self.config, self.partition)
        self.assertEqual(errors, [])
        self.assertEqual(summary["declared"], 1)
        self.assertEqual(summary["checked"], 4)
        self.assertEqual(summary["orphans"], 0)


if __name__ == "__main__":
    unittest.main()
