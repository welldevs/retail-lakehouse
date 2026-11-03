"""Off-machine copies (CR-020): a round trip that comes back identical, and every refusal.

Offline, with the fake S3 and a real partition built on disk: land it, archive the bucket,
restore into an EMPTY bucket, and the restored capture must carry the same capture_id as the
original — the property that lets `freeze-check` say a restored machine holds the same data.
"""

from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
import unittest

from retail_platform import backup, freeze
from retail_platform.land import land

from .fake_s3 import FakeConfig, FakeS3Client
from .support import build_partition


class RoundTripCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp = self._tmp.name
        self.source = FakeConfig(FakeS3Client())
        for wh in ("mad1", "bcn1"):
            land(self.source, build_partition(os.path.join(self.tmp, "data"), warehouse=wh))
        self.out = backup.archive_raw(self.source, os.path.join(self.tmp, "exports"))


class ArchiveTest(RoundTripCase):
    def test_archive_holds_every_object_and_the_capture_id_of_the_bucket(self):
        self.assertEqual(self.out["partitions"], 2)
        self.assertEqual(self.out["objects"], len(self.source.client().keys("retail-raw")))
        self.assertEqual(self.out["capture_id"],
                         freeze.capture_id(freeze.collect(self.source)))

    def test_sidecar_states_the_file_sha256(self):
        with open(self.out["sidecar"], encoding="utf-8") as handle:
            sidecar = json.load(handle)
        self.assertEqual(sidecar["sha256"], backup._sha256_file(self.out["path"]))

    def test_incomplete_partition_is_not_archived(self):
        """An archive would restore it as if it were complete."""
        prefix = "mercadona_catalog_api/ingestion_date=2026-08-24/wh=mad1"
        self.source.client().drop("retail-raw", f"{prefix}/_SUCCESS")
        with self.assertRaisesRegex(backup.BackupError, "without _SUCCESS"):
            backup.archive_raw(self.source, os.path.join(self.tmp, "again"))


class RestoreTest(RoundTripCase):
    def setUp(self):
        super().setUp()
        self.target = FakeConfig(FakeS3Client())

    def test_restore_into_an_empty_bucket_gives_back_the_same_capture(self):
        result = backup.restore_raw(self.target, self.out["path"],
                                    os.path.join(self.tmp, "work"))
        self.assertEqual(len(result["landed"]), 2)
        self.assertEqual(self.target.client().keys("retail-raw"),
                         self.source.client().keys("retail-raw"))
        self.assertEqual(freeze.capture_id(freeze.collect(self.target)), self.out["capture_id"])

    def test_restore_over_identical_partitions_writes_nothing(self):
        backup.restore_raw(self.target, self.out["path"], os.path.join(self.tmp, "w1"))
        puts = self.target.client().puts
        result = backup.restore_raw(self.target, self.out["path"], os.path.join(self.tmp, "w2"))
        self.assertEqual((len(result["landed"]), len(result["already"])), (0, 2))
        self.assertEqual(self.target.client().puts, puts)

    def test_restore_never_overwrites_a_partition_that_differs(self):
        """RAW is immutable: a backup must not be the way it stops being."""
        backup.restore_raw(self.target, self.out["path"], os.path.join(self.tmp, "w1"))
        key = "mercadona_catalog_api/ingestion_date=2026-08-24/wh=mad1/categories/categories.json"
        self.target.client().corrupt("retail-raw", key)
        tampered = self.target.client().store[("retail-raw", key)]
        result = backup.restore_raw(self.target, self.out["path"], os.path.join(self.tmp, "w2"))
        self.assertEqual(len(result["refused"]), 1)
        self.assertEqual(self.target.client().store[("retail-raw", key)], tampered)

    def test_damaged_copy_is_refused_before_anything_is_written(self):
        with open(self.out["path"], "ab") as handle:
            handle.write(b"\0")
        with self.assertRaisesRegex(backup.BackupError, "damaged"):
            backup.restore_raw(self.target, self.out["path"], os.path.join(self.tmp, "w"))
        self.assertEqual(self.target.client().puts, 0)

    def test_member_escaping_the_restore_directory_is_refused(self):
        evil = os.path.join(self.tmp, "evil.tar.gz")
        with tarfile.open(evil, "w:gz") as tar:
            info = tarfile.TarInfo("../outside.json")
            info.size = 2
            tar.addfile(info, io.BytesIO(b"{}"))
        with self.assertRaisesRegex(backup.BackupError, "refused archive member"):
            backup.restore_raw(self.target, evil, os.path.join(self.tmp, "w"))
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "outside.json")))


class PriceExportTest(unittest.TestCase):
    def test_csv_carries_every_row_and_its_manifest(self):
        import duckdb

        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "s.duckdb")
            connection = duckdb.connect(db)
            connection.execute("""
                create table silver_product_price as select * from (values
                  ('112', 'p1', 1.20, date '2026-08-24', 'mad1'),
                  ('112', 'p1', 1.25, date '2026-08-25', 'mad1'),
                  ('113', 'p2', 3.10, date '2026-08-25', 'bcn1'))
                t(category_id, source_product_id, purchasable_unit_price, ingestion_date,
                  warehouse)""")
            connection.close()
            out = backup.export_prices(db, os.path.join(tmp, "exports"))
            self.assertEqual((out["rows"], out["from"], out["to"], out["collections"]),
                             (3, "2026-08-24", "2026-08-25", 2))
            import gzip
            with gzip.open(out["path"], "rt", encoding="utf-8") as handle:
                lines = handle.read().splitlines()
            self.assertEqual(len(lines), 4)  # header + 3
            self.assertTrue(lines[0].startswith("category_id,source_product_id"))
            self.assertEqual(out["sha256"], backup._sha256_file(out["path"]))

    def test_missing_silver_is_an_error_not_an_empty_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(backup.BackupError, "make silver"):
                backup.export_prices(os.path.join(tmp, "none.duckdb"), tmp)


if __name__ == "__main__":
    unittest.main()
