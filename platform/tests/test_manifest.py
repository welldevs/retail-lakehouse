"""Manifest reading enforces the consumer obligations, rather than just documenting them.

No network and no boto3: everything here exercises synthetic partitions on disk.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from retail_platform import manifest as manifest_module
from retail_platform.manifest import ManifestError, read

from .support import build_partition, build_single_axis_partition, write_canonical


class TemporaryRoot(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def _patch_manifest(self, partition, mutate):
        path = os.path.join(partition, "_manifest.json")
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        mutate(payload)
        write_canonical(path, payload)


class TestHappyPath(TemporaryRoot):
    def test_reads_a_valid_partition(self):
        partition = build_partition(self.root, category_ids=(112, 113))
        result = read(partition)
        self.assertEqual(result.ingestion_date, "2026-08-24")
        self.assertEqual(result.warehouse, "mad1")
        self.assertTrue(result.complete)
        self.assertEqual(len(result.files), 3)  # 1 tree + 2 catalogs

    def test_resolves_paths_against_the_snapshot_root_not_the_partition(self):
        """Obligation 4.1: files[].path starts at ingestion_date=.../wh=..., not the partition.

        Resolving against the partition would produce .../wh=mad1/ingestion_date=.../wh=mad1/...
        and the file would simply not exist. It is the easiest mistake to make here.
        """
        partition = build_partition(self.root)
        for entry in read(partition).files:
            self.assertTrue(
                os.path.exists(entry.local_path),
                f"path not resolved against the snapshot root: {entry.local_path}",
            )

    def test_digest_on_disk_recomputes_instead_of_trusting(self):
        partition = build_partition(self.root)
        entry = next(e for e in read(partition).files if e.stage == "catalog")
        observed_sha, observed_size = entry.digest_on_disk()
        self.assertEqual(observed_sha, entry.sha256)
        self.assertEqual(observed_size, entry.bytes)

    def test_prefix_suffix_preserves_the_hive_layout(self):
        partition = build_partition(self.root, ingestion_date="2026-08-16", warehouse="bcn1")
        self.assertEqual(read(partition).prefix_suffix, "ingestion_date=2026-08-16/wh=bcn1")

    def test_undeclared_paths_are_the_three_the_contract_predicts(self):
        partition = build_partition(self.root)
        names = {name for _, name in read(partition).undeclared_paths()}
        self.assertEqual(names, {"_manifest.json", "_run.log", "_SUCCESS"})


class TestRefusals(TemporaryRoot):
    def test_missing_manifest(self):
        partition = build_partition(self.root)
        os.unlink(os.path.join(partition, "_manifest.json"))
        with self.assertRaisesRegex(ManifestError, "manifest not found"):
            read(partition)

    def test_unreadable_manifest(self):
        partition = build_partition(self.root)
        with open(os.path.join(partition, "_manifest.json"), "w", encoding="utf-8") as handle:
            handle.write("{ not json")
        with self.assertRaisesRegex(ManifestError, "unreadable"):
            read(partition)

    def test_unsupported_manifest_version(self):
        """CONTRACT.md section 8: refusing is better than interpreting by guesswork."""
        partition = build_partition(self.root, manifest_version=99)
        with self.assertRaisesRegex(ManifestError, "manifest_version"):
            read(partition)

    def test_success_without_complete_is_incoherent(self):
        """Guarantee 10: _SUCCESS exists if and only if complete."""
        partition = build_partition(self.root, complete=False, write_success=True)
        with self.assertRaisesRegex(ManifestError, "contradicts"):
            read(partition)

    def test_complete_without_success_is_incoherent(self):
        partition = build_partition(self.root, complete=True, write_success=False)
        with self.assertRaisesRegex(ManifestError, "contradicts"):
            read(partition)

    def test_incomplete_partition_is_refused(self):
        partition = build_partition(self.root, complete=False, write_success=False)
        with self.assertRaisesRegex(ManifestError, "incomplete"):
            read(partition)

    def test_failures_in_manifest_are_fatal(self):
        """Guarantee 16: a recorded failure fails the partition."""
        partition = build_partition(self.root, failures=[{"category_id": 112}])
        with self.assertRaisesRegex(ManifestError, "failures"):
            read(partition)

    def test_path_escaping_the_snapshot_root_is_refused(self):
        partition = build_partition(self.root)
        self._patch_manifest(
            partition,
            lambda m: m["files"].insert(0, {"path": "../../../etc/passwd", "sha256": "x",
                                            "bytes": 0, "stage": "catalog"}),
        )
        with self.assertRaisesRegex(ManifestError, "outside the snapshot root"):
            read(partition)

    def test_manifest_entry_without_path_is_refused(self):
        partition = build_partition(self.root)
        self._patch_manifest(partition, lambda m: m["files"].insert(0, {"sha256": "x"}))
        with self.assertRaisesRegex(ManifestError, "malformed"):
            read(partition)

    def test_partition_moved_on_disk_is_refused(self):
        """The warehouse only exists in the path and the manifest (obligation 4.3). If the two
        disagree, the partition was moved and accepting that would lose the identity."""
        partition = build_partition(self.root, warehouse="mad1")
        renamed = partition.replace("wh=mad1", "wh=bcn1")
        os.rename(partition, renamed)
        with self.assertRaisesRegex(ManifestError, "does not match the manifest"):
            read(renamed)

    def test_manifest_without_files_list(self):
        partition = build_partition(self.root)
        self._patch_manifest(partition, lambda m: m.pop("files"))
        with self.assertRaisesRegex(ManifestError, "without the 'files' list"):
            read(partition)


class TestSingleAxisPartition(TemporaryRoot):
    """Shape of a source without a second axis, like INE: just ingestion_date=..., with no
    wh=... or any other axis. Two partition shapes are supported today; there is no third
    case to generalize."""

    def test_reads_a_partition_without_a_second_axis(self):
        partition = build_single_axis_partition(self.root)
        result = read(partition)
        self.assertEqual(result.ingestion_date, "2026-08-24")
        self.assertIsNone(result.axis_name)
        self.assertIsNone(result.axis_value)
        self.assertIsNone(result.warehouse)
        self.assertEqual(result.source_name, "ine_population_api")

    def test_prefix_suffix_has_no_second_segment(self):
        partition = build_single_axis_partition(self.root, ingestion_date="2026-09-01")
        self.assertEqual(read(partition).prefix_suffix, "ingestion_date=2026-09-01")

    def test_resolves_paths_against_the_snapshot_root(self):
        """Same obligation 4.1 as Mercadona, with one level fewer: the snapshot root is one
        directory above the partition, not two."""
        partition = build_single_axis_partition(self.root)
        for entry in read(partition).files:
            self.assertTrue(
                os.path.exists(entry.local_path),
                f"path not resolved against the snapshot root: {entry.local_path}",
            )

    def test_partition_moved_on_disk_is_refused(self):
        partition = build_single_axis_partition(self.root, ingestion_date="2026-08-24")
        renamed = partition.replace("ingestion_date=2026-08-24", "ingestion_date=2026-08-25")
        os.rename(partition, renamed)
        with self.assertRaisesRegex(ManifestError, "does not match the manifest"):
            read(renamed)

    def test_unregistered_source_name_is_refused(self):
        partition = build_single_axis_partition(self.root, source_name="something_unknown")
        with self.assertRaisesRegex(ManifestError, "unknown to this platform"):
            read(partition)


class TestContractConstants(unittest.TestCase):
    def test_undeclared_files_matches_what_the_source_leaves_undeclared(self):
        """The Source does not declare these three in files[]; treating one of them as an
        orphan would fail a correct partition."""
        self.assertEqual(
            set(manifest_module.UNDECLARED_FILES),
            {"_manifest.json", "_run.log", "_SUCCESS"},
        )


if __name__ == "__main__":
    unittest.main()
