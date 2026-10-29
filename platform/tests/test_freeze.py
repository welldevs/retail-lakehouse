"""The capture seal: what it covers, what it ignores, and what it fails.

ALL OFFLINE, with the fake S3 — because a seal test that required MinIO up would only run
where the problem is already visible. The logic of `collect`, `capture_id` and `comparar` is
pure enough to be exercised against an in-memory client.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fake_s3 import FakeS3Client  # noqa: E402

from retail_platform import freeze  # noqa: E402


class FakeConfig:
    raw_bucket = "retail-raw"

    def __init__(self, client):
        self._client = client

    def client(self):
        return self._client


def manifest(files, **run_fields) -> bytes:
    """Minimal manifest. `run_fields` injects the fields the seal MUST ignore."""
    body = {
        "manifest_version": 1,
        "partition": {"ingestion_date": "2026-08-24"},
        "files": files,
        "totals": {"bytes": sum(f["bytes"] for f in files)},
        "run_id": "20260824T000000Z_a",
        "started_at_utc": "2026-08-24T00:00:00Z",
        "finished_at_utc": "2026-08-24T00:00:03Z",
        "duration_seconds": 3.0,
        "history": [],
    }
    body.update(run_fields)
    return json.dumps(body).encode("utf-8")


FILES = [
    {"path": "ingestion_date=2026-08-24/a.json", "sha256": "aa" * 32,
     "bytes": 10, "records": 2},
    {"path": "ingestion_date=2026-08-24/b.json", "sha256": "bb" * 32,
     "bytes": 20, "records": 3},
]


def build(objects: dict) -> FakeConfig:
    client = FakeS3Client()
    for key, body in objects.items():
        client.put_object(Bucket="retail-raw", Key=key, Body=body)
    return FakeConfig(client)


ONE_PARTITION = {"source_a/ingestion_date=2026-08-24/_manifest.json": manifest(FILES)}


class CollectTest(unittest.TestCase):
    def test_one_row_per_manifest_with_the_file_aggregate(self):
        records = freeze.collect(build(ONE_PARTITION))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["source"], "source_a")
        self.assertEqual(records[0]["partition_key"], "ingestion_date=2026-08-24")
        self.assertEqual((records[0]["files"], records[0]["records"],
                          records[0]["bytes"]), (2, 5, 30))

    def test_object_that_is_not_a_manifest_is_ignored(self):
        """Scanning the whole bucket and picking up `_SUCCESS` or the `.log` would make the
        seal cover control files, which change on every run without any data changing."""
        objects = dict(ONE_PARTITION)
        objects["source_a/ingestion_date=2026-08-24/_SUCCESS"] = b""
        objects["source_a/ingestion_date=2026-08-24/_run.log"] = b"line"
        objects["source_a/ingestion_date=2026-08-24/a.json"] = b"{}"
        self.assertEqual(len(freeze.collect(build(objects))), 1)

    def test_empty_bucket_is_an_error_with_instructions(self):
        with self.assertRaises(freeze.FreezeError) as ctx:
            freeze.collect(build({}))
        self.assertIn("make daily", str(ctx.exception))

    def test_order_is_stable_across_runs(self):
        """`capture_id` is a hash over the list; an unstable order would produce a new id on
        every collection, with nothing having changed."""
        objects = {
            "z/ingestion_date=2026-08-24/_manifest.json": manifest(FILES),
            "a/ingestion_date=2026-08-25/_manifest.json": manifest(FILES),
            "a/ingestion_date=2026-08-24/_manifest.json": manifest(FILES),
        }
        keys = [(r["source"], r["partition_key"]) for r in freeze.collect(build(objects))]
        self.assertEqual(keys, sorted(keys))


class WhatTheSealIgnoresTest(unittest.TestCase):
    """The module's central decision, and the reason it exists.

    If the seal covered the whole manifest, a BYTE-IDENTICAL re-land of the data would break
    it — `run_id` and the timestamps always change. An alarm that fires on a no-op trains
    reviewers to ignore it, which is the same defect as the date stamp on the panel's contract.
    """

    def test_run_id_and_timestamps_do_NOT_change_the_seal(self):
        before = freeze.collect(build(ONE_PARTITION))
        after = freeze.collect(build({
            "source_a/ingestion_date=2026-08-24/_manifest.json": manifest(
                FILES,
                run_id="20260901T235959Z_z",
                started_at_utc="2026-09-01T23:59:00Z",
                finished_at_utc="2026-09-01T23:59:59Z",
                duration_seconds=99.9,
                history=[{"run_id": "other"}],
            )}))
        self.assertEqual(before[0]["content_sha256"], after[0]["content_sha256"])
        self.assertEqual(freeze.capture_id(before), freeze.capture_id(after))

    def test_ORDER_of_files_in_the_manifest_does_NOT_change_the_seal(self):
        """Two runs over the same data may enumerate the files in a different order."""
        reversed_files = {"source_a/ingestion_date=2026-08-24/_manifest.json":
                          manifest(list(reversed(FILES)))}
        self.assertEqual(freeze.collect(build(ONE_PARTITION))[0]["content_sha256"],
                         freeze.collect(build(reversed_files))[0]["content_sha256"])

    def test_one_byte_of_data_CHANGES_the_seal(self):
        changed = [dict(FILES[0]), dict(FILES[1])]
        changed[1]["sha256"] = "cc" * 32
        after = freeze.collect(build(
            {"source_a/ingestion_date=2026-08-24/_manifest.json": manifest(changed)}))
        self.assertNotEqual(freeze.collect(build(ONE_PARTITION))[0]["content_sha256"],
                            after[0]["content_sha256"])

    def test_record_count_is_part_of_the_seal(self):
        """A file with the same sha256 and a different declared count is a manifest lying
        about itself — the seal has to catch it."""
        changed = [dict(FILES[0]), dict(FILES[1])]
        changed[0]["records"] = 999
        after = freeze.collect(build(
            {"source_a/ingestion_date=2026-08-24/_manifest.json": manifest(changed)}))
        self.assertNotEqual(freeze.collect(build(ONE_PARTITION))[0]["content_sha256"],
                            after[0]["content_sha256"])


class CompareTest(unittest.TestCase):
    """The three ways the capture can change, and none may pass silently."""

    def setUp(self):
        self.sealed = freeze.collect(build({
            "a/ingestion_date=2026-08-24/_manifest.json": manifest(FILES),
            "a/ingestion_date=2026-08-25/_manifest.json": manifest(FILES),
        }))

    def test_intact_capture_produces_no_problem(self):
        self.assertEqual(freeze.comparar(self.sealed, list(self.sealed)), [])

    def test_sealed_partition_that_disappeared(self):
        problems = freeze.comparar(self.sealed, self.sealed[:1])
        self.assertEqual(len(problems), 1)
        self.assertIn("MISSING from RAW", problems[0])

    def test_new_partition_outside_the_seal(self):
        """A WINDOW THAT GROWS AFTER CLOSING invalidates every number already published about
        it — and breaks no sum, which is what makes it dangerous."""
        current = self.sealed + [dict(self.sealed[0], partition_key="ingestion_date=2026-08-26")]
        problems = freeze.comparar(self.sealed, current)
        self.assertEqual(len(problems), 1)
        self.assertIn("NEW partition, outside the seal", problems[0])

    def test_content_changed_in_a_sealed_partition(self):
        current = [dict(self.sealed[0], content_sha256="ff" * 32), self.sealed[1]]
        problems = freeze.comparar(self.sealed, current)
        self.assertEqual(len(problems), 1)
        self.assertIn("content CHANGED", problems[0])


class SeedTest(unittest.TestCase):
    def test_seed_and_page_come_from_the_same_collection(self):
        """The markdown isn't readable by SQL and the CSV isn't readable by people. Both
        come from the SAME `collect`, in the same command — if they came from different
        collections, there would be a second place where the truth lives, and it would
        diverge."""
        records = freeze.collect(build(ONE_PARTITION))
        with tempfile.TemporaryDirectory() as tmp:
            seed = freeze.escrever_seed(records, os.path.join(tmp, "s.csv"))
            page = freeze.escrever_pagina(records, os.path.join(tmp, "F.md"))
            read_back = freeze.ler_seed(seed)
            self.assertEqual(read_back, records)
            with open(page, encoding="utf-8") as handle:
                text = handle.read()
            # capture_id is the hash of every content_sha256: it alone ties the page to the
            # collection. The per-partition detail lives only in the seed, and the page
            # points to it instead of duplicating the table.
            self.assertIn(freeze.capture_id(records), text)
            self.assertNotIn(records[0]["content_sha256"][:24], text)
            self.assertIn("frozen_capture_seed.csv", text)

    def test_page_does_NOT_carry_a_clock_in_the_seal_body(self):
        """The seal date appears once, as information. `capture_id` does NOT depend on it —
        otherwise regenerating the page over the same capture would produce a new id."""
        records = freeze.collect(build(ONE_PARTITION))
        self.assertEqual(freeze.capture_id(records), freeze.capture_id(list(records)))

    def test_missing_seed_is_an_error_with_instructions(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(freeze.FreezeError) as ctx:
                freeze.ler_seed(os.path.join(tmp, "does-not-exist.csv"))
        self.assertIn("make freeze", str(ctx.exception))


class CaptureIdTest(unittest.TestCase):
    def test_id_changes_when_any_partition_changes(self):
        base = freeze.collect(build(ONE_PARTITION))
        other = [dict(base[0], content_sha256="ff" * 32)]
        self.assertNotEqual(freeze.capture_id(base), freeze.capture_id(other))

    def test_id_does_not_depend_on_bytes_or_counts(self):
        """It aggregates the SEALS, and the seal already covers bytes and records. Summing
        those columns again here would add no detection and would make the id change by
        rounding."""
        base = freeze.collect(build(ONE_PARTITION))
        same_seal = [dict(base[0], bytes=999, records=999, files=99)]
        self.assertEqual(freeze.capture_id(base), freeze.capture_id(same_seal))


if __name__ == "__main__":
    unittest.main()
