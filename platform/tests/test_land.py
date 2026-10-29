"""Object key and checksum conversion. No network."""

from __future__ import annotations

import base64
import hashlib
import tempfile
import unittest

from retail_platform import SOURCE_NAME
from retail_platform.land import _b64_of_hex, object_key
from retail_platform.manifest import read

from .support import build_partition, build_single_axis_partition


class TestChecksumEncoding(unittest.TestCase):
    def test_b64_of_hex_matches_what_s3_expects(self):
        """S3 wants base64 of the RAW digest, not of the hexadecimal. Sending the hex in base64
        would make the server reject every object, or worse, accept without checking."""
        blob = b"content"
        hex_digest = hashlib.sha256(blob).hexdigest()
        self.assertEqual(
            _b64_of_hex(hex_digest),
            base64.b64encode(hashlib.sha256(blob).digest()).decode("ascii"),
        )


class TestObjectKey(unittest.TestCase):
    def test_key_starts_with_the_source_name_and_keeps_the_hive_layout(self):
        """The hive layout must survive the upload: it is what makes DuckDB derive
        ingestion_date and wh as columns with hive_partitioning=1."""
        with tempfile.TemporaryDirectory() as root:
            partition = build_partition(root, ingestion_date="2026-08-16", warehouse="mad1")
            entry = next(e for e in read(partition).files if e.stage == "catalog")
            key = object_key(read(partition), entry.path)
        self.assertEqual(
            key,
            f"{SOURCE_NAME}/ingestion_date=2026-08-16/wh=mad1/catalog/category_id=112.json",
        )

    def test_key_for_a_source_without_a_second_axis_has_no_extra_segment(self):
        """object_key uses partition.source_name, not a fixed constant: a second source
        lands under its own prefix, with no axis at all when there is none."""
        with tempfile.TemporaryDirectory() as root:
            partition = build_single_axis_partition(root, ingestion_date="2026-09-01")
            entry = next(e for e in read(partition).files if e.stage == "tables")
            key = object_key(read(partition), entry.path)
        self.assertEqual(
            key,
            "ine_population_api/ingestion_date=2026-09-01/tables/table_id=31304.json",
        )


if __name__ == "__main__":
    unittest.main()
