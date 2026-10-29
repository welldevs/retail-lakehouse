"""Canonical form: deterministic NDJSON and atomic writes."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from simulated_orders_source import canonical


class JsonlTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "log.jsonl")

    def test_one_line_per_record_and_final_newline(self):
        sha, size, count = canonical.write_jsonl(self.path, [{"b": 1, "a": 2}, {"a": 3}])
        with open(self.path, "rb") as handle:
            blob = handle.read()
        self.assertEqual(count, 2)
        self.assertEqual(size, len(blob))
        self.assertTrue(blob.endswith(b"\n"))
        self.assertEqual(len(blob.decode().splitlines()), 2)
        self.assertEqual(sha, canonical.digest(blob))

    def test_sorted_keys_stabilize_the_digest(self):
        # Two different insertion orders, the same content, the same sha256. Without this the
        # partition digest would depend on the order in which the generator built the dict.
        a, _, _ = canonical.write_jsonl(self.path, [{"b": 1, "a": 2}])
        b, _, _ = canonical.write_jsonl(self.path, [{"a": 2, "b": 1}])
        self.assertEqual(a, b)

    def test_no_indentation(self):
        canonical.write_jsonl(self.path, [{"a": 1, "b": 2}])
        with open(self.path, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), '{"a": 1, "b": 2}\n')

    def test_accent_is_not_escaped(self):
        canonical.write_jsonl(self.path, [{"name": "Cítricos"}])
        with open(self.path, encoding="utf-8") as handle:
            self.assertIn("Cítricos", handle.read())

    def test_round_trip(self):
        rows = [{"a": 1}, {"a": 2, "b": [1, 2]}]
        canonical.write_jsonl(self.path, rows)
        read_back, _ = canonical.read_jsonl(self.path)
        self.assertEqual(read_back, rows)

    def test_invalid_line_is_reported_with_its_number(self):
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write('{"a": 1}\nnot json\n')
        with self.assertRaises(canonical.CorruptFileError) as caught:
            canonical.read_jsonl(self.path)
        self.assertIn("line 2", str(caught.exception))

    def test_empty_log_is_readable(self):
        sha, size, count = canonical.write_jsonl(self.path, [])
        self.assertEqual((size, count), (0, 0))
        self.assertEqual(canonical.read_jsonl(self.path)[0], [])
        self.assertEqual(sha, canonical.digest(b""))

    def test_leaves_no_temporary_file_behind(self):
        canonical.write_jsonl(self.path, [{"a": 1}])
        self.assertEqual(
            [n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], []
        )

    def test_failure_midway_does_not_replace_the_file(self):
        canonical.write_jsonl(self.path, [{"a": 1}])
        with open(self.path, "rb") as handle:
            before = handle.read()

        def explode():
            yield {"a": 2}
            raise RuntimeError("interrupted")

        with self.assertRaises(RuntimeError):
            canonical.write_jsonl(self.path, explode())
        with open(self.path, "rb") as handle:
            self.assertEqual(handle.read(), before)
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], [])


class ManifestTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_manifest_json_follows_the_other_sources_format(self):
        path = os.path.join(self.tmp.name, "_manifest.json")
        canonical.write_json(path, {"b": 1, "a": 2})
        with open(path, encoding="utf-8") as handle:
            text = handle.read()
        self.assertEqual(text, '{\n  "a": 2,\n  "b": 1\n}\n')
        self.assertEqual(json.loads(text), {"a": 2, "b": 1})


if __name__ == "__main__":
    unittest.main()
