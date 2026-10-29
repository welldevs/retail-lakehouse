"""Canonical form, stable digest and atomic write."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from simulated_oltp_source.canonical import (
    CorruptFileError,
    canonical_bytes,
    digest,
    read_bytes,
    read_json,
    write_json,
)


class CanonicalTest(unittest.TestCase):
    def test_key_order_does_not_change_the_digest(self):
        """The SHA-256 depends on the content, not on the order the dict was built in."""
        a = {"b": 1, "a": 2}
        b = {"a": 2, "b": 1}
        self.assertEqual(digest(canonical_bytes(a)), digest(canonical_bytes(b)))

    def test_accent_is_not_escaped(self):
        blob = canonical_bytes({"name": "València"})
        self.assertIn("València", blob.decode("utf-8"))

    def test_ends_with_newline(self):
        self.assertTrue(canonical_bytes({"a": 1}).endswith(b"\n"))


class WriteJsonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_writes_and_returns_digest_and_size(self):
        path = os.path.join(self.tmp.name, "sub", "a.json")
        sha, size = write_json(path, {"a": 1})
        blob = read_bytes(path)
        self.assertEqual(sha, digest(blob))
        self.assertEqual(size, len(blob))
        self.assertEqual(json.loads(blob), {"a": 1})

    def test_leaves_no_temporary_file_behind(self):
        path = os.path.join(self.tmp.name, "a.json")
        write_json(path, {"a": 1})
        leftovers = [n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")]
        self.assertEqual(leftovers, [])

    def test_serialization_failure_leaves_no_partial_file(self):
        path = os.path.join(self.tmp.name, "a.json")
        with self.assertRaises(TypeError):
            write_json(path, {"a": object()})
        self.assertFalse(os.path.exists(path))
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], [])

    def test_rewrite_is_atomic_and_replaces_the_content(self):
        path = os.path.join(self.tmp.name, "a.json")
        write_json(path, {"a": 1})
        write_json(path, {"a": 2})
        payload, _ = read_json(path)
        self.assertEqual(payload, {"a": 2})


class ReadJsonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_corrupt_file_raises_named_error(self):
        path = os.path.join(self.tmp.name, "a.json")
        with open(path, "w") as handle:
            handle.write("{ not json")
        with self.assertRaises(CorruptFileError):
            read_json(path)


if __name__ == "__main__":
    unittest.main()
