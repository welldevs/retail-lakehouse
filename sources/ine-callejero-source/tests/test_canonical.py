"""Canonical JSON manifest + verbatim copy: determinism, atomicity, integrity."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from ine_callejero_source import canonical


class ManifestJsonFormTest(unittest.TestCase):
    def test_key_order_does_not_change_the_digest(self):
        a = {"b": 1, "a": {"z": 1, "y": 2}}
        b = {"a": {"y": 2, "z": 1}, "b": 1}
        self.assertEqual(canonical.canonical_json_bytes(a), canonical.canonical_json_bytes(b))

    def test_canonical_form_has_indentation_utf8_and_trailing_newline(self):
        blob = canonical.canonical_json_bytes({"name": "Población", "n": 1})
        self.assertTrue(blob.endswith(b"\n"))
        self.assertIn(b"\n  ", blob)
        self.assertIn("Población".encode("utf-8"), blob)
        self.assertNotIn(b"\\u", blob)


class ManifestAtomicWriteTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_writes_and_returns_digest_and_size(self):
        path = os.path.join(self.dir, "x.json")
        sha, size = canonical.write_json(path, {"a": 1})
        with open(path, "rb") as handle:
            blob = handle.read()
        self.assertEqual(sha, canonical.digest(blob))
        self.assertEqual(size, len(blob))

    def test_leaves_no_temp_file_behind(self):
        canonical.write_json(os.path.join(self.dir, "x.json"), {"a": 1})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_failure_midway_keeps_previous_file_and_leaves_no_partial(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"version": "good"})
        with mock.patch("os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                canonical.write_json(path, {"version": "bad"})
        payload, _ = canonical.read_json(path)
        self.assertEqual(payload, {"version": "good"})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_creates_the_target_directory(self):
        path = os.path.join(self.dir, "a", "b", "c.json")
        canonical.write_json(path, {"a": 1})
        self.assertTrue(os.path.exists(path))


class ReadJsonTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_truncated_json_raises_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        with open(path, "wb") as handle:
            handle.write(b'{"a": 1')
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_empty_file_raises_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        open(path, "wb").close()
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_valid_file_returns_payload_and_bytes(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"a": [1, 2]})
        payload, blob = canonical.read_json(path)
        self.assertEqual(payload, {"a": [1, 2]})
        self.assertEqual(json.loads(blob.decode("utf-8")), {"a": [1, 2]})


class CopyVerbatimTest(unittest.TestCase):
    """There is NO canonical reserialization form here: the source bytes are the contract."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _write_source(self, blob: bytes) -> str:
        path = os.path.join(self.dir, "source")
        with open(path, "wb") as handle:
            handle.write(blob)
        return path

    def test_copies_bytes_identical_to_original_even_with_latin1_encoding(self):
        # ISO-8859-1 bytes that are NOT valid UTF-8: a lone 0xE9 is invalid in UTF-8.
        blob = b"MADRID \xe9 here\r\n"
        source = self._write_source(blob)
        target = os.path.join(self.dir, "target")
        sha, size = canonical.copy_verbatim(source, target)
        with open(target, "rb") as handle:
            copied = handle.read()
        self.assertEqual(copied, blob)
        self.assertEqual(sha, canonical.digest(blob))
        self.assertEqual(size, len(blob))

    def test_leaves_no_temp_file_behind(self):
        source = self._write_source(b"x")
        canonical.copy_verbatim(source, os.path.join(self.dir, "target"))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_failure_midway_leaves_no_partial(self):
        source = self._write_source(b"x" * 100)
        target = os.path.join(self.dir, "target")
        with mock.patch("os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                canonical.copy_verbatim(source, target)
        self.assertFalse(os.path.exists(target))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_creates_the_target_directory(self):
        source = self._write_source(b"x")
        target = os.path.join(self.dir, "a", "b", "target")
        canonical.copy_verbatim(source, target)
        self.assertTrue(os.path.exists(target))


if __name__ == "__main__":
    unittest.main()
