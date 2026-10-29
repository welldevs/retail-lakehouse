"""Canonical form: determinism, atomicity and corrupt-file detection."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from ine_population_source import canonical


class CanonicalFormTest(unittest.TestCase):
    def test_key_order_does_not_change_the_digest(self):
        a = {"b": 1, "a": {"z": 1, "y": 2}}
        b = {"a": {"y": 2, "z": 1}, "b": 1}
        self.assertEqual(canonical.canonical_bytes(a), canonical.canonical_bytes(b))

    def test_canonical_form_has_indentation_utf8_and_trailing_newline(self):
        blob = canonical.canonical_bytes({"name": "Población", "n": 1})
        self.assertTrue(blob.endswith(b"\n"))
        self.assertIn(b"\n  ", blob)
        self.assertIn("Población".encode("utf-8"), blob)
        self.assertNotIn(b"\\u", blob)

    def test_numbers_do_not_become_strings_nor_vice_versa(self):
        blob = canonical.canonical_bytes({"Valor": 6779888, "Anyo": 2025})
        self.assertIn(b'"Valor": 6779888', blob)
        self.assertNotIn(b'"6779888"', blob)


class AtomicWriteTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_writes_and_returns_digest_and_size(self):
        path = os.path.join(self.dir, "x.json")
        sha, size = canonical.write_json(path, {"a": 1})
        with open(path, "rb") as handle:
            blob = handle.read()
        self.assertEqual(sha, canonical.digest(blob))
        self.assertEqual(size, len(blob))

    def test_leaves_no_temporary_file_behind(self):
        canonical.write_json(os.path.join(self.dir, "x.json"), {"a": 1})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_mid_write_failure_keeps_the_previous_file_and_leaves_no_partial(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"version": "good"})
        with mock.patch("os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                canonical.write_json(path, {"version": "bad"})
        payload, _ = canonical.read_json(path)
        self.assertEqual(payload, {"version": "good"})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_keyboard_interrupt_leaves_no_partial(self):
        path = os.path.join(self.dir, "x.json")
        with mock.patch("os.replace", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                canonical.write_json(path, {"a": 1})
        self.assertFalse(os.path.exists(path))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])


class CanonicalParametersAreLockedTest(unittest.TestCase):
    """The canonical form parameters are contract: changing any of them changes every SHA-256."""

    def test_exact_bytes_of_a_known_payload(self):
        expected = b'{\n  "a": "\xc3\xa1",\n  "b": 1\n}\n'
        self.assertEqual(canonical.canonical_bytes({"b": 1, "a": "á"}), expected)

    def test_declared_parameters_match_usage(self):
        self.assertIs(canonical.JSON_SORT_KEYS, True)
        self.assertIs(canonical.JSON_ENSURE_ASCII, False)
        self.assertEqual(canonical.JSON_INDENT, 2)
        self.assertEqual(canonical.JSON_TRAILING_NEWLINE, "\n")

    def test_indentation_is_two_spaces(self):
        lines = canonical.canonical_bytes({"a": {"b": 1}}).decode("utf-8").split("\n")
        self.assertEqual(lines[1], '  "a": {')
        self.assertEqual(lines[2], '    "b": 1')


class AtomicWriteMechanicsTest(unittest.TestCase):
    """Atomicity depends on details that must be proven, not assumed."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_temporary_is_created_in_the_target_directory(self):
        path = os.path.join(self.dir, "sub", "x.json")
        captured = {}
        real_replace = os.replace

        def spy(src, dst):
            captured["src_dir"] = os.path.dirname(os.path.abspath(src))
            captured["dst_dir"] = os.path.dirname(os.path.abspath(dst))
            return real_replace(src, dst)

        with mock.patch("os.replace", spy):
            canonical.write_json(path, {"a": 1})
        self.assertEqual(captured["src_dir"], captured["dst_dir"])

    def test_fsyncs_before_the_swap(self):
        path = os.path.join(self.dir, "x.json")
        order = []
        real_fsync, real_replace = os.fsync, os.replace
        with mock.patch("os.fsync", lambda fd: (order.append("fsync"), real_fsync(fd))[1]):
            with mock.patch(
                "os.replace",
                lambda s, d: (order.append("replace"), real_replace(s, d))[1],
            ):
                canonical.write_json(path, {"a": 1})
        self.assertEqual(order, ["fsync", "replace"])

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

    def test_invalid_utf8_bytes_raise_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        with open(path, "wb") as handle:
            handle.write(b'{"a": "\xff\xfe"}')
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_valid_file_returns_payload_and_bytes(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"a": [1, 2]})
        payload, blob = canonical.read_json(path)
        self.assertEqual(payload, {"a": [1, 2]})
        self.assertEqual(json.loads(blob.decode("utf-8")), {"a": [1, 2]})


if __name__ == "__main__":
    unittest.main()
