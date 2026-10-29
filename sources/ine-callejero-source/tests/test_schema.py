"""Structural facts observed in a file: line count and width."""

from __future__ import annotations

import os
import tempfile
import unittest

from ine_callejero_source import schema


class LineStatsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _write(self, lines: list[str]) -> str:
        path = os.path.join(self.dir, "file")
        with open(path, "w", encoding="latin-1", newline="") as handle:
            handle.writelines(lines)
        return path

    def test_counts_lines_and_uniform_width(self):
        path = self._write(["0" * 10 + "\r\n"] * 5)
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 5)
        self.assertEqual(stats["widths"], {10: 5})
        self.assertEqual(schema.dominant_width(stats), 10)

    def test_non_uniform_width_is_reported_without_error(self):
        path = self._write(["a" * 10 + "\r\n", "b" * 12 + "\r\n", "c" * 10 + "\r\n"])
        stats = schema.line_stats(path)
        self.assertEqual(stats["widths"], {10: 2, 12: 1})
        self.assertEqual(schema.dominant_width(stats), 10)

    def test_empty_file_does_not_raise(self):
        path = self._write([])
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 0)
        self.assertIsNone(schema.dominant_width(stats))

    def test_missing_file_does_not_raise(self):
        stats = schema.line_stats(os.path.join(self.dir, "does-not-exist"))
        self.assertEqual(stats, {"line_count": 0, "widths": {}})

    def test_latin1_encoding_reads_high_bytes_without_error(self):
        path = os.path.join(self.dir, "latin1")
        with open(path, "wb") as handle:
            handle.write(b"MADRID \xe9\r\n")  # lone 0xE9: invalid in UTF-8
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 1)


if __name__ == "__main__":
    unittest.main()
