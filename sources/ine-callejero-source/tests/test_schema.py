"""Fatos estruturais observados num arquivo: contagem e largura de linha."""

from __future__ import annotations

import os
import tempfile
import unittest

from ine_callejero_source import schema


class LineStatsTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _write(self, lines: list[str]) -> str:
        path = os.path.join(self.dir, "arquivo")
        with open(path, "w", encoding="latin-1", newline="") as handle:
            handle.writelines(lines)
        return path

    def test_conta_linhas_e_largura_uniforme(self):
        path = self._write(["0" * 10 + "\r\n"] * 5)
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 5)
        self.assertEqual(stats["widths"], {10: 5})
        self.assertEqual(schema.dominant_width(stats), 10)

    def test_largura_nao_uniforme_e_relatada_sem_erro(self):
        path = self._write(["a" * 10 + "\r\n", "b" * 12 + "\r\n", "c" * 10 + "\r\n"])
        stats = schema.line_stats(path)
        self.assertEqual(stats["widths"], {10: 2, 12: 1})
        self.assertEqual(schema.dominant_width(stats), 10)

    def test_arquivo_vazio_nao_levanta_excecao(self):
        path = self._write([])
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 0)
        self.assertIsNone(schema.dominant_width(stats))

    def test_arquivo_inexistente_nao_levanta_excecao(self):
        stats = schema.line_stats(os.path.join(self.dir, "nao-existe"))
        self.assertEqual(stats, {"line_count": 0, "widths": {}})

    def test_encoding_latin1_le_bytes_altos_sem_erro(self):
        path = os.path.join(self.dir, "latin1")
        with open(path, "wb") as handle:
            handle.write(b"MADRID \xe9\r\n")  # 0xE9 sozinho: invalido em UTF-8
        stats = schema.line_stats(path)
        self.assertEqual(stats["line_count"], 1)


if __name__ == "__main__":
    unittest.main()
