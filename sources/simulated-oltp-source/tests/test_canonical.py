"""Forma canonica, digest estavel e gravacao atomica."""

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
    def test_ordem_das_chaves_nao_altera_o_digest(self):
        """O SHA-256 depende do conteudo, nao da ordem em que o dict foi montado."""
        a = {"b": 1, "a": 2}
        b = {"a": 2, "b": 1}
        self.assertEqual(digest(canonical_bytes(a)), digest(canonical_bytes(b)))

    def test_acento_nao_e_escapado(self):
        blob = canonical_bytes({"nome": "València"})
        self.assertIn("València", blob.decode("utf-8"))

    def test_termina_com_quebra_de_linha(self):
        self.assertTrue(canonical_bytes({"a": 1}).endswith(b"\n"))


class WriteJsonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_grava_e_devolve_digest_e_tamanho(self):
        path = os.path.join(self.tmp.name, "sub", "a.json")
        sha, size = write_json(path, {"a": 1})
        blob = read_bytes(path)
        self.assertEqual(sha, digest(blob))
        self.assertEqual(size, len(blob))
        self.assertEqual(json.loads(blob), {"a": 1})

    def test_nao_deixa_temporario_para_tras(self):
        path = os.path.join(self.tmp.name, "a.json")
        write_json(path, {"a": 1})
        restos = [n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")]
        self.assertEqual(restos, [])

    def test_falha_na_serializacao_nao_deixa_arquivo_parcial(self):
        path = os.path.join(self.tmp.name, "a.json")
        with self.assertRaises(TypeError):
            write_json(path, {"a": object()})
        self.assertFalse(os.path.exists(path))
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], [])

    def test_reescrita_e_atomica_e_substitui_o_conteudo(self):
        path = os.path.join(self.tmp.name, "a.json")
        write_json(path, {"a": 1})
        write_json(path, {"a": 2})
        payload, _ = read_json(path)
        self.assertEqual(payload, {"a": 2})


class ReadJsonTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_arquivo_corrompido_levanta_erro_nomeado(self):
        path = os.path.join(self.tmp.name, "a.json")
        with open(path, "w") as handle:
            handle.write("{ nao e json")
        with self.assertRaises(CorruptFileError):
            read_json(path)


if __name__ == "__main__":
    unittest.main()
