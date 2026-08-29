"""Forma canonica: NDJSON deterministico e escrita atomica."""

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

    def test_uma_linha_por_registro_e_newline_final(self):
        sha, size, count = canonical.write_jsonl(self.path, [{"b": 1, "a": 2}, {"a": 3}])
        with open(self.path, "rb") as handle:
            blob = handle.read()
        self.assertEqual(count, 2)
        self.assertEqual(size, len(blob))
        self.assertTrue(blob.endswith(b"\n"))
        self.assertEqual(len(blob.decode().splitlines()), 2)
        self.assertEqual(sha, canonical.digest(blob))

    def test_chaves_ordenadas_estabilizam_o_digest(self):
        # Duas ordens de insercao diferentes, o mesmo conteudo, o mesmo sha256. Sem isto o
        # digest da particao dependeria da ordem em que o gerador montou o dict.
        a, _, _ = canonical.write_jsonl(self.path, [{"b": 1, "a": 2}])
        b, _, _ = canonical.write_jsonl(self.path, [{"a": 2, "b": 1}])
        self.assertEqual(a, b)

    def test_sem_indentacao(self):
        canonical.write_jsonl(self.path, [{"a": 1, "b": 2}])
        with open(self.path, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), '{"a": 1, "b": 2}\n')

    def test_acento_nao_vira_escape(self):
        canonical.write_jsonl(self.path, [{"nome": "Cítricos"}])
        with open(self.path, encoding="utf-8") as handle:
            self.assertIn("Cítricos", handle.read())

    def test_ida_e_volta(self):
        linhas = [{"a": 1}, {"a": 2, "b": [1, 2]}]
        canonical.write_jsonl(self.path, linhas)
        lidas, _ = canonical.read_jsonl(self.path)
        self.assertEqual(lidas, linhas)

    def test_linha_invalida_e_reportada_com_o_numero(self):
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write('{"a": 1}\nnao e json\n')
        with self.assertRaises(canonical.CorruptFileError) as caught:
            canonical.read_jsonl(self.path)
        self.assertIn("linha 2", str(caught.exception))

    def test_log_vazio_e_legivel(self):
        sha, size, count = canonical.write_jsonl(self.path, [])
        self.assertEqual((size, count), (0, 0))
        self.assertEqual(canonical.read_jsonl(self.path)[0], [])
        self.assertEqual(sha, canonical.digest(b""))

    def test_nao_deixa_temporario_para_tras(self):
        canonical.write_jsonl(self.path, [{"a": 1}])
        self.assertEqual(
            [n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], []
        )

    def test_falha_no_meio_nao_substitui_o_arquivo(self):
        canonical.write_jsonl(self.path, [{"a": 1}])
        with open(self.path, "rb") as handle:
            antes = handle.read()

        def explode():
            yield {"a": 2}
            raise RuntimeError("interrompido")

        with self.assertRaises(RuntimeError):
            canonical.write_jsonl(self.path, explode())
        with open(self.path, "rb") as handle:
            self.assertEqual(handle.read(), antes)
        self.assertEqual([n for n in os.listdir(self.tmp.name) if n.startswith(".tmp-")], [])


class ManifestoTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def test_json_do_manifesto_segue_a_forma_das_outras_sources(self):
        path = os.path.join(self.tmp.name, "_manifest.json")
        canonical.write_json(path, {"b": 1, "a": 2})
        with open(path, encoding="utf-8") as handle:
            texto = handle.read()
        self.assertEqual(texto, '{\n  "a": 2,\n  "b": 1\n}\n')
        self.assertEqual(json.loads(texto), {"a": 2, "b": 1})


if __name__ == "__main__":
    unittest.main()
