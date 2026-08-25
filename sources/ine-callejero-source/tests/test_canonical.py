"""Manifesto JSON canonico + copia verbatim: determinismo, atomicidade, integridade."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from ine_callejero_source import canonical


class ManifestJsonFormTest(unittest.TestCase):
    def test_ordem_das_chaves_nao_altera_o_digest(self):
        a = {"b": 1, "a": {"z": 1, "y": 2}}
        b = {"a": {"y": 2, "z": 1}, "b": 1}
        self.assertEqual(canonical.canonical_json_bytes(a), canonical.canonical_json_bytes(b))

    def test_forma_canonica_tem_indentacao_utf8_e_newline_final(self):
        blob = canonical.canonical_json_bytes({"nome": "Población", "n": 1})
        self.assertTrue(blob.endswith(b"\n"))
        self.assertIn(b"\n  ", blob)
        self.assertIn("Población".encode("utf-8"), blob)
        self.assertNotIn(b"\\u", blob)


class ManifestAtomicWriteTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_grava_e_devolve_digest_e_tamanho(self):
        path = os.path.join(self.dir, "x.json")
        sha, size = canonical.write_json(path, {"a": 1})
        with open(path, "rb") as handle:
            blob = handle.read()
        self.assertEqual(sha, canonical.digest(blob))
        self.assertEqual(size, len(blob))

    def test_nao_deixa_temporario_para_tras(self):
        canonical.write_json(os.path.join(self.dir, "x.json"), {"a": 1})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_falha_no_meio_preserva_o_arquivo_anterior_e_nao_deixa_parcial(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"versao": "boa"})
        with mock.patch("os.replace", side_effect=OSError("disco cheio")):
            with self.assertRaises(OSError):
                canonical.write_json(path, {"versao": "ruim"})
        payload, _ = canonical.read_json(path)
        self.assertEqual(payload, {"versao": "boa"})
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_cria_o_diretorio_de_destino(self):
        path = os.path.join(self.dir, "a", "b", "c.json")
        canonical.write_json(path, {"a": 1})
        self.assertTrue(os.path.exists(path))


class ReadJsonTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_json_truncado_levanta_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        with open(path, "wb") as handle:
            handle.write(b'{"a": 1')
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_arquivo_vazio_levanta_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        open(path, "wb").close()
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_arquivo_valido_devolve_payload_e_bytes(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"a": [1, 2]})
        payload, blob = canonical.read_json(path)
        self.assertEqual(payload, {"a": [1, 2]})
        self.assertEqual(json.loads(blob.decode("utf-8")), {"a": [1, 2]})


class CopyVerbatimTest(unittest.TestCase):
    """Aqui NAO ha forma canonica de reserializacao: os bytes de origem sao o contrato."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _write_source(self, blob: bytes) -> str:
        path = os.path.join(self.dir, "origem")
        with open(path, "wb") as handle:
            handle.write(blob)
        return path

    def test_copia_bytes_identicos_ao_original_mesmo_com_encoding_latin1(self):
        # bytes ISO-8859-1 que NAO sao UTF-8 valido: 0xE9 sozinho e invalido em UTF-8.
        blob = b"MADRID \xe9 aqui\r\n"
        source = self._write_source(blob)
        target = os.path.join(self.dir, "destino")
        sha, size = canonical.copy_verbatim(source, target)
        with open(target, "rb") as handle:
            copiado = handle.read()
        self.assertEqual(copiado, blob)
        self.assertEqual(sha, canonical.digest(blob))
        self.assertEqual(size, len(blob))

    def test_nao_deixa_temporario_para_tras(self):
        source = self._write_source(b"x")
        canonical.copy_verbatim(source, os.path.join(self.dir, "destino"))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_falha_no_meio_nao_deixa_parcial(self):
        source = self._write_source(b"x" * 100)
        target = os.path.join(self.dir, "destino")
        with mock.patch("os.replace", side_effect=OSError("disco cheio")):
            with self.assertRaises(OSError):
                canonical.copy_verbatim(source, target)
        self.assertFalse(os.path.exists(target))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])

    def test_cria_o_diretorio_de_destino(self):
        source = self._write_source(b"x")
        target = os.path.join(self.dir, "a", "b", "destino")
        canonical.copy_verbatim(source, target)
        self.assertTrue(os.path.exists(target))


if __name__ == "__main__":
    unittest.main()
