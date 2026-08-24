"""Forma canonica: determinismo, atomicidade e deteccao de arquivo corrompido."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from mercadona_catalog_source import canonical


class CanonicalFormTest(unittest.TestCase):
    def test_ordem_das_chaves_nao_altera_o_digest(self):
        a = {"b": 1, "a": {"z": 1, "y": 2}}
        b = {"a": {"y": 2, "z": 1}, "b": 1}
        self.assertEqual(canonical.canonical_bytes(a), canonical.canonical_bytes(b))

    def test_forma_canonica_tem_indentacao_utf8_e_newline_final(self):
        blob = canonical.canonical_bytes({"nome": "Melocotón", "n": 1})
        self.assertTrue(blob.endswith(b"\n"))
        self.assertIn(b"\n  ", blob)
        self.assertIn("Melocotón".encode("utf-8"), blob)
        self.assertNotIn(b"\\u", blob)

    def test_precos_string_nao_sao_convertidos(self):
        blob = canonical.canonical_bytes({"unit_price": "17.75"})
        self.assertIn(b'"17.75"', blob)


class AtomicWriteTest(unittest.TestCase):
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

    def test_interrupcao_de_teclado_nao_deixa_parcial(self):
        path = os.path.join(self.dir, "x.json")
        with mock.patch("os.replace", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                canonical.write_json(path, {"a": 1})
        self.assertFalse(os.path.exists(path))
        self.assertEqual([n for n in os.listdir(self.dir) if n.startswith(".tmp-")], [])


class CanonicalParametersAreLockedTest(unittest.TestCase):
    """Os parametros da forma canonica sao contrato: mudar qualquer um muda todo SHA-256.

    Sem estas assercoes, trocar indent=2 por indent=4 deixaria a suite verde e invalidaria
    silenciosamente todos os checksums ja publicados.
    """

    def test_bytes_exatos_de_um_payload_conhecido(self):
        esperado = b'{\n  "a": "\xc3\xa1",\n  "b": 1\n}\n'
        self.assertEqual(canonical.canonical_bytes({"b": 1, "a": "á"}), esperado)

    def test_digest_conhecido_nao_muda(self):
        # Congelado deliberadamente: se este valor mudar, todos os manifestos ja gravados
        # deixam de ser reproduziveis.
        self.assertEqual(
            canonical.digest(canonical.canonical_bytes({"b": 1, "a": "á"})),
            "91e23d2d2fbbb54af34ce100984ce937a0b2d8e8eca5f616092a9201b004ddcb",
        )

    def test_parametros_declarados_batem_com_o_uso(self):
        self.assertIs(canonical.JSON_SORT_KEYS, True)
        self.assertIs(canonical.JSON_ENSURE_ASCII, False)
        self.assertEqual(canonical.JSON_INDENT, 2)
        self.assertEqual(canonical.JSON_TRAILING_NEWLINE, "\n")

    def test_indentacao_e_de_dois_espacos(self):
        linhas = canonical.canonical_bytes({"a": {"b": 1}}).decode("utf-8").split("\n")
        self.assertEqual(linhas[1], '  "a": {')
        self.assertEqual(linhas[2], '    "b": 1')


class AtomicWriteMechanicsTest(unittest.TestCase):
    """A atomicidade depende de detalhes que precisam estar provados, nao presumidos."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def test_temporario_e_criado_no_mesmo_diretorio_do_alvo(self):
        # os.replace so e atomico dentro do mesmo sistema de arquivos. Um temporario em
        # /tmp quebraria a garantia sem nenhum sintoma visivel.
        path = os.path.join(self.dir, "sub", "x.json")
        capturado = {}
        real_replace = os.replace

        def espiao(src, dst):
            capturado["src_dir"] = os.path.dirname(os.path.abspath(src))
            capturado["dst_dir"] = os.path.dirname(os.path.abspath(dst))
            return real_replace(src, dst)

        with mock.patch("os.replace", espiao):
            canonical.write_json(path, {"a": 1})
        self.assertEqual(capturado["src_dir"], capturado["dst_dir"])

    def test_faz_fsync_antes_da_troca(self):
        path = os.path.join(self.dir, "x.json")
        ordem = []
        real_fsync, real_replace = os.fsync, os.replace
        with mock.patch("os.fsync", lambda fd: (ordem.append("fsync"), real_fsync(fd))[1]):
            with mock.patch(
                "os.replace",
                lambda s, d: (ordem.append("replace"), real_replace(s, d))[1],
            ):
                canonical.write_json(path, {"a": 1})
        self.assertEqual(ordem, ["fsync", "replace"])

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

    def test_bytes_invalidos_em_utf8_levantam_corrupt_file_error(self):
        path = os.path.join(self.dir, "x.json")
        with open(path, "wb") as handle:
            handle.write(b'{"a": "\xff\xfe"}')
        with self.assertRaises(canonical.CorruptFileError):
            canonical.read_json(path)

    def test_arquivo_valido_devolve_payload_e_bytes(self):
        path = os.path.join(self.dir, "x.json")
        canonical.write_json(path, {"a": [1, 2]})
        payload, blob = canonical.read_json(path)
        self.assertEqual(payload, {"a": [1, 2]})
        self.assertEqual(json.loads(blob.decode("utf-8")), {"a": [1, 2]})


if __name__ == "__main__":
    unittest.main()
