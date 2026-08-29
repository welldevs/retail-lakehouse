"""O contrato da CLI: verbos, argumentos obrigatorios e codigos de saida."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout

from simulated_orders_source import __version__
from simulated_orders_source.cli import build_parser, main


class ParserTest(unittest.TestCase):
    def test_dois_verbos(self):
        parser = build_parser()
        for argv in (["extract", "--reference", "r", "--wh", "mad1", "--date", "2026-08-24"],
                     ["validate", "p", "--reference", "r"]):
            self.assertTrue(hasattr(parser.parse_args(argv), "handler"))

    def test_extract_exige_reference_wh_e_date(self):
        # --date e obrigatorio, diferente das outras quatro sources: aqui ingestion_date e a
        # DATA DO PEDIDO, e cair no dia corrente por omissao geraria pedidos num dia que
        # talvez nem tenha catalogo.
        for incompleto in (
            ["extract", "--wh", "mad1", "--date", "2026-08-24"],
            ["extract", "--reference", "r", "--date", "2026-08-24"],
            ["extract", "--reference", "r", "--wh", "mad1"],
        ):
            with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
                build_parser().parse_args(incompleto)

    def test_validate_exige_reference(self):
        # Sem a referencia so daria para conferir checksum, que prova integridade e nao
        # coerencia. E a coerencia que e a garantia central desta Source.
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            build_parser().parse_args(["validate", "p"])

    def test_verbo_desconhecido_e_recusado(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            build_parser().parse_args(["publish"])

    def test_versao_bate_com_o_pacote(self):
        with self.assertRaises(SystemExit), redirect_stdout(io.StringIO()) as saida:
            build_parser().parse_args(["--version"])
        self.assertEqual(saida.getvalue().strip(), __version__)


class CodigoDeSaidaTest(unittest.TestCase):
    def test_referencia_inexistente_sai_com_2(self):
        with redirect_stdout(io.StringIO()) as saida:
            code = main(
                [
                    "extract",
                    "--reference",
                    "/tmp/nao-existe-referencia-alguma",
                    "--wh",
                    "mad1",
                    "--date",
                    "2026-08-24",
                    "--out",
                    "/tmp/nao-existe-saida-alguma",
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("ERRO", saida.getvalue())

    def test_particao_inexistente_no_validate_sai_com_1(self):
        with redirect_stdout(io.StringIO()) as saida:
            code = main(["validate", "/tmp/nao-existe-particao", "--reference", "/tmp/x"])
        self.assertEqual(code, 1)
        self.assertIn("manifesto nao encontrado", saida.getvalue())


if __name__ == "__main__":
    unittest.main()
