"""CLI: parsing, despacho e a semantica dos codigos de saida."""

from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

from mercadona_catalog_source import cli


def run_main(argv):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = cli.main(argv)
    return code, buffer.getvalue()


class ParserTest(unittest.TestCase):
    def test_defaults_de_extract(self):
        args = cli.build_parser().parse_args(["extract"])
        self.assertEqual(args.out, "data/mercadona")
        self.assertEqual(args.wh, "mad1")
        self.assertEqual(args.lang, "es")
        self.assertIsNone(args.date)
        self.assertEqual(args.delay, 1.5)
        self.assertEqual(args.timeout, 30.0)
        self.assertEqual(args.max_retries, 5)
        self.assertIsNone(args.limit)
        self.assertFalse(args.overwrite)

    def test_defaults_de_validate(self):
        args = cli.build_parser().parse_args(["validate", "p"])
        self.assertEqual(args.partition, "p")
        self.assertFalse(args.strict)

    def test_subcomando_e_obrigatorio(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                cli.build_parser().parse_args([])

    def test_subcomando_desconhecido_e_recusado(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                cli.build_parser().parse_args(["transform"])

    def test_version(self):
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(io.StringIO()):
                cli.build_parser().parse_args(["--version"])
        self.assertEqual(ctx.exception.code, 0)


class DispatchTest(unittest.TestCase):
    def test_extract_despacha_para_o_modulo_de_extracao(self):
        with mock.patch("mercadona_catalog_source.extract.run", return_value=0) as alvo:
            self.assertEqual(cli.main(["extract"]), 0)
        alvo.assert_called_once()

    def test_validate_despacha_para_o_modulo_de_validacao(self):
        with mock.patch("mercadona_catalog_source.validate.run", return_value=1) as alvo:
            self.assertEqual(cli.main(["validate", "p"]), 1)
        alvo.assert_called_once()

    def test_codigo_de_saida_e_repassado_sem_alteracao(self):
        for esperado in (0, 1, 2):
            with self.subTest(codigo=esperado):
                with mock.patch(
                    "mercadona_catalog_source.extract.run", return_value=esperado
                ):
                    self.assertEqual(cli.main(["extract"]), esperado)


class UnhandledExceptionTest(unittest.TestCase):
    def test_excecao_nao_tratada_vira_codigo_3_e_nao_traceback(self):
        with mock.patch(
            "mercadona_catalog_source.extract.run", side_effect=RuntimeError("boom")
        ):
            code, output = run_main(["extract"])
        self.assertEqual(code, cli.EXIT_UNHANDLED)
        self.assertEqual(code, 3)
        self.assertIn("ERRO NAO TRATADO", output)
        self.assertIn("RuntimeError", output)

    def test_codigo_3_nao_colide_com_falha_parcial(self):
        # 1 significa falha parcial reexecutavel; 3 significa defeito. Um scheduler que
        # reexecute em 1 nao pode entrar em laco por causa de uma excecao.
        with mock.patch("mercadona_catalog_source.extract.run", return_value=1):
            parcial = cli.main(["extract"])
        with mock.patch(
            "mercadona_catalog_source.extract.run", side_effect=ValueError("x")
        ):
            with contextlib.redirect_stdout(io.StringIO()):
                defeito = cli.main(["extract"])
        self.assertNotEqual(parcial, defeito)

    def test_interrupcao_de_teclado_sai_com_3_e_mensagem_limpa(self):
        with mock.patch(
            "mercadona_catalog_source.extract.run", side_effect=KeyboardInterrupt
        ):
            code, output = run_main(["extract"])
        self.assertEqual(code, 3)
        self.assertIn("interrompido", output)

    def test_erro_no_validate_tambem_vira_3(self):
        with mock.patch(
            "mercadona_catalog_source.validate.run", side_effect=OSError("disco")
        ):
            code, _ = run_main(["validate", "p"])
        self.assertEqual(code, 3)


if __name__ == "__main__":
    unittest.main()
