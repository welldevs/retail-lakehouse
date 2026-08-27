"""A CLI liga os argumentos aos handlers certos e nunca deixa traceback escapar."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from simulated_oltp_source import extract as extract_module
from simulated_oltp_source import validate as validate_module
from simulated_oltp_source.cli import DEFAULT_COUNT, EXIT_UNHANDLED, build_parser, main

from tests import support


class ParserTest(unittest.TestCase):
    def setUp(self):
        self.parser = build_parser()

    def test_extract_liga_no_handler_de_extracao(self):
        args = self.parser.parse_args(["extract", "--reference", "ref", "--wh", "mad1"])
        self.assertIs(args.handler, extract_module.run)
        self.assertEqual(args.count, DEFAULT_COUNT)
        self.assertFalse(args.overwrite)

    def test_validate_liga_no_handler_de_validacao(self):
        args = self.parser.parse_args(["validate", "part", "--reference", "ref"])
        self.assertIs(args.handler, validate_module.run)
        self.assertFalse(args.strict)

    def test_extract_exige_reference_e_wh(self):
        for argv in (["extract", "--wh", "mad1"], ["extract", "--reference", "ref"]):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.parser.parse_args(argv)

    def test_validate_exige_reference(self):
        """Sem a referencia a garantia central nao e verificavel: melhor recusar."""
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                self.parser.parse_args(["validate", "part"])

    def test_subcomando_e_obrigatorio(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                self.parser.parse_args([])

    def test_seed_e_count_sao_inteiros(self):
        args = self.parser.parse_args(
            ["extract", "--reference", "r", "--wh", "w", "--seed", "42", "--count", "9"]
        )
        self.assertEqual((args.seed, args.count), (42, 9))


class MainTest(unittest.TestCase):
    def test_excecao_inesperada_vira_codigo_3_sem_traceback(self):
        """Nenhum traceback escapa com codigo ambiguo: o orquestrador decide por codigo."""
        original = validate_module.run
        self.addCleanup(setattr, validate_module, "run", original)

        def explodir(args):
            raise RuntimeError("boom")

        validate_module.run = explodir  # build_parser le o handler na hora de montar
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            codigo = main(["validate", "p", "--reference", "r"])
        self.assertEqual(codigo, EXIT_UNHANDLED)
        self.assertIn("ERRO NAO TRATADO", buffer.getvalue())
        self.assertNotIn("Traceback", buffer.getvalue())

    def test_fluxo_completo_pela_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            reference = support.write_reference(os.path.join(tmp, "ref"))
            out = os.path.join(tmp, "data")
            with contextlib.redirect_stdout(io.StringIO()):
                extraido = main(
                    ["extract", "--reference", reference, "--out", out,
                     "--wh", support.WH_A, "--date", "2026-08-27", "--count", "20"]
                )
                validado = main(
                    ["validate",
                     os.path.join(out, "ingestion_date=2026-08-27", f"wh={support.WH_A}"),
                     "--reference", reference, "--strict"]
                )
        self.assertEqual(extraido, 0)
        self.assertEqual(validado, 0)


if __name__ == "__main__":
    unittest.main()
