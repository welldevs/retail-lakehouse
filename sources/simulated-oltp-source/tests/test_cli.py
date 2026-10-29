"""The CLI wires arguments to the right handlers and never lets a traceback escape."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from simulated_oltp_source import extract as extract_module
from simulated_oltp_source import validate as validate_module
from simulated_oltp_source.cli import EXIT_UNHANDLED, build_parser, main

from tests import support


class ParserTest(unittest.TestCase):
    def setUp(self):
        self.parser = build_parser()

    def test_extract_wires_to_extract_handler(self):
        args = self.parser.parse_args(["extract", "--reference", "ref", "--wh", "mad1"])
        self.assertIs(args.handler, extract_module.run)
        # `None` and not a number: omitting --count means "use the target the reference
        # derived from that warehouse's population". A default here would reintroduce a
        # base sized by whoever typed the command.
        self.assertIsNone(args.count)
        self.assertFalse(args.overwrite)

    def test_validate_wires_to_validate_handler(self):
        args = self.parser.parse_args(["validate", "part", "--reference", "ref"])
        self.assertIs(args.handler, validate_module.run)
        self.assertFalse(args.strict)

    def test_extract_requires_reference_and_wh(self):
        for argv in (["extract", "--wh", "mad1"], ["extract", "--reference", "ref"]):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.parser.parse_args(argv)

    def test_validate_requires_reference(self):
        """Without the reference the central guarantee is not verifiable: better to refuse."""
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                self.parser.parse_args(["validate", "part"])

    def test_subcommand_is_required(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                self.parser.parse_args([])

    def test_seed_and_count_are_integers(self):
        args = self.parser.parse_args(
            ["extract", "--reference", "r", "--wh", "w", "--seed", "42", "--count", "9"]
        )
        self.assertEqual((args.seed, args.count), (42, 9))


class MainTest(unittest.TestCase):
    def test_unexpected_exception_becomes_code_3_without_traceback(self):
        """No traceback escapes with an ambiguous code: the orchestrator decides by code."""
        original = validate_module.run
        self.addCleanup(setattr, validate_module, "run", original)

        def explode(args):
            raise RuntimeError("boom")

        validate_module.run = explode  # build_parser reads the handler when it is built
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            code = main(["validate", "p", "--reference", "r"])
        self.assertEqual(code, EXIT_UNHANDLED)
        self.assertIn("UNHANDLED ERROR", buffer.getvalue())
        self.assertNotIn("Traceback", buffer.getvalue())

    def test_full_flow_through_the_cli(self):
        with tempfile.TemporaryDirectory() as tmp:
            reference = support.write_reference(os.path.join(tmp, "ref"))
            out = os.path.join(tmp, "data")
            with contextlib.redirect_stdout(io.StringIO()):
                extracted = main(
                    ["extract", "--reference", reference, "--out", out,
                     "--wh", support.WH_A, "--date", "2026-08-27", "--count", "20"]
                )
                validated = main(
                    ["validate",
                     os.path.join(out, "ingestion_date=2026-08-27", f"wh={support.WH_A}"),
                     "--reference", reference, "--strict"]
                )
        self.assertEqual(extracted, 0)
        self.assertEqual(validated, 0)


if __name__ == "__main__":
    unittest.main()
