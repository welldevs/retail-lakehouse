"""CLI: parsing, dispatch and exit-code semantics."""

from __future__ import annotations

import contextlib
import io
import unittest
from unittest import mock

from ine_population_source import cli


def run_main(argv):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = cli.main(argv)
    return code, buffer.getvalue()


class ParserTest(unittest.TestCase):
    def test_extract_defaults(self):
        args = cli.build_parser().parse_args(["extract"])
        self.assertEqual(args.out, "data/ine")
        self.assertEqual(args.tables, ["31304"])
        self.assertIsNone(args.date)
        self.assertEqual(args.delay, 0.5)
        self.assertEqual(args.timeout, 30.0)
        self.assertEqual(args.max_retries, 5)
        self.assertFalse(args.overwrite)

    def test_tables_accepts_comma_separated_list(self):
        args = cli.build_parser().parse_args(["extract", "--tables", "31304,9689,9688"])
        self.assertEqual(args.tables, ["31304", "9689", "9688"])

    def test_validate_defaults(self):
        args = cli.build_parser().parse_args(["validate", "p"])
        self.assertEqual(args.partition, "p")
        self.assertFalse(args.strict)

    def test_subcommand_is_required(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                cli.build_parser().parse_args([])

    def test_unknown_subcommand_is_rejected(self):
        with self.assertRaises(SystemExit):
            with contextlib.redirect_stderr(io.StringIO()):
                cli.build_parser().parse_args(["transform"])

    def test_version(self):
        with self.assertRaises(SystemExit) as ctx:
            with contextlib.redirect_stdout(io.StringIO()):
                cli.build_parser().parse_args(["--version"])
        self.assertEqual(ctx.exception.code, 0)


class DispatchTest(unittest.TestCase):
    def test_extract_dispatches_to_the_extraction_module(self):
        with mock.patch("ine_population_source.extract.run", return_value=0) as target:
            self.assertEqual(cli.main(["extract"]), 0)
        target.assert_called_once()

    def test_validate_dispatches_to_the_validation_module(self):
        with mock.patch("ine_population_source.validate.run", return_value=1) as target:
            self.assertEqual(cli.main(["validate", "p"]), 1)
        target.assert_called_once()

    def test_exit_code_is_passed_through_unchanged(self):
        for expected in (0, 1, 2):
            with self.subTest(code=expected):
                with mock.patch(
                    "ine_population_source.extract.run", return_value=expected
                ):
                    self.assertEqual(cli.main(["extract"]), expected)


class UnhandledExceptionTest(unittest.TestCase):
    def test_unhandled_exception_becomes_code_3_not_traceback(self):
        with mock.patch(
            "ine_population_source.extract.run", side_effect=RuntimeError("boom")
        ):
            code, output = run_main(["extract"])
        self.assertEqual(code, cli.EXIT_UNHANDLED)
        self.assertEqual(code, 3)
        self.assertIn("UNHANDLED ERROR", output)
        self.assertIn("RuntimeError", output)

    def test_code_3_does_not_collide_with_partial_failure(self):
        with mock.patch("ine_population_source.extract.run", return_value=1):
            partial = cli.main(["extract"])
        with mock.patch(
            "ine_population_source.extract.run", side_effect=ValueError("x")
        ):
            with contextlib.redirect_stdout(io.StringIO()):
                defect = cli.main(["extract"])
        self.assertNotEqual(partial, defect)

    def test_keyboard_interrupt_exits_3_with_clean_message(self):
        with mock.patch(
            "ine_population_source.extract.run", side_effect=KeyboardInterrupt
        ):
            code, output = run_main(["extract"])
        self.assertEqual(code, 3)
        self.assertIn("interrupted", output)

    def test_validate_error_also_becomes_3(self):
        with mock.patch(
            "ine_population_source.validate.run", side_effect=OSError("disk")
        ):
            code, _ = run_main(["validate", "p"])
        self.assertEqual(code, 3)


if __name__ == "__main__":
    unittest.main()
