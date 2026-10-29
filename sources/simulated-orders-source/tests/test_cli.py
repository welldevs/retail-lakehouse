"""The CLI contract: verbs, required arguments and exit codes."""

from __future__ import annotations

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout

from simulated_orders_source import __version__
from simulated_orders_source.cli import build_parser, main


class ParserTest(unittest.TestCase):
    def test_two_verbs(self):
        parser = build_parser()
        for argv in (["extract", "--reference", "r", "--wh", "mad1", "--date", "2026-08-24"],
                     ["validate", "p", "--reference", "r"]):
            self.assertTrue(hasattr(parser.parse_args(argv), "handler"))

    def test_extract_requires_reference_wh_and_date(self):
        # --date is required, unlike the other four sources: here ingestion_date is the
        # ORDER DATE, and falling back to the current day would generate orders on a day that
        # may not even have a catalog.
        for incomplete in (
            ["extract", "--wh", "mad1", "--date", "2026-08-24"],
            ["extract", "--reference", "r", "--date", "2026-08-24"],
            ["extract", "--reference", "r", "--wh", "mad1"],
        ):
            with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
                build_parser().parse_args(incomplete)

    def test_validate_requires_reference(self):
        # Without the reference only the checksum could be checked, which proves integrity and
        # not coherence. Coherence is this Source's core guarantee.
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            build_parser().parse_args(["validate", "p"])

    def test_unknown_verb_is_refused(self):
        with self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
            build_parser().parse_args(["publish"])

    def test_version_matches_package(self):
        with self.assertRaises(SystemExit), redirect_stdout(io.StringIO()) as output:
            build_parser().parse_args(["--version"])
        self.assertEqual(output.getvalue().strip(), __version__)


class ExitCodeTest(unittest.TestCase):
    def test_missing_reference_exits_2(self):
        with redirect_stdout(io.StringIO()) as output:
            code = main(
                [
                    "extract",
                    "--reference",
                    "/tmp/no-such-reference-anywhere",
                    "--wh",
                    "mad1",
                    "--date",
                    "2026-08-24",
                    "--out",
                    "/tmp/no-such-output-anywhere",
                ]
            )
        self.assertEqual(code, 2)
        self.assertIn("ERROR", output.getvalue())

    def test_missing_partition_in_validate_exits_1(self):
        with redirect_stdout(io.StringIO()) as output:
            code = main(["validate", "/tmp/no-such-partition", "--reference", "/tmp/x"])
        self.assertEqual(code, 1)
        self.assertIn("manifest not found", output.getvalue())


if __name__ == "__main__":
    unittest.main()
