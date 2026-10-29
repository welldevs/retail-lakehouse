"""Command-line interface of the Source.

    python -m ine_population_source extract  [options]
    python -m ine_population_source validate <partition> [--strict]

Exit codes:
    0  success
    1  partial failure (extract) or validation failed (validate)
    2  fatal failure: nothing was extracted / partition unusable
    3  unhandled exception
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from . import extract as extract_module
from . import validate as validate_module

DEFAULT_ROOT = "data/ine"
DEFAULT_TABLES = ["31304"]
DEFAULT_DELAY = 0.5
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_RETRIES = 5

EXIT_UNHANDLED = 3


def _tables_type(value: str) -> list[str]:
    """--tables accepts a comma-separated list: --tables 31304,9689"""
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ine_population_source",
        description="INE population Source: series by province via the Tempus3 API.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="extract a snapshot of the configured tables into a partition"
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="snapshot root directory")
    extract_parser.add_argument(
        "--tables",
        type=_tables_type,
        default=DEFAULT_TABLES,
        help="INE Tempus3 table_id(s), comma-separated (default: 31304)",
    )
    extract_parser.add_argument(
        "--date", default=None, help="partition date YYYY-MM-DD (default: today in UTC)"
    )
    extract_parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY, help="minimum interval between requests (s)"
    )
    extract_parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT, help="timeout of each request (s)"
    )
    extract_parser.add_argument(
        "--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="extra attempts per request"
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="rewrite an existing partition, even if it is complete",
    )
    extract_parser.set_defaults(handler=extract_module.run)

    validate_parser = subparsers.add_parser(
        "validate", help="validate an already extracted partition"
    )
    validate_parser.add_argument(
        "partition", help="partition path, e.g. data/ine/ingestion_date=2026-08-15"
    )
    validate_parser.add_argument(
        "--strict", action="store_true", help="also fail when a series has no value"
    )
    validate_parser.set_defaults(handler=validate_module.run)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except KeyboardInterrupt:
        print("\ninterrupted by user", flush=True)
        return EXIT_UNHANDLED
    except Exception as exc:  # no traceback escapes with an ambiguous exit code
        print(f"UNHANDLED ERROR: {type(exc).__name__}: {exc}", flush=True)
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(main())
