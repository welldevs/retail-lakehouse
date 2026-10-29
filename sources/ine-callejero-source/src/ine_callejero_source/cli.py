"""Command-line interface of the Source.

    python -m ine_callejero_source extract  --in <dir> [options]
    python -m ine_callejero_source validate <partition> [--strict]

"extract" here does NOT touch the network: it ingests Callejero files already downloaded
manually from the INE website and placed in --in (CONTRACT.md section 2).

Exit codes:
    0  success
    1  partial failure (extract) or validation failed (validate)
    2  fatal failure: nothing was ingested / partition unusable
    3  unhandled exception
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from . import intake as intake_module
from . import validate as validate_module
from .partition import DEFAULT_PROVINCES

DEFAULT_ROOT = "data/callejero"

EXIT_UNHANDLED = 3


def _provinces_type(value: str) -> list[str]:
    """--provinces takes a comma-separated list: --provinces 08,28,41,46"""
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ine_callejero_source",
        description="INE Callejero Source: official geography (census sections, population nuclei, streets, street segments with postal code) per province.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract",
        help="ingest into a partition the Callejero files already downloaded into --in",
    )
    extract_parser.add_argument(
        "--in",
        dest="in_dir",
        required=True,
        help="directory where the Callejero files were downloaded manually",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="snapshot root directory")
    extract_parser.add_argument(
        "--provinces",
        type=_provinces_type,
        default=list(DEFAULT_PROVINCES),
        help="INE province code(s), comma-separated (default: 08,28,41,46)",
    )
    extract_parser.add_argument(
        "--date", default=None, help="partition date YYYY-MM-DD (default: today in UTC)"
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="rewrite an existing partition, even if it is complete",
    )
    extract_parser.set_defaults(handler=intake_module.run)

    validate_parser = subparsers.add_parser(
        "validate", help="validate an already extracted partition"
    )
    validate_parser.add_argument(
        "partition", help="partition path, e.g. data/callejero/ingestion_date=2026-08-25"
    )
    validate_parser.add_argument(
        "--strict", action="store_true", help="also fail when there is a diverging line-width warning"
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
    except Exception as exc:  # no traceback escapes with an ambiguous code
        print(f"UNHANDLED ERROR: {type(exc).__name__}: {exc}", flush=True)
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(main())
