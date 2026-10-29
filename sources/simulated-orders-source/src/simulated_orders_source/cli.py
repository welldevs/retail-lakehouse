"""Command-line interface of the Source.

    python -m simulated_orders_source extract  --reference <dir> --wh <wh> --date <day> [options]
    python -m simulated_orders_source validate <partition> --reference <dir> [--strict]

"extract" does NOT touch the network: it reads the four reference files the platform
pre-computed from Silver with `retail-platform export-orders-reference` and generates the event
log of that (warehouse, day). Same spirit as `extract --in <dir>` in ine-callejero-source.

"validate" REQUIRES --reference, as in the customers Source and for the same reason: this
Source's core guarantees — the product exists in that warehouse's catalog on that date, the
price paid is the observed price, the customer belongs to that warehouse — can only be
re-checked by re-reading the same reference the generation used. A validator that only checks
checksums would prove integrity, not coherence.

`--date` IS REQUIRED for extract, unlike the other four sources. There the partition is
"today's snapshot" and defaulting to today makes sense; here `ingestion_date` is the ORDER
DATE, and silently falling back to the current day would generate orders on a day that may
have no catalog, or that lies outside the exported window.

Exit codes:
    0  success
    1  validation failed
    2  fatal failure: nothing was generated / partition unusable
    3  unhandled exception
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from . import extract as extract_module
from . import validate as validate_module
from .partition import DEFAULT_WAREHOUSES

DEFAULT_ROOT = "data/orders"
DEFAULT_SEED = 20260828

EXIT_UNHANDLED = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulated_orders_source",
        description=(
            "Simulated orders Source: event log with real customer, product and price, "
            "derived from the customers Silver and the Mercadona catalog."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="generate the event log of one (warehouse, day) from the reference"
    )
    extract_parser.add_argument(
        "--reference",
        required=True,
        help="reference directory (output of `retail-platform export-orders-reference`)",
    )
    extract_parser.add_argument(
        "--wh",
        required=True,
        help=f"partition warehouse, e.g.: {'/'.join(DEFAULT_WAREHOUSES)}",
    )
    extract_parser.add_argument(
        "--date",
        required=True,
        help="ORDER DAY, YYYY-MM-DD. Required: here ingestion_date is the order date",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="snapshot root directory")
    extract_parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="generation seed: the same seed reproduces exactly the same output",
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="rewrite an existing partition, even if it is complete",
    )
    extract_parser.set_defaults(handler=extract_module.run)

    validate_parser = subparsers.add_parser(
        "validate",
        help="validate an already extracted partition, including coherence with catalog and customer",
    )
    validate_parser.add_argument(
        "partition",
        help="partition path, e.g.: data/orders/ingestion_date=2026-08-24/wh=mad1",
    )
    validate_parser.add_argument(
        "--reference",
        required=True,
        help="the same reference used for generation: without it coherence cannot be verified",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail on distribution warnings",
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
