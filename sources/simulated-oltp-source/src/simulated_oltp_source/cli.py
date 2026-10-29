"""Command-line interface of the Source.

    python -m simulated_oltp_source extract  --reference <dir> --wh <wh> [options]
    python -m simulated_oltp_source validate <partition> --reference <dir> [--strict]

"extract" here does NOT touch the network: it reads the reference file the platform
precomputed from Silver with `retail-platform export-oltp-reference` and generates the
customers from it (CONTRACT.md section 2). Same spirit as ine-callejero-source's
`extract --in <dir>`.

"validate" REQUIRES --reference, unlike the other three sources: this Source's central
guarantee — every customer lives at a real address in their warehouse's FUA — can only be
re-checked by re-reading the same reference the generation used.

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

DEFAULT_ROOT = "data/oltp"
DEFAULT_SEED = 20260827

EXIT_UNHANDLED = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulated_oltp_source",
        description=(
            "Simulated OLTP Source: synthetic customers with real address and "
            "demographics, derived from the Callejero Silver and INE population."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="generate one warehouse's customers from the reference"
    )
    extract_parser.add_argument(
        "--reference",
        required=True,
        help="reference directory (output of `retail-platform export-oltp-reference`)",
    )
    extract_parser.add_argument(
        "--wh",
        required=True,
        help=f"partition warehouse, e.g. {'/'.join(DEFAULT_WAREHOUSES)}",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="snapshot root directory")
    extract_parser.add_argument(
        "--date", default=None, help="partition date YYYY-MM-DD (default: today in UTC)"
    )
    # NO DEFAULT, and that is the change. Up to Phase 5 the number came from the command line
    # and the Makefile passed 5,000 for all four warehouses — the same number for FUAs that
    # differ 4.6x in population. Omitting --count now means "use the target the reference
    # derived from that warehouse's adult population", and the reference FAILS if it has none.
    # A default here would silently reintroduce a customer base sized by nobody.
    extract_parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="customers to generate. If omitted, uses the reference's customer_allocation "
             "target (warehouse adult population x penetration rate)",
    )
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
        "validate", help="validate an already extracted partition, including geographic coherence"
    )
    validate_parser.add_argument(
        "partition",
        help="partition path, e.g. data/oltp/ingestion_date=2026-08-27/wh=mad1",
    )
    validate_parser.add_argument(
        "--reference",
        required=True,
        help="the same reference used for generation: without it coherence cannot be verified",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail when there is a distribution warning",
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
