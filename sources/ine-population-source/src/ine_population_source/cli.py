"""Interface de linha de comando da Source.

    python -m ine_population_source extract  [opcoes]
    python -m ine_population_source validate <particao> [--strict]

Codigos de saida:
    0  sucesso
    1  falha parcial (extract) ou validacao reprovada (validate)
    2  falha fatal: nada foi extraido / particao inutilizavel
    3  excecao nao tratada
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
    """--tables aceita uma lista separada por virgula: --tables 31304,9689"""
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ine_population_source",
        description="Source de populacao do INE: series por provincia via API Tempus3.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="extrai um snapshot das tabelas configuradas para uma particao"
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="diretorio raiz do snapshot")
    extract_parser.add_argument(
        "--tables",
        type=_tables_type,
        default=DEFAULT_TABLES,
        help="table_id(s) do INE Tempus3, separados por virgula (padrao: 31304)",
    )
    extract_parser.add_argument(
        "--date", default=None, help="data da particao YYYY-MM-DD (padrao: hoje em UTC)"
    )
    extract_parser.add_argument(
        "--delay", type=float, default=DEFAULT_DELAY, help="intervalo minimo entre requisicoes (s)"
    )
    extract_parser.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT, help="timeout de cada requisicao (s)"
    )
    extract_parser.add_argument(
        "--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="tentativas extras por requisicao"
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="reescreve uma particao existente, inclusive se estiver completa",
    )
    extract_parser.set_defaults(handler=extract_module.run)

    validate_parser = subparsers.add_parser(
        "validate", help="valida uma particao ja extraida"
    )
    validate_parser.add_argument(
        "partition", help="caminho da particao, ex.: data/ine/ingestion_date=2026-08-15"
    )
    validate_parser.add_argument(
        "--strict", action="store_true", help="tambem falha quando ha serie sem valor"
    )
    validate_parser.set_defaults(handler=validate_module.run)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except KeyboardInterrupt:
        print("\ninterrompido pelo usuario", flush=True)
        return EXIT_UNHANDLED
    except Exception as exc:  # nenhum traceback escapa com codigo ambiguo
        print(f"ERRO NAO TRATADO: {type(exc).__name__}: {exc}", flush=True)
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(main())
