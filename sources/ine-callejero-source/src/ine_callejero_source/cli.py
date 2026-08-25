"""Interface de linha de comando da Source.

    python -m ine_callejero_source extract  --in <dir> [opcoes]
    python -m ine_callejero_source validate <particao> [--strict]

"extract" aqui NAO busca rede: incorpora arquivos do Callejero ja baixados
manualmente do site do INE e colocados em --in (CONTRACT.md secao 2).

Codigos de saida:
    0  sucesso
    1  falha parcial (extract) ou validacao reprovada (validate)
    2  falha fatal: nada foi incorporado / particao inutilizavel
    3  excecao nao tratada
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
    """--provinces aceita uma lista separada por virgula: --provinces 08,28,41,46"""
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ine_callejero_source",
        description="Source do Callejero do INE: geografia oficial (secoes, nucleos, ruas) por provincia.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract",
        help="incorpora, para uma particao, os arquivos do Callejero ja baixados em --in",
    )
    extract_parser.add_argument(
        "--in",
        dest="in_dir",
        required=True,
        help="diretorio onde os arquivos do Callejero foram baixados manualmente",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="diretorio raiz do snapshot")
    extract_parser.add_argument(
        "--provinces",
        type=_provinces_type,
        default=list(DEFAULT_PROVINCES),
        help="codigo(s) de provincia do INE, separados por virgula (padrao: 08,28,41,46)",
    )
    extract_parser.add_argument(
        "--date", default=None, help="data da particao YYYY-MM-DD (padrao: hoje em UTC)"
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="reescreve uma particao existente, inclusive se estiver completa",
    )
    extract_parser.set_defaults(handler=intake_module.run)

    validate_parser = subparsers.add_parser(
        "validate", help="valida uma particao ja extraida"
    )
    validate_parser.add_argument(
        "partition", help="caminho da particao, ex.: data/callejero/ingestion_date=2026-08-25"
    )
    validate_parser.add_argument(
        "--strict", action="store_true", help="tambem falha quando ha aviso de largura de linha divergente"
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
