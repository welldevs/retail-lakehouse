"""Interface de linha de comando da Source.

    python -m simulated_oltp_source extract  --reference <dir> --wh <wh> [opcoes]
    python -m simulated_oltp_source validate <particao> --reference <dir> [--strict]

"extract" aqui NAO busca rede: le o arquivo de referencia que a plataforma pre-computou
do Silver com `retail-platform export-oltp-reference` e gera os clientes a partir dele
(CONTRACT.md secao 2). Mesmo espirito do `extract --in <dir>` do ine-callejero-source.

"validate" EXIGE --reference, diferente das outras tres sources: a garantia central desta
Source — todo cliente mora num endereco real da AUF do seu warehouse — so pode ser
reconferida relendo a mesma referencia que a geracao usou.

Codigos de saida:
    0  sucesso
    1  validacao reprovada
    2  falha fatal: nada foi gerado / particao inutilizavel
    3  excecao nao tratada
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from . import extract as extract_module
from . import validate as validate_module
from .partition import DEFAULT_WAREHOUSES

DEFAULT_ROOT = "data/oltp"
DEFAULT_REFERENCE = "data/oltp-reference"
DEFAULT_SEED = 20260827

EXIT_UNHANDLED = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulated_oltp_source",
        description=(
            "Source de OLTP simulado: clientes sinteticos com endereco e demografia "
            "reais, derivados do Silver do Callejero e da populacao do INE."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="gera os clientes de um warehouse a partir da referencia"
    )
    extract_parser.add_argument(
        "--reference",
        required=True,
        help="diretorio da referencia (saida de `retail-platform export-oltp-reference`)",
    )
    extract_parser.add_argument(
        "--wh",
        required=True,
        help=f"armazem da particao, ex.: {'/'.join(DEFAULT_WAREHOUSES)}",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="diretorio raiz do snapshot")
    extract_parser.add_argument(
        "--date", default=None, help="data da particao YYYY-MM-DD (padrao: hoje em UTC)"
    )
    # SEM DEFAULT, e isso e a mudanca. Ate a Fase 5 o numero vinha da linha de comando e o
    # Makefile passava 5.000 para os quatro armazens — o mesmo numero para AUFs que diferem
    # por 4,6x em populacao. Omitir --count agora significa "use o alvo que a referencia
    # derivou da populacao adulta daquele armazem", e a referencia REPROVA se nao tiver um.
    # Um default aqui reintroduziria em silencio a base dimensionada por ninguem.
    extract_parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="clientes a gerar. Omitido, usa o alvo de customer_allocation da referencia "
             "(populacao adulta do armazem x taxa de penetracao)",
    )
    extract_parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="semente da geracao: a mesma seed reproduz exatamente a mesma saida",
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="reescreve uma particao existente, inclusive se estiver completa",
    )
    extract_parser.set_defaults(handler=extract_module.run)

    validate_parser = subparsers.add_parser(
        "validate", help="valida uma particao ja extraida, inclusive a coerencia geografica"
    )
    validate_parser.add_argument(
        "partition",
        help="caminho da particao, ex.: data/oltp/ingestion_date=2026-08-27/wh=mad1",
    )
    validate_parser.add_argument(
        "--reference",
        required=True,
        help="a mesma referencia usada na geracao: sem ela a coerencia nao e verificavel",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="tambem falha quando ha aviso de distribuicao",
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
