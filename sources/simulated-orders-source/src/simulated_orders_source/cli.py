"""Interface de linha de comando da Source.

    python -m simulated_orders_source extract  --reference <dir> --wh <wh> --date <dia> [opcoes]
    python -m simulated_orders_source validate <particao> --reference <dir> [--strict]

"extract" NAO busca rede: le os quatro arquivos de referencia que a plataforma pre-computou
do Silver com `retail-platform export-orders-reference` e gera o log de eventos daquele
(armazem, dia). Mesmo espirito do `extract --in <dir>` do ine-callejero-source.

"validate" EXIGE --reference, como na Source de clientes e pelo mesmo motivo: as garantias
centrais desta Source — o produto existe no catalogo daquele armazem naquela data, o preco
pago e o preco observado, o cliente pertence aquele armazem — so podem ser reconferidas
relendo a mesma referencia que a geracao usou. Um validador que so confere checksum provaria
integridade, nao coerencia.

`--date` E OBRIGATORIO no extract, diferente das outras quatro sources. Nelas a particao e
"o snapshot de hoje" e o default de hoje faz sentido; aqui `ingestion_date` e a DATA DO
PEDIDO, e cair no dia corrente por omissao geraria pedidos num dia que talvez nem tenha
catalogo, ou fora da janela exportada.

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

DEFAULT_ROOT = "data/orders"
DEFAULT_SEED = 20260828

EXIT_UNHANDLED = 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simulated_orders_source",
        description=(
            "Source de pedidos simulados: log de eventos com cliente, produto e preco reais, "
            "derivados do Silver de clientes e do catalogo da Mercadona."
        ),
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract_parser = subparsers.add_parser(
        "extract", help="gera o log de eventos de um (armazem, dia) a partir da referencia"
    )
    extract_parser.add_argument(
        "--reference",
        required=True,
        help="diretorio da referencia (saida de `retail-platform export-orders-reference`)",
    )
    extract_parser.add_argument(
        "--wh",
        required=True,
        help=f"armazem da particao, ex.: {'/'.join(DEFAULT_WAREHOUSES)}",
    )
    extract_parser.add_argument(
        "--date",
        required=True,
        help="DIA DO PEDIDO, YYYY-MM-DD. Obrigatorio: aqui ingestion_date e a data do pedido",
    )
    extract_parser.add_argument("--out", default=DEFAULT_ROOT, help="diretorio raiz do snapshot")
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
        "validate",
        help="valida uma particao ja extraida, inclusive a coerencia com catalogo e cliente",
    )
    validate_parser.add_argument(
        "partition",
        help="caminho da particao, ex.: data/orders/ingestion_date=2026-08-24/wh=mad1",
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
