"""Interface de linha de comando da plataforma.

    retail-platform land           <particao>
    retail-platform verify-landing <particao>
    retail-platform query          [sql]
    retail-platform duckdb-secret

Codigos de saida, no mesmo espirito do CONTRACT.md secao 7 da Source, para que o
orquestrador decida por codigo e nao por parsing de log:

    0  sucesso
    1  verificacao reprovada
    2  uso invalido / particao inutilizavel
    3  excecao nao tratada
"""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .config import ConfigError, from_env
from .land import LandingError, land
from .manifest import ManifestError
from .verify import verify

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_FATAL = 2
EXIT_UNHANDLED = 3


def _cmd_land(args) -> int:
    config = from_env()
    result = land(config, args.partition)
    print(f"particao ......... {result.partition}")
    print(f"destino .......... s3://{result.bucket}/{result.prefix}")
    print(f"objetos .......... {result.total} ({result.uploaded} enviados, "
          f"{result.skipped} pulados)")
    print(f"bytes enviados ... {result.bytes_uploaded}")
    print("OK: particao aterrissada, _SUCCESS gravado por ultimo.")
    return EXIT_OK


def _cmd_verify(args) -> int:
    config = from_env()
    errors, summary = verify(config, args.partition)
    print(f"particao ......... {summary['partition']}")
    print(f"destino .......... s3://{summary['bucket']}/{summary['prefix']}")
    print(f"run_id ........... {summary['run_id']}")
    print(f"objetos .......... {summary['checked']} lidos e reconferidos")
    print(f"orfaos ........... {summary['orphans']}")
    print(f"bytes ............ {summary['bytes']}")
    print()
    if errors:
        print(f"FALHOU ({len(errors)} problema(s)):")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED
    print("OK: checksums, tamanhos e inventario reconferidos contra o object storage.")
    return EXIT_OK


DEFAULT_QUERY = """
select ingestion_date, warehouse, count(*) as linhas,
       count(distinct source_product_id) as produtos
from silver_product_price group by 1, 2 order by 1
"""


def _cmd_query(args) -> int:
    from .query import connect, connect_lakehouse

    config = from_env()
    # Padrao: views em memoria sobre o parquet, sem tocar o .duckdb. Ver connect_lakehouse.
    connection = (connect(config, database=args.database) if args.database
                  else connect_lakehouse(config))
    sql = args.sql or DEFAULT_QUERY
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    rows = result.fetchall()
    widths = [
        max(len(columns[i]), *(len(str(r[i])) for r in rows)) if rows else len(columns[i])
        for i in range(len(columns))
    ]
    print("  ".join(name.ljust(widths[i]) for i, name in enumerate(columns)))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print("  ".join(str(value).ljust(widths[i]) for i, value in enumerate(row)))
    print(f"\n{len(rows)} linha(s)")
    return EXIT_OK


def _cmd_duckdb_secret(args) -> int:
    from .query import SECRET_NAME, create_persistent_secret

    storage = create_persistent_secret(from_env())
    print(f"secret '{SECRET_NAME}' gravado ({storage}).")
    print("Agora qualquer cliente DuckDB abre o arquivo sem configurar nada:")
    print("    duckdb platform/dbt/retail.duckdb")
    print("    select count(*) from silver_product_price;")
    return EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="retail-platform",
        description="Plataforma: aterrissa e verifica snapshots da Source no object storage.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    land_parser = subparsers.add_parser("land", help="sobe uma particao para o object storage")
    land_parser.add_argument("partition", help="caminho da particao em disco")
    land_parser.set_defaults(handler=_cmd_land)

    verify_parser = subparsers.add_parser(
        "verify-landing", help="rele do object storage e reconfere a particao"
    )
    verify_parser.add_argument("partition", help="caminho da particao em disco")
    verify_parser.set_defaults(handler=_cmd_verify)

    query_parser = subparsers.add_parser(
        "query", help="consulta o Silver (conexao ja configurada para o object storage)"
    )
    query_parser.add_argument(
        "sql", nargs="?", default=None, help="SQL a executar (padrao: resumo por particao)"
    )
    query_parser.add_argument(
        "--database", default=None,
        help="consulta um arquivo .duckdb em vez das views em memoria sobre o parquet",
    )
    query_parser.set_defaults(handler=_cmd_query)

    secret_parser = subparsers.add_parser(
        "duckdb-secret",
        help="grava um secret do DuckDB para que qualquer cliente abra o .duckdb",
    )
    secret_parser.set_defaults(handler=_cmd_duckdb_secret)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except (ManifestError, LandingError, ConfigError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL
    except KeyboardInterrupt:
        print("\ninterrompido pelo usuario", flush=True)
        return EXIT_UNHANDLED
    except Exception as exc:
        print(f"ERRO NAO TRATADO: {type(exc).__name__}: {exc}")
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(main())
