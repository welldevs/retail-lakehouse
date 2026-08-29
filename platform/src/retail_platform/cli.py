"""Interface de linha de comando da plataforma.

    retail-platform land                  <particao>
    retail-platform verify-landing        <particao>
    retail-platform query                 [sql]
    retail-platform duckdb-secret
    retail-platform has-data              <prefixo-da-source>
    retail-platform prune-local           <particao>
    retail-platform export-oltp-reference [--out --date --seeds-dir]
    retail-platform export-orders-reference --from --to [--out --date --seeds-dir]
    retail-platform orders-oltp-ddl
    retail-platform orders-oltp-init      [--dsn --reset]
    retail-platform orders-apply          <particao> [--dsn --no-verify]
    retail-platform orders-outbox         [--dsn --verify <particao>]
    retail-platform orders-projection-init [--projection-dsn]
    retail-platform orders-publish        [--bootstrap --topic --follow --batch-size]
    retail-platform orders-project        [--bootstrap --topic --group --from-beginning]
    retail-platform orders-lag            [--bootstrap --topic --group]
    retail-platform orders-replay         [--bootstrap --topic --group]
    retail-platform iceberg-init          [--warehouse]
    retail-platform iceberg-metadata      [--quiet]
    retail-platform orders-rebuild-projection [--root]
    retail-platform orders-reconcile      [--dsn]

Codigos de saida, no mesmo espirito do CONTRACT.md secao 7 da Source, para que o
orquestrador decida por codigo e nao por parsing de log:

    0  sucesso
    1  verificacao reprovada
    2  uso invalido / particao inutilizavel
    3  excecao nao tratada
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone

from . import __version__
from .config import ConfigError, from_env
from .land import LandingError, land
from .manifest import ManifestError
from .oltp_reference import DEFAULT_SEEDS_DIR
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


def _cmd_prune_local(args) -> int:
    """Remove a copia LOCAL de uma particao, mas so depois de provar que ela esta intacta
    no object storage.

    O `data/` local e scratch de extracao: depois de `land` + `verify-landing`, o object
    storage e a verdade e a copia local e redundante. Ela nao e pequena — uma particao do
    INE ocupa entre 264 e 384 MB, e nada nunca era apagado.

    A ordem aqui e a garantia, e sao DUAS conferencias antes de qualquer remocao:

      1. a copia local bate com o proprio manifesto (sha256 e tamanho recalculados);
      2. o destino bate com esse mesmo manifesto — a mesma verificacao do verify-landing.

    A primeira nao e redundante. `verify-landing` compara o OBJETO com o MANIFESTO, e
    passaria com um arquivo local corrompido depois de ter sido landado — a remocao
    pareceria segura sem que nada tivesse conferido os bytes locais. Sem (1), a promessa
    "so apago o que provei estar intacto" seria maior do que o que o codigo faz.

    Nao ha modo --force: uma particao que nao esta integra dos dois lados nao e
    redundante, e o unico jeito de perder dado seria apagar exatamente essa.
    """
    import shutil

    from .manifest import ManifestError
    from .manifest import read as read_manifest

    config = from_env()
    partition = os.path.normpath(args.partition)

    # Guarda contra um argumento errado virar um rm -rf em algo que nao e particao.
    manifest_file = os.path.join(partition, "_manifest.json")
    if not os.path.isfile(manifest_file):
        print(f"ERRO: {partition} nao parece uma particao (sem _manifest.json). Nada foi apagado.")
        return EXIT_FATAL

    # (1) a copia local ainda e o que ela declara ser?
    try:
        declared = read_manifest(partition)
    except ManifestError as exc:
        print(f"ERRO: manifesto local inutilizavel ({exc}). Nada foi apagado.")
        return EXIT_FATAL

    locais: list[str] = []
    for entry in declared.files:
        if not os.path.exists(entry.local_path):
            locais.append(f"arquivo local ausente: {entry.path}")
            continue
        sha, size = entry.digest_on_disk()
        if sha != entry.sha256:
            locais.append(f"sha256 local divergente do manifesto: {entry.path}")
        elif size != entry.bytes:
            locais.append(f"tamanho local divergente do manifesto: {entry.path}")
    if locais:
        print(f"particao ......... {partition}")
        print()
        print(f"RECUSADO ({len(locais)} problema(s)) — a copia local NAO foi apagada:")
        for item in locais:
            print(f"  - {item}")
        print("A copia local nao corresponde ao proprio manifesto: olhe antes de remover.")
        return EXIT_FAILED

    # (2) o destino bate com o mesmo manifesto?
    errors, summary = verify(config, partition)
    if errors:
        print(f"particao ......... {summary['partition']}")
        print(f"destino .......... s3://{summary['bucket']}/{summary['prefix']}")
        print()
        print(f"RECUSADO ({len(errors)} problema(s)) — a copia local NAO foi apagada:")
        for item in errors:
            print(f"  - {item}")
        return EXIT_FAILED

    bytes_local = sum(
        os.path.getsize(os.path.join(base, name))
        for base, _, names in os.walk(partition)
        for name in names
    )
    shutil.rmtree(partition)
    print(f"particao ......... {summary['partition']}")
    print(f"destino .......... s3://{summary['bucket']}/{summary['prefix']}")
    print(f"arquivos locais .. {len(declared.files)} reconferidos contra o manifesto")
    print(f"objetos .......... {summary['checked']} reconferidos no destino")
    print(f"liberado ......... {bytes_local} bytes")
    print("OK: copia local removida; o object storage continua com a particao integra.")
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


def _cmd_has_data(args) -> int:
    """Existe ao menos um objeto aterrissado sob <raw_bucket>/<prefixo>/?

    Existe para o Makefile decidir, ANTES de chamar `dbt build`, se exclui do build o
    modelo Silver de uma source que ainda nao aterrissou nada — sem isso, `dbt build`
    do projeto inteiro falharia so por causa de uma source recem-adicionada, mesmo
    quando as demais tem dado normalmente. Generico: qualquer source pode ser
    consultada pelo prefixo, nao so uma em especial.
    """
    config = from_env()
    prefix = args.prefix.rstrip("/") + "/"
    response = config.client().list_objects_v2(
        Bucket=config.raw_bucket, Prefix=prefix, MaxKeys=1
    )
    return EXIT_OK if response.get("KeyCount", 0) > 0 else EXIT_FAILED


def _cmd_export_oltp_reference(args) -> int:
    """Materializa o Silver como tres JSON planos para a Source de OLTP simulado.

    A Source e FROZEN (dependencies = []) e nao pode abrir conexao com o Lakehouse; este
    subcomando e a unica ponte, e ela e um contrato fisico, nao um import. Ver
    oltp_reference.py.
    """
    from .oltp_reference import ReferenceExportError, export

    config = from_env()
    for prefix in ("ine_callejero", "ine_population_api"):
        response = config.client().list_objects_v2(
            Bucket=config.raw_bucket, Prefix=f"{prefix}/", MaxKeys=1
        )
        if not response.get("KeyCount", 0):
            print(
                f"ERRO: nada aterrissado sob {prefix}/. Rode `make callejero-refresh` e "
                f"`make ine-refresh` antes de exportar a referencia."
            )
            return EXIT_FATAL

    out_dir = os.path.join(args.out, f"ingestion_date={args.date}")
    try:
        summary = export(config, out_dir, seeds_dir=args.seeds_dir)
    except ReferenceExportError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    print(f"destino .......... {summary['out_dir']}")
    print(f"warehouses ....... {', '.join(summary['warehouses'])}")
    print(f"enderecos ........ {summary['address_candidates']} candidatos "
          f"({summary['orphan_tramos_excluded']} tramo(s) orfao(s) excluido(s))")
    print(f"municipios ....... {summary['municipalities']}")
    print(f"idades ........... {summary['age_rows']} linhas "
          f"(year={summary['age_year']} fk_periodo={summary['age_fk_periodo']})")
    print(f"populacao ........ year={summary['population_year']}")
    for name, size in sorted(summary["bytes"].items()):
        print(f"  {name} ... {size} bytes")
    print("OK: referencia exportada.")
    return EXIT_OK


def _cmd_export_orders_reference(args) -> int:
    """Materializa o Silver como quatro JSON planos para a Source de Orders simulados.

    Mesma ponte de `export-oltp-reference`, um nivel adiante: a Source e FROZEN e nao pode
    abrir conexao com o Lakehouse, entao cliente, catalogo, calendario de preco e a tabela de
    premissas atravessam como contrato fisico. Ver orders_reference.py.
    """
    from .orders_reference import OrdersReferenceError, export

    config = from_env()
    for prefix in ("mercadona_catalog_api", "simulated_oltp"):
        response = config.client().list_objects_v2(
            Bucket=config.raw_bucket, Prefix=f"{prefix}/", MaxKeys=1
        )
        if not response.get("KeyCount", 0):
            print(
                f"ERRO: nada aterrissado sob {prefix}/. Um pedido precisa de cliente e de "
                f"catalogo reais: rode `make daily` e `make oltp-refresh-all` antes."
            )
            return EXIT_FATAL

    out_dir = os.path.join(args.out, f"ingestion_date={args.date}")
    try:
        summary = export(
            config, out_dir, args.window_from, args.window_to, seeds_dir=args.seeds_dir
        )
    except OrdersReferenceError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    print(f"destino .......... {summary['out_dir']}")
    print(f"janela ........... {summary['window_from']} a {summary['window_to']} "
          f"({summary['days']} dia(s))")
    print(f"warehouses ....... {', '.join(summary['warehouses'])}")
    print(f"clientes ......... {summary['customers']} "
          f"(geracoes: {', '.join(summary['customer_ingestion_dates'])})")
    print(f"catalogo ......... {summary['catalog_rows']} linhas em "
          f"{len(summary['catalog_ingestion_dates'])} snapshot(s): "
          f"{', '.join(summary['catalog_ingestion_dates'])}")
    print(f"carry-forward .... {summary['carried_forward_rows']} par(es) (armazem, dia) "
          f"sem preco observado no proprio dia")
    print(f"premissas ........ sha256={summary['premises_sha256'][:16]}... (todas sinteticas)")
    for name, size in sorted(summary["bytes"].items()):
        print(f"  {name} ... {size} bytes")
    print("OK: referencia exportada.")
    return EXIT_OK


def _cmd_orders_oltp_ddl(args) -> int:
    """Imprime a DDL do OLTP sem conectar em nada.

    Mesmo motivo de `snowflake-ddl`: rever o esquema — os CHECKs, o indice parcial, o
    vocabulario de status — nao deveria exigir um banco de pe. E o que a plataforma promete,
    em texto, antes de prometer em rede.
    """
    from .orders_oltp import DDL

    print(DDL.strip())
    return EXIT_OK


def _cmd_orders_oltp_init(args) -> int:
    from .orders_oltp import OrdersOltpError, connect, create_schema, drop_schema

    try:
        with connect(args.dsn) as connection:
            if args.reset:
                # `prune-local` nao tem `--force` porque apagar uma particao aterrissada e
                # decisao de quem opera. Aqui o criterio e outro e a assimetria e
                # deliberada: o OLTP e uma REPLICA do log, reconstruivel em minutos por
                # `orders-apply`. O que ele guarda de proprio — `published_at` — se refaz
                # drenando o outbox de novo. Nada aqui e a unica copia de nada.
                print("--reset: derrubando orders, order_line e outbox...")
                drop_schema(connection)
            create_schema(connection)
        print(f"dsn .............. {args.dsn}")
        print("OK: esquema do OLTP pronto (orders, order_line, outbox).")
        return EXIT_OK
    except OrdersOltpError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL


def _cmd_orders_apply(args) -> int:
    """Replica o log de eventos no OLTP: estado e outbox na MESMA transacao, por evento.

    E aqui que o gatilho do Kafka passa a estar satisfeito. Nao porque um evento foi para
    algum lugar, mas porque ele passou a NASCER dentro da transacao que muda o pedido —
    que e a unica forma de o topico e o banco nao divergirem em silencio.
    """
    from .orders_oltp import OrdersOltpError, apply_partition, connect

    try:
        with connect(args.dsn) as connection:
            result = apply_partition(connection, args.partition, verify=not args.no_verify)
    except (OrdersOltpError, ManifestError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    print(f"particao ......... {result.partition}")
    print(f"eventos no log ... {result.events}")
    print(f"aplicados ........ {result.applied}")
    print(f"ja aplicados ..... {result.skipped} (event_id ja no outbox — replay idempotente)")
    print(f"pedidos .......... {result.orders}")
    for kind, count in sorted(result.by_type.items()):
        print(f"  {kind} ... {count}")
    print("OK: estado e outbox gravados na mesma transacao, um evento por transacao.")
    return EXIT_OK


def _cmd_orders_outbox(args) -> int:
    from .orders_oltp import OrdersOltpError, connect, outbox_summary, verify_outbox

    try:
        with connect(args.dsn) as connection:
            summary = outbox_summary(connection)
            proof = verify_outbox(connection, args.verify) if args.verify else None
    except (OrdersOltpError, ManifestError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    print(f"linhas no outbox . {summary['outbox_rows']}")
    print(f"nao publicadas ... {summary['unpublished']}")
    print(f"pedidos .......... {summary['orders_in_outbox']}")
    for kind, count in sorted(summary["by_event_type"].items()):
        print(f"  {kind} ... {count}")
    print("estado dos pedidos no OLTP:")
    for status, count in sorted(summary["orders_by_status"].items()):
        print(f"  {status} ... {count}")

    if proof is None:
        return EXIT_OK

    print()
    print(f"particao ......... {proof['partition']}")
    print(f"log declarado .... {proof['declared_rows']} linhas, {proof['declared_bytes']} bytes")
    print(f"                   sha256 {proof['declared_sha256']}")
    print(f"log do outbox .... {proof['outbox_rows']} linhas, {proof['outbox_bytes']} bytes")
    print(f"                   sha256 {proof['outbox_sha256']}")
    if not proof["matches"]:
        print(
            "REPROVADO: o log reconstituido do outbox NAO reproduz o do manifesto. "
            "O outbox perdeu, duplicou ou alterou evento."
        )
        return EXIT_FAILED
    print("OK: o outbox reconstitui o log da particao byte a byte.")
    return EXIT_OK


def _cmd_orders_projection_init(args) -> int:
    from .orders_stream import (
        OrdersStreamError, create_projection_schema, ensure_database, projection_connect,
    )

    try:
        created = ensure_database(args.projection_dsn)
        with projection_connect(args.projection_dsn) as connection:
            create_projection_schema(connection)
    except OrdersStreamError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL
    print(f"dsn .............. {args.projection_dsn}")
    print(f"banco ............ {'criado' if created else 'ja existia'}")
    print("OK: live_order_state pronta. E um READ MODEL: banco separado do OLTP de proposito.")
    return EXIT_OK


def _cmd_orders_publish(args) -> int:
    """Drena o outbox para o topico. AT-LEAST-ONCE, e isso esta escrito, nao escondido.

    Publicar antes de marcar `published_at` significa que morrer no meio REPUBLICA. A ordem
    inversa PERDERIA. Perder e irreversivel; duplicar e absorvivel pelo consumidor — e e por
    isso que o consumidor tem de ser idempotente por obrigacao, nao por elegancia.
    """
    from .orders_oltp import OrdersOltpError, connect
    from .orders_stream import OrdersStreamError, build_producer, publish

    try:
        producer = build_producer(args.bootstrap)
        with connect(args.dsn) as connection:
            total_published = 0
            rounds = 0
            while True:
                result = publish(connection, producer, args.topic,
                                 batch_size=args.batch_size)
                total_published += result.published
                rounds += 1
                if not args.follow:
                    break
                if result.published:
                    print(f"  lote {rounds}: {result.published} publicados")
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\ninterrompido. {total_published} evento(s) publicados e marcados.")
        return EXIT_OK
    except (OrdersOltpError, OrdersStreamError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    print(f"broker ........... {args.bootstrap}")
    print(f"topico ........... {args.topic}")
    print(f"publicados ....... {total_published}")
    print("OK: outbox drenado. Semantica: at-least-once do outbox ao broker.")
    return EXIT_OK


def _cmd_orders_project(args) -> int:
    """Consome o topico e mantem `live_order_state`. Idempotente por (order_id, sequence_no).

    Deduplicar por sequencia contra o estado que ja existe — e nao por um conjunto de
    event_id que cresce sem limite — so e possivel porque a ordem por chave e garantida.
    A mesma comparacao detecta BURACO, que e perda, e recusa em vez de aplicar.
    """
    import contextlib

    from .orders_stream import (
        OrdersStreamError, PostgresProjection, build_consumer, project,
        projection_connect, read_sla_minutes,
    )

    # A COSTURA DECLARADA NO MARCO 5, exercida. Trocar o armazenamento do read model nao muda
    # uma linha de `project()`: os dois sinks expoem `load/save/commit/rollback/count/digest`
    # e nada mais. Se este ramo precisasse de um metodo a mais, a costura teria vazado.
    def abrir_sink():
        if args.sink == "iceberg":
            from .orders_projection import IcebergProjection, catalog, ensure_table

            cat = catalog()
            return contextlib.nullcontext(
                IcebergProjection(ensure_table(cat), cat=cat)
            )
        connection = projection_connect(args.projection_dsn)
        return contextlib.closing(_PostgresSink(connection))

    class _PostgresSink(PostgresProjection):
        def close(self):
            self._connection.close()

    try:
        sla = args.sla_minutes or read_sla_minutes(args.seeds_dir)
        consumer = build_consumer(args.bootstrap, args.group, topic=args.topic,
                                  from_beginning=args.from_beginning)
        with abrir_sink() as projection:
            before = projection.digest()
            try:
                result = project(consumer, projection, sla_minutes=sla,
                                 batch_size=args.batch_size,
                                 idle_timeout=args.idle_timeout,
                                 strict=not args.allow_gaps)
            finally:
                consumer.close()
            after = projection.digest()
            rows = projection.count()
    except OrdersStreamError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED
    except Exception as exc:
        from .orders_projection import OrdersProjectionError

        if not isinstance(exc, OrdersProjectionError):
            raise
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    print(f"broker ........... {args.bootstrap}")
    print(f"grupo ............ {args.group}")
    print(f"sink ............. {args.sink}")
    print(f"SLA de separacao . {sla} min (de order_premises_seed.csv)")
    print(f"aplicados ........ {result.applied}")
    print(f"duplicatas ....... {result.duplicates} (descartadas por sequence_no)")
    print(f"buracos .......... {len(result.gaps)}")
    print(f"pedidos na projecao {rows}")
    print(f"SLA estourado .... {result.sla_breaches}")
    print(f"digest antes ..... {before[:32]}")
    print(f"digest depois .... {after[:32]}")
    if before == after and result.applied == 0:
        print("OK: nada novo — a projecao ja estava em dia (replay idempotente).")
    else:
        print("OK: offset commitado DEPOIS da escrita — at-least-once na entrega, "
              "efeito exactly-once na projecao.")
    return EXIT_OK


def _cmd_orders_lag(args) -> int:
    from .orders_stream import OrdersStreamError, group_lag

    try:
        lag = group_lag(args.bootstrap, args.topic, args.group)
    except OrdersStreamError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED
    print(f"topico ........... {args.topic}")
    print(f"grupo ............ {args.group}")
    total = 0
    for partition in sorted(lag):
        entry = lag[partition]
        total += entry["lag"]
        print(f"  particao {partition} ... commitado={entry['committed']} "
              f"high={entry['high']} lag={entry['lag']}")
    print(f"lag total ........ {total}")
    return EXIT_OK


def _cmd_orders_replay(args) -> int:
    """Rebobina o grupo para o inicio. NAO apaga a projecao — esse e o ponto.

    Reprocessar tudo POR CIMA do estado existente e o que prova consumo idempotente. Apagar
    antes provaria que o fold e deterministico, que e outra coisa e ja esta provada.
    """
    from .orders_stream import OrdersStreamError, reset_group

    try:
        partitions = reset_group(args.bootstrap, args.topic, args.group)
    except OrdersStreamError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED
    print(f"grupo {args.group!r} rebobinado em {partitions} particao(oes).")
    print("A projecao NAO foi apagada: reprocessar por cima e o que prova idempotencia.")
    print("Rode `make orders-project` para reprocessar.")
    return EXIT_OK


def _cmd_iceberg_init(args) -> int:
    from .orders_projection import (
        OrdersProjectionError, catalog, ensure_catalog_database, ensure_table,
    )

    try:
        criado = ensure_catalog_database()
        cat = catalog(warehouse=args.warehouse)
        tabela = ensure_table(cat)
    except OrdersProjectionError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL
    print(f"banco do catalogo  {'criado' if criado else 'ja existia'}")
    print(f"warehouse ........ {args.warehouse}")
    print(f"tabela ........... {tabela.name()}")
    print(f"metadado corrente  {tabela.metadata_location}")
    print("OK: live_order_state pronta em Iceberg, com catalogo SQL no Postgres do OLTP.")
    return EXIT_OK


def _cmd_iceberg_metadata(args) -> int:
    """Onde esta o metadado CORRENTE, segundo o catalogo.

    O `make silver` chama isto para passar o caminho como var ao dbt. O DuckDB recusa
    descobrir sozinho — e a recusa esta certa: varrer o storage atras do metadado mais novo
    pode encontrar um que ainda nao foi commitado.
    """
    from .orders_projection import OrdersProjectionError, metadata_location

    try:
        caminho = metadata_location()
    except OrdersProjectionError as exc:
        if not args.quiet:
            print(f"ERRO: {exc}")
        return EXIT_FAILED
    print(caminho)
    return EXIT_OK


def _cmd_orders_rebuild_projection(args) -> int:
    """A camada em LOTE do par: reconstroi live_order_state a partir do RAW.

    E o segundo escritor da mesma tabela — que e literalmente o gatilho do Iceberg. Ele
    compartilha o fold com o consumidor, e por isso o acordo entre os dois NAO e evidencia de
    correcao: e evidencia de que nao se atropelam. A correcao vem de `orders-reconcile`.
    """
    from .orders_projection import (
        OrdersProjectionError, catalog, partitions_of, rebuild,
    )

    particoes = partitions_of(args.root, through=args.through)
    if not particoes:
        print(f"ERRO: nenhuma particao completa sob {args.root}.")
        return EXIT_FATAL
    try:
        resultado = rebuild(catalog(), particoes, seeds_dir=args.seeds_dir)
    except (OrdersProjectionError, ManifestError) as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    print(f"particoes lidas .. {resultado.partitions}")
    print(f"eventos .......... {resultado.events}")
    print(f"pedidos .......... {resultado.orders}")
    print(f"commits .......... {resultado.commits}")
    print(f"conflitos ........ {resultado.conflicts} (resolvidos por releitura e retry)")
    print(f"linhas descartadas {resultado.dropped_as_stale} (o streaming ja tinha estado "
          f"igual ou mais novo)")
    print("OK: reconstrucao em lote gravada na MESMA tabela que o streaming escreve.")
    return EXIT_OK


def _cmd_orders_reconcile(args) -> int:
    """Compara os tres folds. Divergiu, sai 1.

    `silver_order` e a unica das tres que nao compartilha codigo com nenhuma outra — window
    function em SQL sobre o log inteiro. E contra ela que isto vale como verificacao.
    """
    from .orders_projection import OrdersProjectionError, reconcile

    try:
        relatorio = reconcile(dsn=args.dsn)
    except OrdersProjectionError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FAILED

    for nome, quantidade in sorted(relatorio["orders"].items()):
        print(f"pedidos em {nome:.<10} {quantidade}")
    print(f"comparados ....... {relatorio['compared']}")
    for nome, faltando in sorted(relatorio["missing"].items()):
        print(f"AUSENTES em {nome}: {', '.join(faltando)}")
    if relatorio["divergences"]:
        print(f"divergencias ..... {relatorio['divergence_count']}")
        for linha in relatorio["divergences"]:
            valores = " ".join(
                f"{k}={linha[k]!r}" for k in ("iceberg", "silver", "oltp")
            )
            print(f"  {linha['order_id']} {linha['coluna']}: {valores}")
    if not relatorio["ok"]:
        print("REPROVADO: os folds divergiram. Stream e lote NAO convergiram.")
        return EXIT_FAILED
    print("OK: Iceberg, Silver e OLTP concordam em todos os pedidos comparados.")
    return EXIT_OK


def _cmd_export_snowflake(args) -> int:
    """Recorta o Silver para parquet local. NAO fala com o Snowflake.

    Separado de `load-snowflake` pelo mesmo motivo que `land` e `verify-landing` sao
    separados: produzir o artefato e move-lo falham por razoes diferentes, e juntar os
    dois num verbo so tornaria "o recorte esta errado" indistinguivel de "a rede caiu".
    """
    from .snowflake_export import SnowflakeExportError, export

    try:
        resumo = export(from_env(), args.out)
    except SnowflakeExportError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    total = sum(d["rows"] for d in resumo.values())
    print(f"destino .......... {args.out}")
    for nome in sorted(resumo):
        d = resumo[nome]
        print(f"  {nome:<30} {d['rows']:>9,} linhas  {d['bytes'] / 1024:>8.1f} KB  {d['grain']}")
    print(f"  {'TOTAL':<30} {total:>9,} linhas")
    print("OK: recorte do Silver escrito em parquet.")
    return EXIT_OK


def _cmd_snowflake_ddl(args) -> int:
    """Imprime o DDL do STAGE. Nao conecta em lugar nenhum — serve para revisao."""
    from .query import connect_lakehouse
    from .snowflake_export import dump_ddl

    connection = connect_lakehouse(from_env())
    try:
        print(dump_ddl(connection, args.database))
    finally:
        connection.close()
    return EXIT_OK


def _cmd_snowflake_bootstrap(args) -> int:
    """Database, schemas, papeis e grants. Roda uma vez, exige ACCOUNTADMIN."""
    from .snowflake_load import ROLES, SnowflakeLoadError, bootstrap, connect

    try:
        connection = connect(args.connection)
    except SnowflakeLoadError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    cursor = connection.cursor()
    try:
        feito = bootstrap(cursor, args.database, args.grant_to_user, args.warehouse)
    except Exception as exc:
        print(f"ERRO: o destino recusou: {exc}")
        return EXIT_FAILED
    finally:
        cursor.close()
        connection.close()

    print(f"destino .......... {args.database} (conexao '{args.connection}')")
    print(f"comandos ......... {len(feito)} aplicados")
    for nome, descricao in ROLES.items():
        print(f"  {nome:<20} {descricao}")

    # Grant aplicado nao e grant funcionando. A verificacao roda com `use secondary roles
    # none`, sem o qual ela passaria por engano numa conta onde o usuario tambem tem
    # ACCOUNTADMIN — ver check_isolation.
    from .snowflake_load import check_isolation

    problemas = check_isolation(
        lambda papel: connect(args.connection, role=papel), args.database
    )
    if problemas:
        print("\nISOLAMENTO NAO CONFERE:")
        for p in problemas:
            print(f"  - {p}")
        return EXIT_FAILED
    print("isolamento ....... conferido papel a papel (com secondary roles desligados)")
    print("OK: schemas, papeis e grants aplicados e verificados.")
    return EXIT_OK


def _cmd_load_snowflake(args) -> int:
    """Cria as tabelas STAGE, sobe os parquet por PUT e carrega com COPY INTO."""
    from .query import connect_lakehouse
    from .snowflake_export import SPECS
    from .snowflake_load import SnowflakeLoadError, connect, load, verify

    esperado = {}
    for spec in SPECS:
        caminho = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
        if not os.path.exists(caminho):
            print(f"ERRO: parquet ausente: {caminho}. Rode `make warehouse-export` antes.")
            return EXIT_FATAL

    duck = connect_lakehouse(from_env())
    try:
        # A contagem de origem sai do PROPRIO parquet, nao de uma reexecucao da query:
        # reconferir contra o SQL provaria que o SQL e deterministico, nao que os bytes
        # que subiram sao os que foram gerados.
        for spec in SPECS:
            caminho = os.path.join(args.stage_dir, f"{spec['name']}.parquet")
            esperado[spec["name"]] = {
                "rows": duck.execute(
                    f"select count(*) from read_parquet('{caminho}')"
                ).fetchone()[0]
            }

        try:
            connection = connect(args.connection, role=args.role)
        except SnowflakeLoadError as exc:
            print(f"ERRO: {exc}")
            return EXIT_FATAL

        try:
            resumo = load(connection, duck, args.stage_dir, args.database)
            problemas = verify(connection, args.stage_dir, esperado, args.database)
        except SnowflakeLoadError as exc:
            print(f"ERRO: {exc}")
            return EXIT_FATAL
        except Exception as exc:
            print(f"ERRO: o destino recusou: {exc}")
            return EXIT_FAILED
        finally:
            connection.close()
    finally:
        duck.close()

    print(f"destino .......... {args.database}.STAGE (conexao '{args.connection}')")
    print(f"papel ............ {args.role}")
    for nome in sorted(resumo):
        print(f"  {nome:<30} {resumo[nome]['rows']:>9,} linhas")
    if problemas:
        print("\nDIVERGENCIA entre o parquet gerado e o que chegou ao destino:")
        for p in problemas:
            print(f"  - {p}")
        return EXIT_FAILED
    print(f"  {'TOTAL':<30} {sum(d['rows'] for d in resumo.values()):>9,} linhas")
    print("OK: carregado e reconferido contagem a contagem.")
    return EXIT_OK


def _cmd_stream_evidence(args) -> int:
    """Observa OLTP, broker e projecao e escreve a evidencia datada.

    NAO reprova quando um dos tres esta fora do ar, e isso e deliberado: uma evidencia
    parcial e util, e a secao ausente aparece como AUSENCIA DECLARADA em vez de numero
    inventado. Codigo 1 fica reservado para o caso em que NENHUM dos tres respondeu — ai
    nao ha evidencia nenhuma, so um arquivo com quatro avisos.
    """
    from . import config as config_module
    from .stream_evidence import collect, render, write

    dados = collect(
        config_module.from_env(),
        bootstrap=args.bootstrap,
        topic=args.topic,
        dsn=args.dsn,
    )

    planos = ("oltp", "broker", "projecao", "reconciliacao")
    observados = [nome for nome in planos if "erro" not in dados[nome]]

    if args.out == "-":
        print(render(dados), end="")
    else:
        destino = write(dados, args.out)
        for nome in planos:
            estado = "ok" if "erro" not in dados[nome] else "ausente"
            print(f"{nome:.<17} {estado}")
        print(f"escrito em ....... {os.path.relpath(destino)}")

    if not observados:
        print("ERRO: nenhum dos tres planos respondeu. Suba com `make stream-up`.")
        return EXIT_FAILED
    return EXIT_OK


def _cmd_snowflake_evidence(args) -> int:
    """Observa o destino e escreve o relatorio. Nao valida nada — quem valida e o dbt."""
    from .snowflake_evidence import collect, render
    from .snowflake_load import SnowflakeLoadError, connect

    try:
        connection = connect(args.connection)
    except SnowflakeLoadError as exc:
        print(f"ERRO: {exc}")
        return EXIT_FATAL

    try:
        dados = collect(
            connection, lambda papel: connect(args.connection, role=papel), args.database
        )
    except Exception as exc:
        print(f"ERRO: o destino recusou: {exc}")
        return EXIT_FAILED
    finally:
        connection.close()

    texto = render(dados)
    if args.out == "-":
        print(texto, end="")
    else:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as arquivo:
            arquivo.write(texto)
        ident = dados["identidade"]
        print(f"conta ............ {ident['conta']} (papel {ident['papel']})")
        print(f"objetos .......... {len(dados['objetos'])} em STAGE/GOLD/MART")
        print(f"isolamento ....... {'confere' if not dados['isolamento'] else 'NAO CONFERE'}")
        print(f"escrito em ....... {args.out}")

    # Evidencia que registra um isolamento quebrado nao e evidencia, e um alerta.
    return EXIT_FAILED if dados["isolamento"] else EXIT_OK


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

    prune_parser = subparsers.add_parser(
        "prune-local",
        help="apaga a copia local de uma particao, so se ela estiver integra no destino",
    )
    prune_parser.add_argument("partition", help="caminho da particao em disco")
    prune_parser.set_defaults(handler=_cmd_prune_local)

    oltp_parser = subparsers.add_parser(
        "export-oltp-reference",
        help="materializa o Silver como JSON plano para a Source de OLTP simulado",
    )
    oltp_parser.add_argument(
        "--out", default="data/oltp-reference", help="diretorio raiz da referencia"
    )
    oltp_parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="data da referencia YYYY-MM-DD (padrao: hoje em UTC)",
    )
    oltp_parser.add_argument(
        "--seeds-dir",
        default=DEFAULT_SEEDS_DIR,
        help="diretorio dos seeds do dbt (nao sao alcancaveis pelas views do Lakehouse)",
    )
    oltp_parser.set_defaults(handler=_cmd_export_oltp_reference)

    orders_parser = subparsers.add_parser(
        "export-orders-reference",
        help="materializa o Silver como JSON plano para a Source de Orders simulados",
    )
    orders_parser.add_argument(
        "--out", default="data/orders-reference", help="diretorio raiz da referencia"
    )
    orders_parser.add_argument(
        "--date",
        default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        help="data da referencia YYYY-MM-DD (padrao: hoje em UTC)",
    )
    # A janela e obrigatoria e nao tem default: quem sabe de quando ate quando os pedidos
    # existem e quem opera, e adivinhar aqui produziria uma base de fatos silenciosamente
    # diferente a cada execucao. Mesmo espirito do `--reference` obrigatorio no validate.
    orders_parser.add_argument(
        "--from",
        dest="window_from",
        required=True,
        help="primeiro dia de pedido da janela, YYYY-MM-DD",
    )
    orders_parser.add_argument(
        "--to",
        dest="window_to",
        required=True,
        help="ultimo dia de pedido da janela, YYYY-MM-DD",
    )
    orders_parser.add_argument(
        "--seeds-dir",
        default=DEFAULT_SEEDS_DIR,
        help="diretorio dos seeds do dbt (nao sao alcancaveis pelas views do Lakehouse)",
    )
    orders_parser.set_defaults(handler=_cmd_export_orders_reference)

    # ---- OLTP de pedidos e outbox ---------------------------------------------
    # Quatro verbos separados pelo mesmo criterio de sempre: imprimir a DDL nao precisa de
    # banco, criar o esquema nao precisa de log, aplicar nao precisa saber publicar, e
    # conferir o outbox nao pode compartilhar codigo com quem o escreveu.
    from .orders_oltp import DEFAULT_DSN as OLTP_DEFAULT_DSN

    oltp_ddl = subparsers.add_parser(
        "orders-oltp-ddl", help="imprime a DDL do OLTP de pedidos sem conectar em nada"
    )
    oltp_ddl.set_defaults(handler=_cmd_orders_oltp_ddl)

    oltp_init = subparsers.add_parser(
        "orders-oltp-init", help="cria orders, order_line e outbox no OLTP"
    )
    oltp_init.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="DSN do OLTP")
    oltp_init.add_argument(
        "--reset",
        action="store_true",
        help="derruba as tres tabelas antes de criar (o OLTP e replica do log)",
    )
    oltp_init.set_defaults(handler=_cmd_orders_oltp_init)

    oltp_apply = subparsers.add_parser(
        "orders-apply",
        help="replica o log da particao no OLTP: estado e outbox na mesma transacao",
    )
    oltp_apply.add_argument("partition", help="caminho da particao de pedidos")
    oltp_apply.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="DSN do OLTP")
    oltp_apply.add_argument(
        "--no-verify",
        action="store_true",
        help="pula a conferencia do sha256 do log contra o manifesto (nao use)",
    )
    oltp_apply.set_defaults(handler=_cmd_orders_apply)

    oltp_outbox = subparsers.add_parser(
        "orders-outbox", help="estado do outbox e do OLTP; com --verify, reconstitui o log"
    )
    oltp_outbox.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="DSN do OLTP")
    oltp_outbox.add_argument(
        "--verify",
        metavar="PARTICAO",
        help="reconstitui o log a partir do outbox e compara o sha256 com o manifesto",
    )
    oltp_outbox.set_defaults(handler=_cmd_orders_outbox)

    # ---- transporte, replay e consumo ------------------------------------------
    from .orders_stream import (
        DEFAULT_BOOTSTRAP as KAFKA_BOOTSTRAP,
        DEFAULT_GROUP as KAFKA_GROUP,
        DEFAULT_PROJECTION_DSN as PROJECTION_DSN,
        DEFAULT_SEEDS_DIR as STREAM_SEEDS_DIR,
        DEFAULT_TOPIC as KAFKA_TOPIC,
    )

    proj_init = subparsers.add_parser(
        "orders-projection-init", help="cria o banco e a tabela do read model"
    )
    proj_init.add_argument("--projection-dsn", default=PROJECTION_DSN)
    proj_init.set_defaults(handler=_cmd_orders_projection_init)

    pub = subparsers.add_parser(
        "orders-publish", help="drena o outbox para o topico (at-least-once, por desenho)"
    )
    pub.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="DSN do OLTP")
    pub.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    pub.add_argument("--topic", default=KAFKA_TOPIC)
    pub.add_argument("--batch-size", type=int, default=1000)
    pub.add_argument("--follow", action="store_true", help="fica drenando em laco")
    pub.add_argument("--interval", type=float, default=2.0, help="pausa entre lotes no --follow")
    pub.set_defaults(handler=_cmd_orders_publish)

    prj = subparsers.add_parser(
        "orders-project", help="consome o topico e mantem live_order_state (idempotente)"
    )
    prj.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    prj.add_argument("--topic", default=KAFKA_TOPIC)
    prj.add_argument("--group", default=KAFKA_GROUP)
    prj.add_argument("--projection-dsn", default=PROJECTION_DSN)
    prj.add_argument("--batch-size", type=int, default=500)
    prj.add_argument("--idle-timeout", type=float, default=6.0,
                     help="segundos sem mensagem antes de considerar o topico seco")
    prj.add_argument("--from-beginning", action="store_true",
                     help="grupo novo comeca do offset 0 em vez do fim")
    prj.add_argument("--sla-minutes", type=int,
                     help="limiar de separacao (padrao: order_premises_seed.csv)")
    prj.add_argument("--seeds-dir", default=STREAM_SEEDS_DIR)
    # Buraco e PERDA. `--allow-gaps` existe so para diagnosticar um topico ja truncado por
    # retencao; usar isso num pipeline seria trocar "perdi evento" por "nao reparei".
    prj.add_argument("--allow-gaps", action="store_true",
                     help="registra buracos em vez de reprovar (so para diagnostico)")
    # O read model nao muda de semantica com o armazenamento: e a mesma interface pequena
    # dos dois lados. `iceberg` e o que da o segundo escritor a mesma tabela.
    prj.add_argument("--sink", choices=("postgres", "iceberg"), default="postgres",
                     help="onde o read model vive (padrao: postgres)")
    prj.set_defaults(handler=_cmd_orders_project)

    lag = subparsers.add_parser("orders-lag", help="lag do grupo consumidor, por particao")
    lag.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    lag.add_argument("--topic", default=KAFKA_TOPIC)
    lag.add_argument("--group", default=KAFKA_GROUP)
    lag.set_defaults(handler=_cmd_orders_lag)

    replay = subparsers.add_parser(
        "orders-replay", help="rebobina o grupo para o inicio (nao apaga a projecao)"
    )
    replay.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    replay.add_argument("--topic", default=KAFKA_TOPIC)
    replay.add_argument("--group", default=KAFKA_GROUP)
    replay.set_defaults(handler=_cmd_orders_replay)

    # ---- projecao viva em Iceberg -----------------------------------------------
    from .orders_projection import WAREHOUSE as ICEBERG_WAREHOUSE

    ice_init = subparsers.add_parser(
        "iceberg-init", help="cria o catalogo SQL e a tabela live_order_state"
    )
    ice_init.add_argument("--warehouse", default=ICEBERG_WAREHOUSE)
    ice_init.set_defaults(handler=_cmd_iceberg_init)

    ice_meta = subparsers.add_parser(
        "iceberg-metadata", help="imprime o metadata_location corrente (o dbt precisa dele)"
    )
    ice_meta.add_argument("--quiet", action="store_true",
                          help="nao imprime o erro; so o codigo de saida importa")
    ice_meta.set_defaults(handler=_cmd_iceberg_metadata)

    ice_rebuild = subparsers.add_parser(
        "orders-rebuild-projection",
        help="reconstroi live_order_state do RAW — o SEGUNDO escritor da tabela",
    )
    ice_rebuild.add_argument("--root", default="data/orders")
    ice_rebuild.add_argument(
        "--through", metavar="YYYY-MM-DD",
        help="so as particoes ate esta data — o recorte do par lambda")
    ice_rebuild.add_argument("--seeds-dir", default=STREAM_SEEDS_DIR)
    ice_rebuild.set_defaults(handler=_cmd_orders_rebuild_projection)

    ice_rec = subparsers.add_parser(
        "orders-reconcile", help="compara Iceberg x Silver x OLTP; sai 1 em qualquer divergencia"
    )
    ice_rec.add_argument("--dsn", default=OLTP_DEFAULT_DSN, help="DSN do OLTP")
    ice_rec.set_defaults(handler=_cmd_orders_reconcile)

    # ---- Snowflake ------------------------------------------------------------
    # Tres verbos, nao um: o recorte roda sem credencial, o DDL roda sem conexao, e so o
    # load precisa dos dois. Juntar tudo faria "o recorte esta errado" ser reportado como
    # falha de rede.
    from .snowflake_export import DEFAULT_OUT as SF_DEFAULT_OUT
    from .snowflake_load import DEFAULT_LOAD_ROLE as SF_DEFAULT_LOAD_ROLE
    from .snowflake_load import DEFAULT_WAREHOUSE as SF_DEFAULT_WAREHOUSE

    sf_export = subparsers.add_parser(
        "export-snowflake",
        help="recorta o Silver para parquet local (nao fala com o Snowflake)",
    )
    sf_export.add_argument("--out", default=SF_DEFAULT_OUT, help="diretorio do recorte")
    sf_export.set_defaults(handler=_cmd_export_snowflake)

    sf_ddl = subparsers.add_parser(
        "snowflake-ddl", help="imprime o DDL do STAGE, derivado do proprio recorte"
    )
    sf_ddl.add_argument("--database", default="RETAIL")
    sf_ddl.set_defaults(handler=_cmd_snowflake_ddl)

    sf_boot = subparsers.add_parser(
        "snowflake-bootstrap",
        help="cria database, schemas, papeis e grants (uma vez, exige ACCOUNTADMIN)",
    )
    sf_boot.add_argument("--database", default="RETAIL")
    sf_boot.add_argument("--connection", default="spark_retail")
    sf_boot.add_argument(
        "--grant-to-user", default=None,
        help="concede os tres papeis a este usuario e DESLIGA os papeis secundarios dele",
    )
    sf_boot.add_argument(
        "--warehouse", default=SF_DEFAULT_WAREHOUSE,
        help=f"warehouse cujo usage e concedido aos tres papeis (default {SF_DEFAULT_WAREHOUSE})",
    )
    sf_boot.set_defaults(handler=_cmd_snowflake_bootstrap)

    sf_load = subparsers.add_parser(
        "load-snowflake", help="cria as tabelas STAGE, sobe por PUT e carrega com COPY INTO"
    )
    sf_load.add_argument("--stage-dir", default=SF_DEFAULT_OUT)
    sf_load.add_argument("--database", default="RETAIL")
    sf_load.add_argument(
        "--connection", default="spark_retail",
        help="nome da conexao em ~/.snowflake/config.toml (nenhum segredo vive no repo)",
    )
    # O papel da CARGA sobrescreve o da conexao. A conexao aponta para o papel
    # administrativo porque e ela que roda o bootstrap; carregar parquet nao precisa disso
    # e nao deveria poder criar database.
    sf_load.add_argument(
        "--role", default=SF_DEFAULT_LOAD_ROLE,
        help=f"papel que executa a carga (default {SF_DEFAULT_LOAD_ROLE})",
    )
    sf_load.set_defaults(handler=_cmd_load_snowflake)

    sf_ev = subparsers.add_parser(
        "snowflake-evidence",
        help="observa o destino e escreve a evidencia datada (posse, volume, isolamento)",
    )
    sf_ev.add_argument("--database", default="RETAIL")
    sf_ev.add_argument("--connection", default="spark_retail")
    sf_ev.add_argument(
        "--out", default="docs/warehouse-evidence/README.md",
        help="destino do markdown; '-' escreve na saida padrao",
    )
    sf_ev.set_defaults(handler=_cmd_snowflake_evidence)

    stream_ev = subparsers.add_parser(
        "stream-evidence",
        help="observa OLTP, broker e projecao e escreve a evidencia datada do stream",
    )
    stream_ev.add_argument("--bootstrap", default=None, help="broker; padrao KAFKA_BOOTSTRAP")
    stream_ev.add_argument("--topic", default=None, help="topico; padrao KAFKA_ORDERS_TOPIC")
    stream_ev.add_argument("--dsn", default=None, help="DSN do OLTP; padrao ORDERS_OLTP_DSN")
    stream_ev.add_argument(
        "--out", default="docs/stream-evidence/README.md",
        help="destino do markdown; '-' escreve na saida padrao",
    )
    stream_ev.set_defaults(handler=_cmd_stream_evidence)

    has_data_parser = subparsers.add_parser(
        "has-data",
        help="codigo 0 se existe algum objeto aterrissado sob o prefixo, 1 se nao",
    )
    has_data_parser.add_argument("prefix", help="prefixo da source, ex.: ine_population_api")
    has_data_parser.set_defaults(handler=_cmd_has_data)

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
