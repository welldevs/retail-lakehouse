#!/usr/bin/env python3
"""EXPERIMENTO FECHADO: Iceberg + pyiceberg + catalogo SQL + DuckDB.

POR QUE ISTO EXISTE ANTES DO MARCO 6, E NAO DENTRO DELE.

O plano da Fase 3 registrou uma premissa como "a mais fragil": *o DuckDB pode nao ler o
catalogo SQL do pyiceberg*. A extensao `iceberg` e de leitura e historicamente esperava um
caminho de metadado ou um catalogo REST — nao um catalogo SQL em Postgres.

Se essa premissa cair no meio da construcao, o retrabalho e caro e a tentacao e pior:
contornar com um caminho que "quase" funciona e chamar de projecao. Entao ela e testada
ANTES, isolada, contra o stack de verdade, e com uma resposta binaria por pergunta.

Este script nao constroi nada do Marco 6. Ele responde nove perguntas, limpa o que criou, e
imprime um veredito. Namespace e prefixo proprios (`spike`, `iceberg-spike/`) para nao tocar
em nada do lakehouse.

    make spike-iceberg
"""

from __future__ import annotations

import os
import sys
import traceback

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

from retail_platform.config import from_env  # noqa: E402

NAMESPACE = "spike"
TABLE = "spike.live_order_state_probe"
WAREHOUSE = "s3://retail-lakehouse/iceberg-spike"
CATALOG_DB = "iceberg_catalog"

resultados: list[tuple[str, bool, str]] = []


def responde(pergunta: str, ok: bool, detalhe: str = "") -> None:
    resultados.append((pergunta, ok, detalhe))
    print(f"  [{'SIM  ' if ok else 'NAO  '}] {pergunta}")
    if detalhe:
        for linha in detalhe.split("\n"):
            print(f"          {linha}")


def falhou(pergunta: str, exc: Exception) -> None:
    responde(pergunta, False, f"{type(exc).__name__}: {str(exc)[:400]}")


def ensure_catalog_db(dsn_admin: str) -> None:
    import psycopg

    with psycopg.connect(dsn_admin, autocommit=True) as connection:
        with connection.cursor() as cur:
            cur.execute("select 1 from pg_database where datname = %s", (CATALOG_DB,))
            if not cur.fetchone():
                cur.execute(f'create database "{CATALOG_DB}"')


def main() -> int:
    config = from_env()
    oltp_port = os.environ.get("OLTP_PORT", "5433")
    admin = f"postgresql://oltp:oltp@localhost:{oltp_port}/postgres"
    catalog_uri = f"postgresql+psycopg://oltp:oltp@localhost:{oltp_port}/{CATALOG_DB}"

    print(f"catalogo ......... {catalog_uri}")
    print(f"warehouse ........ {WAREHOUSE}")
    print(f"endpoint S3 ...... {config.endpoint}")
    print()

    import duckdb
    import pyarrow as pa
    import pyiceberg

    print(f"pyiceberg {pyiceberg.__version__} · pyarrow {pa.__version__} · "
          f"duckdb {duckdb.__version__}")
    print()

    # ---- Q1: catalogo SQL em Postgres --------------------------------------------------
    print("Q1. o pyiceberg monta um catalogo SQL sobre o Postgres?")
    catalog = None
    try:
        ensure_catalog_db(admin)
        from pyiceberg.catalog.sql import SqlCatalog

        catalog = SqlCatalog("spike", **{
            "uri": catalog_uri,
            "warehouse": WAREHOUSE,
            # PyArrowFileIO em vez de s3fs: s3fs arrasta um botocore antigo e conflita com
            # o boto3 que a plataforma ja usa para o RAW.
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.endpoint": config.endpoint,
            "s3.access-key-id": config.access_key,
            "s3.secret-access-key": config.secret_key,
            "s3.region": config.region,
        })
        catalog.create_namespace_if_not_exists(NAMESPACE)
        responde("catalogo SQL criado e namespace disponivel", True,
                 f"namespaces: {[n[0] for n in catalog.list_namespaces()]}")
    except Exception as exc:
        falhou("catalogo SQL criado e namespace disponivel", exc)
        traceback.print_exc()
        return veredito()

    # ---- Q2: criar tabela e escrever ---------------------------------------------------
    print()
    print("Q2. da para criar a tabela e escrever nela pelo pyiceberg?")
    schema = pa.schema([
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("wh", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("last_sequence_no", pa.int32(), nullable=False),
        pa.field("net_amount", pa.decimal128(12, 2), nullable=True),
    ])
    tabela = None
    try:
        if catalog.table_exists(TABLE):
            catalog.drop_table(TABLE)
        tabela = catalog.create_table(TABLE, schema=schema)
        lote = pa.Table.from_pylist([
            {"order_id": f"ord_spike_{i:03d}", "wh": "mad1", "status": "PLACED",
             "last_sequence_no": 1, "net_amount": None}
            for i in range(100)
        ], schema=schema)
        tabela.append(lote)
        responde("tabela criada e primeiro append gravado", True,
                 f"snapshot={tabela.current_snapshot().snapshot_id}")
    except Exception as exc:
        falhou("tabela criada e primeiro append gravado", exc)
        traceback.print_exc()
        return veredito(catalog)

    # ---- Q3: reler pelo pyiceberg ------------------------------------------------------
    print()
    print("Q3. o pyiceberg le de volta o que escreveu?")
    try:
        lidas = tabela.scan().to_arrow().num_rows
        responde("releitura pelo pyiceberg devolve as linhas", lidas == 100, f"{lidas} linhas")
    except Exception as exc:
        falhou("releitura pelo pyiceberg devolve as linhas", exc)

    # ---- Q4: UPSERT por chave, que e o que a projecao precisa --------------------------
    print()
    print("Q4. da para fazer UPSERT por order_id? (e o que live_order_state exige)")
    try:
        atualizacao = pa.Table.from_pylist([
            {"order_id": "ord_spike_000", "wh": "mad1", "status": "DELIVERED",
             "last_sequence_no": 7, "net_amount": None},
            {"order_id": "ord_spike_999", "wh": "bcn1", "status": "PLACED",
             "last_sequence_no": 1, "net_amount": None},
        ], schema=schema)
        resultado = tabela.upsert(atualizacao, join_cols=["order_id"])
        depois = tabela.scan().to_arrow()
        estado = {r["order_id"]: r["status"] for r in depois.to_pylist()}
        ok = (depois.num_rows == 101
              and estado.get("ord_spike_000") == "DELIVERED"
              and estado.get("ord_spike_999") == "PLACED")
        responde("upsert por chave funciona (atualiza 1, insere 1)", ok,
                 f"linhas={depois.num_rows} atualizadas={resultado.rows_updated} "
                 f"inseridas={resultado.rows_inserted}")
    except Exception as exc:
        falhou("upsert por chave funciona (atualiza 1, insere 1)", exc)

    # ---- Q5: dois escritores concorrentes ----------------------------------------------
    #
    # A PRIMEIRA VERSAO DESTE TESTE ESTAVA ERRADA, e vale registrar por que. Ela exigia que
    # "as duas escritas sobrevivessem" a duas referencias carregadas ao mesmo tempo, e
    # reprovou com CommitFailedException. Mas isso e o Iceberg fazendo EXATAMENTE o certo:
    # concorrencia otimista significa que o segundo commit, partindo de um snapshot que ja
    # nao e o corrente, TEM de ser recusado. Se ele passasse em silencio, seria lost update
    # — e ai sim havia motivo para nao usar Iceberg.
    #
    # A propriedade correta e em tres partes: recusa, retry depois de refresh, e ambas
    # presentes no fim. E o padrao que a projecao do Marco 6 vai precisar implementar.
    print()
    print("Q5. dois escritores na MESMA tabela: o perdedor e recusado ou perdido em silencio?")
    try:
        a = catalog.load_table(TABLE)
        b = catalog.load_table(TABLE)  # referencia carregada ANTES do commit de `a`
        antes = a.scan().to_arrow().num_rows

        a.append(pa.Table.from_pylist([
            {"order_id": "ord_writer_a", "wh": "mad1", "status": "PLACED",
             "last_sequence_no": 1, "net_amount": None}], schema=schema))

        from pyiceberg.exceptions import CommitFailedException

        recusado = False
        try:
            b.append(pa.Table.from_pylist([
                {"order_id": "ord_writer_b", "wh": "bcn1", "status": "PLACED",
                 "last_sequence_no": 1, "net_amount": None}], schema=schema))
        except CommitFailedException as conflito:
            recusado = True
            detalhe = str(conflito)[:120]
        responde("5a. o escritor com snapshot velho e RECUSADO, nao perdido", recusado,
                 detalhe if recusado else "commitou em silencio — isso seria lost update")

        # 5b. o padrao correto: recarregar e tentar de novo.
        b = catalog.load_table(TABLE)
        b.append(pa.Table.from_pylist([
            {"order_id": "ord_writer_b", "wh": "bcn1", "status": "PLACED",
             "last_sequence_no": 1, "net_amount": None}], schema=schema))
        final = catalog.load_table(TABLE)
        ids = {r["order_id"] for r in final.scan().to_arrow().to_pylist()}
        responde("5b. depois de recarregar, o retry commita", "ord_writer_b" in ids)
        responde("5c. as duas escritas estao presentes no fim",
                 {"ord_writer_a", "ord_writer_b"} <= ids,
                 f"{antes} -> {final.scan().to_arrow().num_rows} linhas")
    except Exception as exc:
        falhou("5a. o escritor com snapshot velho e RECUSADO, nao perdido", exc)

    # ---- Q5.1: o laco de retry sob concorrencia real (threads) -------------------------
    print()
    print("Q5.1 com DOIS ESCRITORES DE VERDADE e laco de retry, nada se perde?")
    try:
        import threading

        from pyiceberg.exceptions import CommitFailedException

        conflitos = {"n": 0}
        trava = threading.Lock()

        def escritor(nome, quantidade):
            for k in range(quantidade):
                for tentativa in range(20):
                    try:
                        t = catalog.load_table(TABLE)
                        t.append(pa.Table.from_pylist([
                            {"order_id": f"ord_{nome}_{k:02d}", "wh": "vlc1",
                             "status": "PLACED", "last_sequence_no": 1,
                             "net_amount": None}], schema=schema))
                        break
                    except CommitFailedException:
                        with trava:
                            conflitos["n"] += 1
                else:
                    raise RuntimeError(f"{nome}: 20 tentativas sem sucesso")

        base = catalog.load_table(TABLE).scan().to_arrow().num_rows
        threads = [threading.Thread(target=escritor, args=(nome, 5))
                   for nome in ("stream", "lote")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        depois = catalog.load_table(TABLE).scan().to_arrow()
        ids = {r["order_id"] for r in depois.to_pylist()}
        esperados = {f"ord_{n}_{k:02d}" for n in ("stream", "lote") for k in range(5)}
        responde("5.1 as 10 escritas concorrentes chegaram todas", esperados <= ids,
                 f"{base} -> {depois.num_rows} linhas; "
                 f"{conflitos['n']} conflito(s) resolvidos por retry")
    except Exception as exc:
        falhou("5.1 as 10 escritas concorrentes chegaram todas", exc)

    # ---- Q6: isolamento de snapshot ----------------------------------------------------
    print()
    print("Q6. um leitor que abriu antes ve um snapshot ESTAVEL?")
    try:
        leitor = catalog.load_table(TABLE)
        scan_antigo = leitor.scan()
        contagem_antes = scan_antigo.to_arrow().num_rows
        snapshot_antes = leitor.current_snapshot().snapshot_id

        escritor = catalog.load_table(TABLE)
        escritor.append(pa.Table.from_pylist([
            {"order_id": "ord_durante_leitura", "wh": "svq1", "status": "PLACED",
             "last_sequence_no": 1, "net_amount": None}], schema=schema))

        # O MESMO objeto de leitura, relido: o snapshot que ele carrega nao mudou.
        contagem_depois = scan_antigo.to_arrow().num_rows
        novo = catalog.load_table(TABLE)
        ok = (contagem_depois == contagem_antes
              and novo.current_snapshot().snapshot_id != snapshot_antes
              and novo.scan().to_arrow().num_rows == contagem_antes + 1)
        responde("o leitor antigo continua no snapshot antigo; o novo ve a escrita", ok,
                 f"leitor antigo={contagem_depois} linhas, releitura={novo.scan().to_arrow().num_rows}")
    except Exception as exc:
        falhou("o leitor antigo continua no snapshot antigo; o novo ve a escrita", exc)

    # ---- Q7: time travel ---------------------------------------------------------------
    print()
    print("Q7. da para voltar a um snapshot anterior?")
    try:
        atual = catalog.load_table(TABLE)
        historico = list(atual.metadata.snapshots)
        primeiro = historico[0].snapshot_id
        antigo = atual.scan(snapshot_id=primeiro).to_arrow().num_rows
        responde("time travel devolve o estado antigo", antigo == 100,
                 f"{len(historico)} snapshots; o primeiro tinha {antigo} linhas")
    except Exception as exc:
        falhou("time travel devolve o estado antigo", exc)

    # ---- Q8: A PREMISSA FRAGIL — o DuckDB le isto? -------------------------------------
    print()
    print("Q8. O DUCKDB LE A TABELA? (a premissa que este experimento existe para testar)")
    atual = catalog.load_table(TABLE)
    esperado = atual.scan().to_arrow().num_rows
    metadata_location = atual.metadata_location
    print(f"      metadata_location = {metadata_location}")

    con = duckdb.connect()
    try:
        con.execute("install iceberg"); con.execute("load iceberg")
        con.execute("install httpfs"); con.execute("load httpfs")
        host = config.endpoint.split("://", 1)[-1]
        con.execute(f"""
            create or replace secret minio (
                type s3, key_id '{config.access_key}', secret '{config.secret_key}',
                region '{config.region}', endpoint '{host}',
                use_ssl {'true' if config.endpoint.startswith('https') else 'false'},
                url_style 'path')
        """)
    except Exception as exc:
        falhou("duckdb com iceberg + httpfs configurados", exc)
        return veredito(catalog)

    # 8a. pelo caminho do metadado, que e o que o pyiceberg entrega de graca
    try:
        linhas = con.execute(
            f"select count(*) from iceberg_scan('{metadata_location}')"
        ).fetchone()[0]
        responde("8a. iceberg_scan(metadata_location) le a tabela", linhas == esperado,
                 f"{linhas} linhas (esperado {esperado})")
    except Exception as exc:
        falhou("8a. iceberg_scan(metadata_location) le a tabela", exc)

    # 8b. pela raiz da tabela, sem saber qual e o metadado corrente
    try:
        linhas = con.execute(
            f"select count(*) from iceberg_scan('{atual.location()}')"
        ).fetchone()[0]
        responde("8b. iceberg_scan(raiz da tabela) le a tabela", linhas == esperado,
                 f"{linhas} linhas")
    except Exception as exc:
        falhou("8b. iceberg_scan(raiz da tabela) le a tabela", exc)

    # 8c. ATTACH de catalogo — o DuckDB 1.5 fala REST/Glue, nao catalogo SQL
    try:
        con.execute(f"attach '{catalog_uri}' as ice (type iceberg)")
        responde("8c. ATTACH do catalogo SQL", True, "atachou")
    except Exception as exc:
        responde("8c. ATTACH do catalogo SQL", False,
                 f"{type(exc).__name__}: {str(exc)[:200]}")

    # 8d. metadado e snapshot antigo — time travel pelo DuckDB
    try:
        snaps = con.execute(
            f"select count(*) from iceberg_snapshots('{metadata_location}')"
        ).fetchone()[0]
        responde("8d. iceberg_snapshots lista o historico", snaps >= 2, f"{snaps} snapshots")
    except Exception as exc:
        falhou("8d. iceberg_snapshots lista o historico", exc)

    # 8e. o atalho inseguro, medido para que a recusa dele seja informada
    try:
        con.execute("set unsafe_enable_version_guessing = true")
        linhas = con.execute(
            f"select count(*) from iceberg_scan('{atual.location()}')"
        ).fetchone()[0]
        responde("8e. com unsafe_enable_version_guessing, a raiz funciona", linhas == esperado,
                 f"{linhas} linhas — mas o proprio DuckDB chama isto de inseguro: adivinhar\n"
                 f"a versao varrendo o storage pode ler metadado ainda nao commitado")
    except Exception as exc:
        falhou("8e. com unsafe_enable_version_guessing, a raiz funciona", exc)

    # ---- Q9: o fallback declarado no plano ---------------------------------------------
    print()
    print("Q9. se nada acima servisse: o fallback (pyiceberg -> parquet efemero) funciona?")
    try:
        import tempfile

        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as tmp:
            destino = os.path.join(tmp, "projection.parquet")
            pq.write_table(atual.scan().to_arrow(), destino)
            linhas = duckdb.connect().execute(
                f"select count(*) from read_parquet('{destino}')"
            ).fetchone()[0]
        responde("fallback pyiceberg -> parquet -> duckdb funciona", linhas == esperado,
                 f"{linhas} linhas")
    except Exception as exc:
        falhou("fallback pyiceberg -> parquet -> duckdb funciona", exc)

    return veredito(catalog)


def veredito(catalog=None) -> int:
    if catalog is not None:
        try:
            if catalog.table_exists(TABLE):
                catalog.drop_table(TABLE)
            catalog.drop_namespace(NAMESPACE)
            print()
            print("limpeza: tabela e namespace do experimento removidos.")
        except Exception as exc:  # pragma: no cover
            print(f"limpeza incompleta: {type(exc).__name__}: {exc}")

    print()
    print("=" * 78)
    essenciais = [
        "catalogo SQL criado e namespace disponivel",
        "tabela criada e primeiro append gravado",
        "releitura pelo pyiceberg devolve as linhas",
        "upsert por chave funciona (atualiza 1, insere 1)",
        "5a. o escritor com snapshot velho e RECUSADO, nao perdido",
        "5b. depois de recarregar, o retry commita",
        "5c. as duas escritas estao presentes no fim",
        "5.1 as 10 escritas concorrentes chegaram todas",
        "o leitor antigo continua no snapshot antigo; o novo ve a escrita",
    ]
    leitura = [p for p, ok, _ in resultados if p.startswith("8") and ok]
    faltando = [p for p in essenciais if not any(q == p and ok for q, ok, _ in resultados)]

    for pergunta, ok, _ in resultados:
        print(f"  {'SIM' if ok else 'NAO'}  {pergunta}")
    print()
    if faltando:
        print("VEREDITO: o experimento REPROVOU. Nao construa a projecao concorrente ainda.")
        for p in faltando:
            print(f"  falta: {p}")
        return 1
    if not leitura:
        print("VEREDITO: escrita e concorrencia OK, mas NENHUM caminho de leitura pelo")
        print("DuckDB funcionou. O fallback do plano (parquet efemero) e obrigatorio.")
        return 2
    print("VEREDITO: APROVADO. Escrita, upsert, concorrencia e isolamento de snapshot")
    print(f"funcionam, e o DuckDB le pelos caminhos: {', '.join(leitura)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
