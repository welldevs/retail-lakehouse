#!/usr/bin/env python3
"""Prova, contra o sistema real, se os seis sinais de observabilidade ja existem.

POR QUE ISTO EXISTE ANTES DE INSTRUMENTAR QUALQUER COISA.

`AI_ENGINEERING_CONSTRAINTS.md` (secao 17, Observability) e `BACKLOG.md` sao explicitos:
antes de montar `application -> OpenTelemetry -> Collector -> backend`, e preciso PROVAR que
os seis sinais exigidos ja nao existem por outro caminho — latencia, erro, throughput,
falhas, processamento, estado do pipeline. Assumir a ausencia por padrao e a mesma doenca de
qualquer outra afirmacao nao medida deste projeto.

Este script nao instrumenta nada. Ele LE o sistema real (particoes locais em `data/`, o SQL
da plataforma, os arquivos de DAG) e devolve um veredito SIM/PARCIAL/NAO por sinal, com o
detalhe que sustenta o veredito. E seguro por construcao: so leitura, nenhuma escrita.

    make observability-prove-signals
    make observability-prove-signals --warehouse   (opcional: confere o Snowflake real)

Reexecutar depois de uma mudanca (ex.: CR-005) e o proprio mecanismo de prova: um sinal que
estava NAO e devolve SIM demonstra o fechamento, sem numero inventado.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "platform", "src"))

DATA_SOURCES = {
    "mercadona_catalog_api": "data/mercadona",
    "ine_population_api": "data/ine",
    "ine_callejero": "data/callejero",
    "simulated_oltp": "data/oltp",
    "simulated_orders": "data/orders",
}
RUNLOG_THRESHOLD_SECONDS = 10.0

CLI_PY = os.path.join(REPO, "platform", "src", "retail_platform", "cli.py")
SNOWFLAKE_EXPORT_PY = os.path.join(REPO, "platform", "src", "retail_platform", "snowflake_export.py")
FACT_INGESTION_RUN_SQL = os.path.join(
    REPO, "platform", "dbt", "models", "warehouse", "gold", "fact_ingestion_run.sql"
)
SILVER_MODELS_DIR = os.path.join(REPO, "platform", "dbt", "models", "silver")
DAGS_DIR = os.path.join(REPO, "orchestration", "airflow", "dags")

resultados: list[tuple[str, str, str]] = []  # (sinal, veredito SIM/PARCIAL/NAO, detalhe)


def registra(sinal: str, veredito: str, detalhe: str) -> None:
    resultados.append((sinal, veredito, detalhe))
    print(f"  [{veredito:8}] {sinal}")
    for linha in detalhe.split("\n"):
        print(f"             {linha}")


def le(caminho: str) -> str:
    with open(caminho, encoding="utf-8") as f:
        return f.read()


# ---- Q1: PROCESSING — log de progresso proporcional a duracao medida ------------------

def q1_processing() -> None:
    print()
    print("Q1. PROCESSING — cada Source grava progresso proporcional a sua duracao medida?")
    faltando: list[str] = []
    achou_algum = False
    for nome, rel in DATA_SOURCES.items():
        raiz = os.path.join(REPO, rel)
        manifests = sorted(glob.glob(os.path.join(raiz, "**", "_manifest.json"), recursive=True))
        if not manifests:
            print(f"    {nome}: nenhuma particao local em {rel}/ — nao medido nesta maquina")
            continue
        achou_algum = True
        duracoes = []
        sem_log = []
        for caminho in manifests:
            try:
                manifesto = json.loads(le(caminho))
            except (OSError, json.JSONDecodeError):
                continue
            duracao = manifesto.get("duration_seconds")
            if duracao is None:
                continue
            duracoes.append(duracao)
            tem_run_log = os.path.exists(os.path.join(os.path.dirname(caminho), "_run.log"))
            if duracao > RUNLOG_THRESHOLD_SECONDS and not tem_run_log:
                sem_log.append((caminho, duracao))
        if not duracoes:
            continue
        marca = "sim" if not sem_log else "NAO"
        print(f"    {nome}: {len(manifests)} particao(oes), duracao {min(duracoes):.1f}-"
              f"{max(duracoes):.1f}s, _run.log quando > {RUNLOG_THRESHOLD_SECONDS:.0f}s: {marca}")
        if sem_log:
            faltando.append(nome)
            for caminho, duracao in sem_log[:2]:
                rel_caminho = os.path.relpath(caminho, REPO)
                print(f"        falta _run.log: {rel_caminho} ({duracao:.1f}s)")

    if not achou_algum:
        registra("processing", "NAO MEDIDO",
                  "nenhuma particao local em data/ — rode make daily / oltp-refresh-all / "
                  "orders-refresh-all / ine-refresh / callejero-refresh antes de medir")
        return
    if faltando:
        registra("processing", "NAO",
                  "source(s) com duracao acima do limiar e sem log de progresso: "
                  + ", ".join(faltando))
        return
    registra("processing", "SIM",
              "cada Source com duracao medida acima do limiar tem _run.log; as demais "
              "rodam rapido o bastante para o mecanismo nao se justificar (ver detalhe acima)")


# ---- Q2: THROUGHPUT / FAILURES — ja chegam ao Gold? ------------------------------------

def q2_throughput_failures() -> None:
    print()
    print("Q2. THROUGHPUT / FAILURES — declared_rows/failure_count/anomaly_count/complete "
          "chegam ao Gold?")
    sql = le(FACT_INGESTION_RUN_SQL)
    campos = ["declared_rows", "failure_count", "anomaly_count", "complete"]
    faltando = [c for c in campos if c not in sql]
    if faltando:
        registra("throughput/failures (Gold)", "NAO",
                  f"fact_ingestion_run.sql nao seleciona: {', '.join(faltando)}")
        return
    registra("throughput/failures (Gold)", "SIM",
              f"fact_ingestion_run.sql seleciona {', '.join(campos)}")


# ---- Q3: LATENCY — duration_seconds/started_at_utc/finished_at_utc chegam ao Gold? -----

def q3_latency() -> None:
    print()
    print("Q3. LATENCY — duration_seconds/started_at_utc/finished_at_utc chegam ao Gold?")
    sql_gold = le(FACT_INGESTION_RUN_SQL)
    sql_export = le(SNOWFLAKE_EXPORT_PY)
    campos = ["duration_seconds", "started_at_utc", "finished_at_utc"]

    no_gold = [c for c in campos if c not in sql_gold]
    no_stage = [c for c in campos if c not in sql_export]

    # Onde o dado JA existe hoje, uma camada abaixo (Silver) — para deixar claro que o
    # gap e de PROJECAO, nao de dado ausente.
    modelos_silver = {
        "raw_manifest.sql": os.path.join(SILVER_MODELS_DIR, "mercadona", "raw_manifest.sql"),
        "silver_oltp_manifest.sql": os.path.join(
            SILVER_MODELS_DIR, "simulated_oltp", "silver_oltp_manifest.sql"),
        "silver_orders_manifest.sql": os.path.join(
            SILVER_MODELS_DIR, "simulated_orders", "silver_orders_manifest.sql"),
    }
    tem_no_silver = []
    for nome, caminho in modelos_silver.items():
        if os.path.exists(caminho) and all(c in le(caminho) for c in campos):
            tem_no_silver.append(nome)

    if not no_gold:
        registra("latency (Gold)", "SIM",
                  f"fact_ingestion_run.sql ja seleciona {', '.join(campos)}")
        return

    detalhe = (f"ausentes em fact_ingestion_run.sql: {', '.join(no_gold)}; "
               f"ausentes na projecao STG_INGESTION_RUN (snowflake_export.py): "
               f"{', '.join(no_stage) or '(nenhum — ja union-ados, so falta o Gold)'}")
    if len(tem_no_silver) == len(modelos_silver):
        detalhe += (f"\nOS TRES CAMPOS JA EXISTEM no Silver ({', '.join(tem_no_silver)}) — "
                    "o gap e de projecao SQL, nao de dado ausente.")
    registra("latency (Gold)", "NAO", detalhe)


# ---- Q4: FAILURES — cobertura de FACT_INGESTION_RUN por source ------------------------

def q4_coverage() -> None:
    print()
    print("Q4. COBERTURA — quantas das 5 Sources entram em FACT_INGESTION_RUN?")
    sql_export = le(SNOWFLAKE_EXPORT_PY)
    modelos_esperados = {
        "mercadona_catalog_api": "raw_manifest",
        "simulated_oltp": "silver_oltp_manifest",
        "simulated_orders": "silver_orders_manifest",
    }
    cobertas = [nome for nome, modelo in modelos_esperados.items() if f"from {modelo}" in sql_export]
    faltando = ["ine_population_api", "ine_callejero"]
    # PARCIAL, nao SIM: 3/5 e o estado real, mesmo que a ausencia das outras duas seja uma
    # decisao aceita (CR-005c) e nao um defeito. Rotular como SIM esconderia a fracao.
    registra("coverage (Gold)",
              "SIM" if len(cobertas) == 5 else "PARCIAL",
              f"cobertas ({len(cobertas)}/5, com eixo wh): {', '.join(cobertas)}\n"
              f"fora (sem eixo wh, sem modelo de manifesto): {', '.join(faltando)}")


# ---- Q5: ERROR — traceback preservado nas excecoes nao mapeadas de cli.py -------------

def q5_error() -> None:
    print()
    print("Q5. ERROR — as excecoes nao mapeadas de cli.py preservam traceback?")
    codigo = le(CLI_PY)
    sites = [m.start() for m in re.finditer(r"except Exception as exc:", codigo)]
    tem_logging_import = bool(re.search(r"^import logging\s*$", codigo, re.MULTILINE))
    com_traceback = 0
    for pos in sites:
        # Uma janela pequena apos o "except Exception as exc:" ate a proxima linha de
        # controle — e onde `logging.exception(...)` teria que aparecer para este site.
        trecho = codigo[pos:pos + 300]
        if "logging.exception(" in trecho:
            com_traceback += 1

    if not sites:
        registra("error (cli.py)", "NAO MEDIDO", "nenhum `except Exception` encontrado")
        return
    if com_traceback == len(sites) and tem_logging_import:
        registra("error (cli.py)", "SIM",
                  f"{com_traceback}/{len(sites)} sites de excecao nao mapeada chamam "
                  "logging.exception(...) — traceback completo preservado")
        return
    registra("error (cli.py)", "NAO",
              f"{len(sites)} sites de `except Exception as exc:` (linhas aproximadas via "
              f"offset de texto), {com_traceback} com logging.exception(...) — os demais "
              "imprimem so str(exc), sem traceback")


# ---- Q6: PIPELINE STATE — falha de task do Airflow deixa registro estruturado? --------

def q6_pipeline_state() -> None:
    print()
    print("Q6. PIPELINE STATE — uma falha de task do Airflow deixa registro alem do "
          "scheduler?")
    if not os.path.isdir(DAGS_DIR):
        registra("pipeline state (Airflow)", "NAO MEDIDO", f"{DAGS_DIR} nao encontrado")
        return
    dags = sorted(glob.glob(os.path.join(DAGS_DIR, "*.py")))
    com_callback = []
    sem_callback = []
    for caminho in dags:
        nome = os.path.basename(caminho)
        if "on_failure_callback" in le(caminho):
            com_callback.append(nome)
        else:
            sem_callback.append(nome)

    if not dags:
        registra("pipeline state (Airflow)", "NAO MEDIDO", "nenhuma DAG encontrada")
        return
    if not sem_callback:
        registra("pipeline state (Airflow)", "SIM",
                  f"{len(com_callback)}/{len(dags)} DAGs com on_failure_callback")
        return
    registra("pipeline state (Airflow)", "NAO",
              f"0/{len(dags)} DAGs com on_failure_callback/sla= — uma falha de task fica so "
              "no estado interno do scheduler")


# ---- --warehouse: confere o Snowflake real, opcional -----------------------------------

def q_warehouse_opcional(connection_name: str, database: str) -> None:
    print()
    print("EXTRA (--warehouse). O FACT_INGESTION_RUN do Snowflake REAL ja tem as 3 colunas "
          "de latencia?")
    try:
        from retail_platform.snowflake_load import connect
    except ImportError as exc:
        registra("latency (Snowflake ao vivo)", "NAO VERIFICADO",
                  f"nao foi possivel importar retail_platform: {exc}")
        return
    try:
        connection = connect(connection_name)
    except Exception as exc:  # config ausente, conta expirada, etc. — nao e o foco deste script
        registra("latency (Snowflake ao vivo)", "NAO VERIFICADO",
                  f"sem conexao ({type(exc).__name__}: {str(exc)[:200]}) — rode com uma "
                  "conta Snowflake configurada para conferir ao vivo")
        return
    try:
        cursor = connection.cursor()
        cursor.execute(
            f"select column_name from {database}.information_schema.columns "
            "where table_schema = 'GOLD' and table_name = 'FACT_INGESTION_RUN'"
        )
        colunas = {row[0].lower() for row in cursor.fetchall()}
        campos = {"duration_seconds", "started_at_utc", "finished_at_utc"}
        faltando = campos - colunas
        registra("latency (Snowflake ao vivo)", "NAO" if faltando else "SIM",
                  f"colunas ausentes no destino real: {', '.join(sorted(faltando)) or '(nenhuma)'}")
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--warehouse", action="store_true",
                         help="tambem confere o FACT_INGESTION_RUN do Snowflake real")
    parser.add_argument("--connection", default=os.environ.get("SNOWFLAKE_CONNECTION", "spark_retail"))
    parser.add_argument("--database", default=os.environ.get("SNOWFLAKE_DATABASE", "RETAIL"))
    args = parser.parse_args()

    print("prova dos seis sinais de AI_ENGINEERING_CONSTRAINTS.md secao 17 (Observability)")
    print("=" * 88)

    q1_processing()
    q2_throughput_failures()
    q3_latency()
    q4_coverage()
    q5_error()
    q6_pipeline_state()
    if args.warehouse:
        q_warehouse_opcional(args.connection, args.database)

    print()
    print("=" * 88)
    print("resumo:")
    for sinal, veredito, _ in resultados:
        print(f"  [{veredito:8}] {sinal}")

    nao = [s for s, v, _ in resultados if v == "NAO"]
    if nao:
        print()
        print(f"{len(nao)} sinal(is) genuinamente ausente(s): {', '.join(nao)}")
        print("isso e o que autoriza fechar exatamente esses — nada mais.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
