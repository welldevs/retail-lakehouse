"""DAG do warehouse analitico: recorte do Silver -> Snowflake -> GOLD -> MART.

E a PRIMEIRA DAG deste repo que atravessa a fronteira entre dois motores. As outras tres
ficam inteiras do lado do lakehouse (fonte externa -> RAW -> Silver, tudo DuckDB); esta
pega o Silver que qualquer uma delas produziu e o leva para outro banco, com outra
credencial, em outra rede. E exatamente onde retry e codigo de saida importam, e por isso
a aresta e do Airflow e nao de um script solto.

TRES DECISOES, cada uma com o motivo no ponto onde e imposta:

  1. DAG PROPRIA, e nao um TaskGroup no fim de mercadona_catalog_daily. O recorte le o
     Silver de TODAS as sources — catalogo, populacao, Callejero e clientes sinteticos —
     entao pendura-lo numa delas faria a atualizacao do warehouse depender de qual source
     rodou por ultimo. Quem quiser encadear usa TriggerDagRunOperator; quem quiser rodar
     sozinho dispara esta.

  2. NAO RECEBE `wh`. Diferente das DAGs de source, aqui nao ha eixo de armazem: o recorte
     cobre os quatro de uma consulta so, e o modelo dimensional e global.

  3. `export` E `load` SAO TAREFAS SEPARADAS, pelo mesmo motivo que `land` e
     `verify-landing` sao. O recorte falha por dado (uma query errada, um Silver vazio); o
     load falha por rede ou credencial. Juntar os dois faria "o recorte esta errado" chegar
     como falha de conexao, e o retry insistiria numa coisa que retry nao conserta — por
     isso `export` tem retries=0 e `load` tem 2.

O `bootstrap` (database, schemas, papeis, grants) NAO esta aqui de proposito: exige
ACCOUNTADMIN e roda uma vez por conta, nao todo dia. Ver `make warehouse-bootstrap`.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
STAGE_DIR = f"{REPO}/data/snowflake-stage"

PLATFORM_SRC = f"{REPO}/platform/src"
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")
DBT = os.environ.get("RETAIL_DBT", f"{REPO}/platform/.venv/bin/dbt")

SNOWFLAKE_DATABASE = os.environ.get("RETAIL_SNOWFLAKE_DATABASE", "RETAIL")
SNOWFLAKE_CONNECTION = os.environ.get("RETAIL_SNOWFLAKE_CONNECTION", "spark_retail")
# EXPLICITO na DAG, embora seja o default do carregador: e aqui que se le qual papel a
# automacao usa. A conexao aponta para o papel administrativo porque e ela que roda o
# bootstrap; a carga sobrescreve, e nao pode criar database nem ler GOLD.
SNOWFLAKE_LOAD_ROLE = os.environ.get("RETAIL_SNOWFLAKE_LOAD_ROLE", "RETAIL_LOADER")

# As sources cujo Silver o recorte le. Se nenhuma aterrissou, nao ha o que carregar e a DAG
# curto-circuita em vez de falhar — mesmo criterio de `has-data` das outras tres.
SOURCES = ["mercadona_catalog_api", "ine_callejero", "ine_population_api", "simulated_oltp", "simulated_orders"]

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    print("$", " ".join(argv))
    process = subprocess.Popen(
        argv, cwd=REPO, env=env, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1,
    )
    with process.stdout:
        for line in process.stdout:
            print(line, end="")
    return process.wait()


def _run_platform(argv: list[str]) -> int:
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv], {"PYTHONPATH": PLATFORM_SRC})


def silver_has_data(**_) -> bool:
    """Sem nenhuma source aterrissada nao existe Silver para recortar.

    Curto-circuito, nao falha: um repositorio recem-clonado antes da primeira ingestao nao
    esta com defeito — so nao tem o que carregar ainda.
    """
    presentes = [s for s in SOURCES if _run_platform(["has-data", s]) == EXIT_OK]
    if not presentes:
        print("nenhuma source aterrissada: nada a recortar.")
        return False
    print(f"sources com dado: {', '.join(presentes)}")
    return True


def export_recorte(**_) -> None:
    """Le o Silver e escreve parquet local. NAO fala com o Snowflake.

    retries=0 na tarefa: falha aqui e falha de DADO (query errada, Silver vazio, tipo sem
    mapeamento). Repetir a mesma consulta sobre o mesmo dado da o mesmo erro.
    """
    code = _run_platform(["export-snowflake", "--out", STAGE_DIR])
    if code != EXIT_OK:
        raise RuntimeError(f"export-snowflake falhou (exit {code}); retry nao resolve")


def load_stage(**_) -> None:
    """PUT em stage interno + COPY INTO + reconferencia contagem a contagem."""
    code = _run_platform([
        "load-snowflake", "--stage-dir", STAGE_DIR,
        "--database", SNOWFLAKE_DATABASE, "--connection", SNOWFLAKE_CONNECTION,
        "--role", SNOWFLAKE_LOAD_ROLE,
    ])
    if code == EXIT_FATAL:
        raise RuntimeError(f"load-snowflake falhou de forma fatal (exit {code})")
    if code != EXIT_OK:
        raise RuntimeError(f"load-snowflake reprovou a reconferencia (exit {code})")


def build_gold(**_) -> None:
    """`--target snowflake` desliga a arvore inteira do Silver pelo guard do dbt_project."""
    code = _run([DBT, "build", "--project-dir", f"{REPO}/platform/dbt",
                 "--profiles-dir", f"{REPO}/platform/dbt", "--target", "snowflake"],
                {"PYTHONPATH": PLATFORM_SRC})
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build --target snowflake falhou (exit {code})")


with DAG(
    dag_id="warehouse_load",
    description="Recorte do Silver para o Snowflake e reconstrucao de GOLD e MART",
    # Sem cron: o warehouse deve ser atualizado DEPOIS de uma ingestao, nao num horario
    # fixo que pode cair no meio de uma. Encadeie com TriggerDagRunOperator a partir da
    # DAG de source, ou dispare a mao (`make warehouse-trigger`).
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["layer:warehouse", "engine:snowflake"],
    doc_md=__doc__,
) as dag:

    gate = ShortCircuitOperator(
        task_id="skip_if_no_silver",
        python_callable=silver_has_data,
        ignore_downstream_trigger_rules=False,
    )

    do_export = PythonOperator(
        task_id="export_recorte",
        python_callable=export_recorte,
        # Falha de dado nao e retentavel: a mesma query sobre o mesmo Silver erra igual.
        retries=0,
    )

    do_load = PythonOperator(
        task_id="load_stage",
        python_callable=load_stage,
        # Rede E retentavel, e a carga e idempotente: as tabelas STAGE sao full refresh e o
        # PUT usa OVERWRITE, entao repetir nao duplica linha.
        retries=2,
    )

    do_build = PythonOperator(
        task_id="build_gold_and_mart",
        python_callable=build_gold,
        retries=1,
    )

    gate >> do_export >> do_load >> do_build
