"""DAG sob demanda: RAW no object storage e Silver a partir da populacao do INE.

Ao contrario de mercadona_catalog_daily, este DAG NAO tem cron. Tres decisoes vem disso:

  1. schedule=None: o INE publica Cifras de Poblacion de forma irregular — as vezes
     meses entre uma atualizacao e outra. Um cron diario ou mensal daria a falsa
     impressao de garantia de frescor que a fonte nao tem. Disparo manual, quando
     alguem sabe que o INE publicou algo novo (`make ine-trigger` ou pela UI).
  2. sem pool dedicado: o pool `mercadona_api` existe porque concorrencia MEDIDA deu
     ~20% de 403 sob fan-out de 4 armazens. Aqui nao ha fan-out — uma run busca um
     table_id so, sequencial por natureza — entao nao ha nada medido que justifique
     inventar infra de throttle agora.
  3. sem guard_date: a garantia da Mercadona existe porque a API so serve o preco de
     HOJE, e um run datado do passado gravaria dado de hoje sob rotulo errado. A API do
     INE devolve a serie historica inteira em qualquer momento em que for chamada —
     nao ha "dado de hoje rotulado como ontem" possivel aqui. A imutabilidade de
     particao (extract.py) ja protege contra reescrita acidental.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
DATA_ROOT = f"{REPO}/data/ine"

INE_SOURCE_SRC = f"{REPO}/sources/ine-population-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")
DBT = os.environ.get("RETAIL_DBT", f"{REPO}/platform/.venv/bin/dbt")

TABLES = os.environ.get("RETAIL_INE_TABLES", "31304")

# Codigos de saida do CONTRACT.md secao 7 da source (mesmos valores da Mercadona, sources
# diferentes e independentes — nao ha acoplamento em comparti-los aqui).
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
    """Executa e ENCAMINHA a saida linha a linha para o log da tarefa."""
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


def _run_source(argv: list[str]) -> int:
    """Invoca a Source no interpretador do Airflow. Seguro porque dependencies = []."""
    return _run([SOURCE_PYTHON, "-m", "ine_population_source", *argv],
                {"PYTHONPATH": INE_SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoca a plataforma no venv separado, com o codigo vindo do repo montado."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def partition_path(ingestion_date: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}"


def already_landed(ds: str, **_) -> bool:
    """Short-circuit olhando o DESTINO, nao o disco local — mesmo raciocinio da Mercadona:
    exit 0 = nada a fazer; exit 1 = aterrissada mas divergente, `land` se auto-corrige;
    exit 2 = nem existe, segue para extract."""
    code = _run_platform(["verify-landing", partition_path(ds)])
    if code == EXIT_OK:
        print("particao ja aterrissada e verificada no destino: nada a fazer.")
        return False
    print(f"verify-landing retornou {code}: seguindo o pipeline.")
    return True


def extract(ds: str, **_) -> None:
    if os.path.exists(os.path.join(partition_path(ds), "_SUCCESS")):
        print("particao ja completa e imutavel em disco: extract pulado (nao e falha).")
        return

    code = _run_source(["extract", "--out", DATA_ROOT, "--tables", TABLES, "--date", ds])
    if code == EXIT_FATAL:
        raise RuntimeError(f"extract falhou de forma fatal (exit {code}); retry nao resolve")
    if code == EXIT_PARTIAL:
        raise RuntimeError(f"extract parcial (exit {code}): ha entradas em failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract retornou exit {code}")


def validate(ds: str, **_) -> None:
    code = _run_source(["validate", partition_path(ds), "--strict"])
    if code != EXIT_OK:
        raise RuntimeError(f"validate reprovou a particao (exit {code})")


def land(ds: str, **_) -> None:
    code = _run_platform(["land", partition_path(ds)])
    if code != EXIT_OK:
        raise RuntimeError(f"land falhou (exit {code})")


def verify_landing(ds: str, **_) -> None:
    code = _run_platform(["verify-landing", partition_path(ds)])
    if code != EXIT_OK:
        raise RuntimeError(f"verify-landing reprovou o destino (exit {code})")


def silver(**_) -> None:
    # Mesma invocacao da Mercadona: `dbt build` cobre o projeto inteiro, entao o modelo
    # silver_ine_population_series e pego automaticamente, sem alvo dedicado. Faz o mesmo
    # `retail-platform has-data` do Makefile antes de excluir o modelo do build quando
    # nada foi aterrissado ainda — ver Makefile, alvo `silver`.
    has_data = _run_platform(["has-data", "ine_population_api"]) == EXIT_OK
    exclude = [] if has_data else ["--exclude", "silver_ine_population_series"]
    code = _run([DBT, "build", "--project-dir", f"{REPO}/platform/dbt",
                 "--profiles-dir", f"{REPO}/platform/dbt", *exclude],
                {"PYTHONPATH": PLATFORM_SRC})
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build falhou (exit {code})")


with DAG(
    dag_id="ine_population_on_demand",
    description="RAW no object storage e Silver a partir da populacao por provincia do INE",
    schedule=None,
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["source:ine", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    gate = ShortCircuitOperator(
        task_id="skip_if_landed",
        python_callable=already_landed,
        ignore_downstream_trigger_rules=False,
    )

    do_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
        retries=0,
    )

    do_validate = PythonOperator(
        task_id="validate",
        python_callable=validate,
        retries=0,
    )

    do_land = PythonOperator(
        task_id="land",
        python_callable=land,
        retries=2,
    )

    do_verify = PythonOperator(
        task_id="verify_landing",
        python_callable=verify_landing,
        retries=1,
    )

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # Mesma trigger_rule da Mercadona: se o gate curto-circuitou (nada novo), silver
        # ainda roda (barato, idempotente) em vez de ficar `skipped` arrastado pelo gate.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    gate >> do_extract >> do_validate >> do_land >> do_verify >> build_silver
