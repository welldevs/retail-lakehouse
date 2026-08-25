"""DAG sob demanda: RAW no object storage e Silver a partir do Callejero do INE.

Terceira source, ao lado de mercadona_catalog_daily e ine_population_on_demand. Sem
cron, igual a populacao — mas por um motivo estrutural diferente e ainda mais forte:

  1. schedule=None: o Callejero NAO TEM API. E publicado semestralmente para download
     manual no site do INE. Uma DAG so pode rodar depois que alguem baixou os arquivos
     novos e colocou em CALLEJERO_IN dentro do repositorio montado — nao ha nada pra
     esta DAG "verificar" sozinha, ao contrario da populacao (que ao menos tem uma API
     pra consultar sob demanda). Disparo manual (`make callejero-trigger` ou pela UI),
     sempre depois de um download humano.
  2. sem pool dedicado: nao ha requisicao de rede nenhuma aqui — `extract` so copia
     arquivos locais (CONTRACT.md da source, secao 1). Nao ha o que throttle.
  3. sem guard_date: mesma razao da populacao — a particao imutavel (partition.py)
     ja protege contra reescrita acidental, e nao ha "dado de hoje rotulado como
     ontem" possivel quando a fonte e um diretorio local, nao uma API com relogio.
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
DATA_ROOT = f"{REPO}/data/callejero"

CALLEJERO_SOURCE_SRC = f"{REPO}/sources/ine-callejero-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")
DBT = os.environ.get("RETAIL_DBT", f"{REPO}/platform/.venv/bin/dbt")

# Onde os arquivos baixados manualmente do site do INE devem estar, DENTRO do
# repositorio montado no container, antes de disparar esta DAG.
CALLEJERO_IN = os.environ.get("RETAIL_CALLEJERO_IN", f"{REPO}/temp")
PROVINCES = os.environ.get("RETAIL_CALLEJERO_PROVINCES", "08,28,41,46")

SILVER_MODELS = [
    "silver_callejero_sections",
    "silver_callejero_population_units",
    "silver_callejero_streets",
    "silver_callejero_pseudo_addresses",
]

# Codigos de saida do CONTRACT.md secao 7 da source (mesmos valores das outras duas
# sources, independentes — nao ha acoplamento em comparti-los aqui).
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
    return _run([SOURCE_PYTHON, "-m", "ine_callejero_source", *argv],
                {"PYTHONPATH": CALLEJERO_SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoca a plataforma no venv separado, com o codigo vindo do repo montado."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def partition_path(ingestion_date: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}"


def already_landed(ds: str, **_) -> bool:
    """Short-circuit olhando o DESTINO, nao o disco local — mesmo raciocinio das outras
    duas sources: exit 0 = nada a fazer; exit 1 = aterrissada mas divergente, `land`
    se auto-corrige; exit 2 = nem existe, segue para extract."""
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
    if not os.path.isdir(CALLEJERO_IN):
        raise RuntimeError(
            f"CALLEJERO_IN nao existe: {CALLEJERO_IN}. Baixe os arquivos do Callejero "
            f"do site do INE e coloque nesse diretorio (dentro do repositorio montado) "
            f"antes de disparar esta DAG."
        )

    code = _run_source(
        ["extract", "--in", CALLEJERO_IN, "--out", DATA_ROOT, "--provinces", PROVINCES, "--date", ds]
    )
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
    # Mesma invocacao das outras duas sources: `dbt build` cobre o projeto inteiro, entao
    # os 4 modelos silver_callejero_* sao pegos automaticamente, sem alvo dedicado. Faz o
    # mesmo `retail-platform has-data` do Makefile antes de excluir os modelos do build
    # quando nada foi aterrissado ainda — ver Makefile, alvo `silver`.
    has_data = _run_platform(["has-data", "ine_callejero"]) == EXIT_OK
    exclude = [] if has_data else ["--exclude", *SILVER_MODELS]
    code = _run([DBT, "build", "--project-dir", f"{REPO}/platform/dbt",
                 "--profiles-dir", f"{REPO}/platform/dbt", *exclude],
                {"PYTHONPATH": PLATFORM_SRC})
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build falhou (exit {code})")


with DAG(
    dag_id="ine_callejero_on_demand",
    description="RAW no object storage e Silver a partir do Callejero (geografia oficial) do INE",
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
        # Mesma trigger_rule das outras duas: se o gate curto-circuitou (nada novo),
        # silver ainda roda (barato, idempotente) em vez de ficar `skipped` arrastado.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    gate >> do_extract >> do_validate >> do_land >> do_verify >> build_silver
