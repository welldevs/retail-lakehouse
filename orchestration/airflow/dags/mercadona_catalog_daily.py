"""DAG diario: extract -> validate -> land -> verify -> silver.

Cinco decisoes deste DAG vem de comportamento MEDIDO da fonte e da Source, nao de
convencao. Cada uma esta comentada no ponto onde e imposta:

  1. exit 2 do `extract` NAO significa falha do dia (particao completa e imutavel);
  2. catchup=False e guard_date: backfill e IMPOSSIVEL nesta fonte;
  3. pool de 1 slot: o throttle vive dentro do processo, nao entre processos;
  4. a Source roda com PYTHONPATH e zero dependencia, sem conflitar com o Airflow;
  5. data/ e scratch; a verdade passa a ser o object storage.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta, timezone

from airflow import DAG

from airflow.utils.trigger_rule import TriggerRule

try:  # Airflow 3.x
    from airflow.providers.standard.operators.python import (
        PythonOperator,
        ShortCircuitOperator,
    )
except ImportError:  # Airflow 2.x
    from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Raiz do repositorio, montada no container ou o proprio checkout no host.
REPO = os.environ.get("RETAIL_REPO_ROOT", "/opt/retail-lakehouse")
DATA_ROOT = f"{REPO}/data/source"

# Nenhum codigo e importado deste DAG: tudo e invocado por subprocesso, com PYTHONPATH
# apontando para o repositorio montado. Assim editar um modulo nao exige rebuild da
# imagem, e o DAG nao carrega dependencia nenhuma da Source ou da plataforma.
SOURCE_SRC = f"{REPO}/sources/mercadona-catalog-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

# DUAS RUNTIMES. A Source tem dependencies = [] e roda no proprio interpretador do
# Airflow — nao existe pacote de terceiros para conflitar. A plataforma precisa de
# boto3/duckdb/dbt, que colidem com os pins do Airflow (jinja2, click, pydantic), entao
# vive num venv separado. Os defaults abaixo apontam para o venv do HOST, para que
# `airflow dags test` funcione a partir do checkout; no container o compose sobrescreve
# com /opt/platform-venv.
SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")
DBT = os.environ.get("RETAIL_DBT", f"{REPO}/platform/.venv/bin/dbt")

WAREHOUSES = ["mad1", "bcn1"]

# UM slot. O throttle da Source e POR PROCESSO
# (http_client.py: 1/delay req/s medido do inicio da requisicao anterior), entao dois
# `extract` concorrentes dobram a taxa real contra o host. Medido: 152 requisicoes em 9 s
# produziram ~20% de 403 e bloqueio intermitente por minutos; sequencial a 1,5 s deu 0
# falhas. Com varios armazens, este pool e a UNICA coisa preservando a taxa segura.
API_POOL = "mercadona_api"

# Codigos de saida do CONTRACT.md secao 7 da Source.
EXIT_OK = 0
EXIT_PARTIAL = 1
EXIT_FATAL = 2


def _run(argv: list[str], env_extra: dict | None = None) -> int:
    """Executa e ENCAMINHA a saida linha a linha para o log da tarefa.

    Deixar o subprocesso herdar os descritores parece funcionar e nao funciona: a saida
    vai para o stdout do container, nao para o log da tarefa, e um exit code diferente de
    zero chega ao Airflow sem nenhuma explicacao ao lado. Streaming linha a linha (em vez
    de capturar tudo no fim) importa porque o extract leva ~227 s: sem isto a tarefa fica
    muda durante quatro minutos.
    """
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
    return _run([SOURCE_PYTHON, "-m", "mercadona_catalog_source", *argv],
                {"PYTHONPATH": SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoca a plataforma no venv separado, com o codigo vindo do repo montado."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def partition_path(ingestion_date: str, warehouse: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}/wh={warehouse}"


def guard_date(ds: str, **_) -> None:
    """Falha se o DAG run nao for de hoje (UTC).

    NAO EXISTE BACKFILL nesta fonte: a API serve apenas o preco de HOJE. Um run datado de
    uma data passada gravaria os precos de hoje sob a chave daquela data — dado
    silenciosamente errado, que nenhum teste a jusante pegaria, porque a particao seria
    internamente consistente. O vao de 8 dias entre 2026-08-16 e 2026-08-24 e
    irrecuperavel por essa razao.

    TRES CAMADAS, verificadas:
      1. o proprio Airflow recusa execution_date no futuro;
      2. catchup=False + start_date impedem o agendamento de runs anteriores ao inicio;
      3. esta guarda cobre o disparo MANUAL de qualquer data != hoje.

    Enquanto start_date == hoje, a camada 3 e redundante. Ela passa a ser a UNICA
    protecao no dia seguinte, quando datas entre start_date e hoje se tornam
    agendaveis por trigger manual — que e justamente quando o erro fica possivel.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if ds != today:
        raise ValueError(
            f"run datado de {ds}, hoje e {today}. A API serve apenas o preco de hoje: "
            f"extrair para uma data passada produziria dado errado. Backfill nao existe "
            f"nesta fonte."
        )
    print(f"data confere: {ds}")


def already_landed(ds: str, warehouse: str, **_) -> bool:
    """Short-circuit olhando o DESTINO, nao o disco local.

    Reusar `verify-landing` como gate, em vez de checar um marcador, tem tres efeitos que
    um `os.path.exists` nao daria:

      - exit 0 -> a particao esta no destino E com checksum conferido: nada a fazer;
      - exit 1 -> aterrissada mas divergente: seguimos, e `land` a conserta (objeto com
        checksum errado e reenviado), ou seja o DAG se auto-corrige;
      - exit 2 -> nem existe localmente: seguimos para extract.

    Checar o `_SUCCESS` LOCAL aqui seria um erro: pularia o `land` de uma particao
    extraida a mao e nunca aterrissada.
    """
    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code == EXIT_OK:
        print("particao ja aterrissada e verificada no destino: nada a fazer.")
        return False
    print(f"verify-landing retornou {code}: seguindo o pipeline.")
    return True


def extract(ds: str, warehouse: str, **_) -> None:
    """Roda o extract da Source.

    A Source tem ZERO dependencia de runtime, entao roda no proprio interpretador do
    worker via PYTHONPATH, sem venv e sem conflitar com as dependencias pinadas do
    Airflow. E o retorno pratico da decisao de manter dependencies = [].
    """
    # Particao completa em disco e IMUTAVEL: reexecutar retorna exit 2. Nao e falha do
    # dia — e a garantia 5 do contrato funcionando. Detectamos ANTES de chamar, para nao
    # transformar sucesso em erro. Uma particao PARCIAL nao entra aqui: a retomada e
    # exatamente o que queremos que aconteca (garantia 7).
    if os.path.exists(os.path.join(partition_path(ds, warehouse), "_SUCCESS")):
        print("particao ja completa e imutavel em disco: extract pulado (nao e falha).")
        return

    code = _run_source(["extract", "--out", DATA_ROOT, "--wh", warehouse, "--date", ds])
    if code == EXIT_FATAL:
        # Particao inutilizavel ou uso invalido. Retry nao ajuda: o insumo nao muda.
        raise RuntimeError(f"extract falhou de forma fatal (exit {code}); retry nao resolve")
    if code == EXIT_PARTIAL:
        # Categorias em failures[]. A particao existe mas o contrato a reprova na
        # validacao (garantia 16), entao paramos aqui com a causa explicita.
        raise RuntimeError(f"extract parcial (exit {code}): ha entradas em failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract retornou exit {code}")


def validate(ds: str, warehouse: str, **_) -> None:
    """Gate. Exit 1 reprova sem retry: reler os mesmos bytes daria o mesmo resultado."""
    code = _run_source(["validate", partition_path(ds, warehouse), "--strict"])
    if code != EXIT_OK:
        raise RuntimeError(f"validate reprovou a particao (exit {code})")


def land(ds: str, warehouse: str, **_) -> None:
    code = _run_platform(["land", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"land falhou (exit {code})")


def verify_landing(ds: str, warehouse: str, **_) -> None:
    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"verify-landing reprovou o destino (exit {code})")


def silver(**_) -> None:
    code = _run([DBT, "build", "--project-dir", f"{REPO}/platform/dbt",
                 "--profiles-dir", f"{REPO}/platform/dbt"],
                {"PYTHONPATH": PLATFORM_SRC})
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build falhou (exit {code})")


with DAG(
    dag_id="mercadona_catalog_daily",
    description="RAW no object storage e Silver a partir do catalogo da Mercadona",
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 24),
    # BACKFILL E IMPOSSIVEL: a API so serve o preco de hoje. Ligar catchup encheria o
    # lake de particoes datadas no passado contendo precos de hoje.
    catchup=False,
    # Uma execucao por vez. Duas concorrentes disputariam a mesma particao em disco e
    # dobrariam a taxa de requisicao contra a fonte.
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["source:mercadona", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    for warehouse in WAREHOUSES:
        check_date = PythonOperator(
            task_id=f"guard_date_{warehouse}",
            python_callable=guard_date,
        )

        gate = ShortCircuitOperator(
            task_id=f"skip_if_landed_{warehouse}",
            python_callable=already_landed,
            op_kwargs={"warehouse": warehouse},
            # PADRAO E True, e com mais de um armazem isso esta ERRADO: o short-circuit
            # pula TODO o downstream ignorando a trigger_rule de cada tarefa — inclusive a
            # do `silver`, que e compartilhado. Resultado observado: bcn1 aterrissava com
            # sucesso e o `silver` era pulado mesmo assim, porque mad1 curto-circuitou.
            #
            # Com False, o gate pula apenas o SEU ramo, e a cascata normal de skip cuida
            # do resto. O `silver` volta a decidir pela propria trigger_rule.
            ignore_downstream_trigger_rules=False,
        )

        do_extract = PythonOperator(
            task_id=f"extract_{warehouse}",
            python_callable=extract,
            op_kwargs={"warehouse": warehouse},
            pool=API_POOL,
            retries=0,
        )

        do_validate = PythonOperator(
            task_id=f"validate_{warehouse}",
            python_callable=validate,
            op_kwargs={"warehouse": warehouse},
            retries=0,
        )

        do_land = PythonOperator(
            task_id=f"land_{warehouse}",
            python_callable=land,
            op_kwargs={"warehouse": warehouse},
            # Rede E retentavel, e `land` e idempotente: objeto com o checksum esperado
            # e pulado, nao reenviado.
            retries=2,
        )

        do_verify = PythonOperator(
            task_id=f"verify_landing_{warehouse}",
            python_callable=verify_landing,
            op_kwargs={"warehouse": warehouse},
            retries=1,
        )

        check_date >> gate >> do_extract >> do_validate >> do_land >> do_verify

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # A regra PADRAO (all_success) esta ERRADA aqui, e o erro so aparece com mais de um
        # armazem: o short-circuit de um deles marca seu verify_landing como `skipped`, e
        # com all_success isso arrasta o `silver` junto — o dado recem-aterrissado do OUTRO
        # armazem nunca seria transformado.
        #
        # none_failed_min_one_success da o comportamento certo nos tres casos:
        #   todos curto-circuitaram  -> nada novo, silver `skipped` (correto, e barato);
        #   ao menos um aterrissou   -> silver RODA;
        #   algum falhou             -> silver nao roda sobre um RAW suspeito.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # O Silver le do object storage, entao depende do verify de TODOS os armazens.
    for task in dag.tasks:
        if task.task_id.startswith("verify_landing_"):
            task >> build_silver
