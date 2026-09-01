"""DAG sob demanda: base de clientes sinteticos ancorados em geografia real.

Quarta source, e a UNICA DERIVADA: as outras tres sao upstream de dado externo, esta
consome o Silver que elas produziram. Isso muda a forma do DAG em dois pontos que nao tem
paralelo nas outras:

  1. `export_reference` roda UMA VEZ e alimenta os quatro armazens. A Source e FROZEN
     (dependencies = []) e nao pode abrir conexao com o Lakehouse, entao a plataforma
     materializa antes o que ela precisa em tres JSON planos. Poe-lo dentro do ramo de
     cada armazem repetiria quatro vezes a mesma consulta pesada (216 mil candidatos de
     endereco) para produzir o mesmo arquivo.
  2. NAO HA REDE em passo nenhum — nem pool, nem throttle, nem guard_date. `extract` le
     JSON local e escreve JSON local; o unico I/O externo e o `land` para o object
     storage. A extracao inteira dos quatro armazens leva ~6 s.

schedule=None pelo mesmo motivo estrutural do Callejero: nao ha "dado novo" para buscar.
A base de clientes muda quando ALGUEM decide muda-la — mais clientes, outra seed, ou
porque o Callejero/populacao a montante foram reingeridos. Disparo manual, com os
parametros abaixo.

REGERAR A BASE E ADITIVO. O gerador consome uma unica random.Random(seed) em ordem fixa e
nada antes do laco depende de `count`, entao os primeiros N clientes de uma geracao maior
sao byte a byte os mesmos de antes — verificado ponta a ponta (200 -> 5.000 preservou os
200). Por isso `overwrite` existe como parametro em vez de ser proibido: aumentar
`customers_per_wh` nao invalida os clientes que ja existem. Condicoes: mesma seed, mesma
referencia, mesma ingestion_date, e o MESMO ESCOPO DE CADASTRO — desde a Fase 6, a idade
minima, a taxa de penetracao e a regra de alocacao entram na lista, porque as tres trocam as
pessoas por tras dos mesmos ids tanto quanto a seed troca. O manifesto guarda todas em
`config`/`reference`, e o `history` acumula as execucoes anteriores, para que uma regeracao
deixe rastro.

O TAMANHO DA BASE, POREM, NAO VEM MAIS DAQUI por padrao. `customers_per_wh` e nulo, e nesse
caso o `extract` usa o alvo que `export-oltp-reference` derivou da populacao adulta de cada
armazem. Preencher o parametro e override explicito, e o manifesto registra
`count_source: "cli"`.
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
DATA_ROOT = f"{REPO}/data/oltp"
REFERENCE_ROOT = f"{REPO}/data/oltp-reference"

OLTP_SOURCE_SRC = f"{REPO}/sources/simulated-oltp-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")

# Os mesmos quatro armazens da Mercadona, e nao por coincidencia: a AUF de cada um vem de
# warehouse_service_area, derivada em torno do municipio-sede de cada `wh`.
WAREHOUSES = os.environ.get("RETAIL_OLTP_WAREHOUSES", "mad1,bcn1,svq1,vlc1").split(",")

# Codigos de saida do CONTRACT.md secao 7 da source.
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
    return _run([SOURCE_PYTHON, "-m", "simulated_oltp_source", *argv],
                {"PYTHONPATH": OLTP_SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoca a plataforma no venv separado, com o codigo vindo do repo montado."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def partition_path(ingestion_date: str, warehouse: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={ingestion_date}/wh={warehouse}"


def reference_path(ingestion_date: str) -> str:
    return f"{REFERENCE_ROOT}/ingestion_date={ingestion_date}"


def export_reference(ds: str, **_) -> None:
    """A ponte entre o Lakehouse e a Source FROZEN. Roda uma vez, cobre os quatro wh.

    `export-oltp-reference` ja recusa com mensagem acionavel se o Callejero ou a populacao
    nao tiverem aterrissado — nao ha gate a duplicar aqui.
    """
    code = _run_platform(["export-oltp-reference", "--out", REFERENCE_ROOT, "--date", ds])
    if code != EXIT_OK:
        raise RuntimeError(f"export-oltp-reference falhou (exit {code})")


def already_landed(warehouse: str, ds: str, params: dict, **_) -> bool:
    """Short-circuit olhando o DESTINO, mesmo raciocinio das outras tres sources.

    Com `overwrite`, o gate se DESLIGA. Sem isto o parametro seria inerte: quem dispara
    para regerar a base pede exatamente o caso que o gate considera "nada a fazer" — a
    particao ja esta aterrissada — e o ramo inteiro seria pulado antes do extract.
    """
    if params.get("overwrite"):
        print("overwrite pedido: gate desligado, o ramo segue mesmo ja aterrissado.")
        return True

    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code == EXIT_OK:
        print("particao ja aterrissada e verificada no destino: nada a fazer.")
        return False
    print(f"verify-landing retornou {code}: seguindo o pipeline.")
    return True


def extract(warehouse: str, ds: str, params: dict, **_) -> None:
    partition = partition_path(ds, warehouse)
    overwrite = bool(params.get("overwrite"))

    if os.path.exists(os.path.join(partition, "_SUCCESS")) and not overwrite:
        print("particao ja completa e imutavel em disco: extract pulado (nao e falha).")
        print("Dispare com overwrite=true para regerar a base.")
        return

    reference = reference_path(ds)
    if not os.path.isdir(reference):
        raise RuntimeError(
            f"referencia ausente: {reference}. A tarefa export_reference deveria te-la "
            f"produzido — verifique o log dela."
        )

    argv = [
        "extract",
        "--reference", reference,
        "--out", DATA_ROOT,
        "--wh", warehouse,
        "--date", ds,
        "--seed", str(params["seed"]),
    ]
    # --count SO quando alguem pede explicitamente. Omitido, a Source usa o alvo que
    # `export-oltp-reference` derivou da populacao adulta daquele armazem — ver a docstring
    # do parametro. Passar um numero aqui por padrao era o que fazia os quatro armazens
    # nascerem do mesmo tamanho.
    if params.get("customers_per_wh"):
        argv.extend(["--count", str(params["customers_per_wh"])])
    if overwrite:
        argv.append("--overwrite")

    code = _run_source(argv)
    if code == EXIT_FATAL:
        raise RuntimeError(f"extract falhou de forma fatal (exit {code}); retry nao resolve")
    if code == EXIT_PARTIAL:
        raise RuntimeError(f"extract parcial (exit {code}): ha entradas em failures[]")
    if code != EXIT_OK:
        raise RuntimeError(f"extract retornou exit {code}")


def validate(warehouse: str, ds: str, **_) -> None:
    """--reference NAO e opcional aqui, diferente das outras tres sources.

    A garantia central — todo cliente mora num endereco real da AUF do seu armazem — so
    pode ser reconferida relendo a mesma referencia que gerou a particao.
    """
    code = _run_source([
        "validate", partition_path(ds, warehouse),
        "--reference", reference_path(ds), "--strict",
    ])
    if code != EXIT_OK:
        raise RuntimeError(f"validate reprovou a particao (exit {code})")


def land(warehouse: str, ds: str, **_) -> None:
    code = _run_platform(["land", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"land falhou (exit {code})")


def verify_landing(warehouse: str, ds: str, **_) -> None:
    code = _run_platform(["verify-landing", partition_path(ds, warehouse)])
    if code != EXIT_OK:
        raise RuntimeError(f"verify-landing reprovou o destino (exit {code})")


def silver(**_) -> None:
    """`dbt build` do Silver, com o portao aplicado pela plataforma.

    O PORTAO NAO MORA AQUI, e ja morou — este era o defeito. Cada DAG carregava a propria
    copia parcial da decisao (a exclusao da PROPRIA source, e mais nenhuma), o Makefile
    carregava a versao completa, e as seis divergiam. Esta DAG nao tinha portao algum e
    reprovava todo dia desde que `silver_live_order_state` nasceu, porque a projecao Iceberg
    so pode ser lida quando o catalogo responde — e o catalogo nao tem nada a ver com a
    source desta DAG.

    Agora ha um verbo: `retail_platform silver-build` observa o que aterrissou e se o
    catalogo responde, e monta `--exclude`/`--vars` sozinho. Ver silver_gate.py.
    """
    code = _run_platform([
        "silver-build",
        "--project-dir", f"{REPO}/platform/dbt",
        "--profiles-dir", f"{REPO}/platform/dbt",
    ])
    if code != EXIT_OK:
        raise RuntimeError(f"dbt build falhou (exit {code})")


with DAG(
    dag_id="simulated_oltp_customers",
    description="Base de clientes sinteticos com endereco real do Callejero (RAW + Silver)",
    schedule=None,
    start_date=datetime(2026, 8, 27),
    catchup=False,
    max_active_runs=1,
    params={
        # NULO, e nao um numero. O tamanho da base e derivado da populacao adulta de cada
        # armazem pela taxa de penetracao declarada em customer_premises_seed, e vem no
        # cabecalho da referencia. Preencher aqui e OVERRIDE explicito, registrado como
        # `count_source: cli` no manifesto — nao o caminho normal.
        "customers_per_wh": None,
        "seed": 20260827,
        # Regerar deliberadamente. Aumentar customers_per_wh com a MESMA seed e a MESMA
        # data e aditivo: os clientes existentes sao preservados byte a byte. Trocar a
        # seed troca as pessoas por tras dos mesmos ids — e trocar a idade minima, a taxa
        # ou a regra de alocacao tambem. O manifesto registra tudo isso em `history`.
        "overwrite": False,
    },
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["source:simulated", "layer:raw", "layer:silver"],
    doc_md=__doc__,
) as dag:

    build_reference = PythonOperator(
        task_id="export_reference",
        python_callable=export_reference,
        retries=1,
    )

    build_silver = PythonOperator(
        task_id="silver",
        python_callable=silver,
        retries=1,
        # Mesma regra do DAG da Mercadona, e pelo mesmo defeito medido la: com
        # all_success, um unico armazem curto-circuitado arrastaria o silver para
        # `skipped` e o dado recem-aterrissado dos outros tres nunca seria transformado.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for warehouse in WAREHOUSES:
        gate = ShortCircuitOperator(
            task_id=f"skip_if_landed_{warehouse}",
            python_callable=already_landed,
            op_kwargs={"warehouse": warehouse},
            # Com mais de um armazem o padrao True esta ERRADO: o short-circuit pularia
            # TODO o downstream ignorando a trigger_rule de cada tarefa, inclusive a do
            # `silver`, que e compartilhado. Ver mercadona_catalog_daily.
            ignore_downstream_trigger_rules=False,
        )

        do_extract = PythonOperator(
            task_id=f"extract_{warehouse}",
            python_callable=extract,
            op_kwargs={"warehouse": warehouse},
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

        build_reference >> gate >> do_extract >> do_validate >> do_land >> do_verify
        do_verify >> build_silver
