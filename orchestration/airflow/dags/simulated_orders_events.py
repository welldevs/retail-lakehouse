"""DAG sob demanda: pedidos sinteticos como LOG DE EVENTOS (RAW + Silver).

Quinta source, e a SEGUNDA DERIVADA: consome o Silver que as outras produziram e devolve
uma RAW nova. A regra de ouro da Fase 1 vale igual, deslocada um nivel — o PEDIDO e
inventado; quem compra, o que se compra, quanto custa e onde mora nao.

TRES DIFERENCAS DE FORMA EM RELACAO AS OUTRAS QUATRO
-----------------------------------------------------
1. `export_reference` roda UMA VEZ e alimenta os quatro armazens E a janela inteira. A
   Source e FROZEN (dependencies = []) e nao pode abrir conexao com o Lakehouse, entao a
   plataforma materializa antes cliente, catalogo, calendario de preco e premissas em
   quatro JSON planos. Po-lo dentro do ramo de cada armazem repetiria a mesma consulta 16
   vezes para produzir o mesmo arquivo.

2. CADA RAMO PERCORRE A JANELA DE DIAS POR DENTRO, em vez de existirem 4 x N tarefas. O
   grafo fica estavel quando a janela cresce, e a granularidade que se perde nao custa
   nada em retry: `extract` pula particao com `_SUCCESS` (nao e falha), `land` pula objeto
   com o checksum esperado, e `validate`/`verify-landing` sao puras. Reexecutar o ramo
   inteiro depois de uma falha no terceiro dia refaz so o que faltava.

3. A JANELA E PARAMETRO, e nao "hoje". Nas outras quatro a particao e o snapshot do dia da
   execucao; aqui `ingestion_date` e a DATA DO PEDIDO, e ela e limitada pelo que existe de
   catalogo. `export-orders-reference` RECUSA uma janela cujo (armazem, dia) nao tenha
   nenhum snapshot de catalogo anterior ou igual — pedido nesse dia teria de inventar
   preco. Medido em 2026-08-28: os 4 armazens tem catalogo de 08-24 a 08-27.

NAO HA REDE em passo nenhum da geracao — nem pool, nem throttle, nem guard_date. `extract`
le JSON local e escreve JSON local; o unico I/O externo e o `land`.

schedule=None pelo mesmo motivo estrutural do Callejero e da base de clientes: nao ha
"dado novo" para buscar. A janela de pedidos muda quando ALGUEM decide muda-la.

REGERAR TROCA OS PEDIDOS, e isso e diferente da base de clientes. La aumentar `count` com a
mesma seed e aditivo. Aqui o que e aditivo e o EIXO DO TEMPO: cada (armazem, dia) deriva a
propria sub-seed de sha256(seed|wh|dia), entao acrescentar um dia a janela deixa os dias ja
gerados byte a byte identicos. O que NAO e aditivo: outra seed, outra referencia, ou outra
tabela de premissas — as tres trocam os pedidos por tras dos mesmos order_id, e o manifesto
registra as tres em `history`.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime, timedelta

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
DATA_ROOT = f"{REPO}/data/orders"
REFERENCE_ROOT = f"{REPO}/data/orders-reference"

ORDERS_SOURCE_SRC = f"{REPO}/sources/simulated-orders-source/src"
PLATFORM_SRC = f"{REPO}/platform/src"

SOURCE_PYTHON = os.environ.get("RETAIL_SOURCE_PYTHON", "python3")
PLATFORM_PY = os.environ.get("RETAIL_PLATFORM_PYTHON", f"{REPO}/platform/.venv/bin/python")

# Os mesmos quatro armazens das outras sources: a area de servico de cada um vem de
# warehouse_service_area, e um cliente de W so pede de W.
WAREHOUSES = os.environ.get("RETAIL_ORDERS_WAREHOUSES", "mad1,bcn1,svq1,vlc1").split(",")

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
    return _run([SOURCE_PYTHON, "-m", "simulated_orders_source", *argv],
                {"PYTHONPATH": ORDERS_SOURCE_SRC})


def _run_platform(argv: list[str]) -> int:
    """Invoca a plataforma no venv separado, com o codigo vindo do repo montado."""
    return _run([PLATFORM_PY, "-m", "retail_platform", *argv],
                {"PYTHONPATH": PLATFORM_SRC})


def partition_path(order_date: str, warehouse: str) -> str:
    return f"{DATA_ROOT}/ingestion_date={order_date}/wh={warehouse}"


def reference_path(window_to: str) -> str:
    return f"{REFERENCE_ROOT}/ingestion_date={window_to}"


def _window(params: dict) -> list[str]:
    """Dias da janela, inclusive nas duas pontas. Recusa janela invertida.

    A janela e explicita e nao derivada de `ds`: `ingestion_date` aqui e a data do PEDIDO, e
    cair no dia da execucao geraria pedidos num dia que talvez nem tenha catalogo.
    """
    inicio = date.fromisoformat(params["window_from"])
    fim = date.fromisoformat(params["window_to"])
    if fim < inicio:
        raise RuntimeError(
            f"janela invertida: window_from={inicio} > window_to={fim}"
        )
    return [(inicio + timedelta(days=n)).isoformat() for n in range((fim - inicio).days + 1)]


def export_reference(params: dict, **_) -> None:
    """A ponte entre o Lakehouse e a Source FROZEN. Roda uma vez, cobre os 4 wh e a janela.

    `export-orders-reference` ja recusa com mensagem acionavel quando falta catalogo ou
    cliente, e quando algum (armazem, dia) da janela nao tem base de preco — nao ha gate a
    duplicar aqui.
    """
    dias = _window(params)
    code = _run_platform([
        "export-orders-reference",
        "--out", REFERENCE_ROOT,
        "--date", params["window_to"],
        "--from", params["window_from"],
        "--to", params["window_to"],
    ])
    if code != EXIT_OK:
        raise RuntimeError(f"export-orders-reference falhou (exit {code})")
    print(f"referencia cobre {len(dias)} dia(s): {dias[0]} a {dias[-1]}")


def already_landed(warehouse: str, params: dict, **_) -> bool:
    """Short-circuit olhando o DESTINO, mesmo raciocinio das outras quatro sources.

    Aqui o gate pergunta pela JANELA INTEIRA: segue em frente se QUALQUER dia ainda nao
    estiver aterrissado e verificado. Um gate por dia nao existe porque o ramo e por
    armazem; um gate que exigisse os quatro dias completos para pular seria o mesmo.

    Com `overwrite`, o gate se DESLIGA. Sem isto o parametro seria inerte: quem dispara para
    regerar pede exatamente o caso que o gate considera "nada a fazer".
    """
    if params.get("overwrite"):
        print("overwrite pedido: gate desligado, o ramo segue mesmo ja aterrissado.")
        return True

    pendentes = []
    for dia in _window(params):
        code = _run_platform(["verify-landing", partition_path(dia, warehouse)])
        if code != EXIT_OK:
            pendentes.append(dia)
    if not pendentes:
        print("janela inteira ja aterrissada e verificada no destino: nada a fazer.")
        return False
    print(f"{len(pendentes)} dia(s) pendente(s): {pendentes}. Seguindo o pipeline.")
    return True


def extract(warehouse: str, params: dict, **_) -> None:
    overwrite = bool(params.get("overwrite"))
    reference = reference_path(params["window_to"])
    if not os.path.isdir(reference):
        raise RuntimeError(
            f"referencia ausente: {reference}. A tarefa export_reference deveria te-la "
            f"produzido — verifique o log dela."
        )

    for dia in _window(params):
        partition = partition_path(dia, warehouse)
        if os.path.exists(os.path.join(partition, "_SUCCESS")) and not overwrite:
            print(f"{dia}: particao ja completa e imutavel: extract pulado (nao e falha).")
            continue

        argv = [
            "extract",
            "--reference", reference,
            "--out", DATA_ROOT,
            "--wh", warehouse,
            "--date", dia,
            "--seed", str(params["seed"]),
        ]
        if overwrite:
            argv.append("--overwrite")

        code = _run_source(argv)
        if code == EXIT_FATAL:
            raise RuntimeError(
                f"{dia}: extract falhou de forma fatal (exit {code}); retry nao resolve"
            )
        if code == EXIT_PARTIAL:
            raise RuntimeError(f"{dia}: extract parcial (exit {code}): ha entradas em failures[]")
        if code != EXIT_OK:
            raise RuntimeError(f"{dia}: extract retornou exit {code}")


def validate(warehouse: str, params: dict, **_) -> None:
    """--reference NAO e opcional aqui, como na source de clientes e pelo mesmo motivo.

    As garantias centrais — o produto existe no catalogo daquele armazem naquela data, o
    preco pago e o preco observado, o cliente e daquele armazem — so podem ser reconferidas
    relendo a mesma referencia que gerou a particao. Um validador que so confere checksum
    provaria integridade, nao coerencia.
    """
    reference = reference_path(params["window_to"])
    for dia in _window(params):
        code = _run_source([
            "validate", partition_path(dia, warehouse),
            "--reference", reference, "--strict",
        ])
        if code != EXIT_OK:
            raise RuntimeError(f"{dia}: validate reprovou a particao (exit {code})")


def land(warehouse: str, params: dict, **_) -> None:
    for dia in _window(params):
        code = _run_platform(["land", partition_path(dia, warehouse)])
        if code != EXIT_OK:
            raise RuntimeError(f"{dia}: land falhou (exit {code})")


def verify_landing(warehouse: str, params: dict, **_) -> None:
    for dia in _window(params):
        code = _run_platform(["verify-landing", partition_path(dia, warehouse)])
        if code != EXIT_OK:
            raise RuntimeError(f"{dia}: verify-landing reprovou o destino (exit {code})")


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
    dag_id="simulated_orders_events",
    description="Pedidos sinteticos como log de eventos, com cliente e preco reais (RAW + Silver)",
    schedule=None,
    start_date=datetime(2026, 8, 28),
    catchup=False,
    max_active_runs=1,
    params={
        # Medido em 2026-08-28: os 4 armazens tem catalogo de 08-24 a 08-27, e a base de
        # clientes existe desde 08-24. Fora disso o export RECUSA, em vez de inventar preco.
        "window_from": "2026-08-24",
        "window_to": "2026-08-27",
        "seed": 20260828,
        # Regerar deliberadamente. Acrescentar um DIA nao precisa disto — cada dia deriva a
        # propria sub-seed e os anteriores ficam identicos. `overwrite` e para trocar a seed
        # ou a tabela de premissas, que trocam os pedidos por tras dos mesmos order_id.
        "overwrite": False,
    },
    default_args={
        "owner": "data-platform",
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["source:simulated", "layer:raw", "layer:silver", "model:events"],
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
        # Mesma regra das outras DAGs, e pelo mesmo defeito medido: com all_success, um
        # unico armazem curto-circuitado arrastaria o silver para `skipped` e o dado
        # recem-aterrissado dos outros tres nunca seria transformado.
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    for warehouse in WAREHOUSES:
        gate = ShortCircuitOperator(
            task_id=f"skip_if_landed_{warehouse}",
            python_callable=already_landed,
            op_kwargs={"warehouse": warehouse},
            # Com mais de um armazem o padrao True esta ERRADO: o short-circuit pularia TODO
            # o downstream ignorando a trigger_rule de cada tarefa, inclusive a do `silver`,
            # que e compartilhado. Ver mercadona_catalog_daily.
            ignore_downstream_trigger_rules=False,
        )

        do_extract = PythonOperator(
            task_id=f"extract_{warehouse}",
            python_callable=extract,
            op_kwargs={"warehouse": warehouse},
            # Falha de dado. Retry nao conserta referencia incoerente nem janela invalida.
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
            # Rede E retentavel, e `land` e idempotente: objeto com o checksum esperado e
            # pulado, nao reenviado.
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
