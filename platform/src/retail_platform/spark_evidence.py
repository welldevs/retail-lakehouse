"""Spark evidence — INCLUDING the measurement that argues against it.

WHY THIS PAGE EXISTS, and why it publishes an unfavorable number on purpose.

ARCHITECTURE claims Spark was NOT adopted for performance. A claim like that, without a
measurement, is rhetorical modesty — the same disease as a hand-copied number, just with the
sign flipped. This project's constraints document forbids "claiming performance without a
benchmark", and the obligation is symmetric: claiming an ABSENCE of performance also requires
measuring.

So this page runs the SAME stock job on both implementations — Spark and plain Python — over
the SAME input, and publishes both times side by side, no matter which one wins.

THE THREE THINGS IT RECORDS, and none of them is "Spark is fast":

  1. INTEROP — how many distinct writers the Iceberg catalog has today, and which ones. This
     was the property that justified Iceberg since Phase 3 and that had been claimed and
     never demonstrated, because both writers were Python.
  2. SHAPE — the result of the two implementations has to be IDENTICAL. If they diverge, the
     time comparison means nothing, because they are measuring different things.
  3. TIME — both, with the JVM overhead included and stated.

And it records the TRIGGER THAT DID NOT FIRE: the basket self-join, the natural candidate for
"volume that requires Spark", measured in DuckDB.

WHAT THIS MODULE DOES NOT DO: it does not validate, does not fix, and does not decide. It
observes and writes. If Spark is not available, the section says so — partial evidence is
useful, invented evidence is not.
"""

from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timezone

DEFAULT_OUT = os.path.join("docs", "spark-evidence", "README.md")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _tabela(colunas: list[str], linhas: list) -> list[str]:
    return [
        "| " + " | ".join(colunas) + " |",
        "|" + "|".join("---" for _ in colunas) + "|",
        *["| " + " | ".join("" if v is None else str(v) for v in linha) + " |"
          for linha in linhas],
    ]


# ======================================================================================
# 1. INTEROP: who writes to the catalog
# ======================================================================================

def _observar_catalogo(config=None) -> dict:
    """Which tables the catalog has, and WHO wrote each one.

    `written_by` is what turns "three writers" into a queryable fact instead of a
    documentation sentence. Without this column, interop would be claimed all over again.
    """
    from .orders_projection import TABLE_NAME as PROJECAO
    from .stock_ledger import CONSUMPTION_TABLE, LEDGER_TABLE, catalog

    saida = {"tabelas": [], "escritores": {}}
    cat = catalog(config)
    for nome in (PROJECAO, CONSUMPTION_TABLE, LEDGER_TABLE):
        if not cat.table_exists(nome):
            saida["tabelas"].append({"nome": nome, "erro": "nao existe no catalogo"})
            continue
        tabela = cat.load_table(nome)
        arrow = tabela.scan().to_arrow()
        escritores = {}
        if "written_by" in arrow.column_names:
            for valor in arrow.column("written_by").to_pylist():
                escritores[valor] = escritores.get(valor, 0) + 1
                saida["escritores"][valor] = saida["escritores"].get(valor, 0) + 1
        saida["tabelas"].append({
            "nome": nome,
            "linhas": arrow.num_rows,
            "snapshots": len(list(tabela.metadata.snapshots)),
            "escritores": escritores,
            "metadata_location": tabela.metadata_location,
        })
    return saida


# ======================================================================================
# 2 AND 3. THE SAME COMPUTATION, ON BOTH ENGINES
# ======================================================================================

def _ledger_em_python(config=None) -> dict:
    """The SAME loop as the Spark job, in plain Python over the same Iceberg table.

    THIS IS NOT AN APPROXIMATE REIMPLEMENTATION — it is the same algorithm: the
    `applyInPandas` function is imported from `jobs/spark/stock_ledger.py`, so there's no way
    for the two to diverge over a translation detail. The only thing that changes is WHO
    iterates over the groups: here a `for` in one process, there Spark distributing it. If
    these were two different implementations, the time comparison would measure the skill of
    whoever wrote each one.
    """
    import sys

    import pandas as pd

    if REPO not in sys.path:
        sys.path.insert(0, REPO)
    from jobs.spark.stock_ledger import construir_ledger, ler_premissas  # noqa: E402

    from .stock_ledger import CONSUMPTION_TABLE, catalog

    premissas = ler_premissas(os.path.join(
        REPO, "platform", "dbt", "seeds", "stock_premises_seed.csv"))
    ledger = construir_ledger(premissas)

    inicio = time.time()
    consumo = catalog(config).load_table(CONSUMPTION_TABLE).scan().to_arrow().to_pandas()
    dias = pd.date_range(consumo["stock_date"].min(), consumo["stock_date"].max(), freq="D")
    n_dias = len(dias)

    # A MESMA DENSIFICACAO do job: uma linha por serie por dia, inclusive sem venda.
    series = consumo.groupby(["wh", "source_product_id"], as_index=False)["units_consumed"].sum()
    series["mean_daily_demand"] = series["units_consumed"] / float(n_dias)
    grade = series.merge(pd.DataFrame({"stock_date": dias.date}), how="cross")
    entrada = grade.merge(
        consumo, on=["wh", "source_product_id", "stock_date"], how="left",
        suffixes=("_total", ""))
    entrada["units_demanded"] = entrada["units_consumed"].fillna(0).astype("int64")

    linhas = []
    for chave, grupo in entrada.groupby(["wh", "source_product_id"], sort=False):
        linhas.append(ledger(chave, grupo))
    resultado = pd.concat(linhas, ignore_index=True)
    decorrido = time.time() - inicio

    return {
        "segundos": round(decorrido, 1),
        "linhas": int(len(resultado)),
        "series": int(len(series)),
        "demanda": int(resultado["units_demanded"].sum()),
        "atendido": int(resultado["units_fulfilled"].sum()),
        "ruptura": int(resultado["units_short"].sum()),
        "ordens": int((resultado["reorder_units"] > 0).sum()),
        "chegadas": int((resultado["units_received"] > 0).sum()),
    }


def _ledger_em_spark() -> dict:
    """Runs the real job, through the same target the operation would use, and reads what it prints.

    THE TIME INCLUDES THE JVM STARTUP and the `docker compose run`, and that is stated on
    the page instead of discounted. Discounting the cost of starting the engine would measure
    a Spark that doesn't exist — whoever runs the job pays that cost.
    """
    comando = ["docker", "compose", "--env-file", ".env", "-f", "infra/docker-compose.yml",
               "--profile", "spark", "run", "--rm", "--no-deps", "spark",
               "python3", "jobs/spark/stock_ledger.py"]
    inicio = time.time()
    proc = subprocess.run(comando, cwd=REPO, capture_output=True, text=True)
    decorrido = time.time() - inicio
    if proc.returncode != 0:
        return {"erro": (proc.stdout + proc.stderr)[-400:]}

    metricas = {"segundos_com_jvm": round(decorrido, 1)}
    for linha in proc.stdout.splitlines():
        if "." in linha and " " in linha:
            chave, _, valor = linha.partition(" ")
            chave = chave.rstrip(".")
            if chave in ("linhas", "series", "demanda", "atendido", "ruptura",
                         "ordens", "chegadas", "segundos", "dias_com_ruptura",
                         "unidades_pedidas", "cobertura_media"):
                try:
                    metricas[chave] = float(valor) if "." in valor else int(valor)
                except ValueError:
                    pass
    return metricas


# ======================================================================================
# THE TRIGGER THAT DID NOT FIRE
# ======================================================================================

SELF_JOIN = """
    with cesta as (
        select order_id, fulfilled_source_product_id as produto
        from silver_order_line
        where line_status in ('fulfilled', 'substituted')
          and fulfilled_source_product_id is not null
    )
    select count(*) as pares, count(distinct a.produto || '|' || b.produto) as distintos
    from cesta a
    join cesta b on a.order_id = b.order_id and a.produto < b.produto
"""


def _observar_nao_gatilho(config=None) -> dict:
    """The basket self-join in DuckDB: the natural candidate for "volume that requires Spark".

    It is here because the decision to adopt Spark has to come with the measurement that
    does NOT support it. If this number ever turns into minutes and gigabytes, the volume
    trigger will have fired — and then Spark's justification changes, which is also
    information.
    """
    import resource

    from .config import from_env
    from .query import connect_lakehouse

    config = config or from_env()
    conexao = connect_lakehouse(config)
    antes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    inicio = time.time()
    pares, distintos = conexao.execute(SELF_JOIN).fetchone()
    decorrido = time.time() - inicio
    depois = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "pares": pares,
        "distintos": distintos,
        "segundos": round(decorrido, 2),
        "pico_gb": round(depois / (1024 * 1024), 2),
        "delta_gb": round(max(depois - antes, 0) / (1024 * 1024), 2),
    }


# ======================================================================================
# collect and render
# ======================================================================================

def collect(config=None, *, rodar_spark: bool = True) -> dict:
    dados = {"gerado_em": _utc_now()}
    for nome, funcao in (
        ("catalogo", lambda: _observar_catalogo(config)),
        ("nao_gatilho", lambda: _observar_nao_gatilho(config)),
        ("python", lambda: _ledger_em_python(config)),
    ):
        try:
            dados[nome] = funcao()
        except Exception as exc:  # a pagina declara a ausencia; nunca inventa o numero
            dados[nome] = {"erro": f"{type(exc).__name__}: {str(exc)[:300]}"}
    dados["spark"] = _ledger_em_spark() if rodar_spark else {"erro": "nao executado"}
    return dados


def _ausente(titulo: str, erro: str) -> list[str]:
    return [f"### {titulo}", "",
            f"**Not observed on this run.** `{erro}`", "",
            "The section declares the absence instead of disappearing: a page that hides",
            "what it couldn't measure is indistinguishable from one that measured and",
            "didn't like the result.", ""]


def render(dados: dict) -> str:
    linhas = [
        "# Spark evidence — and the measurement that argues against it",
        "",
        f"**Generated by `make spark-evidence` on {dados['gerado_em']}.** Do not edit by hand.",
        "",
        "This page exists to back a **negative** claim: Spark was not adopted for",
        "performance. Saying so without measuring would be rhetorical modesty — the same",
        "disease as a hand-copied number, with the sign flipped. So the same job runs on",
        "both engines, over the same input, and both times end up published.",
        "",
        "## Why Spark is in this project",
        "",
        "Two reasons, and performance is not one of them.",
        "",
        "**1. It is the Iceberg catalog's third writer, and the first outside Python.**",
        "Iceberg was justified by *interop between engines* since Phase 3, and that half of",
        "the justification was **claimed and never demonstrated**: both writers were Python",
        "using the same library. `make spike-spark-iceberg` was the gate that tested this",
        "before any line of this phase existed — with both outcomes declared in advance,",
        "including erasing the interop clause if it didn't hold up.",
        "",
        "**2. The job's shape is not SQL.** The stock balance is a running sum whose",
        "*inputs are generated by decisions made from the state itself*: the balance drops",
        "below the reorder point, an order is issued, it arrives days later and changes the",
        "next balance, which decides whether there's a new order. A window function reads",
        "the whole partition but doesn't write back into it.",
        "",
    ]

    # ---- interop ---------------------------------------------------------------------
    catalogo = dados.get("catalogo", {})
    if "erro" in catalogo:
        linhas += _ausente("Catalog writers", catalogo["erro"])
    else:
        linhas += [
            "## The catalog's writers, today",
            "",
            "`written_by` is what turns \"three writers\" into a **queryable fact** instead",
            "of a documentation sentence.",
            "",
        ]
        linhas += _tabela(
            ["Table", "Rows", "Snapshots", "Writers"],
            [[t["nome"], t.get("linhas", "—"), t.get("snapshots", "—"),
              ", ".join(f"`{k}` ({v})" for k, v in sorted(t.get("escritores", {}).items()))
              or t.get("erro", "—")]
             for t in catalogo["tabelas"]],
        )
        distintos = sorted(catalogo.get("escritores", {}))
        # A FRASE E DERIVADA DA LISTA, e nao cravada. A primeira versao dizia "`stream` e
        # `rebuild` sao Python" — e os escritores presentes eram `platform` e `rebuild`. Um
        # texto fixo ao lado de uma tabela gerada e a mesma doenca do numero copiado a mao,
        # numa pagina que existe justamente para nao ter nenhum.
        jvm = [e for e in distintos if e == "spark"]
        python = [e for e in distintos if e != "spark"]
        linhas += ["",
                   f"**Distinct writers in the catalog: {len(distintos)}** — "
                   + ", ".join(f"`{e}`" for e in distintos) + ".",
                   "",
                   ", ".join(f"`{e}`" for e in python)
                   + (" is Python" if len(python) == 1 else " are Python")
                   + (f"; {', '.join(f'`{e}`' for e in jvm)} is the JVM." if jvm
                      else ". **No writer outside Python** — interop remains "
                           "claimed and not demonstrated."),
                   "",
                   "The property that justified Iceberg since Phase 3 was *interop between",
                   "engines*, and it stops being a claim only once this list has a name",
                   "that isn't Python.",
                   ""]

    # ---- os dois motores --------------------------------------------------------------
    py, sp = dados.get("python", {}), dados.get("spark", {})
    linhas += ["## The same job, on both engines", "",
               "The loop function is **imported** from `jobs/spark/stock_ledger.py` by both",
               "paths — it is not an approximate reimplementation. The only thing that",
               "changes is who iterates over the groups: a `for` in one process, or Spark",
               "distributing it. If these were two different implementations, the",
               "comparison would measure the skill of whoever wrote each one.", ""]
    if "erro" in py or "erro" in sp:
        linhas += _ausente("Comparison", py.get("erro") or sp.get("erro"))
    else:
        iguais = all(py.get(k) == sp.get(k)
                     for k in ("linhas", "series", "demanda", "atendido", "ruptura",
                               "ordens", "chegadas"))
        linhas += _tabela(
            ["", "Plain Python", "Spark"],
            [["rows", py.get("linhas"), sp.get("linhas")],
             ["series", py.get("series"), sp.get("series")],
             ["demand", py.get("demanda"), sp.get("demanda")],
             ["fulfilled", py.get("atendido"), sp.get("atendido")],
             ["stockout", py.get("ruptura"), sp.get("ruptura")],
             ["orders issued", py.get("ordens"), sp.get("ordens")],
             ["arrivals", py.get("chegadas"), sp.get("chegadas")],
             ["**seconds**", f"**{py.get('segundos')}**",
              f"**{sp.get('segundos')}** (job) · "
              f"{sp.get('segundos_com_jvm')} with the JVM and the container"]],
        )
        linhas += ["",
                   f"**The two results are {'IDENTICAL' if iguais else 'DIFFERENT'}.** "
                   + ("Without that, the time comparison would mean nothing, because the "
                      "two would be measuring different things."
                      if iguais else
                      "**This is a defect**: while they diverge, neither of the two times is "
                      "comparable, and the divergence is what needs investigating before "
                      "any reading of this page."),
                   "",
                   "Spark's time **includes the JVM startup and the `docker compose run`**,",
                   "and that is not discounted on purpose: whoever runs the job pays that",
                   "cost. The \"job\" column is what the job itself times, so the difference",
                   "between the two stays visible instead of hidden in a footnote.",
                   ""]
        if py.get("segundos") and sp.get("segundos"):
            mais_rapido = "Plain Python" if py["segundos"] < sp["segundos"] else "Spark"
            razao = max(py["segundos"], sp["segundos"]) / max(
                min(py["segundos"], sp["segundos"]), 0.1)
            linhas += [
                f"**At this volume, {mais_rapido} is ~{razao:.1f}x faster.** If the winner",
                "is Python — which is expected at this scale — the number gets published",
                "just the same. It is the proof that Spark isn't here for speed, and a page",
                "that only published favorable results wouldn't prove anything.",
                "",
            ]

    # ---- o gatilho que nao disparou ----------------------------------------------------
    ng = dados.get("nao_gatilho", {})
    linhas += ["## The volume trigger, which did NOT fire", ""]
    if "erro" in ng:
        linhas += _ausente("Basket self-join", ng["erro"])
    else:
        linhas += [
            "This project's natural candidate for \"volume that requires Spark\" is the",
            "basket self-join — every pair of products bought together, which is the basis",
            "of any affinity analysis. Measured in DuckDB, on a single node:",
            "",
        ]
        linhas += _tabela(
            ["Pairs", "Distinct pairs", "Seconds", "Peak RSS (GB)"],
            [[f"{ng['pares']:,}".replace(",", "."),
              f"{ng['distintos']:,}".replace(",", "."),
              ng["segundos"], ng["pico_gb"]]],
        )
        linhas += ["",
                   "**The volume trigger did not fire, and it is measured.** ARCHITECTURE",
                   "declares Spark's trigger as *\"a partition DuckDB can't hold in",
                   "memory\"*; this number is what says it stays closed. If it ever turns",
                   "into minutes and tens of gigabytes, Spark's justification changes — and",
                   "that is also information.",
                   ""]

    linhas += [
        "## What this page does NOT prove",
        "",
        "- **That Spark scales here.** It runs `local[*]`: driver and executor on the same",
        "  JVM. There is no shuffle between nodes, no cluster, and a fake cluster wouldn't",
        "  prove either scale or interoperability.",
        "- **That the job needs Spark today.** It needs an engine that expresses",
        "  per-series feedback; plain Python expresses that too. What Spark adds is being",
        "  the writer outside Python and parallelizing by series once the series grow.",
        "- **That the stock is real.** The balance is calculated from observed consumption",
        "  plus a policy declared in a seed. No source in this repository measures stock.",
        "",
    ]
    return "\n".join(linhas) + "\n"


def write(dados: dict, out: str = DEFAULT_OUT) -> str:
    destino = os.path.join(REPO, out) if not os.path.isabs(out) else out
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    temporario = destino + ".tmp"
    with open(temporario, "w", encoding="utf-8") as arquivo:
        arquivo.write(render(dados))
    os.replace(temporario, destino)
    return destino
