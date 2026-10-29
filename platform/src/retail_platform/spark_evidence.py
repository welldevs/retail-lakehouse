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


def _table(columns: list[str], lines: list) -> list[str]:
    return [
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
        *["| " + " | ".join("" if v is None else str(v) for v in line) + " |"
          for line in lines],
    ]


# ======================================================================================
# 1. INTEROP: who writes to the catalog
# ======================================================================================

def _observe_catalog(config=None) -> dict:
    """Which tables the catalog has, and WHO wrote each one.

    `written_by` is what turns "three writers" into a queryable fact instead of a
    documentation sentence. Without this column, interop would be claimed all over again.
    """
    from .orders_projection import TABLE_NAME as PROJECTION
    from .stock_bridge import CONSUMPTION_TABLE, LEDGER_TABLE, catalog

    out = {"tabelas": [], "escritores": {}}
    cat = catalog(config)
    for name in (PROJECTION, CONSUMPTION_TABLE, LEDGER_TABLE):
        if not cat.table_exists(name):
            out["tabelas"].append({"nome": name, "erro": "does not exist in the catalog"})
            continue
        table = cat.load_table(name)
        arrow = table.scan().to_arrow()
        writers = {}
        if "written_by" in arrow.column_names:
            for value in arrow.column("written_by").to_pylist():
                writers[value] = writers.get(value, 0) + 1
                out["escritores"][value] = out["escritores"].get(value, 0) + 1
        out["tabelas"].append({
            "nome": name,
            "linhas": arrow.num_rows,
            "snapshots": len(list(table.metadata.snapshots)),
            "escritores": writers,
            "metadata_location": table.metadata_location,
        })
    return out


# ======================================================================================
# 2 AND 3. THE SAME COMPUTATION, ON BOTH ENGINES
# ======================================================================================

def _ledger_in_python(config=None) -> dict:
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

    from .stock_bridge import CONSUMPTION_TABLE, catalog

    premises = ler_premissas(os.path.join(
        REPO, "platform", "dbt", "seeds", "stock_premises_seed.csv"))
    ledger = construir_ledger(premises)

    start = time.time()
    consumption = catalog(config).load_table(CONSUMPTION_TABLE).scan().to_arrow().to_pandas()
    days = pd.date_range(consumption["stock_date"].min(), consumption["stock_date"].max(), freq="D")
    n_days = len(days)

    # The SAME DENSIFICATION as the job: one row per series per day, including days without sales.
    series = consumption.groupby(["wh", "source_product_id"], as_index=False)["units_consumed"].sum()
    series["mean_daily_demand"] = series["units_consumed"] / float(n_days)
    grid = series.merge(pd.DataFrame({"stock_date": days.date}), how="cross")
    ledger_input = grid.merge(
        consumption, on=["wh", "source_product_id", "stock_date"], how="left",
        suffixes=("_total", ""))
    ledger_input["units_demanded"] = ledger_input["units_consumed"].fillna(0).astype("int64")

    lines = []
    for key, group in ledger_input.groupby(["wh", "source_product_id"], sort=False):
        lines.append(ledger(key, group))
    result = pd.concat(lines, ignore_index=True)
    elapsed = time.time() - start

    return {
        "segundos": round(elapsed, 1),
        "linhas": int(len(result)),
        "series": int(len(series)),
        "demanda": int(result["units_demanded"].sum()),
        "atendido": int(result["units_fulfilled"].sum()),
        "ruptura": int(result["units_short"].sum()),
        "ordens": int((result["reorder_units"] > 0).sum()),
        "chegadas": int((result["units_received"] > 0).sum()),
    }


def _ledger_in_spark() -> dict:
    """Runs the real job, through the same target the operation would use, and reads what it prints.

    THE TIME INCLUDES THE JVM STARTUP and the `docker compose run`, and that is stated on
    the page instead of discounted. Discounting the cost of starting the engine would measure
    a Spark that doesn't exist — whoever runs the job pays that cost.
    """
    command = ["docker", "compose", "--env-file", ".env", "-f", "infra/docker-compose.yml",
               "--profile", "spark", "run", "--rm", "--no-deps", "spark",
               "python3", "jobs/spark/stock_ledger.py"]
    start = time.time()
    proc = subprocess.run(command, cwd=REPO, capture_output=True, text=True)
    elapsed = time.time() - start
    if proc.returncode != 0:
        return {"erro": (proc.stdout + proc.stderr)[-400:]}

    metrics = {"segundos_com_jvm": round(elapsed, 1)}
    for line in proc.stdout.splitlines():
        if "." in line and " " in line:
            key, _, value = line.partition(" ")
            key = key.rstrip(".")
            if key in ("linhas", "series", "demanda", "atendido", "ruptura",
                         "ordens", "chegadas", "segundos", "dias_com_ruptura",
                         "unidades_pedidas", "cobertura_media"):
                try:
                    metrics[key] = float(value) if "." in value else int(value)
                except ValueError:
                    pass
    return metrics


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


def _observe_non_trigger(config=None) -> dict:
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
    connection = connect_lakehouse(config)
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    start = time.time()
    pairs, distinct = connection.execute(SELF_JOIN).fetchone()
    elapsed = time.time() - start
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "pares": pairs,
        "distintos": distinct,
        "segundos": round(elapsed, 2),
        "pico_gb": round(after / (1024 * 1024), 2),
        "delta_gb": round(max(after - before, 0) / (1024 * 1024), 2),
    }


# ======================================================================================
# collect and render
# ======================================================================================

def collect(config=None, *, rodar_spark: bool = True) -> dict:
    data = {"gerado_em": _utc_now()}
    for name, func in (
        ("catalogo", lambda: _observe_catalog(config)),
        ("nao_gatilho", lambda: _observe_non_trigger(config)),
        ("python", lambda: _ledger_in_python(config)),
    ):
        try:
            data[name] = func()
        except Exception as exc:  # the page declares the absence; it never invents the number
            data[name] = {"erro": f"{type(exc).__name__}: {str(exc)[:300]}"}
    data["spark"] = _ledger_in_spark() if rodar_spark else {"erro": "not run"}
    return data


def _absent(title: str, error: str) -> list[str]:
    return [f"### {title}", "",
            f"**Not observed on this run.** `{error}`", "",
            "The section declares the absence instead of disappearing: a page that hides",
            "what it couldn't measure is indistinguishable from one that measured and",
            "didn't like the result.", ""]


def render(data: dict) -> str:
    lines = [
        "# Spark evidence — and the measurement that argues against it",
        "",
        f"**Generated by `make spark-evidence` on {data['gerado_em']}.** Do not edit by hand.",
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
        "using the same library. `make check-spark-iceberg` was the gate that tested this",
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
    catalog_data = data.get("catalogo", {})
    if "erro" in catalog_data:
        lines += _absent("Catalog writers", catalog_data["erro"])
    else:
        lines += [
            "## The catalog's writers, today",
            "",
            "`written_by` is what turns \"three writers\" into a **queryable fact** instead",
            "of a documentation sentence.",
            "",
        ]
        lines += _table(
            ["Table", "Rows", "Snapshots", "Writers"],
            [[t["nome"], t.get("linhas", "—"), t.get("snapshots", "—"),
              ", ".join(f"`{k}` ({v})" for k, v in sorted(t.get("escritores", {}).items()))
              or t.get("erro", "—")]
             for t in catalog_data["tabelas"]],
        )
        distinct = sorted(catalog_data.get("escritores", {}))
        # THE SENTENCE IS DERIVED FROM THE LIST, not hard-coded. The first version said
        # "`stream` and `rebuild` are Python" — and the writers present were `platform` and
        # `rebuild`. Fixed text next to a generated table is the same disease as a
        # hand-copied number, on a page that exists precisely to have none.
        jvm = [e for e in distinct if e == "spark"]
        python = [e for e in distinct if e != "spark"]
        lines += ["",
                   f"**Distinct writers in the catalog: {len(distinct)}** — "
                   + ", ".join(f"`{e}`" for e in distinct) + ".",
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

    # ---- the two engines --------------------------------------------------------------
    py, sp = data.get("python", {}), data.get("spark", {})
    lines += ["## The same job, on both engines", "",
               "The loop function is **imported** from `jobs/spark/stock_ledger.py` by both",
               "paths — it is not an approximate reimplementation. The only thing that",
               "changes is who iterates over the groups: a `for` in one process, or Spark",
               "distributing it. If these were two different implementations, the",
               "comparison would measure the skill of whoever wrote each one.", ""]
    if "erro" in py or "erro" in sp:
        lines += _absent("Comparison", py.get("erro") or sp.get("erro"))
    else:
        identical = all(py.get(k) == sp.get(k)
                     for k in ("linhas", "series", "demanda", "atendido", "ruptura",
                               "ordens", "chegadas"))
        lines += _table(
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
        lines += ["",
                   f"**The two results are {'IDENTICAL' if identical else 'DIFFERENT'}.** "
                   + ("Without that, the time comparison would mean nothing, because the "
                      "two would be measuring different things."
                      if identical else
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
            faster = "Plain Python" if py["segundos"] < sp["segundos"] else "Spark"
            ratio = max(py["segundos"], sp["segundos"]) / max(
                min(py["segundos"], sp["segundos"]), 0.1)
            lines += [
                f"**At this volume, {faster} is ~{ratio:.1f}x faster.** If the winner",
                "is Python — which is expected at this scale — the number gets published",
                "just the same. It is the proof that Spark isn't here for speed, and a page",
                "that only published favorable results wouldn't prove anything.",
                "",
            ]

    # ---- the trigger that did not fire -------------------------------------------------
    ng = data.get("nao_gatilho", {})
    lines += ["## The volume trigger, which did NOT fire", ""]
    if "erro" in ng:
        lines += _absent("Basket self-join", ng["erro"])
    else:
        lines += [
            "This project's natural candidate for \"volume that requires Spark\" is the",
            "basket self-join — every pair of products bought together, which is the basis",
            "of any affinity analysis. Measured in DuckDB, on a single node:",
            "",
        ]
        lines += _table(
            ["Pairs", "Distinct pairs", "Seconds", "Peak RSS (GB)"],
            [[f"{ng['pares']:,}",
              f"{ng['distintos']:,}",
              ng["segundos"], ng["pico_gb"]]],
        )
        lines += ["",
                   "**The volume trigger did not fire, and it is measured.** ARCHITECTURE",
                   "declares Spark's trigger as *\"a partition DuckDB can't hold in",
                   "memory\"*; this number is what says it stays closed. If it ever turns",
                   "into minutes and tens of gigabytes, Spark's justification changes — and",
                   "that is also information.",
                   ""]

    lines += [
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
    return "\n".join(lines) + "\n"


def write(data: dict, out: str = DEFAULT_OUT) -> str:
    target = os.path.join(REPO, out) if not os.path.isabs(out) else out
    os.makedirs(os.path.dirname(target), exist_ok=True)
    tmp_path = target + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(render(data))
    os.replace(tmp_path, target)
    return target
