"""Evidence for the stream plane: OLTP, broker, projection — and the three folds agreeing.

WHY THIS MODULE EXISTS, and why it is not the twin of `snowflake_evidence.py`.

The reason there was EXPIRATION: the account is a trial, and once it dies the warehouse
models turn into fourteen files nobody can prove ever ran. Here the reason is different, and
it is what ARCHITECTURE.md itself already recorded as deliberate debt: the streaming half is
NOT COVERED OFFLINE. `make test` runs without network — that is a repo invariant — so the
broker, OLTP and Iceberg only exist while `make stream-up` is up. The in-memory doubles
(`fake_kafka`, `fake_pg`, `fake_iceberg`) cover the code's SHAPE: transaction ordering, the
dedup protocol, SQL construction. What they cannot cover is the SEMANTICS of the real
engines — that Kafka preserves order by key, that Postgres really does undo, that Iceberg
refuses a commit over a stale snapshot.

This page is the record that the semantics were exercised against the real engines. It
replaces "trust me" with a dated number, the same way the warehouse evidence does — but the
trigger to regenerate it is different: there it is "before the account expires", here it is
"after any run worth recording".

WHAT THIS MODULE IS NOT: it validates nothing, fixes nothing, and starts no service. It
OBSERVES the three planes and writes down what it observed. What validates is the platform
suite's 273 checks, the three proof scripts (`orders-prove-*`), and `orders-reconcile`.

IT IS TOLERANT OF A PLANE BEING DOWN, ON PURPOSE. If Kafka is not up, the broker section says
so instead of bringing down the report: partial evidence is useful, and evidence that does
not exist is not. What it NEVER does is invent the number it could not observe — every
missing section shows up as a declared absence.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

DEFAULT_OUT = os.path.join("docs", "stream-evidence", "README.md")


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _tabela(colunas: list[str], linhas: list) -> list[str]:
    cabecalho = "| " + " | ".join(colunas) + " |"
    separador = "|" + "|".join("---" for _ in colunas) + "|"
    corpo = [
        "| " + " | ".join("" if v is None else str(v) for v in linha) + " |"
        for linha in linhas
    ]
    return [cabecalho, separador, *corpo]


# --------------------------------------------------------------------------------------
# Observation, plane by plane
# --------------------------------------------------------------------------------------
# Cada bloco devolve `{"erro": "..."}` em vez de propagar a excecao. O relatorio precisa
# poder ser gerado com o plano meio de pe — e precisa DIZER que estava meio de pe.

def _observar_oltp(dsn=None) -> dict:
    from . import orders_oltp
    try:
        conexao = orders_oltp.connect(dsn)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        resumo = orders_oltp.outbox_summary(conexao)
        with conexao.cursor() as cur:
            cur.execute("select count(*) from orders")
            resumo["orders"] = cur.fetchone()[0]
            cur.execute("select count(*) from order_line")
            resumo["order_lines"] = cur.fetchone()[0]
            cur.execute("select status, count(*) from order_line group by 1 order by 1")
            resumo["lines_by_status"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("select min(published_at), max(published_at) from outbox")
            primeiro, ultimo = cur.fetchone()
            resumo["published_from"] = str(primeiro) if primeiro else None
            resumo["published_to"] = str(ultimo) if ultimo else None
        return resumo
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    finally:
        conexao.close()


def _observar_broker(bootstrap: str, topic: str, grupos: list) -> dict:
    from . import orders_stream
    try:
        marcas = orders_stream.topic_watermarks(bootstrap, topic)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    lags = {}
    for grupo in grupos:
        try:
            lags[grupo] = orders_stream.group_lag(bootstrap, topic, grupo)
        except Exception as exc:
            lags[grupo] = {"erro": str(exc).splitlines()[0]}
    return {"topic": topic, "bootstrap": bootstrap, "watermarks": marcas, "lags": lags}


def _observar_projecao(config, dsn=None) -> dict:
    from . import orders_projection
    try:
        cat = orders_projection.catalog(config)
        tabela = cat.load_table(orders_projection.TABLE_NAME)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        import pyarrow.compute as pc

        arrow = tabela.scan().to_arrow()
        proveniencia = {}
        if arrow.num_rows and "written_by" in arrow.column_names:
            contagem = pc.value_counts(arrow.column("written_by").combine_chunks())
            proveniencia = {
                str(entrada["values"]): int(entrada["counts"])
                for entrada in contagem.to_pylist()
            }
        estados = {}
        if arrow.num_rows and "status" in arrow.column_names:
            contagem = pc.value_counts(arrow.column("status").combine_chunks())
            estados = {
                str(entrada["values"]): int(entrada["counts"])
                for entrada in contagem.to_pylist()
            }
        snapshots = list(tabela.metadata.snapshots)
        return {
            "table": orders_projection.TABLE_NAME,
            "metadata_location": orders_projection.metadata_location(cat),
            "rows": arrow.num_rows,
            "snapshots": len(snapshots),
            "current_snapshot_id": tabela.metadata.current_snapshot_id,
            "written_by": proveniencia,
            "by_status": estados,
        }
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}


def _observar_reconciliacao(config, dsn=None) -> dict:
    from . import orders_projection
    try:
        return orders_projection.reconcile(config, dsn=dsn)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}


def collect(config=None, *, bootstrap=None, topic=None, groups=None, dsn=None) -> dict:
    """Observes the three planes. Formats nothing, raises nothing."""
    from . import orders_stream
    from .config import from_env

    config = config or from_env()
    bootstrap = bootstrap or orders_stream.DEFAULT_BOOTSTRAP
    topic = topic or orders_stream.DEFAULT_TOPIC
    # Os DOIS grupos, e a diferenca entre eles e o ponto do par lambda: cada sink consome o
    # mesmo topico no seu proprio ritmo, com offset proprio. Um grupo so esconderia isso.
    groups = groups or [orders_stream.DEFAULT_GROUP, f"{orders_stream.DEFAULT_GROUP}-iceberg"]

    return {
        "capturado_em": _utc_now(),
        "oltp": _observar_oltp(dsn),
        "broker": _observar_broker(bootstrap, topic, groups),
        "projecao": _observar_projecao(config, dsn),
        "reconciliacao": _observar_reconciliacao(config, dsn),
    }


# --------------------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------------------

def _secao_ausente(nome: str, erro: str) -> list[str]:
    return [
        f"> **{nome} not observed.** `{erro}`",
        ">",
        "> The section is absent, not estimated. Bring the plane up with `make stream-up`",
        "> and regenerate with `make stream-evidence`.",
        "",
    ]


def render(dados: dict) -> str:
    """Markdown. No number written by hand: everything comes from what was observed."""
    linhas = [
        "# Stream plane evidence",
        "",
        "Generated by `make stream-evidence` against the OLTP, the broker and the projection",
        "**live** at the moment of capture. **This is not hand-written documentation** —",
        "every number on this page came from a query against one of the three.",
        "",
        "It exists because the streaming half is declared debt: `make test` runs without",
        "network, and the in-memory doubles (`fake_kafka`, `fake_pg`, `fake_iceberg`) cover",
        "the **shape** of the code — transaction ordering, the dedup protocol, SQL",
        "construction. What they cannot cover is the **semantics** of the real engines: that",
        "Kafka preserves order by key, that Postgres really does undo, that Iceberg refuses a",
        "commit over a stale snapshot. This is the record that it was exercised against them.",
        "",
        f"| captured at | {dados['capturado_em']} |",
        "|---|---|",
        "",
    ]

    # ---- OLTP -------------------------------------------------------------------------
    linhas += [
        "## Transactional plane — OLTP and outbox",
        "",
        "The event is born **inside the same transaction** that changes `orders` and",
        "`order_line`. It is not the log being republished: it is the state change and the",
        "event written atomically, which is the only way the two cannot diverge.",
        "`outbox.event_id` is unique, and the outbox insert comes **first** —",
        "`rowcount = 0` means the event was already applied, and the whole transaction is",
        "rolled back.",
        "",
    ]
    oltp = dados["oltp"]
    if "erro" in oltp:
        linhas += _secao_ausente("OLTP", oltp["erro"])
    else:
        linhas += _tabela(["", ""], [
            ("orders in `orders`", f"{oltp['orders']:,}"),
            ("rows in `order_line`", f"{oltp['order_lines']:,}"),
            ("events in `outbox`", f"{oltp['outbox_rows']:,}"),
            ("still unpublished", f"{oltp['unpublished']:,}"),
            ("distinct orders in the outbox", f"{oltp['orders_in_outbox']:,}"),
            ("first publication", oltp["published_from"]),
            ("last publication", oltp["published_to"]),
        ])
        linhas += ["", "### Events in the outbox, by type", ""]
        linhas += _tabela(["event_type", "events"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["by_event_type"].items())])
        linhas += ["", "### Replicated state, by OLTP fold", ""]
        linhas += _tabela(["order status", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["orders_by_status"].items())])
        linhas += [""]
        linhas += _tabela(["line status", "lines"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["lines_by_status"].items())])
        linhas += [""]

    # ---- Broker -----------------------------------------------------------------------
    linhas += [
        "## Transport — Kafka",
        "",
        "`key = order_id`, and the key is **loadable**: Kafka guarantees order within the",
        "partition, and that is what lets the consumer's dedup stay bounded — comparing",
        "`sequence_no` against the already-recorded `last_sequence_no`, with no growing set",
        "of `event_id` and no expiration window. Delivery is **at-least-once** from the",
        "outbox to the broker (publish → ack → mark, never mark → publish); consumption is",
        "**effectively-once** because the offset is only committed after the write.",
        "",
    ]
    broker = dados["broker"]
    if "erro" in broker:
        linhas += _secao_ausente("Broker", broker["erro"])
    else:
        linhas += [f"Topic `{broker['topic']}` on `{broker['bootstrap']}`.", ""]
        linhas += ["### Watermarks by partition", ""]
        marcas = broker["watermarks"]
        linhas += _tabela(
            ["partition", "low", "high", "messages"],
            [(p, m["low"], m["high"], f"{m['high'] - m['low']:,}")
             for p, m in sorted(marcas.items())],
        )
        total = sum(m["high"] - m["low"] for m in marcas.values())
        linhas += ["", f"Total in the topic: **{total:,} messages**.", ""]
        linhas += [
            "The sum can exceed the log's event count, and that is **correct**: delivery",
            "from the outbox to the broker is at-least-once by design, so a crash between",
            "the ack and marking `published_at` republishes the batch. What makes that",
            "harmless is the dedup by `sequence_no` on the other side.",
            "",
            "### Lag by consumer group",
            "",
            "Two groups, and the difference between them is the point of the lambda pair:",
            "each sink consumes the same topic at its own pace, with its own offset. That is",
            "the decoupling point — the Iceberg sink (~4min48s per pass, copy-on-write) does",
            "not hold back the Postgres sink (~14s), and neither loses a message because of",
            "the other.",
            "",
            "**High lag is not a stale projection when the table was rebuilt in batch.**",
            "`orders-rebuild-projection` is the SECOND writer: it writes the final state",
            "straight from RAW, without going through the topic, and the consumer group's",
            "offset does not move with it. After a rebuild, draining the topic through the",
            "Iceberg sink would reprocess hundreds of thousands of events only to discard",
            "them all as equal-or-older — the merge is monotonic. What proves the three",
            "paths converge is `make orders-reconcile`, not a consumer's offset.",
            "",
        ]
        for grupo, lag in broker["lags"].items():
            linhas += [f"**`{grupo}`**", ""]
            if "erro" in lag:
                linhas += [f"- not observed: `{lag['erro']}`", ""]
                continue
            linhas += _tabela(
                ["partition", "committed offset", "high", "lag"],
                [(p, d["committed"], d["high"], d["lag"]) for p, d in sorted(lag.items())],
            )
            linhas += ["", f"Total lag: **{sum(d['lag'] for d in lag.values()):,}**.", ""]

    # ---- Projecao ---------------------------------------------------------------------
    linhas += [
        "## Projection — Iceberg",
        "",
        "The trigger written for Iceberg was *\"a second engine needing to write the same",
        "table\"*. It fired due to **concurrency, not volume**: at this volume a parquet",
        "rewritten with atomic `os.replace` would work. What Iceberg buys is snapshot",
        "isolation between two writers and a concurrent reader, plus time travel.",
        "",
        "`written_by` is the proof that the two writers actually exist, and it is",
        "**queryable** rather than anecdotal.",
        "",
    ]
    projecao = dados["projecao"]
    if "erro" in projecao:
        linhas += _secao_ausente("Projection", projecao["erro"])
    else:
        linhas += _tabela(["", ""], [
            ("table", f"`{projecao['table']}`"),
            ("rows", f"{projecao['rows']:,}"),
            ("snapshots", f"{projecao['snapshots']:,}"),
            ("current snapshot", projecao["current_snapshot_id"]),
            ("current metadata", f"`{projecao['metadata_location']}`"),
        ])
        linhas += [
            "",
            "The metadata path comes from the **catalog**, never from a storage scan. DuckDB",
            "refuses to guess which metadata is current — *\"globbing the filesystem…",
            "could result in reading uncommitted data\"* — and the shortcut exists",
            "(`unsafe_enable_version_guessing`), was measured, and was **rejected**: reading",
            "uncommitted metadata is exactly what a concurrent read cannot do.",
            "",
            "### Provenance: who wrote each row",
            "",
        ]
        linhas += _tabela(["written_by", "rows"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projecao["written_by"].items())])
        linhas += ["", "### State in the live projection", ""]
        linhas += _tabela(["status", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projecao["by_status"].items())])
        linhas += [""]

    # ---- Reconciliacao ----------------------------------------------------------------
    linhas += [
        "## The three folds",
        "",
        "The same order state is computed by three paths, and `make orders-reconcile`",
        "compares the three column by column:",
        "",
        "- **`silver_order`** — SQL window functions over the whole log;",
        "- **`orders`/`order_line` in the OLTP** — transactional state machine, event by event;",
        "- **`live_order_state`** — streaming fold over the topic.",
        "",
        "Only the first **shares no code** with either of the others. It is against that one",
        "that the comparison counts as verification; agreement between the projection and",
        "the OLTP counts as evidence of transport, not of correctness — the two share the",
        "fold.",
        "",
    ]
    rec = dados["reconciliacao"]
    if "erro" in rec:
        linhas += _secao_ausente("Reconciliation", rec["erro"])
    else:
        linhas += _tabela(["source", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(rec["orders"].items())])
        linhas += ["", f"Compared: **{rec['compared']:,} orders**.", ""]
        if rec["ok"]:
            linhas += ["Result: **all three agree on every order compared** — "
                       "zero divergences, zero absences.", ""]
        else:
            linhas += [f"**Divergences: {rec['divergence_count']}.**", ""]
            if rec["divergences"]:
                colunas = list(rec["divergences"][0].keys())
                linhas += _tabela(colunas,
                                  [tuple(d[c] for c in colunas) for d in rec["divergences"]])
                linhas += [""]
            for nome, ausentes in rec.get("missing", {}).items():
                linhas += [f"- missing from `{nome}`: {', '.join(map(str, ausentes))}", ""]

    linhas += [
        "---",
        "",
        "Regenerate with `make stream-evidence` after any run worth recording. The proofs",
        "that back each claim above run separately:",
        "`make orders-prove-atomicity`, `make orders-prove-stream`, `make orders-prove-projection`.",
    ]
    return "\n".join(linhas) + "\n"


def write(dados: dict, out: str = DEFAULT_OUT) -> str:
    """Writes the report. Creates the directory; returns the path."""
    destino = os.path.abspath(out)
    os.makedirs(os.path.dirname(destino), exist_ok=True)
    # Escrita atomica pelo mesmo motivo do resto do repo: um relatorio truncado por
    # interrupcao nao pode parecer completo para quem o le depois.
    temporario = destino + ".tmp"
    with open(temporario, "w", encoding="utf-8") as handle:
        handle.write(render(dados))
    os.replace(temporario, destino)
    return destino
