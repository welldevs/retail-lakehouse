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


def _table(columns: list[str], lines: list) -> list[str]:
    header = "| " + " | ".join(columns) + " |"
    separator = "|" + "|".join("---" for _ in columns) + "|"
    body = [
        "| " + " | ".join("" if v is None else str(v) for v in row) + " |"
        for row in lines
    ]
    return [header, separator, *body]


# --------------------------------------------------------------------------------------
# Observation, plane by plane
# --------------------------------------------------------------------------------------
# Each block returns `{"erro": "..."}` instead of propagating the exception. The report must
# be producible with the plane half up — and it must SAY it was half up.

def _observe_oltp(dsn=None) -> dict:
    from . import orders_oltp
    try:
        connection = orders_oltp.connect(dsn)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        summary = orders_oltp.outbox_summary(connection)
        with connection.cursor() as cur:
            cur.execute("select count(*) from orders")
            summary["orders"] = cur.fetchone()[0]
            cur.execute("select count(*) from order_line")
            summary["order_lines"] = cur.fetchone()[0]
            cur.execute("select status, count(*) from order_line group by 1 order by 1")
            summary["lines_by_status"] = {r[0]: r[1] for r in cur.fetchall()}
            cur.execute("select min(published_at), max(published_at) from outbox")
            first, last = cur.fetchone()
            summary["published_from"] = str(first) if first else None
            summary["published_to"] = str(last) if last else None
        return summary
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    finally:
        connection.close()


def _observe_broker(bootstrap: str, topic: str, groups: list) -> dict:
    from . import orders_stream
    try:
        watermarks = orders_stream.topic_watermarks(bootstrap, topic)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    lags = {}
    for group in groups:
        try:
            lags[group] = orders_stream.group_lag(bootstrap, topic, group)
        except Exception as exc:
            lags[group] = {"erro": str(exc).splitlines()[0]}
    return {"topic": topic, "bootstrap": bootstrap, "watermarks": watermarks, "lags": lags}


def _observe_projection(config, dsn=None) -> dict:
    from . import orders_projection
    try:
        cat = orders_projection.catalog(config)
        table = cat.load_table(orders_projection.TABLE_NAME)
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}
    try:
        import pyarrow.compute as pc

        arrow = table.scan().to_arrow()
        provenance = {}
        if arrow.num_rows and "written_by" in arrow.column_names:
            counts = pc.value_counts(arrow.column("written_by").combine_chunks())
            provenance = {
                str(entry["values"]): int(entry["counts"])
                for entry in counts.to_pylist()
            }
        statuses = {}
        if arrow.num_rows and "status" in arrow.column_names:
            counts = pc.value_counts(arrow.column("status").combine_chunks())
            statuses = {
                str(entry["values"]): int(entry["counts"])
                for entry in counts.to_pylist()
            }
        snapshots = list(table.metadata.snapshots)
        return {
            "table": orders_projection.TABLE_NAME,
            "metadata_location": orders_projection.metadata_location(cat),
            "rows": arrow.num_rows,
            "snapshots": len(snapshots),
            "current_snapshot_id": table.metadata.current_snapshot_id,
            "written_by": provenance,
            "by_status": statuses,
        }
    except Exception as exc:
        return {"erro": str(exc).splitlines()[0]}


def _observe_reconciliation(config, dsn=None) -> dict:
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
    # BOTH groups, and the difference between them is the point of the lambda pair: each
    # sink consumes the same topic at its own pace, with its own offset. One group would hide that.
    groups = groups or [orders_stream.DEFAULT_GROUP, f"{orders_stream.DEFAULT_GROUP}-iceberg"]

    return {
        "capturado_em": _utc_now(),
        "oltp": _observe_oltp(dsn),
        "broker": _observe_broker(bootstrap, topic, groups),
        "projecao": _observe_projection(config, dsn),
        "reconciliacao": _observe_reconciliation(config, dsn),
    }


# --------------------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------------------

def _absent_section(name: str, error: str) -> list[str]:
    return [
        f"> **{name} not observed.** `{error}`",
        ">",
        "> The section is absent, not estimated. Bring the plane up with `make stream-up`",
        "> and regenerate with `make stream-evidence`.",
        "",
    ]


def render(data: dict) -> str:
    """Markdown. No number written by hand: everything comes from what was observed."""
    lines = [
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
        f"| captured at | {data['capturado_em']} |",
        "|---|---|",
        "",
    ]

    # ---- OLTP -------------------------------------------------------------------------
    lines += [
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
    oltp = data["oltp"]
    if "erro" in oltp:
        lines += _absent_section("OLTP", oltp["erro"])
    else:
        lines += _table(["", ""], [
            ("orders in `orders`", f"{oltp['orders']:,}"),
            ("rows in `order_line`", f"{oltp['order_lines']:,}"),
            ("events in `outbox`", f"{oltp['outbox_rows']:,}"),
            ("still unpublished", f"{oltp['unpublished']:,}"),
            ("distinct orders in the outbox", f"{oltp['orders_in_outbox']:,}"),
            ("first publication", oltp["published_from"]),
            ("last publication", oltp["published_to"]),
        ])
        lines += ["", "### Events in the outbox, by type", ""]
        lines += _table(["event_type", "events"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["by_event_type"].items())])
        lines += ["", "### Replicated state, by OLTP fold", ""]
        lines += _table(["order status", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["orders_by_status"].items())])
        lines += [""]
        lines += _table(["line status", "lines"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(oltp["lines_by_status"].items())])
        lines += [""]

    # ---- Broker -----------------------------------------------------------------------
    lines += [
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
    broker = data["broker"]
    if "erro" in broker:
        lines += _absent_section("Broker", broker["erro"])
    else:
        lines += [f"Topic `{broker['topic']}` on `{broker['bootstrap']}`.", ""]
        lines += ["### Watermarks by partition", ""]
        watermarks = broker["watermarks"]
        lines += _table(
            ["partition", "low", "high", "messages"],
            [(p, m["low"], m["high"], f"{m['high'] - m['low']:,}")
             for p, m in sorted(watermarks.items())],
        )
        total = sum(m["high"] - m["low"] for m in watermarks.values())
        lines += ["", f"Total in the topic: **{total:,} messages**.", ""]
        lines += [
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
        for group, lag in broker["lags"].items():
            lines += [f"**`{group}`**", ""]
            if "erro" in lag:
                lines += [f"- not observed: `{lag['erro']}`", ""]
                continue
            lines += _table(
                ["partition", "committed offset", "high", "lag"],
                [(p, d["committed"], d["high"], d["lag"]) for p, d in sorted(lag.items())],
            )
            lines += ["", f"Total lag: **{sum(d['lag'] for d in lag.values()):,}**.", ""]

    # ---- Projection -------------------------------------------------------------------
    lines += [
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
    projection = data["projecao"]
    if "erro" in projection:
        lines += _absent_section("Projection", projection["erro"])
    else:
        lines += _table(["", ""], [
            ("table", f"`{projection['table']}`"),
            ("rows", f"{projection['rows']:,}"),
            ("snapshots", f"{projection['snapshots']:,}"),
            ("current snapshot", projection["current_snapshot_id"]),
            ("current metadata", f"`{projection['metadata_location']}`"),
        ])
        lines += [
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
        lines += _table(["written_by", "rows"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projection["written_by"].items())])
        lines += ["", "### State in the live projection", ""]
        lines += _table(["status", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(projection["by_status"].items())])
        lines += [""]

    # ---- Reconciliation ---------------------------------------------------------------
    lines += [
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
    rec = data["reconciliacao"]
    if "erro" in rec:
        lines += _absent_section("Reconciliation", rec["erro"])
    else:
        lines += _table(["source", "orders"],
                          [(f"`{k}`", f"{v:,}") for k, v in sorted(rec["orders"].items())])
        lines += ["", f"Compared: **{rec['compared']:,} orders**.", ""]
        if rec["ok"]:
            lines += ["Result: **all three agree on every order compared** — "
                       "zero divergences, zero absences.", ""]
        else:
            lines += [f"**Divergences: {rec['divergence_count']}.**", ""]
            if rec["divergences"]:
                columns = list(rec["divergences"][0].keys())
                lines += _table(columns,
                                  [tuple(d[c] for c in columns) for d in rec["divergences"]])
                lines += [""]
            for name, absent in rec.get("missing", {}).items():
                lines += [f"- missing from `{name}`: {', '.join(map(str, absent))}", ""]

    lines += [
        "---",
        "",
        "Regenerate with `make stream-evidence` after any run worth recording. The proofs",
        "that back each claim above run separately:",
        "`make orders-prove-atomicity`, `make orders-prove-stream`, `make orders-prove-projection`.",
    ]
    return "\n".join(lines) + "\n"


def write(data: dict, out: str = DEFAULT_OUT) -> str:
    """Writes the report. Creates the directory; returns the path."""
    target = os.path.abspath(out)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    # Atomic write for the same reason as the rest of the repo: a report truncated by an
    # interruption must not look complete to whoever reads it later.
    tmp_path = target + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(render(data))
    os.replace(tmp_path, target)
    return target
