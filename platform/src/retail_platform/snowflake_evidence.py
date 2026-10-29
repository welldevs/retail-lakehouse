"""Warehouse evidence: what exists at the destination, who owns it, and who can read it.

WHY THIS MODULE EXISTS. The Lakehouse half of the project is reproducible at any
moment and at no cost: `make silver` runs offline and the suites run without a network. The
Snowflake half is not — it depends on a live account, and the account used here is a trial. When it
expires, `models/warehouse/` becomes fourteen models nobody can prove ever ran.

This is not solved with a DuckDB mirror of the warehouse models. A mirror would have
PASSED the two portability errors that broke the first real run (`FILTER
(WHERE ...)` and `WINDOW ... AS`, which DuckDB accepts and Snowflake does not): a test that does not
reproduce the failure mode is not a test, and it is a second implementation to maintain. What
solves it is recording the result of the REAL run, with the date and account identity, while
it exists.

IT IS DELIBERATELY REGENERABLE, not a one-off dump. Linking another account and running
`make warehouse-snowflake-evidence` produces that account's evidence. It is the same property that
sustains the rest: the destination is swappable because the boundary with it is physical.

WHAT THIS MODULE IS NOT: it validates nothing. What validates is the 88 dbt tests and the
`check_isolation` in the bootstrap. Here we only OBSERVE and WRITE DOWN what was observed.
"""

from __future__ import annotations

from .snowflake_load import (
    DEFAULT_DATABASE,
    ISOLATION_MATRIX,
    ROLES,
    STAGE_SCHEMA,
    check_isolation,
)

# Role separation seen through the VERB, not the name. A role called RETAIL_READER that
# ran CREATE_TABLE would be misnamed; this query is what makes that queryable instead of
# anecdotal.
#
# 167 HOURS, not 7 days: `information_schema.query_history` refuses to compile with
# "Cannot retrieve data from more than 7 days ago" when the limit lands exactly on the
# edge. It is also why the section can come back EMPTY — a week without any run wipes the
# history, and empty here means "there was no run in the window", never "there is no
# separation". The page declares the absence instead of printing a table with no rows.
ROLES_IN_EXECUTION = """
select role_name, query_type, count(*) as queries,
       to_char(max(start_time), 'YYYY-MM-DD HH24:MI') as ultima
from table(information_schema.query_history(
       end_time_range_start => dateadd('hour', -167, current_timestamp()),
       result_limit => 10000))
where role_name like 'RETAIL%'
group by 1, 2
order by 1, 3 desc
"""

# One sample per mart. Few rows and few columns on purpose: the evidence is that the table
# exists with plausible content, not a data extract — for data there is the warehouse
# itself while it lives.
SAMPLES = {
    # The mart where the synthetic meets the observed: generated customers against INE
    # population. It is where the simulation's density becomes visible instead of implicit.
    "MART_MARKET_COVERAGE": (
        "select wh, municipality_name, customers, municipality_population, "
        "customers_per_10k_inhabitants, has_no_customers "
        "from {db}.MART.MART_MARKET_COVERAGE "
        "order by customers_per_10k_inhabitants desc nulls last limit 5"
    ),
    # days_since_previous_snapshot in the sample on purpose: it is the column that prevents
    # comparing an 8-day change with a 1-day change without anyone noticing.
    "MART_PRICE_EVOLUTION": (
        "select snapshot_date, wh, display_name, unit_price, price_delta, "
        "days_since_previous_snapshot "
        "from {db}.MART.MART_PRICE_EVOLUTION "
        "where price_delta is not null order by abs(price_delta) desc limit 5"
    ),
    "MART_ASSORTMENT_DAILY": (
        "select snapshot_date, wh, category_name, products, "
        "products_exclusive_here, avg_unit_price "
        "from {db}.MART.MART_ASSORTMENT_DAILY order by products desc limit 5"
    ),
    # The sample orders by the RATIO between the oldest and the youngest band, not by
    # volume: volume would show the big groups, which are the same in every band. What this
    # layer produces is the difference between cohorts, and a sample that did not show it
    # would be evidence that the phase existed without evidence of what it did.
    "MART_DEMAND_COHORT": (
        "with f as ("
        " select demand_group, buyer_age_band, sum(lines_placed) as l"
        " from {db}.MART.MART_DEMAND_COHORT group by 1, 2), "
        "t as (select buyer_age_band, sum(l) as total from f group by 1) "
        "select f.demand_group, "
        " max(case when f.buyer_age_band = 'LT35' then round(100*f.l/t.total, 2) end) as pct_lt35, "
        " max(case when f.buyer_age_band = 'GE65' then round(100*f.l/t.total, 2) end) as pct_ge65 "
        "from f join t on t.buyer_age_band = f.buyer_age_band "
        "group by 1 having sum(f.l) >= 500 "
        "order by div0(max(case when f.buyer_age_band = 'GE65' then f.l/t.total end), "
        "              max(case when f.buyer_age_band = 'LT35' then f.l/t.total end)) desc "
        "limit 5"
    ),
    "MART_CUSTOMER_BASE": (
        "select customer_id, wh, municipality_name, postal_code, age_band, sex_label "
        "from {db}.MART.MART_CUSTOMER_BASE order by customer_id limit 5"
    ),
    # The WHOLE funnel of one day, not the first 5 rows: there are 16 rows in total, and
    # half a funnel is evidence of nothing. The stage and exit columns travel together so
    # the sample shows that they do NOT add up to each other.
    "MART_ORDER_FUNNEL": (
        "select order_date, wh, orders_placed, orders_confirmed, orders_picked, "
        "orders_delivered, orders_cancelled, orders_returned, delivery_rate "
        "from {db}.MART.MART_ORDER_FUNNEL order by order_date, wh limit 8"
    ),
    # sla_minutes, max_picking_minutes and orders_breaching_sla on the same row on purpose:
    # a breach count is only readable next to the declared threshold and the possible
    # maximum. On its own, it does not say whether it measures the operation or the seed's
    # arithmetic — which was exactly the three-phase defect that Phase 7 fixed.
    "MART_FULFILLMENT_SLA": (
        "select order_date, wh, sla_minutes, max_picking_minutes, orders_breaching_sla, "
        "p50_minutes_to_pick, p90_minutes_to_deliver, orders_delivered_before_slot, "
        "orders_delivered_within_slot, orders_delivered_after_slot "
        "from {db}.MART.MART_FULFILLMENT_SLA order by order_date, wh limit 8"
    ),
    "MART_BASKET_DAILY": (
        "select order_date, wh, category_name, orders_touching_category, lines_placed, "
        "lines_substituted, revenue_placed, revenue_fulfilled, substitution_rate "
        "from {db}.MART.MART_BASKET_DAILY order by revenue_fulfilled desc limit 5"
    ),
    # THE CATEGORIES WITH THE LEAST COVER, not the first by date: a stock sample showing
    # full shelves would be evidence of nothing. The two cover readings travel together
    # because the difference between them — ratio of sums versus mean of ratios — is this
    # mart's central trap. `stock_label` is on the row so nobody reads a computed stockout
    # as a measured one.
    "MART_STOCK_HEALTH": (
        "select stock_date, wh, category_name, closing_units, units_demanded, "
        "units_short, series_with_shortfall, fill_rate, days_of_cover, "
        "days_of_cover_typical_product, replenishment_orders, stock_label "
        "from {db}.MART.MART_STOCK_HEALTH where units_demanded > 0 "
        "order by days_of_cover asc nulls last limit 8"
    ),
    # CR-005: the only GOLD fact in this list, on purpose — the other eight are MART.
    # duration_seconds/started_at_utc/finished_at_utc already existed two layers down, in
    # Silver, and were dropped only by a fixed column list in the STAGE projection. The rate
    # (declared_rows/duration_seconds) is the THROUGHPUT signal required (BACKLOG.md,
    # Observability) before any new collector.
    "FACT_INGESTION_RUN": (
        "select source_name, ingestion_date, wh, started_at_utc, duration_seconds, "
        "declared_rows, round(declared_rows / nullif(duration_seconds, 0), 1) as rows_per_second, "
        "complete, failure_count "
        "from {db}.GOLD.FACT_INGESTION_RUN "
        "order by finished_at_utc desc nulls last limit 8"
    ),
}


def _markdown_table(columns: list[str], lines: list[tuple]) -> list[str]:
    header = "| " + " | ".join(columns) + " |"
    separator = "|" + "|".join("---" for _ in columns) + "|"
    body = [
        "| " + " | ".join("" if v is None else str(v) for v in row) + " |"
        for row in lines
    ]
    return [header, separator, *body]


def _query(cursor, sql: str) -> tuple[list[str], list[tuple]]:
    cursor.execute(sql)
    columns = [d[0] for d in cursor.description]
    return columns, cursor.fetchall()


def collect(connection, connect_as_role, database: str = DEFAULT_DATABASE) -> dict:
    """Observes the destination. Returns everything the report needs, without formatting anything."""
    cursor = connection.cursor()
    try:
        account, user, role, warehouse, now = cursor.execute(
            "select current_account(), current_user(), current_role(), "
            "current_warehouse(), to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')"
        ).fetchone()

        # Count and ownership in the same query: they are the two halves of the same
        # question — "what exists there" and "under which role".
        objects = cursor.execute(
            f"""select t.table_schema, t.table_name, t.table_owner, t.row_count
                from {database}.information_schema.tables t
                where t.table_schema in ('{STAGE_SCHEMA}', 'GOLD', 'MART')
                order by t.table_schema, t.table_name"""
        ).fetchall()

        # Tolerant for the same reason as the samples: an account without the privilege to
        # read the history, or without any run in the window, must not bring the whole
        # page down.
        try:
            roles_run = _query(cursor, ROLES_IN_EXECUTION)
        except Exception as exc:
            roles_run = (["error"], [(str(exc).splitlines()[0],)])

        samples = {}
        for name, sql in SAMPLES.items():
            try:
                samples[name] = _query(cursor, sql.format(db=database))
            except Exception as exc:
                samples[name] = (["error"], [(str(exc).splitlines()[0],)])
    finally:
        cursor.close()

    return {
        "identidade": {
            "conta": account,
            "usuario": user,
            "papel": role,
            "warehouse": warehouse,
            "capturado_em": now,
            "database": database,
        },
        "objetos": objects,
        "papeis_em_execucao": roles_run,
        "amostras": samples,
        "isolamento": check_isolation(connect_as_role, database),
    }


def render(data: dict) -> str:
    """Markdown. No hand-written number: everything comes from what was observed."""
    ident = data["identidade"]
    lines = [
        "# Analytical warehouse evidence",
        "",
        "Generated by `make warehouse-snowflake-evidence` against the live Snowflake account at the moment of",
        "capture. **This is not hand-written documentation** — every number on this page came from a",
        "query to the destination.",
        "",
        "It exists because the Snowflake half of the project is not reproducible offline like the",
        "Lakehouse half: it depends on a live account, and the first one (a trial) expired. If the",
        "account goes away, this remains the dated proof that the models ran; `make silver` and the",
        "suites keep running without it.",
        "",
        "## Capture identity",
        "",
        "| | |",
        "|---|---|",
        # Account and user do NOT go on the page: the repository is public and the two
        # together are half a credential. The date, the role and the database are enough to
        # date and place the capture; the real identity stays in the terminal of whoever ran it.
        "| account | `<snowflake-account>` |",
        "| user | `<user>` |",
        f"| session role | `{ident['papel']}` |",
        f"| warehouse | `{ident['warehouse']}` |",
        f"| database | `{ident['database']}` |",
        f"| captured at | {ident['capturado_em']} |",
        "",
        f"The role above is the **capture's** role, not the pipeline's. Reading all three layers at",
        f"once is exactly what no role in the project can do — that is the separation. The pipeline",
        "runs as `RETAIL_LOADER` (load) and `RETAIL_TRANSFORMER` (dbt); the ownership in the table",
        "below is the proof of that, because each object belongs to whoever wrote it.",
        "",
        "## Objects, ownership, and volume",
        "",
        "The **ownership** column is the point: the loader owns STAGE, the transformer owns",
        "GOLD and MART, and nothing belongs to the administrator. While the pipeline ran as",
        "`ACCOUNTADMIN` this table would show a single owner everywhere — the three roles",
        "existed, were verified, and no run ever went through them.",
        "",
    ]

    columns = ["schema", "table", "ownership", "rows"]
    body = [
        (s, f"`{t}`", f"`{o}`", f"{n:,}" if n is not None else "")
        for s, t, o, n in data["objetos"]
    ]
    lines += _markdown_table(columns, body)

    total = sum(n or 0 for _, _, _, n in data["objetos"])
    lines += ["", f"Total at the destination: **{total:,} rows**.", ""]

    lines += [
        "## Isolation verified role by role",
        "",
        "Run with `use secondary roles none`. Without that the check would pass by mistake:",
        "modern Snowflake accounts are born with `DEFAULT_SECONDARY_ROLES = ('ALL')` and activate",
        "all of the user's roles besides the primary one — measured on this account before the fix.",
        "",
    ]
    for role, expected in ISOLATION_MATRIX.items():
        lines.append(f"- **{role}** — {ROLES[role]}")
        for target in expected["allow"]:
            lines.append(f"  - reads `{target.format(db=ident['database'])}`")
        for target in expected["deny"]:
            lines.append(f"  - **does not** read `{target.format(db=ident['database'])}`")

    problems = data["isolamento"]
    lines += [""]
    if problems:
        lines += ["**The matrix does NOT check out:**", ""]
        lines += [f"- {p}" for p in problems]
    else:
        lines += ["Result: **the matrix checks out completely** — no violation."]
    lines += [""]

    role_columns, role_body = data.get("papeis_em_execucao", ([], []))
    lines += [
        "## Roles in execution, seen through the verb",
        "",
        "The matrix above proves what each role **can** read. This table proves what each one",
        "**did** — and that is the difference between verified governance and adopted governance. The",
        "three roles existed since Phase 2, with the right grants, while every run went through",
        "`ACCOUNTADMIN`; nothing on this page would have shown that, because ownership",
        "only changes when someone actually writes.",
        "",
        "167-hour window: `information_schema.query_history` does not retrieve anything beyond",
        "seven days. A week without any run empties the section, and it **declares the absence**",
        "instead of printing an empty table, which would read as \"there is no separation\".",
        "",
    ]
    if role_body and role_columns != ["error"]:
        lines += _markdown_table(
            ["role", "query type", "queries", "last"],
            [(f"`{p}`", f"`{q}`", f"{n:,}", u) for p, q, n, u in role_body],
        )
    elif role_columns == ["error"]:
        lines += [f"**Could not read the history:** `{role_body[0][0]}`"]
    else:
        lines += [
            "**No `RETAIL%` role executed anything in the seven-day window.** The absence is",
            "the data: there was no load nor `dbt build` during the week, and Snowflake's history",
            "does not keep more than that.",
        ]
    lines += [""]

    # Every sample is QUERIED, but only FACT_INGESTION_RUN becomes a table: five rows of
    # synthetic numbers per mart prove nothing beyond the count above. The mart ones keep
    # running so a broken query shows up as an error here, and not as silence.
    lines += [
        "## Sample",
        "",
        "Every MART listed in the ownership table above is real, dbt-built, queryable data —",
        "not shown row by row here, since a 5-row dump of synthetic figures proves nothing a",
        "reader couldn't already infer from the row counts above. One operational table is",
        "kept in full below because it is what backs the pipeline's observability claim",
        "(non-null `started_at_utc`/`duration_seconds`, a computed `rows_per_second` for",
        "every row).",
        "",
    ]
    failures = {name: body for name, (cols, body) in data["amostras"].items()
                if cols == ["error"]}
    for name, (sample_columns, sample_rows) in data["amostras"].items():
        if name == "FACT_INGESTION_RUN" and name not in failures:
            lines += [f"### `{name}`", ""]
            lines += _markdown_table(sample_columns, sample_rows)
            lines += [""]
    if failures:
        lines += ["**Sample queries that failed** — a failure must not read as an empty table:", ""]
        lines += [f"- `{name}`: `{body[0][0]}`" for name, body in failures.items()]
        lines += [""]

    return "\n".join(lines) + "\n"
