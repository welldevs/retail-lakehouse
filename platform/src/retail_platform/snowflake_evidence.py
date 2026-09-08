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
`make warehouse-evidence` produces that account's evidence. It is the same property that
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

# Uma amostra por mart. Poucas linhas e poucas colunas de proposito: a evidencia e de que a
# tabela existe com conteudo plausivel, nao um extrato de dado — para dado ha o proprio
# warehouse enquanto ele viver.
# A separacao de papeis vista no VERBO, e nao no nome. Um papel chamado RETAIL_READER
# que executasse CREATE_TABLE estaria nomeado errado; esta consulta e o que torna isso
# consultavel em vez de anedotico.
#
# 167 HORAS, e nao 7 dias: `information_schema.query_history` recusa a compilacao com
# "Cannot retrieve data from more than 7 days ago" quando o limite bate exatamente na
# borda. E tambem por que a secao pode vir VAZIA — uma semana sem execucao apaga o
# historico, e vazio aqui significa "nao houve execucao na janela", nunca "nao ha
# separacao". A pagina declara a ausencia em vez de imprimir uma tabela sem linhas.
PAPEIS_EM_EXECUCAO = """
select role_name, query_type, count(*) as queries,
       to_char(max(start_time), 'YYYY-MM-DD HH24:MI') as ultima
from table(information_schema.query_history(
       end_time_range_start => dateadd('hour', -167, current_timestamp()),
       result_limit => 10000))
where role_name like 'RETAIL%'
group by 1, 2
order by 1, 3 desc
"""

AMOSTRAS = {
    # O mart onde o sintetico encontra o observado: clientes gerados contra populacao do
    # INE. E onde a densidade da simulacao fica visivel em vez de implicita.
    "MART_MARKET_COVERAGE": (
        "select wh, municipality_name, customers, municipality_population, "
        "customers_per_10k_inhabitants, has_no_customers "
        "from {db}.MART.MART_MARKET_COVERAGE "
        "order by customers_per_10k_inhabitants desc nulls last limit 5"
    ),
    # days_since_previous_snapshot na amostra de proposito: e a coluna que impede comparar
    # uma variacao de 8 dias com uma de 1 dia sem ninguem notar.
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
    # A amostra ordena pela RAZAO entre a faixa mais velha e a mais nova, e nao pelo volume:
    # o volume mostraria os grupos grandes, que sao os mesmos em toda faixa. O que esta
    # camada produz e a diferenca entre coortes, e uma amostra que nao a mostrasse seria
    # evidencia de que a fase existiu sem evidencia do que ela fez.
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
    # O funil INTEIRO de um dia, e nao as 5 primeiras linhas: sao 16 linhas no total, e
    # metade de um funil nao e evidencia de nada. As colunas de etapa e de saida viajam
    # juntas para que a amostra mostre que elas NAO somam entre si.
    "MART_ORDER_FUNNEL": (
        "select order_date, wh, orders_placed, orders_confirmed, orders_picked, "
        "orders_delivered, orders_cancelled, orders_returned, delivery_rate "
        "from {db}.MART.MART_ORDER_FUNNEL order by order_date, wh limit 8"
    ),
    # sla_minutes, max_picking_minutes e orders_breaching_sla na mesma linha de proposito:
    # uma contagem de violacoes so e legivel ao lado do limiar declarado e do maximo
    # possivel. Separada, ela nao diz se mede a operacao ou a aritmetica do seed — que foi
    # exatamente o defeito de tres fases que a Fase 7 corrigiu.
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
    # AS CATEGORIAS COM MENOS COBERTURA, e nao as primeiras por data: uma amostra de estoque
    # que mostrasse prateleiras cheias nao seria evidencia de nada. As duas leituras de
    # cobertura viajam juntas porque a diferenca entre elas — razao das somas contra media
    # das razoes — e a armadilha central deste mart. `stock_label` esta na linha para que
    # ninguem leia ruptura calculada como ruptura medida.
    "MART_STOCK_HEALTH": (
        "select stock_date, wh, category_name, closing_units, units_demanded, "
        "units_short, series_with_shortfall, fill_rate, days_of_cover, "
        "days_of_cover_typical_product, replenishment_orders, stock_label "
        "from {db}.MART.MART_STOCK_HEALTH where units_demanded > 0 "
        "order by days_of_cover asc nulls last limit 8"
    ),
    # CR-005: o unico fato de GOLD nesta lista, de proposito — os outros oito sao MART.
    # duration_seconds/started_at_utc/finished_at_utc ja existiam duas camadas abaixo, no
    # Silver, e eram descartados so por uma lista fixa de colunas na projecao do STAGE. A
    # taxa (declared_rows/duration_seconds) e o sinal de THROUGHPUT que
    # AI_ENGINEERING_CONSTRAINTS.md §17 pedia provado antes de qualquer coletor novo.
    "FACT_INGESTION_RUN": (
        "select source_name, ingestion_date, wh, started_at_utc, duration_seconds, "
        "declared_rows, round(declared_rows / nullif(duration_seconds, 0), 1) as rows_per_second, "
        "complete, failure_count "
        "from {db}.GOLD.FACT_INGESTION_RUN "
        "order by finished_at_utc desc nulls last limit 8"
    ),
}


def _tabela_markdown(colunas: list[str], linhas: list[tuple]) -> list[str]:
    cabecalho = "| " + " | ".join(colunas) + " |"
    separador = "|" + "|".join("---" for _ in colunas) + "|"
    corpo = [
        "| " + " | ".join("" if v is None else str(v) for v in linha) + " |"
        for linha in linhas
    ]
    return [cabecalho, separador, *corpo]


def _consulta(cursor, sql: str) -> tuple[list[str], list[tuple]]:
    cursor.execute(sql)
    colunas = [d[0] for d in cursor.description]
    return colunas, cursor.fetchall()


def collect(connection, connect_as_role, database: str = DEFAULT_DATABASE) -> dict:
    """Observes the destination. Returns everything the report needs, without formatting anything."""
    cursor = connection.cursor()
    try:
        conta, usuario, papel, warehouse, agora = cursor.execute(
            "select current_account(), current_user(), current_role(), "
            "current_warehouse(), to_char(current_timestamp, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')"
        ).fetchone()

        # Contagem e posse na mesma consulta: sao as duas metades da mesma pergunta —
        # "o que existe la" e "sob que papel".
        objetos = cursor.execute(
            f"""select t.table_schema, t.table_name, t.table_owner, t.row_count
                from {database}.information_schema.tables t
                where t.table_schema in ('{STAGE_SCHEMA}', 'GOLD', 'MART')
                order by t.table_schema, t.table_name"""
        ).fetchall()

        # Tolerante pela mesma razao das amostras: uma conta sem privilegio de ler o
        # historico, ou sem execucao na janela, nao pode derrubar a pagina inteira.
        try:
            papeis = _consulta(cursor, PAPEIS_EM_EXECUCAO)
        except Exception as exc:
            papeis = (["erro"], [(str(exc).splitlines()[0],)])

        amostras = {}
        for nome, sql in AMOSTRAS.items():
            try:
                amostras[nome] = _consulta(cursor, sql.format(db=database))
            except Exception as exc:
                amostras[nome] = (["erro"], [(str(exc).splitlines()[0],)])
    finally:
        cursor.close()

    return {
        "identidade": {
            "conta": conta,
            "usuario": usuario,
            "papel": papel,
            "warehouse": warehouse,
            "capturado_em": agora,
            "database": database,
        },
        "objetos": objetos,
        "papeis_em_execucao": papeis,
        "amostras": amostras,
        "isolamento": check_isolation(connect_as_role, database),
    }


def render(dados: dict) -> str:
    """Markdown. No hand-written number: everything comes from what was observed."""
    ident = dados["identidade"]
    linhas = [
        "# Analytical warehouse evidence",
        "",
        "Generated by `make warehouse-evidence` against the live Snowflake account at the moment of",
        "capture. **This is not hand-written documentation** — every number on this page came from a",
        "query to the destination.",
        "",
        "It exists because the account is a trial and the Snowflake half of the project is not",
        "reproducible offline like the Lakehouse half. When the account expires, this remains the",
        "dated proof that the models ran; `make silver` and the suites keep running without it.",
        "",
        "## Capture identity",
        "",
        "| | |",
        "|---|---|",
        f"| account | `{ident['conta']}` |",
        f"| user | `{ident['usuario']}` |",
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

    colunas = ["schema", "table", "ownership", "rows"]
    corpo = [
        (s, f"`{t}`", f"`{o}`", f"{n:,}" if n is not None else "")
        for s, t, o, n in dados["objetos"]
    ]
    linhas += _tabela_markdown(colunas, corpo)

    total = sum(n or 0 for _, _, _, n in dados["objetos"])
    linhas += ["", f"Total at the destination: **{total:,} rows**.", ""]

    linhas += [
        "## Isolation verified role by role",
        "",
        "Run with `use secondary roles none`. Without that the check would pass by mistake:",
        "modern Snowflake accounts are born with `DEFAULT_SECONDARY_ROLES = ('ALL')` and activate",
        "all of the user's roles besides the primary one — measured on this account before the fix.",
        "",
    ]
    for papel, esperado in ISOLATION_MATRIX.items():
        linhas.append(f"- **{papel}** — {ROLES[papel]}")
        for alvo in esperado["allow"]:
            linhas.append(f"  - reads `{alvo.format(db=ident['database'])}`")
        for alvo in esperado["deny"]:
            linhas.append(f"  - **does not** read `{alvo.format(db=ident['database'])}`")

    problemas = dados["isolamento"]
    linhas += [""]
    if problemas:
        linhas += ["**The matrix does NOT check out:**", ""]
        linhas += [f"- {p}" for p in problemas]
    else:
        linhas += ["Result: **the matrix checks out completely** — no violation."]
    linhas += [""]

    colunas_papel, corpo_papel = dados.get("papeis_em_execucao", ([], []))
    linhas += [
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
    if corpo_papel and colunas_papel != ["erro"]:
        linhas += _tabela_markdown(
            ["role", "query type", "queries", "last"],
            [(f"`{p}`", f"`{q}`", f"{n:,}", u) for p, q, n, u in corpo_papel],
        )
    elif colunas_papel == ["erro"]:
        linhas += [f"**Could not read the history:** `{corpo_papel[0][0]}`"]
    else:
        linhas += [
            "**No `RETAIL%` role executed anything in the seven-day window.** The absence is",
            "the data: there was no load nor `dbt build` during the week, and Snowflake's history",
            "does not keep more than that.",
        ]
    linhas += [""]

    linhas += [
        "## Samples",
        "",
        "A few rows per table below (every MART, plus FACT_INGESTION_RUN from GOLD), only",
        "so the content stays inspectable after the destination no longer exists. They do",
        "not replace the warehouse while it is alive.",
        "",
    ]
    for nome, (colunas_amostra, linhas_amostra) in dados["amostras"].items():
        linhas += [f"### `{nome}`", ""]
        linhas += _tabela_markdown(colunas_amostra, linhas_amostra)
        linhas += [""]

    return "\n".join(linhas) + "\n"
