"""Evidencia do warehouse: o que existe no destino, quem e dono, e quem consegue ler.

POR QUE ESTE MODULO EXISTE. A metade Lakehouse do projeto e reproduzivel a qualquer
momento e sem custo: `make silver` roda offline e as suites rodam sem rede. A metade
Snowflake nao — ela depende de uma conta viva, e a conta usada aqui e um trial. Quando ela
expirar, `models/warehouse/` vira quatorze modelos que ninguem consegue provar que rodaram.

Isto nao se resolve com um espelho DuckDB dos modelos do warehouse. Um espelho teria
PASSADO nos dois erros de portabilidade que quebraram a primeira execucao real (`FILTER
(WHERE ...)` e `WINDOW ... AS`, que o DuckDB aceita e o Snowflake nao): um teste que nao
reproduz o modo de falha nao e teste, e uma segunda implementacao para manter. O que
resolve e registrar o resultado da execucao REAL, com data e identidade da conta, enquanto
ela existe.

E REGENERAVEL DE PROPOSITO, nao um despejo unico. Vincular outra conta e rodar
`make warehouse-evidence` produz a evidencia daquela conta. E a mesma propriedade que
sustenta o resto: o destino e trocavel porque a fronteira com ele e fisica.

O QUE ESTE MODULO NAO E: nao valida nada. Quem valida sao os 88 testes do dbt e o
`check_isolation` do bootstrap. Aqui so se OBSERVA e se ESCREVE o que foi observado.
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
    # o zero de violacoes so e legivel ao lado do limiar declarado (90) e do maximo
    # observado (80). Separados, o zero pareceria uma operacao impecavel.
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
    """Observa o destino. Devolve tudo o que o relatorio precisa, sem formatar nada."""
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
        "amostras": amostras,
        "isolamento": check_isolation(connect_as_role, database),
    }


def render(dados: dict) -> str:
    """Markdown. Sem numero escrito a mao: tudo vem do que foi observado."""
    ident = dados["identidade"]
    linhas = [
        "# Evidência do warehouse analítico",
        "",
        "Gerado por `make warehouse-evidence` contra a conta Snowflake viva no momento da",
        "captura. **Não é documentação escrita à mão** — todo número desta página saiu de uma",
        "consulta ao destino.",
        "",
        "Existe porque a conta é um trial e a metade Snowflake do projeto não é reproduzível",
        "offline como a metade Lakehouse. Quando a conta expirar, isto continua sendo a prova",
        "datada de que os modelos rodaram; `make silver` e as suítes seguem rodando sem ela.",
        "",
        "## Identidade da captura",
        "",
        "| | |",
        "|---|---|",
        f"| conta | `{ident['conta']}` |",
        f"| usuário | `{ident['usuario']}` |",
        f"| papel da sessão | `{ident['papel']}` |",
        f"| warehouse | `{ident['warehouse']}` |",
        f"| database | `{ident['database']}` |",
        f"| capturado em | {ident['capturado_em']} |",
        "",
        f"O papel acima é o da **captura**, não o do pipeline. Ler as três camadas de uma vez é",
        f"justamente o que nenhum papel do projeto pode fazer — é essa a separação. O pipeline",
        "roda como `RETAIL_LOADER` (carga) e `RETAIL_TRANSFORMER` (dbt); a posse na tabela",
        "abaixo é a prova disso, porque cada objeto pertence a quem o escreveu.",
        "",
        "## Objetos, posse e volume",
        "",
        "A coluna **posse** é o ponto: o carregador é dono do STAGE, o transformador é dono de",
        "GOLD e MART, e nada é do administrador. Enquanto o pipeline rodava como",
        "`ACCOUNTADMIN` esta tabela mostraria um único dono em toda parte — os três papéis",
        "existiam, estavam verificados, e nenhuma execução passava por eles.",
        "",
    ]

    colunas = ["schema", "tabela", "posse", "linhas"]
    corpo = [
        (s, f"`{t}`", f"`{o}`", f"{n:,}" if n is not None else "")
        for s, t, o, n in dados["objetos"]
    ]
    linhas += _tabela_markdown(colunas, corpo)

    total = sum(n or 0 for _, _, _, n in dados["objetos"])
    linhas += ["", f"Total no destino: **{total:,} linhas**.", ""]

    linhas += [
        "## Isolamento verificado papel a papel",
        "",
        "Executado com `use secondary roles none`. Sem isso a verificação passaria por engano:",
        "contas Snowflake modernas nascem com `DEFAULT_SECONDARY_ROLES = ('ALL')` e ativam",
        "todos os papéis do usuário além do primário — medido nesta conta antes da correção.",
        "",
    ]
    for papel, esperado in ISOLATION_MATRIX.items():
        linhas.append(f"- **{papel}** — {ROLES[papel]}")
        for alvo in esperado["allow"]:
            linhas.append(f"  - lê `{alvo.format(db=ident['database'])}`")
        for alvo in esperado["deny"]:
            linhas.append(f"  - **não** lê `{alvo.format(db=ident['database'])}`")

    problemas = dados["isolamento"]
    linhas += [""]
    if problemas:
        linhas += ["**A matriz NÃO confere:**", ""]
        linhas += [f"- {p}" for p in problemas]
    else:
        linhas += ["Resultado: **a matriz confere inteira** — nenhuma violação."]
    linhas += [""]

    linhas += [
        "## Amostras",
        "",
        "Poucas linhas por mart, só para que o conteúdo seja inspecionável depois que o",
        "destino não existir mais. Não substituem o warehouse enquanto ele viver.",
        "",
    ]
    for nome, (colunas_amostra, linhas_amostra) in dados["amostras"].items():
        linhas += [f"### `{nome}`", ""]
        linhas += _tabela_markdown(colunas_amostra, linhas_amostra)
        linhas += [""]

    return "\n".join(linhas) + "\n"
