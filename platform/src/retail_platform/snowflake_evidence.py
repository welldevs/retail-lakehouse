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

    colunas_papel, corpo_papel = dados.get("papeis_em_execucao", ([], []))
    linhas += [
        "## Papéis em execução, vistos pelo verbo",
        "",
        "A matriz acima prova o que cada papel **pode** ler. Esta tabela prova o que cada um",
        "**fez** — e é a diferença entre governança verificada e governança adotada. Os três",
        "papéis existiam desde a Fase 2, com os grants certos, enquanto todas as execuções",
        "passavam por `ACCOUNTADMIN`; nada nesta página teria mostrado isso, porque a posse",
        "só muda quando alguém escreve de fato.",
        "",
        "Janela de 167 horas: `information_schema.query_history` não recupera nada além de",
        "sete dias. Uma semana sem execução esvazia a seção, e ela **declara a ausência** em",
        "vez de imprimir uma tabela vazia, que se leria como \"não há separação\".",
        "",
    ]
    if corpo_papel and colunas_papel != ["erro"]:
        linhas += _tabela_markdown(
            ["papel", "tipo de query", "queries", "última"],
            [(f"`{p}`", f"`{q}`", f"{n:,}", u) for p, q, n, u in corpo_papel],
        )
    elif colunas_papel == ["erro"]:
        linhas += [f"**Não foi possível ler o histórico:** `{corpo_papel[0][0]}`"]
    else:
        linhas += [
            "**Nenhuma execução de papel `RETAIL%` na janela de sete dias.** A ausência é o",
            "dado: não houve carga nem `dbt build` na semana, e o histórico do Snowflake não",
            "guarda mais que isso.",
        ]
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
