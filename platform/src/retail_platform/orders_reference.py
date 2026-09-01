"""Exporta, do Silver, as referencias planas que a Source de Orders simulados consome.

POR QUE ESTE MODULO EXISTE, E POR QUE ELE FICA NA PLATAFORMA
------------------------------------------------------------
Mesmo mecanismo de `oltp_reference.py`, um nivel adiante na cadeia. `simulated-orders-source`
precisa de cliente, produto e preco REAIS para que o pedido seja a unica coisa inventada —
mas toda Source deste repo e FROZEN (`dependencies = []`, verificado por AST) e
`duckdb`/`boto3` sao dependencias exclusivas da plataforma por design. A PLATAFORMA consulta
o Silver e escreve quatro JSON planos; o `extract` da Source os le com `json` da stdlib.
Nenhum lado importa o codigo do outro.

A REGRA DE OURO, APLICADA A ORDERS
-----------------------------------
O pedido e inventado; quem compra, o que se compra, quanto custa e onde mora nao. Cliente
vem de `silver_customer`, produto e preco vem de `silver_product_price` do MESMO armazem na
MESMA data. Nada aqui fabrica produto, preco ou cliente.

DE ONDE VEM CADA COISA (e o que foi medido antes de escrever isto)
------------------------------------------------------------------
  * `silver_product_price` tem MAIS LINHAS que produtos distintos por particao (ex.: 4.581
    linhas para 4.311 produtos em mad1/2026-08-24): um produto aparece em mais de uma
    categoria, e isso e semantica da fonte, nao defeito. O dedup por
    `(warehouse, ingestion_date, source_product_id)` e SEGURO porque o preco e identico
    entre as aparicoes — garantia que ja tem teste proprio no repo
    (`assert_price_is_consistent_across_appearances`). A categoria escolhida e a menor
    `(category_id, subgroup_id)`: e uma regra de DESEMPATE deterministica, nao de negocio.
  * `unit_price` viaja como STRING, nunca float. E a obrigacao 4.4 do contrato da Mercadona
    ("converter para float perde precisao decimal em moeda"), e o gerador faz a aritmetica
    com `decimal.Decimal`, que e stdlib e portanto nao quebra a fronteira FROZEN.
  * A ESCOLHA DO PRODUTO E UNIFORME, de proposito, e isso nao e "cesta realista". Nenhuma
    fonte deste repo mede venda, giro ou cesta. Ponderar produto inventaria uma distribuicao
    que ninguem mediu — a mesma proibicao que a Fase 1 aplicou a escolha do tramo. A
    consequencia declarada: o mix por categoria espelha o TAMANHO do sortimento, e isso e
    consequencia de uma premissa, nao afirmacao sobre o mercado.
  * A JANELA E DERIVADA, NUNCA PRESUMIDA. Para cada `(wh, order_date)` o preco vem do maior
    snapshot de catalogo daquele armazem em data <= order_date. Igual: `price_source =
    'observed'`. Anterior: `'carried_forward'`, explicito. Medido no disco em 2026-08-28: os
    4 armazens tem catalogo de 08-24 a 08-27; mad1 tem tambem 08-15/08-16, com buraco de
    08-17 a 08-23. Um varejista vende todo dia; a fonte so foi observada em alguns.
  * NENHUM PEDIDO ANTES DE O CLIENTE EXISTIR. `first_ingestion_date` por cliente e exportado
    para que o gerador possa recusar. A base e append-only (crescer preserva os primeiros N
    byte a byte), entao um cliente presente numa data esta presente em todas as posteriores.
  * As PREMISSAS sao lidas do CSV do seed, e nao do Lakehouse, pelo mesmo motivo ja medido em
    `oltp_reference.py`: seeds do dbt nao tem `location =` e nunca viram parquet sob
    `silver/`, que e tudo que `connect_lakehouse()` enxerga.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone

from . import demand_profile

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")
PROVINCE_MAP_SEED = "warehouse_province_map_seed.csv"
PREMISES_SEED = "order_premises_seed.csv"

CUSTOMERS_FILE = "customers.json"
CATALOG_FILE = "catalog.json"
CALENDAR_FILE = "calendar.json"
PREMISES_FILE = "premises.json"
DEMAND_FILE = "demand_profile.json"

PRICE_OBSERVED = "observed"
PRICE_CARRIED_FORWARD = "carried_forward"

# Rotulo unico permitido na tabela de premissas. Nao existe premissa `observed` nem `proxy`
# aqui: nenhuma fonte deste repo mede cesta, cadencia ou disponibilidade. Um rotulo
# diferente e erro de export, nao aviso.
PREMISE_LABEL = "synthetic"

# Chaves que o gerador exige. Um seed truncado ou renomeado tem de reprovar o export, nunca
# produzir pedidos com um default escondido no gerador.
REQUIRED_PREMISES = (
    "daily_order_rate",
    "min_buyer_age",
    "basket_lines_min",
    "basket_lines_mode",
    "basket_lines_max",
    "quantity_max",
    "substitution_rate",
    "removal_rate",
    "payment_failure_rate",
    "cancellation_rate",
    "delivery_failure_rate",
    "return_rate",
    "order_hour_min",
    "order_hour_max",
    "slot_hours",
    "slot_lead_hours_min",
    "slot_lead_hours_max",
    "minutes_to_payment_min",
    "minutes_to_payment_max",
    "minutes_to_cancel_min",
    "minutes_to_cancel_max",
    "minutes_to_picking_min",
    "minutes_to_picking_max",
    "minutes_per_line_picked",
    "minutes_to_dispatch_min",
    "minutes_to_dispatch_max",
    "minutes_to_delivered_min",
    "minutes_to_delivered_max",
    "minutes_to_return_min",
    "minutes_to_return_max",
    "sla_minutes_picking",
)


class OrdersReferenceError(Exception):
    """O Silver nao tem o que este export precisa, ou a cobertura regrediu."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sql_literal(value: str) -> str:
    """Literal SQL. Uma aspa simples quebraria a query em silencio: melhor recusar."""
    if "'" in value:
        raise OrdersReferenceError(f"valor com aspa simples nao suportado em SQL: {value}")
    return f"'{value}'"


def _seed(seeds_dir: str, name: str) -> str:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise OrdersReferenceError(
            f"seed nao encontrado: {path}. Rode a partir da raiz do repo, ou passe "
            f"--seeds-dir apontando para o diretorio de seeds do dbt."
        )
    return path


def _rows(connection, sql: str) -> list[dict]:
    """Executa e devolve linhas como dicionarios, na ordem que o SQL determinou."""
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _write_json(path: str, payload, indent: int | None) -> int:
    """Grava JSON atomicamente (temporario no mesmo diretorio + os.replace)."""
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    blob = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=indent) + "\n"
    ).encode("utf-8")
    handle, temp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_path, path)
    except BaseException:
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise
    return len(blob)


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise OrdersReferenceError(f"data invalida: {value!r}. Formato esperado YYYY-MM-DD.") from exc


def _date_range(window_from: str, window_to: str) -> list[str]:
    start, end = _parse_date(window_from), _parse_date(window_to)
    if end < start:
        raise OrdersReferenceError(f"janela invertida: --from {window_from} > --to {window_to}")
    days = (end - start).days + 1
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]


def _warehouses(seeds_dir: str) -> list[str]:
    """Armazens em escopo. Vem do seed, nao do que por acaso tem catalogo.

    O vinculo warehouse->provincia e decisao DESTA plataforma, nao propriedade do INE nem da
    Mercadona — ja registrado no ARCHITECTURE.md. Derivar a lista do dado disponivel faria um
    armazem sem catalogo desaparecer em silencio em vez de reprovar.
    """
    path = _seed(seeds_dir, PROVINCE_MAP_SEED)
    with open(path, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise OrdersReferenceError(f"{path}: seed vazio")
    if "wh" not in rows[0]:
        raise OrdersReferenceError(f"{path}: seed sem a coluna 'wh'")
    return sorted({row["wh"] for row in rows})


# --------------------------------------------------------------------------------
# 1. Premissas declaradas
# --------------------------------------------------------------------------------

def _build_premises(seeds_dir: str) -> dict:
    """Le a tabela de premissas do CSV e a rotula com o proprio sha256.

    O digest viaja ate o manifesto da particao. Sem ele, trocar uma taxa e regerar produziria
    pedidos diferentes sob a mesma seed sem deixar rastro — a mesma classe de problema que a
    Fase 1 resolveu guardando a seed de cada execucao em `history`.
    """
    path = _seed(seeds_dir, PREMISES_SEED)
    with open(path, "rb") as handle:
        blob = handle.read()
    digest = hashlib.sha256(blob).hexdigest()

    rows = list(csv.DictReader(blob.decode("utf-8").splitlines()))
    if not rows:
        raise OrdersReferenceError(f"{path}: seed de premissas vazio")

    values: dict[str, str] = {}
    for position, row in enumerate(rows):
        key = (row.get("premise_key") or "").strip()
        if not key:
            raise OrdersReferenceError(f"{path}: linha {position} sem premise_key")
        if key in values:
            raise OrdersReferenceError(f"{path}: premise_key duplicada: {key!r}")
        label = (row.get("label") or "").strip()
        if label != PREMISE_LABEL:
            raise OrdersReferenceError(
                f"{path}: premissa {key!r} rotulada {label!r}. O unico rotulo aceito e "
                f"{PREMISE_LABEL!r}: nenhuma fonte deste repo mede cesta, cadencia ou "
                f"disponibilidade, entao chamar qualquer uma destas de 'observed' ou 'proxy' "
                f"prometeria um dado que nao existe."
            )
        try:
            float(row.get("value"))
        except (TypeError, ValueError) as exc:
            raise OrdersReferenceError(
                f"{path}: premissa {key!r} com valor nao numerico: {row.get('value')!r}"
            ) from exc
        values[key] = str(row["value"]).strip()

    missing = sorted(set(REQUIRED_PREMISES) - set(values))
    if missing:
        raise OrdersReferenceError(
            f"{path}: premissa(s) obrigatoria(s) ausente(s): {missing}. O gerador nao tem "
            f"default para nenhuma delas, de proposito."
        )

    return {
        "generated_at_utc": _utc_now(),
        "seed_path": os.path.normpath(path),
        "seed_sha256": digest,
        "label": PREMISE_LABEL,
        "note": (
            "Toda linha desta tabela e SINTETICA e declarada. Nenhuma fonte ingerida por esta "
            "plataforma mede venda, cesta, cadencia de compra ou disponibilidade de produto. "
            "Estes numeros nao sao proxy de nada observado: sao parametros de simulacao, e "
            "mudar qualquer um deles muda os pedidos gerados sob a mesma seed."
        ),
        "values": values,
        "rows": [
            {
                "premise_key": row["premise_key"].strip(),
                "value": str(row["value"]).strip(),
                "unit": (row.get("unit") or "").strip(),
                "label": (row.get("label") or "").strip(),
                "rationale": (row.get("rationale") or "").strip(),
            }
            for row in rows
        ],
    }


# --------------------------------------------------------------------------------
# 2. Calendario de preco: qual snapshot sustenta cada (wh, dia)
# --------------------------------------------------------------------------------

def _calendar_sql(warehouses: list[str], days: list[str]) -> str:
    wh_values = ", ".join(f"({_sql_literal(w)})" for w in warehouses)
    day_values = ", ".join(f"(date {_sql_literal(d)})" for d in days)
    return f"""
    with wh_list(wh) as (values {wh_values}),
    dias(order_date) as (values {day_values}),
    catalog_dates as (
        select distinct warehouse as wh, ingestion_date
        from silver_product_price
    ),
    grid as (
        select w.wh, d.order_date from wh_list w cross join dias d
    )
    select
        g.wh,
        cast(g.order_date as varchar) as order_date,
        cast((
            select max(c.ingestion_date) from catalog_dates c
            where c.wh = g.wh and c.ingestion_date <= g.order_date
        ) as varchar) as price_as_of
    from grid g
    order by g.wh, g.order_date
    """


def _build_calendar(connection, warehouses: list[str], days: list[str]) -> dict:
    rows = _rows(connection, _calendar_sql(warehouses, days))

    sem_preco = [(r["wh"], r["order_date"]) for r in rows if r["price_as_of"] is None]
    if sem_preco:
        raise OrdersReferenceError(
            f"{len(sem_preco)} par(es) (armazem, dia) sem NENHUM snapshot de catalogo em data "
            f"anterior ou igual: {sem_preco[:10]}. Um pedido nesse dia teria de inventar "
            f"preco. Estreite a janela com --from, ou extraia o catalogo daquele armazem."
        )

    for row in rows:
        row["price_source"] = (
            PRICE_OBSERVED if row["price_as_of"] == row["order_date"] else PRICE_CARRIED_FORWARD
        )

    carried = sum(1 for r in rows if r["price_source"] == PRICE_CARRIED_FORWARD)
    return {
        "generated_at_utc": _utc_now(),
        "window_from": days[0],
        "window_to": days[-1],
        "warehouses": warehouses,
        "catalog_ingestion_dates": sorted({r["price_as_of"] for r in rows}),
        "carried_forward_rows": carried,
        "note": (
            "price_as_of e o maior snapshot de catalogo daquele armazem em data <= order_date. "
            "price_source='observed' quando as duas datas coincidem; 'carried_forward' quando o "
            "dia nao foi observado. Um varejista vende todo dia; a fonte so foi observada em "
            "alguns, e essa diferenca fica explicita em vez de dissolvida."
        ),
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# 3. Catalogo: produto e preco reais, por armazem e por snapshot
# --------------------------------------------------------------------------------

def _catalog_sql(price_dates: list[str]) -> str:
    dates_in = ", ".join(f"date {_sql_literal(d)}" for d in price_dates)
    return f"""
    with scoped as (
        select
            warehouse                          as wh,
            ingestion_date,
            source_product_id,
            display_name,
            category_id,
            category_name,
            subgroup_id,
            subgroup_name,
            product_level1_category_name,
            unit_price,
            -- O PRECO QUE O GERADOR USA. Difere de unit_price so nas linhas `bunch`, onde
            -- a fonte devolve reference_price * 99 — o teto do seletor de peso, e nao o
            -- preco de nada que um domicilio compre (obrigacao 5 do contrato da Mercadona).
            purchasable_unit_price,
            price_basis,
            net_content_kg_l,
            tax_percentage,
            -- Dedup de GRAO, nao regra de negocio: um produto aparece em mais de uma
            -- categoria (semantica da fonte). O preco e identico entre as aparicoes, e isso
            -- ja tem teste proprio no repo. Escolher a menor (category_id, subgroup_id) e
            -- desempate deterministico.
            row_number() over (
                partition by warehouse, ingestion_date, source_product_id
                order by category_id, subgroup_id
            ) as rn
        from silver_product_price
        where ingestion_date in ({dates_in})
          and purchasable_unit_price is not null
          and purchasable_unit_price > 0
    )
    select
        wh,
        cast(ingestion_date as varchar) as price_as_of,
        source_product_id,
        display_name,
        category_id,
        category_name,
        subgroup_id,
        subgroup_name,
        product_level1_category_name    as l1,
        -- String, nunca float: obrigacao 4.4 do contrato da Mercadona. O gerador faz a
        -- aritmetica com decimal.Decimal, que e stdlib.
        --
        -- `unit_price` AQUI E O PRECO DA PORCAO COMPRAVEL. O valor cru que a API devolveu
        -- viaja ao lado em `source_unit_price`, para que a diferenca seja auditavel em vez
        -- de ficar so no Silver: nas 10 linhas de granel ela e de tres ordens de grandeza.
        cast(purchasable_unit_price as varchar) as unit_price,
        cast(unit_price as varchar)             as source_unit_price,
        price_basis,
        cast(net_content_kg_l as varchar)       as net_content_kg_l,
        cast(tax_percentage as varchar) as tax_percentage
    from scoped
    where rn = 1
    order by wh, ingestion_date, category_id, subgroup_id, source_product_id
    """


def _tree_triples_sql(price_dates: list[str]) -> str:
    """Toda trinca (nivel 1, categoria, subgrupo) da arvore, ANTES do dedup.

    MEDIDO, e a razao de esta query existir: o recorte do catalogo guarda UMA linha por
    (armazem, data, produto), com desempate pela menor (category_id, subgroup_id). Um
    produto que vive em `Congelados > Carne` e tambem em `Carne > Carne congelada` some da
    primeira depois do dedup — e tres regras de mapeamento perfeitamente corretas pareceram
    mortas. Conferir cobertura contra o recorte mediria o desempate, nao o mapeamento.
    """
    dates_in = ", ".join(f"date {_sql_literal(d)}" for d in price_dates)
    return f"""
    select distinct
        product_level1_category_name as l1,
        category_name                as l2,
        subgroup_name                as l3
    from silver_product_price
    where ingestion_date in ({dates_in})
    order by 1, 2, 3
    """


def _stamp_demand_group(connection, rows: list[dict], price_dates: list[str], seeds_dir: str) -> dict:
    """Carimba `demand_group` em cada linha do catalogo e devolve a contagem por grupo.

    A RESOLUCAO ACONTECE AQUI, NA PLATAFORMA, e nao no gerador. E deliberado: a Source e
    FROZEN e nao pode carregar a tabela de-para nem a arvore de categorias; e, mais
    importante, o recorte que atravessa a fronteira continua BURRO — uma coluna ja
    resolvida, sem regra de negocio do outro lado.
    """
    mapping = demand_profile.load_mapping(seeds_dir)
    contagem: dict[str, int] = {}
    cache: dict[tuple, str] = {}
    for row in rows:
        chave = (row["l1"], row["category_name"], row["subgroup_name"])
        grupo = cache.get(chave)
        if grupo is None:
            grupo = demand_profile.resolve(mapping, *chave)
            cache[chave] = grupo
        row["demand_group"] = grupo
        contagem[grupo] = contagem.get(grupo, 0) + 1

    # Cobertura contra a ARVORE, nao contra o recorte. Toda trinca precisa de regra mesmo
    # que o dedup a esconda hoje: o desempate pode mudar quando a fonte reordenar as
    # categorias de um produto, e ai a trinca escondida vira a escolhida.
    arvore = [
        (row["l1"], row["l2"], row["l3"])
        for row in _rows(connection, _tree_triples_sql(price_dates))
    ]
    for l1, l2, l3 in arvore:
        demand_profile.resolve(mapping, l1, l2, l3)

    mortas = demand_profile.unused_rules(mapping, arvore)
    if mortas:
        raise OrdersReferenceError(
            f"{len(mortas)} regra(s) de {demand_profile.MAPPING_SEED} nao casam com nenhuma "
            f"trinca da arvore de categorias: "
            f"{[(m['l1'], m['l2'], m['l3']) for m in mortas[:5]]}. Regra morta documenta uma "
            f"decisao que nao esta em vigor, e quem ler o seed vai acreditar nela."
        )
    return contagem


def _build_catalog(connection, price_dates: list[str], seeds_dir: str) -> dict:
    rows = _rows(connection, _catalog_sql(price_dates))
    if not rows:
        raise OrdersReferenceError(
            f"nenhuma linha de catalogo para as datas {price_dates}. "
            f"`silver_product_price` foi construido?"
        )
    grupos = _stamp_demand_group(connection, rows, price_dates, seeds_dir)
    return {
        "demand_groups": dict(sorted(grupos.items())),
        "generated_at_utc": _utc_now(),
        "catalog_ingestion_dates": sorted(price_dates),
        "dedup_rule": (
            "uma linha por (wh, price_as_of, source_product_id); entre aparicoes do mesmo "
            "produto em varias categorias fica a de menor (category_id, subgroup_id)"
        ),
        "price_note": (
            "unit_price e string, nao numero. Converter para float perde precisao decimal em "
            "moeda (obrigacao 4.4 do contrato da Mercadona). A fonte NAO declara moeda: a var "
            "`currency` do projeto dbt e premissa do consumidor e e o Gold que a materializa."
        ),
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# 4. Clientes: quem pode pedir, de qual armazem, a partir de quando
# --------------------------------------------------------------------------------

_CUSTOMERS_SQL = """
with versions as (
    select
        customer_id, wh, province_code, municipality_code, postal_code, birth_year,
        ingestion_date,
        row_number() over (partition by customer_id order by ingestion_date desc) as rn
    from silver_customer
),
first_seen as (
    select customer_id, min(ingestion_date) as first_ingestion_date
    from silver_customer
    group by 1
)
select
    v.customer_id,
    v.wh,
    v.province_code,
    v.municipality_code,
    v.postal_code,
    -- `birth_year` e nao a faixa etaria: a faixa depende do DIA DO PEDIDO, que so o gerador
    -- conhece. Carimba-la aqui congelaria a idade do cliente na data do export, e um
    -- aniversario dentro da janela passaria despercebido.
    v.birth_year,
    cast(f.first_ingestion_date as varchar) as first_ingestion_date
from versions v
join first_seen f on v.customer_id = f.customer_id
where v.rn = 1
order by v.wh, v.customer_id
"""

# Um cliente que aparece com dois armazens quebraria a restricao central ("cliente de W so
# pede de W") sem que nenhum teste a jusante percebesse: o pedido seria coerente com uma das
# duas linhas. Reconferido aqui, e o export RECUSA em vez de escolher.
_CUSTOMER_WAREHOUSE_DRIFT_SQL = """
select customer_id, count(distinct wh) as warehouses
from silver_customer
group by 1
having count(distinct wh) > 1
order by 1
limit 10
"""


def _build_customers(connection, warehouses: list[str]) -> dict:
    drift = _rows(connection, _CUSTOMER_WAREHOUSE_DRIFT_SQL)
    if drift:
        raise OrdersReferenceError(
            f"cliente(s) com mais de um armazem em silver_customer: {drift}. A restricao "
            f"'um cliente de W so pede de W' deixaria de ser verificavel."
        )

    rows = _rows(connection, _CUSTOMERS_SQL)
    if not rows:
        raise OrdersReferenceError(
            "silver_customer esta vazio. Rode `make oltp-refresh-all` antes deste export."
        )

    dates = _rows(
        connection,
        "select distinct cast(ingestion_date as varchar) as d from silver_customer order by 1",
    )
    return {
        "generated_at_utc": _utc_now(),
        "customer_ingestion_dates": [row["d"] for row in dates],
        "roster_ingestion_date": dates[-1]["d"],
        "warehouses": warehouses,
        "note": (
            "Cada linha e a versao MAIS RECENTE do cliente; first_ingestion_date e a primeira "
            "geracao em que ele apareceu. A base e append-only (crescer preserva os primeiros "
            "N byte a byte), entao um cliente presente numa data esta presente em todas as "
            "posteriores. O gerador resolve a versao vigente na data do pedido e RECUSA gerar "
            "pedido anterior a first_ingestion_date."
        ),
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# 5. Coorte do cliente: a ponte entre um atributo observado e um corte do MAPA
# --------------------------------------------------------------------------------

def _warehouse_regions(seeds_dir: str) -> dict:
    """armazem -> comunidade autonoma, via a provincia que ja estava declarada.

    Dois seeds em cadeia e nao um: `warehouse_province_map_seed` e decisao DESTA plataforma
    (onde ficam os armazens), e `ine_ccaa_map_seed` e geografia administrativa do INE (a
    que comunidade uma provincia pertence). Juntar as duas num arquivo so faria uma decisao
    nossa parecer um fato oficial.
    """
    path = _seed(seeds_dir, PROVINCE_MAP_SEED)
    with open(path, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)

    regions: dict[str, str] = {}
    for row in rows:
        wh = (row.get("wh") or "").strip()
        ccaa = demand_profile.ccaa_of(province_ccaa, row.get("province_code"))
        anterior = regions.setdefault(wh, ccaa)
        if anterior != ccaa:
            raise OrdersReferenceError(
                f"o armazem {wh!r} aparece em duas comunidades ({anterior}, {ccaa}). A "
                f"inclinacao regional deixaria de ser definida para ele."
            )
    return regions


def _cohort_counts(
    customer_rows: list[dict],
    seeds_dir: str,
    min_buyer_age: int,
    reference_date: str,
) -> tuple[dict, dict]:
    """(faixa, ccaa) -> quantos clientes ELEGIVEIS, mais um resumo do que ficou de fora.

    A IDADE E CALCULADA NA DATA DE REFERENCIA DA JANELA, e nao no relogio: o export tem de
    ser reprodutivel no ano que vem. Um cliente pode cruzar a fronteira de uma faixa dentro
    de uma janela longa; a massa das coortes e propriedade da JANELA, e a data usada viaja
    no payload para que a escolha fique visivel em vez de implicita.

    O CORTE POR IDADE MINIMA ENTRA AQUI, e nao so no gerador. Se a massa fosse contada
    sobre a base inteira, o IPF fecharia a conta contra uma distribuicao de coortes que
    inclui quem nunca vai colocar um pedido — e o agregado sairia do alvo por um caminho
    que nenhum teste do gerador alcanca.
    """
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)
    ano = int(reference_date[:4])

    counts: dict[tuple, int] = {}
    menores = 0
    sem_ano = 0
    for row in customer_rows:
        birth_year = row.get("birth_year")
        if birth_year is None:
            sem_ano += 1
            continue
        idade = ano - int(birth_year)
        if idade < min_buyer_age:
            menores += 1
            continue
        banda = demand_profile.age_band_of(idade)
        ccaa = demand_profile.ccaa_of(province_ccaa, row.get("province_code"))
        chave = (banda, ccaa)
        counts[chave] = counts.get(chave, 0) + 1

    if sem_ano:
        raise OrdersReferenceError(
            f"{sem_ano} cliente(s) sem birth_year em silver_customer. Sem ano de nascimento "
            f"nao ha faixa etaria, e um cliente sem faixa nao tem propensao definida."
        )
    if not counts:
        raise OrdersReferenceError(
            f"nenhum cliente com {min_buyer_age} anos ou mais em {reference_date}. Com "
            f"min_buyer_age={min_buyer_age} a base inteira ficaria inelegivel."
        )

    resumo = {
        "reference_date": reference_date,
        "min_buyer_age": min_buyer_age,
        "eligible": sum(counts.values()),
        "below_min_age": menores,
        "note": (
            "Medido em 2026-08-31, antes desta fase: 18,01% da base tinha menos de 18 anos "
            "(3.602 de 20.000), com idades a partir de zero. Isso NAO e defeito da Source de "
            "OLTP — o contrato dela declara que a idade vem da distribuicao POPULACIONAL do "
            "INE, e e isso que ela entrega. O que faltava declarado era a diferenca entre "
            "residente e quem coloca um pedido, e ela so passou a importar quando a idade "
            "comecou a governar a demanda."
        ),
    }
    return counts, resumo


# --------------------------------------------------------------------------------
# Coerencia entre os quatro arquivos
# --------------------------------------------------------------------------------

def _assert_coverage(
    customers: dict,
    catalog: dict,
    calendar: dict,
    premises: dict,
    demand: dict,
    warehouse_regions: dict,
) -> None:
    """Cobertura verificada em tempo de execucao, nunca presumida como permanente."""
    warehouses = set(calendar["warehouses"])

    com_cliente = {row["wh"] for row in customers["rows"]}
    sem_cliente = sorted(warehouses - com_cliente)
    if sem_cliente:
        raise OrdersReferenceError(
            f"armazem(ns) em escopo sem nenhum cliente: {sem_cliente}. Pedido sem cliente nao "
            f"e pedido."
        )
    fora_de_escopo = sorted(com_cliente - warehouses)
    if fora_de_escopo:
        raise OrdersReferenceError(
            f"cliente(s) em armazem fora do seed de escopo: {fora_de_escopo}"
        )

    # Todo par (wh, price_as_of) que o calendario aponta precisa existir no catalogo.
    catalogo_por_par: dict[tuple, int] = {}
    for row in catalog["rows"]:
        key = (row["wh"], row["price_as_of"])
        catalogo_por_par[key] = catalogo_por_par.get(key, 0) + 1

    exigidos = {(row["wh"], row["price_as_of"]) for row in calendar["rows"]}
    ausentes = sorted(exigidos - set(catalogo_por_par))
    if ausentes:
        raise OrdersReferenceError(
            f"{len(ausentes)} par(es) (armazem, price_as_of) apontados pelo calendario e "
            f"ausentes do catalogo: {ausentes[:10]}"
        )

    # Uma cesta e sorteada SEM REPOSICAO: se o catalogo de um par tiver menos produtos que a
    # maior cesta possivel, o gerador nao teria como montar a cesta e o defeito apareceria
    # como um pedido menor, nao como erro.
    maior_cesta = int(float(premises["values"]["basket_lines_max"]))
    magros = sorted(
        (par, total) for par, total in catalogo_por_par.items() if total < maior_cesta
    )
    if magros:
        raise OrdersReferenceError(
            f"par(es) (armazem, price_as_of) com menos de basket_lines_max={maior_cesta} "
            f"produtos: {magros[:10]}"
        )

    # TODO GRUPO COM PESO PRECISA TER PRODUTO EM TODO PAR (armazem, price_as_of).
    # Sem esta checagem, um grupo vazio num armazem seria renormalizado em silencio no
    # sorteio e aquele armazem passaria a ter um mix diferente dos outros — plausivel, sem
    # erro, e impossivel de notar num total. E a mesma classe de defeito que
    # `_duplicates_sql` pega em oltp_reference.py.
    com_peso = {
        g["demand_group"] for g in demand["groups"]
        if float(g["line_weight"]) > 0
    }
    por_par: dict[tuple, set] = {}
    for row in catalog["rows"]:
        por_par.setdefault((row["wh"], row["price_as_of"]), set()).add(row["demand_group"])
    buracos = sorted(
        (par, sorted(com_peso - presentes)[:5])
        for par, presentes in por_par.items()
        if com_peso - presentes
    )
    if buracos:
        raise OrdersReferenceError(
            f"{len(buracos)} par(es) (armazem, price_as_of) sem produto em algum grupo com "
            f"peso: {buracos[:5]}. O sorteio renormalizaria em silencio e o mix daquele "
            f"armazem divergiria dos demais sem erro nenhum."
        )

    # TODO ARMAZEM PRECISA DE UMA COMUNIDADE COM INDICE DE FREQUENCIA, e toda coorte
    # precisa de um vetor de pesos que cubra os mesmos grupos do perfil agregado. Um vetor
    # curto faria o sorteio daquela coorte ignorar um grupo inteiro — sem erro, sem nulo, e
    # com um mix que continua somando 1.
    cohorts = demand.get("cohorts")
    if not cohorts:
        raise OrdersReferenceError(
            "o perfil de demanda saiu sem a secao `cohorts`. Um perfil sem a camada de "
            "coorte carimbado com a versao que a promete seria um no-op silencioso."
        )
    com_indice = {r["ccaa_code"] for r in cohorts["regions"]}
    sem_regiao = sorted(
        wh for wh in warehouses if warehouse_regions.get(wh) not in com_indice
    )
    if sem_regiao:
        raise OrdersReferenceError(
            f"armazem(ns) sem comunidade com indice de frequencia: {sem_regiao}"
        )

    todos_grupos = {g["demand_group"] for g in demand["groups"]}
    for vetor in cohorts["weights"]:
        presentes = {g["demand_group"] for g in vetor["groups"]}
        faltando = sorted(todos_grupos - presentes)
        if faltando:
            raise OrdersReferenceError(
                f"a coorte {vetor['cohort']!r} nao tem peso para {len(faltando)} grupo(s): "
                f"{faltando[:5]}. O sorteio daquela coorte ignoraria o grupo inteiro."
            )
        soma = sum(float(g["line_weight"]) for g in vetor["groups"])
        if abs(soma - 1.0) > 1e-6:
            raise OrdersReferenceError(
                f"os pesos da coorte {vetor['cohort']!r} somam {soma}, e nao 1"
            )


def build(connection, window_from: str, window_to: str, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Monta os quatro payloads a partir de uma conexao DuckDB ja aberta.

    Separado de `export` para que a suite exercite as queries contra fixtures DuckDB reais,
    sem object storage e sem rede.
    """
    days = _date_range(window_from, window_to)
    warehouses = _warehouses(seeds_dir)

    premises = _build_premises(seeds_dir)
    calendar = _build_calendar(connection, warehouses, days)
    price_dates = sorted({row["price_as_of"] for row in calendar["rows"]})
    catalog = _build_catalog(connection, price_dates, seeds_dir)
    customers = _build_customers(connection, warehouses)

    # A coorte do cliente e resolvida AQUI, num lugar so, pelo mesmo motivo que
    # `_stamp_demand_group` resolve o mapeamento num lugar so: duas resolucoes divergem no
    # primeiro ajuste e a divergencia seria invisivel, porque as duas dariam mix plausivel.
    warehouse_regions = _warehouse_regions(seeds_dir)
    cohort_counts, cohort_summary = _cohort_counts(
        customers["rows"],
        seeds_dir,
        int(float(premises["values"]["min_buyer_age"])),
        days[0],
    )
    customers["cohort_summary"] = cohort_summary

    demand = demand_profile.build(
        catalog["rows"],
        seeds_dir,
        customers_by_cohort=cohort_counts,
        warehouse_regions=warehouse_regions,
    )

    _assert_coverage(customers, catalog, calendar, premises, demand, warehouse_regions)
    return {
        "customers": customers,
        "catalog": catalog,
        "calendar": calendar,
        "premises": premises,
        "demand": demand,
    }


def write(payloads: dict, out_dir: str) -> dict:
    """Grava os quatro arquivos. Devolve o tamanho de cada um.

    `catalog.json` sai compacto pelo mesmo motivo de `address_candidates.json`: e insumo
    intermediario de dezenas de milhares de linhas, nao particao pousada e hash-verificada.
    """
    return {
        CUSTOMERS_FILE: _write_json(
            os.path.join(out_dir, CUSTOMERS_FILE), payloads["customers"], indent=None
        ),
        CATALOG_FILE: _write_json(
            os.path.join(out_dir, CATALOG_FILE), payloads["catalog"], indent=None
        ),
        CALENDAR_FILE: _write_json(
            os.path.join(out_dir, CALENDAR_FILE), payloads["calendar"], indent=2
        ),
        PREMISES_FILE: _write_json(
            os.path.join(out_dir, PREMISES_FILE), payloads["premises"], indent=2
        ),
        # Indentado: sao dezenas de grupos, nao dezenas de milhares de produtos, e este e o
        # arquivo que alguem vai abrir para conferir de onde saiu um peso.
        DEMAND_FILE: _write_json(
            os.path.join(out_dir, DEMAND_FILE), payloads["demand"], indent=2
        ),
    }


def export(
    config,
    out_dir: str,
    window_from: str,
    window_to: str,
    seeds_dir: str = DEFAULT_SEEDS_DIR,
) -> dict:
    """Escreve os quatro arquivos de referencia em out_dir. Devolve um resumo."""
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    try:
        payloads = build(connection, window_from, window_to, seeds_dir)
    finally:
        connection.close()

    written = write(payloads, out_dir)
    calendar = payloads["calendar"]
    return {
        "out_dir": out_dir,
        "window_from": calendar["window_from"],
        "window_to": calendar["window_to"],
        "warehouses": calendar["warehouses"],
        "days": len({row["order_date"] for row in calendar["rows"]}),
        "customers": len(payloads["customers"]["rows"]),
        "customer_ingestion_dates": payloads["customers"]["customer_ingestion_dates"],
        "catalog_rows": len(payloads["catalog"]["rows"]),
        "catalog_ingestion_dates": calendar["catalog_ingestion_dates"],
        "carried_forward_rows": calendar["carried_forward_rows"],
        "premises_sha256": payloads["premises"]["seed_sha256"],
        "demand_model_version": payloads["demand"]["demand_model_version"],
        "demand_groups": len(payloads["demand"]["groups"]),
        "demand_seeds_sha256": payloads["demand"]["seeds_sha256"],
        "demand_cohorts": len(payloads["demand"]["cohorts"]["weights"]),
        "demand_ipf_iterations": payloads["demand"]["cohorts"]["ipf_iterations"],
        "eligible_customers": payloads["customers"]["cohort_summary"]["eligible"],
        "below_min_buyer_age": payloads["customers"]["cohort_summary"]["below_min_age"],
        "bytes": written,
    }
