"""Reality check of demand: BEFORE · MAPA · AFTER, across the three dimensions.

WHY THREE DIMENSIONS, AND NOT JUST REVENUE
-----------------------------------------
Revenue alone is the worst indicator of realism that exists for this problem, because it
mixes two things that need to be judged separately: HOW MUCH is bought and HOW MUCH what
is bought COSTS. The case that opened this phase is exactly that — seafood showed up with
22,8% of revenue and 3,4% of units, and the correct reading ("the price on 10 lines is
wrong by three orders of magnitude") was invisible in the revenue column.

    units         how many unit-lines of each group go out
    kg / liter    how much weight or volume — the dimension MAPA publishes
    revenue       the money, which is a CONSEQUENCE of the two above and the observed price

And the average price per kg alongside it, which is the only column directly comparable to
the report with no share conversion at all.

THE CONTENT IS DERIVED FROM THE LINE'S PRICE, NOT FROM TODAY'S CATALOG
---------------------------------------------------------------
`kg = quantity * (unit_price_of_the_line / reference_price)`.

It looks like a detail and it is not. If the content came from today's catalog's
`net_content_kg_l` column, the BEFORE would be retroactively corrected: the bulk lines the
simulator charged at 1.084,05 would show up with 0,15 kg instead of the 99 kg that price
implies. The BEFORE has to describe the world as it was, defects included — otherwise the
comparison hides exactly what it exists to show.

THE SAME FUNCTION MEASURES BOTH SIDES. If BEFORE and AFTER were measured by different
queries, part of the difference would come from the query.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from . import demand_profile

DEFAULT_OUT = os.path.join("docs", "demand-evidence", "README.md")
DEFAULT_SNAPSHOT_DIR = os.path.join("docs", "demand-evidence")

# Consulta unica dos dois lados. O join carrega (wh, price_as_of, produto, categoria,
# subgrupo) — a mesma chave composta que o gerador usou para escolher a linha, e nao apenas
# o id do produto: um produto vive em mais de uma categoria e o join por id sozinho
# multiplicaria linhas.
MIX_SQL = """
with catalogo as (
    select
        warehouse, ingestion_date, source_product_id, category_id, subgroup_id,
        product_level1_category_name as l1,
        category_name                as l2,
        subgroup_name                as l3,
        reference_price,
        reference_format
    from silver_product_price
),
linhas as (
    select
        c.l1, c.l2, c.l3,
        l.quantity,
        l.line_amount,
        -- CONTEUDO IMPLICADO PELO PRECO DA LINHA. Nulo quando reference_format nao e massa
        -- nem volume: converter ovos para kg exigiria um peso por ovo que nenhuma fonte
        -- deste repo mede, e um numero inventado aqui contaminaria o denominador de EUR/kg.
        case
            when c.reference_format in ('kg', 'L') and c.reference_price > 0
                then l.quantity * (l.unit_price / c.reference_price)
            when c.reference_format in ('100 g', '100 ml') and c.reference_price > 0
                then l.quantity * (l.unit_price / c.reference_price) * 0.1
        end as kg_l
    from silver_order_line l
    join catalogo c
      on  c.warehouse        = l.wh
      and c.ingestion_date   = l.price_as_of
      and c.source_product_id = l.source_product_id
      and c.category_id      = l.category_id
      and c.subgroup_id      = l.subgroup_id
    where l.line_status <> 'removed'
)
select
    l1, l2, l3,
    count(*)                                     as linhas,
    sum(quantity)                                as unidades,
    sum(line_amount)                             as receita,
    sum(kg_l)                                    as kg_l,
    count(*) filter (where kg_l is null)         as linhas_sem_kg
from linhas
group by 1, 2, 3
order by 1, 2, 3
"""


# O MIX POR COORTE, que e a unica dimensao em que a Fase 5 se enxerga. O agregado nao se
# move de proposito — criterio de aceitacao do IPF — entao um relatorio que mostrasse so
# totais concluiria que a fase nao fez nada.
#
# `demand_group` vem carimbado na LINHA e `buyer_age_band` no PEDIDO, os dois de dentro do
# proprio evento. Nenhum join com o catalogo nem com o cadastro: o que se quer e o que valia
# no momento do pedido, e e exatamente isso que os dois carimbos guardam.
COHORT_SQL = """
select
    o.buyer_age_band,
    o.wh,
    l.demand_group,
    count(*)          as linhas,
    sum(l.quantity)   as unidades,
    sum(l.line_amount) as receita
from silver_order_line l
join silver_order o on o.order_id = l.order_id
where l.line_status <> 'removed'
group by 1, 2, 3
order by 1, 2, 3
"""

# Quantos pedidos cada armazem colocou. E aqui que a inclinacao regional de FREQUENCIA se
# enxerga: antes desta fase os quatro armazens tinham a mesma contagem por construcao.
WAREHOUSE_SQL = """
select wh, count(*) as pedidos, count(distinct customer_id) as clientes
from silver_order
group by 1
order by 1
"""

# A POPULACAO QUE CADA ARMAZEM SERVE, e a janela em dias. Sao os dois numeros que faltam para
# fechar o canal: o modelo produziu tanto volume; o informe diz quanto uma populacao daquele
# tamanho consome por ano; a taxa diz que fracao disso e online. Sem medi-los aqui eles
# teriam de ser escritos a mao nesta pagina, que e o que ela existe para nao fazer.
CHANNEL_SQL = """
with servida as (
    select
        a.wh                        as wh,
        max(p.province_code)        as province_code,
        sum(p.population_value)     as population_total
    from silver_ine_population_by_municipality p
    join warehouse_service_area a
      on p.province_code = a.province_code
     and p.municipality_code = a.municipality_code
    where p.is_latest_ingestion
      and p.sex_label = 'Total'
      and p.year = (
            select max(year) from silver_ine_population_by_municipality
            where is_latest_ingestion
          )
    group by 1
),
janela as (
    select count(distinct ingestion_date) as dias,
           min(ingestion_date)            as primeiro,
           max(ingestion_date)            as ultimo
    from silver_order
)
select s.wh, s.province_code, cast(s.population_total as bigint) as population_total,
       j.dias, cast(j.primeiro as varchar) as primeiro, cast(j.ultimo as varchar) as ultimo
from servida s cross join janela j
order by s.wh
"""


class DemandCheckError(Exception):
    """There is nothing to measure, or the requested snapshot does not exist."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _decimal(value) -> Decimal:
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


def measure(connection, seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR) -> dict:
    """Observed mix across the three dimensions, aggregated by demand group."""
    resultado = connection.execute(MIX_SQL)
    colunas = [d[0] for d in resultado.description]
    trincas = [dict(zip(colunas, linha)) for linha in resultado.fetchall()]
    if not trincas:
        raise DemandCheckError(
            "no order line to measure. Was `silver_order_line` built?"
        )

    mapping = demand_profile.load_mapping(seeds_dir)
    grupos: dict[str, dict] = {}
    for row in trincas:
        key = demand_profile.resolve(mapping, row["l1"], row["l2"], row["l3"])
        alvo = grupos.setdefault(
            key,
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0"),
             "kg_l": Decimal("0"), "linhas_sem_kg": 0},
        )
        alvo["linhas"] += int(row["linhas"])
        alvo["unidades"] += _decimal(row["unidades"])
        alvo["receita"] += _decimal(row["receita"])
        alvo["kg_l"] += _decimal(row["kg_l"])
        alvo["linhas_sem_kg"] += int(row["linhas_sem_kg"])

    return {
        "measured_at_utc": _utc_now(),
        "groups": {
            key: {
                "linhas": valor["linhas"],
                "unidades": str(valor["unidades"]),
                "receita": str(valor["receita"]),
                "kg_l": str(valor["kg_l"]),
                "linhas_sem_kg": valor["linhas_sem_kg"],
            }
            for key, valor in sorted(grupos.items())
        },
        **_measure_cohorts(connection),
        **_measure_channel(connection),
    }


def _measure_channel(connection) -> dict:
    """Population served per warehouse, and the size of the window.

    TOLERANT for the same reason as `_measure_cohorts`, and not out of generosity: the
    BEFORE snapshots frozen in earlier phases do not have this block, and they need to
    stay readable. The absence is recorded as `None` — empty would be indistinguishable
    from "I measured, and the population served is zero".
    """
    try:
        resultado = connection.execute(CHANNEL_SQL)
    except Exception:  # noqa: BLE001 - modelo ausente numa janela anterior a esta fase
        return {"channel": None}
    colunas = [d[0] for d in resultado.description]
    linhas = [dict(zip(colunas, linha)) for linha in resultado.fetchall()]
    if not linhas:
        return {"channel": None}
    return {
        "channel": {
            "dias": int(linhas[0]["dias"]),
            "primeiro_dia": linhas[0]["primeiro"],
            "ultimo_dia": linhas[0]["ultimo"],
            "warehouses": [
                {
                    "wh": row["wh"],
                    "province_code": row["province_code"],
                    "population_total": int(row["population_total"]),
                }
                for row in linhas
            ],
        }
    }


def _measure_cohorts(connection) -> dict:
    """Mix per cohort and count per warehouse, when the window already carries them.

    TOLERANT FOR ONE SPECIFIC REASON, and not out of generosity: the BEFORE of this
    phase is a window generated by `mapa_2025_v1`, whose `silver_order` does not have
    the `buyer_age_band` column. It needs to be freezable — without a BEFORE there is
    no way to PROVE the aggregate did not move, and that proof is the phase's
    acceptance criterion. An error here would block precisely the measurement that
    justifies the change.

    The absence is RECORDED (`cohorts: None`) instead of turning into an empty dict:
    empty would be indistinguishable from "I measured, and there was nothing".
    """
    try:
        resultado = connection.execute(COHORT_SQL)
    except Exception:  # noqa: BLE001 - coluna ausente na janela anterior a esta fase
        return {"cohorts": None, "warehouses": None}

    colunas = [d[0] for d in resultado.description]
    linhas = [dict(zip(colunas, linha)) for linha in resultado.fetchall()]
    coortes: dict[str, dict] = {}
    for row in linhas:
        banda = row["buyer_age_band"]
        if banda is None:
            continue
        alvo = coortes.setdefault(banda, {})
        grupo = alvo.setdefault(
            row["demand_group"],
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0")},
        )
        grupo["linhas"] += int(row["linhas"])
        grupo["unidades"] += _decimal(row["unidades"])
        grupo["receita"] += _decimal(row["receita"])

    if not coortes:
        return {"cohorts": None, "warehouses": None}

    resultado = connection.execute(WAREHOUSE_SQL)
    colunas = [d[0] for d in resultado.description]
    armazens = {
        row["wh"]: {"pedidos": int(row["pedidos"]), "clientes": int(row["clientes"])}
        for row in (dict(zip(colunas, linha)) for linha in resultado.fetchall())
    }

    return {
        "cohorts": {
            banda: {
                grupo: {
                    "linhas": valor["linhas"],
                    "unidades": str(valor["unidades"]),
                    "receita": str(valor["receita"]),
                }
                for grupo, valor in sorted(grupos.items())
            }
            for banda, grupos in sorted(coortes.items())
        },
        "warehouses": armazens,
    }


def _shares(snapshot: dict) -> dict:
    """Percentage shares per group, across the three dimensions, plus EUR/kg."""
    grupos = snapshot["groups"]
    total_un = sum(_decimal(g["unidades"]) for g in grupos.values())
    total_kg = sum(_decimal(g["kg_l"]) for g in grupos.values())
    total_eur = sum(_decimal(g["receita"]) for g in grupos.values())
    saida = {}
    for key, g in grupos.items():
        un, kg, eur = _decimal(g["unidades"]), _decimal(g["kg_l"]), _decimal(g["receita"])
        saida[key] = {
            "unidades": un,
            "kg_l": kg,
            "receita": eur,
            "pct_unidades": (un / total_un * 100) if total_un else None,
            "pct_kg": (kg / total_kg * 100) if total_kg else None,
            "pct_receita": (eur / total_eur * 100) if total_eur else None,
            "eur_por_kg": (eur / kg) if kg else None,
            "linhas_sem_kg": g["linhas_sem_kg"],
        }
    saida["__total__"] = {
        "unidades": total_un, "kg_l": total_kg, "receita": total_eur,
        "eur_por_kg": (total_eur / total_kg) if total_kg else None,
    }
    return saida


def save_snapshot(snapshot: dict, path: str) -> str:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(snapshot, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")
    return path


def load_snapshot(path: str) -> dict:
    if not os.path.exists(path):
        raise DemandCheckError(
            f"missing snapshot: {path}. Freeze the BEFORE with "
            f"`make demand-reality-check SNAPSHOT=before` before regenerating the window "
            f"— after regenerating, the BEFORE no longer exists anywhere."
        )
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def _fmt(value, casas: int = 2) -> str:
    if value is None:
        return "—"
    return f"{Decimal(value):,.{casas}f}".replace(",", " ").replace(".", ",")


def _targets(benchmark: dict, params: dict) -> dict:
    """Volume target per group: MAPA's share tilted by channel, renormalized.

    THIS IS THE COLUMN THE MODEL MUST BE JUDGED AGAINST, and not MAPA's raw share.
    The report measures domestic consumption; the simulator represents e-commerce, and
    e-commerce does NOT consume in the same proportion — 1,1% of fresh volume against
    2,8% of the rest, over an average of 2,2%. Comparing the simulator to the raw share
    would flag as an error precisely the channel correction this entire phase exists to
    apply: fresh fruit MUST show up below the domestic 14,13% in an online basket.
    """
    pesaveis = [k for k, v in benchmark.items() if v["use_as_weight"]]
    bruto = {}
    for key in pesaveis:
        tilt, _base = demand_profile.channel_tilt(benchmark[key], params)
        bruto[key] = benchmark[key]["volume_share_pct"] * tilt
    total = sum(bruto.values())
    return {k: (v / total * 100 if total else None) for k, v in bruto.items()}


def _block_shares(shares: dict, chaves: list) -> dict:
    """Renormalizes shares within a subset of groups.

    Without this, AFTER would be over the total (including non-food) and the target over
    the calibrated block: two different bases in the same table, which is how you end up
    comparing wrong without noticing.
    """
    total = sum(Decimal(shares[k]["kg_l"]) for k in chaves if k in shares)
    return {
        k: (Decimal(shares[k]["kg_l"]) / total * 100 if total else None)
        for k in chaves if k in shares
    }


def render(
    depois: dict,
    antes: dict | None,
    seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR,
    antes_nome: str | None = None,
) -> str:
    """Markdown for the reality check. No number written by hand."""
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(depois)
    sa = _shares(antes) if antes else None
    alvos = _targets(benchmark, params)
    pesaveis = sorted(alvos, key=lambda k: -(alvos[k] or 0))

    dep_bloco = _block_shares(sd, pesaveis)
    ant_bloco = _block_shares(sa, pesaveis) if sa else {}

    linhas: list[str] = []
    linhas.append("# Reality check of synthetic demand")
    linhas.append("")
    linhas.append(
        "GENERATED by `make demand-reality-check`. No number on this page was written by "
        "hand; regenerate it instead of editing it."
    )
    linhas.append("")
    linhas.append(f"- demand model: **{params['demand_model_version']}**")
    linhas.append(f"- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025")
    linhas.append(f"- measured at: {depois['measured_at_utc']}")
    if antes:
        rotulo = f" (`{antes_nome}`)" if antes_nome else ""
        linhas.append(
            f"- BEFORE frozen at: {antes['measured_at_utc']}{rotulo} — the state "
            f"IMMEDIATELY prior to this model version, not the oldest one that "
            f"exists. `before_mapa_2025_v1` keeps the uniform mix from before the "
            f"aggregate calibration and stays on disk; mixing the two into one column "
            f"would just make the effects of two phases get read as one."
        )
    else:
        linhas.append(
            "- BEFORE: **absent**. Without it this page shows only the current state "
            "against MAPA, and the model-effect column does not exist."
        )
    linhas.append("")
    linhas.append("## How to read the columns")
    linhas.append("")
    linhas.append(
        "**MAPA %** is gross domestic consumption. **TARGET %** is the same number "
        "tilted by e-commerce participation and renormalized — it is against this that "
        "the model must be judged. The two differ on purpose: the report measures what "
        "the resident consumes, and the online basket does not have the same "
        "composition (1,1% of fresh volume arrives via e-commerce against 2,8% of the "
        "rest). Fresh fruit MUST show up below the domestic 14,13% in an online store; "
        "if it showed up at 14,13% the model would be confusing total consumption with "
        "channel."
    )
    linhas.append("")
    linhas.append(
        "Revenue alone does not work as an indicator of realism: it mixes HOW MUCH is "
        "bought with HOW MUCH IT COSTS. That is how 10 catalog lines with an API price "
        "ceiling accounted for 16,6% of all simulated revenue without any total closing "
        "wrong."
    )
    linhas.append("")

    # --- totais ------------------------------------------------------------------
    td, ta = sd["__total__"], (sa["__total__"] if sa else None)
    linhas.append("## Totals")
    linhas.append("")
    linhas.append("| dimension | BEFORE | AFTER | change |")
    linhas.append("|---|---:|---:|---:|")
    for rotulo, chave, casas in (
        ("units", "unidades", 0), ("kg or liter", "kg_l", 1),
        ("revenue (EUR)", "receita", 2), ("EUR per kg", "eur_por_kg", 2),
    ):
        atual = td[chave]
        anterior = ta[chave] if ta else None
        if anterior and atual is not None and anterior != 0:
            var = f"{(Decimal(atual) / Decimal(anterior) - 1) * 100:+.1f} %".replace(".", ",")
        else:
            var = "—"
        linhas.append(f"| {rotulo} | {_fmt(anterior, casas)} | {_fmt(atual, casas)} | {var} |")
    linhas.append("")
    linhas.append(
        "MAPA's EUR/kg for total domestic food consumption in 2025 is **3,25**. Ours "
        "also covers the non-food third of the catalog, which the report does not "
        "measure, so the two are not directly comparable — the useful comparison is "
        "group by group, further below."
    )
    linhas.append("")

    # --- blocos -------------------------------------------------------------------
    linhas.append("## The three blocks")
    linhas.append("")
    linhas.append(
        "The food share is a declared premise; the split between calibrated and "
        "uncalibrated comes from the benchmark's own coverage, not from a new number."
    )
    linhas.append("")
    linhas.append("| block | declared weight | % of observed volume | weight origin |")
    linhas.append("|---|---:|---:|---|")
    total_kg = td["kg_l"]
    blocos = (
        ("calibrated by MAPA", "benchmark_block", pesaveis,
         f"food_line_share x benchmark coverage ({params['benchmark_volume_coverage_pct']}%)"),
        ("food without benchmark", "sin_benchmark_block", [demand_profile.SIN_BENCHMARK],
         "complement, divided by assortment size"),
        ("non-food", "no_food_block", [demand_profile.NO_FOOD],
         "1 - food_line_share; outside MAPA's universe"),
    )
    food = Decimal(params["food_line_share"])
    cob = Decimal(params["benchmark_volume_coverage_pct"]) / 100
    declarado = {
        "benchmark_block": food * cob,
        "sin_benchmark_block": food * (1 - cob),
        "no_food_block": 1 - food,
    }
    for rotulo, chave, membros, origem in blocos:
        obtido = sum(Decimal(sd[k]["kg_l"]) for k in membros if k in sd)
        pct = (obtido / total_kg * 100) if total_kg else None
        linhas.append(
            f"| {rotulo} | {_fmt(declarado[chave] * 100)} % (lines) | {_fmt(pct)} % (kg) | {origem} |"
        )
    linhas.append("")
    linhas.append(
        "Declared weight is a share of LINES; the observed column is a share of "
        "VOLUME. They should not coincide: a pack of water weighs 3,5 kg and a spice "
        "sachet weighs 30 g."
    )
    linhas.append("")

    # --- volume, dentro do bloco calibrado -----------------------------------------
    linhas.append("## Volume (kg or liter) — within the calibrated block")
    linhas.append("")
    linhas.append(
        "All columns sum to 100 over the same groups. This is the dimension MAPA "
        "publishes and where calibration acts."
    )
    linhas.append("")
    linhas.append("| group | BEFORE % | MAPA % | TARGET % | AFTER % | error | channel |")
    linhas.append("|---|---:|---:|---:|---:|---:|---|")
    erros = []
    for key in pesaveis:
        b = benchmark[key]
        _tilt, base = demand_profile.channel_tilt(b, params)
        alvo, obtido = alvos[key], dep_bloco.get(key)
        if alvo is not None and obtido is not None:
            erro = obtido - alvo
            erros.append(abs(erro))
            erro_txt = f"{erro:+.2f}".replace(".", ",")
        else:
            erro_txt = "—"
        linhas.append(
            f"| {key} | {_fmt(ant_bloco.get(key))} | {_fmt(b['volume_share_pct'])} "
            f"| {_fmt(alvo)} | {_fmt(obtido)} | {erro_txt} | {base} |"
        )
    linhas.append("")
    if erros:
        medio = sum(erros) / len(erros)
        pior = max(erros)
        linhas.append(
            f"Mean absolute error against the TARGET: **{_fmt(medio, 3)}** points; "
            f"largest deviation: **{_fmt(pior, 3)}** points."
        )
        linhas.append("")
        linhas.append(
            "The goal is NOT to minimize this number. It is here so that a large "
            "deviation is explainable, not to be chased — an error of zero would mean "
            "the catalog's assortment matches the Spanish basket perfectly, which "
            "would be suspicious, not good."
        )
        linhas.append("")
    fallback = [
        k for k in pesaveis
        if sd.get(k, {}).get("linhas_sem_kg", 0) > 0
    ]
    if fallback:
        linhas.append(
            "Groups with lines that have no kg conversion (their deviation against the "
            "TARGET does not measure calibration, it measures conversion coverage): "
            + ", ".join(f"`{k}` ({sd[k]['linhas_sem_kg']} lines)" for k in fallback)
        )
        linhas.append("")

    # --- unidades e receita ---------------------------------------------------------
    chaves_todas = [k for k in sorted(sd, key=lambda k: -(float(sd[k].get("pct_kg") or 0)))
                    if k != "__total__"]

    linhas.append("## Units — over the total, including non-food")
    linhas.append("")
    linhas.append(
        "Not comparable to MAPA: the report measures weight and volume, never piece "
        "count. This table exists to show how many LINES each group occupies in the "
        "basket."
    )
    linhas.append("")
    linhas.append("| group | BEFORE % | AFTER % |")
    linhas.append("|---|---:|---:|")
    for key in chaves_todas:
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('pct_unidades') if sa else None)} "
            f"| {_fmt(sd[key].get('pct_unidades'))} |"
        )
    linhas.append("")

    linhas.append("## Revenue — over the total")
    linhas.append("")
    linhas.append(
        "The MAPA column is the domestic VALUE share, with no channel tilt. It is NOT "
        "a target: revenue is a consequence of volume and Mercadona's observed price, "
        "and converges with MAPA only to the extent that Spanish prices and Mercadona's "
        "prices look alike. Volume and value should DIVERGE from each other — seafood is "
        "0,81% of volume and 2,88% of domestic value, and a simulator that made them "
        "equal would be wrong."
    )
    linhas.append("")
    linhas.append("| group | BEFORE % | MAPA value % | AFTER % |")
    linhas.append("|---|---:|---:|---:|")
    for key in chaves_todas:
        b = benchmark.get(key, {})
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('pct_receita') if sa else None)} "
            f"| {_fmt(b.get('value_share_pct'))} | {_fmt(sd[key].get('pct_receita'))} |"
        )
    linhas.append("")

    # --- preco medio ------------------------------------------------------------------
    linhas.append("## Average price per kg — the column comparable with no conversion at all")
    linhas.append("")
    linhas.append(
        "Share requires converting volume into lines and tilting by channel; EUR/kg "
        "requires nothing. A group whose EUR/kg matches the report has a healthy "
        "observed price, regardless of how many lines of it go out — and it was this "
        "column that flagged the bulk-pricing defect before calibration existed."
    )
    linhas.append("")
    linhas.append("| group | BEFORE EUR/kg | MAPA EUR/kg | AFTER EUR/kg | lines without kg |")
    linhas.append("|---|---:|---:|---:|---:|")
    for key in chaves_todas:
        b = benchmark.get(key, {})
        linhas.append(
            f"| {key} | {_fmt(sa.get(key, {}).get('eur_por_kg') if sa else None)} "
            f"| {_fmt(b.get('avg_price_eur_kg'))} | {_fmt(sd[key].get('eur_por_kg'))} "
            f"| {sd[key].get('linhas_sem_kg', 0)} |"
        )
    linhas.append("")

    # --- o que o benchmark nao alcanca -------------------------------------------------
    linhas.append("## What the benchmark does not reach")
    linhas.append("")
    sem_alvo = sorted(
        k for k, v in benchmark.items()
        if not v["use_as_weight"] and v["provenance"] in ("informe_prose", "none")
        and k not in demand_profile.SCOPE_LABELS
    )
    linhas.append(
        f"- **{demand_profile.NO_FOOD}** and **{demand_profile.SIN_BENCHMARK}** have no "
        f"MAPA target and are never added to the calibrated block. The report measures "
        f"food and beverages; drugstore, cleaning, makeup and pet items are outside its "
        f"universe."
    )
    linhas.append(
        f"- declared food share: **{params['food_line_share']}** — `synthetic` premise, "
        f"with no benchmark. No source in this repo measures the food composition of an "
        f"online basket, and MAPA does not measure drugstore items."
    )
    linhas.append(
        f"- benchmark coverage: **{params['benchmark_volume_coverage_pct']}%** of "
        f"Spanish domestic volume, summing the weighable leaves. The complement goes "
        f"to `SIN_BENCHMARK`, divided by assortment size."
    )
    for key in sem_alvo:
        b = benchmark[key]
        secao = b["informe_section"] or "no section"
        linhas.append(f"- `{key}` ({b['mapa_label']}): no published share — section {secao}")
    linhas.append("")
    linhas.append("### Seasonality")
    linhas.append("")
    linhas.append(
        "NEUTRAL profile across the 12 months, for lack of numeric evidence: the "
        "report's monthly charts are images, and there are only five monthly numbers "
        "in prose — all for the food TOTAL, never by category. Besides that, the "
        "simulator's window covers only August, so there is no monthly axis to "
        "exercise. The mechanism exists, applies to the order rate, and has a test "
        "proving that a non-neutral profile changes the output; the trigger for "
        "proposing a profile is the window covering November and December."
    )
    linhas.extend(_cohort_section(depois, antes))
    linhas.extend(_channel_section(depois, seeds_dir, params))
    linhas.append("")
    linhas.append("## Boundary the calibration does not cross")
    linhas.append("")
    linhas.append(
        "MAPA measures the resident's domestic consumption. It does NOT measure "
        "online-store orders, basket, purchase cadence, or ticket per channel. That is "
        "why `daily_order_rate`, `basket_lines_min/mode/max` and `quantity_max` remain "
        "`synthetic` premises in `order_premises_seed.csv` and received NO calibration "
        "whatsoever in this phase. Calling the benchmark a source for those numbers "
        "would turn it into a false representation of reality."
    )
    linhas.append("")
    return "\n".join(linhas) + "\n"


def _channel_section(depois: dict, seeds_dir: str, params: dict) -> list[str]:
    """The account that only closes once the base was sized by population.

    THE CIRCULAR ARGUMENT THIS BREAKS. The customer base became 2,2% of the adults of
    the four AUFs because 2,2% of food volume goes through e-commerce (MAPA, section
    3). If that transposition — from volume share to people share — were coherent with
    the rest of the model, the volume the orders produce would also have to be 2,2% of
    the domestic consumption of those same AUFs. There is no guarantee that it is:
    `daily_order_rate` and basket size were declared in Phase 3, with no relation to
    the penetration rate, and now the three meet for the first time.

    THIS SECTION IS NOT A TARGET TO CHASE. Closing the gap by tweaking
    `daily_order_rate` would be moving a premise to fit a result — and no source in
    this repository measures purchase cadence or online basket size, so there is
    nothing to consult to decide which of the two is wrong. The number is measured,
    published, and recorded as a gap.
    """
    canal = depois.get("channel")
    if not canal:
        return [
            "",
            "## Channel closure",
            "",
            "Not measured: this snapshot was frozen before the served population entered "
            "the measurement.",
        ]

    regioes = demand_profile.load_region_reference(seeds_dir)
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)
    taxa = Decimal(str(params["channel_reference_pct"]))
    dias = Decimal(canal["dias"])

    # ESCOPO ALIMENTAR, e nao o total do modelo. O per capita do informe e de "alimentacion
    # y bebidas" e NAO cobre drogaria; somar NO_FOOD do lado do modelo compararia dois
    # universos diferentes e inflaria a razao sem que nada estivesse errado. SIN_BENCHMARK
    # FICA: sao grupos alimentares que o informe simplesmente nao detalha, e eles pertencem ao
    # mesmo universo que o per capita mede.
    kg_medido = Decimal("0")
    eur_medido = Decimal("0")
    kg_no_food = Decimal("0")
    eur_no_food = Decimal("0")
    for chave, valor in depois["groups"].items():
        if chave == demand_profile.NO_FOOD:
            kg_no_food += Decimal(valor["kg_l"])
            eur_no_food += Decimal(valor["receita"])
            continue
        kg_medido += Decimal(valor["kg_l"])
        eur_medido += Decimal(valor["receita"])

    linhas = ["", "## Channel closure", ""]
    linhas.append(
        f"The customer base was sized as **{taxa}% of the adults** of the four AUFs, "
        f"because {taxa}% of food volume goes through e-commerce. The question this "
        f"section answers is whether the model, after that, produces {taxa}% of the "
        f"consumption of those same AUFs — or whether the penetration rate and the "
        f"order cadence, declared in different phases with no relation to each other, "
        f"contradict one another."
    )
    linhas.append("")
    linhas.append(
        f"Measured window: **{canal['dias']} day(s)**, from {canal['primeiro_dia']} to "
        f"{canal['ultimo_dia']}. The report's consumption is annual and is divided by 365."
    )
    linhas.append("")

    linhas.append("| warehouse | population served | community | kg-L/capita/yr | EUR/capita/yr |")
    linhas.append("|---|---:|---|---:|---:|")
    kg_alvo = Decimal("0")
    eur_alvo = Decimal("0")
    populacao_total = 0
    for entrada in canal["warehouses"]:
        ccaa = demand_profile.ccaa_of(province_ccaa, entrada["province_code"])
        regiao = regioes[ccaa]
        populacao = Decimal(entrada["population_total"])
        populacao_total += entrada["population_total"]
        kg_alvo += populacao * regiao["per_capita_kg_l"]
        eur_alvo += populacao * (regiao["per_capita_eur"] or Decimal("0"))
        linhas.append(
            f"| {entrada['wh']} | {entrada['population_total']:,} | {regiao['ccaa_label']} "
            f"| {regiao['per_capita_kg_l']} | {regiao['per_capita_eur']} |"
        )
    linhas.append(f"| **total** | **{populacao_total:,}** | | | |")
    linhas.append("")

    # Do consumo anual da populacao para o pedaco online da janela medida.
    fator = taxa / Decimal("100") * dias / Decimal("365")
    kg_alvo_janela = kg_alvo * fator
    eur_alvo_janela = eur_alvo * fator

    linhas.append("| dimension | expected channel | model | ratio |")
    linhas.append("|---|---:|---:|---:|")
    for rotulo, medido, alvo in (
        ("kg or liter", kg_medido, kg_alvo_janela),
        ("revenue (EUR)", eur_medido, eur_alvo_janela),
    ):
        if medido is None or not alvo:
            linhas.append(f"| {rotulo} | — | — | — |")
            continue
        razao = medido / alvo
        linhas.append(
            f"| {rotulo} | {_fmt(alvo, 0)} | {_fmt(medido, 0)} | **{_fmt(razao, 2)}x** |"
        )
    linhas.append("")
    linhas.append(
        f"The scope on both sides is FOOD. `{demand_profile.NO_FOOD}` is left out of "
        f"the numerator — {_fmt(kg_no_food, 0)} kg-L and {_fmt(eur_no_food, 2)} EUR in "
        f"the window — because the report's per capita is for food and beverages and "
        f"does not cover drugstore items. Adding it would compare two different "
        f"universes and inflate the ratio without anything being wrong. "
        f"`SIN_BENCHMARK` STAYS: these are food groups the report does not detail, "
        f"but that belong to the same universe the per capita measures."
    )
    linhas.append("")
    linhas.append(
        "**None of these numbers was adjusted to get closer to the other, and that is "
        "what makes them interesting.** The penetration rate entered in Phase 6, "
        "anchored on the report. `daily_order_rate`, `basket_lines_*` and "
        "`quantity_max` entered in Phase 3, chosen with no relation to it and with no "
        "source that measured them. The two halves only meet in this table, and the "
        "rest between them is what you see above."
    )
    linhas.append("")
    linhas.append(
        "The distance that remains must NOT be closed by tweaking `daily_order_rate` "
        "until the ratio becomes 1,00: that would make a premise fit a result without "
        "anything having been measured, and no source in this repository measures "
        "domestic purchase cadence or online basket — there is no criterion to decide "
        "which side is wrong. As long as that is so, this ratio is an OBSERVATION, not "
        "a target. TRIGGER: a source that measures domestic purchase frequency or "
        "average ticket per channel turns this line into a test."
    )
    linhas.append("")
    linhas.append(
        "A caveat about the denominator, for the same reason it already exists for the "
        "cohort: the report's per capita consumption is for the community's ENTIRE "
        "population, children included, while the customers are adults. That is "
        "correct here — a household's consumption shows up in the per capita of all "
        "its members — but it means the ratio above cannot be read as 'each customer "
        "buys X% of what they should'."
    )
    return linhas


def _cohort_section(depois: dict, antes: dict | None) -> list[str]:
    """The only dimension in which the cohort layer shows itself.

    THE AGGREGATE DOES NOT MOVE ON PURPOSE — that is the IPF's acceptance criterion —
    so a page that only showed totals would conclude the phase did nothing. This
    section exists because the effect is entirely conditional, and something that
    only shows up in the conditional needs a place of its own to be seen.
    """
    coortes = depois.get("cohorts")
    linhas: list[str] = ["", "## Propensity by buyer cohort", ""]

    if not coortes:
        linhas.append(
            "**Not present in this window.** `silver_order.buyer_age_band` is not "
            "populated — the window was generated by a model prior to the cohort "
            "layer. This is not a measurement of zero, it is the absence of the stamp."
        )
        return linhas

    linhas.append(
        "The cohort has two dimensions, and they are the two ONLY ones in which an "
        "observed customer attribute coincides with a cut published by the report: "
        "**age** (`birth_year`, from the INE's provincial distribution) and "
        "**autonomous community** (`province_code`, from the Callejero). Household "
        "life cycle and socioeconomic level are MAPA's richest cuts and were left out: "
        "the customer has no household composition or income, and assigning them "
        "would mean inventing the attribute."
    )
    linhas.append("")
    linhas.append(
        "**The aggregate does not move, and that is the acceptance criterion.** The "
        "per-cohort weights, weighted by the real distribution of cohorts among "
        "orders, reproduce the weights of the aggregate calibration — that is what "
        "the IPF exists for. Anyone who looks for this layer's effect in a total will "
        "not find it: it lives entirely in the columns below."
    )

    bandas = [b for b in ("LT35", "35_49", "50_64", "GE65") if b in coortes]
    totais = {
        b: sum(int(v["linhas"]) for v in coortes[b].values()) for b in bandas
    }

    linhas.append("")
    linhas.append("### Each group's slice WITHIN the cohort (% of lines)")
    linhas.append("")
    linhas.append(
        "The denominator is the cohort itself, not the total: that is how the "
        "comparison between bands isolates propensity from cohort size. The `x` "
        "column is the ratio between the oldest and the youngest band — the summary "
        "of an entire row."
    )
    linhas.append("")
    cabecalho = "| group | " + " | ".join(f"{b} %" for b in bandas) + " | x GE65/LT35 |"
    linhas.append(cabecalho)
    linhas.append("|---|" + "---:|" * (len(bandas) + 1))

    grupos = sorted({g for b in bandas for g in coortes[b]})
    ordenados = []
    for grupo in grupos:
        fatias = {}
        for b in bandas:
            total = totais[b] or 1
            fatias[b] = Decimal(coortes[b].get(grupo, {}).get("linhas", 0)) * 100 / total
        razao = None
        if "GE65" in fatias and "LT35" in fatias and fatias["LT35"] > 0:
            razao = fatias["GE65"] / fatias["LT35"]
        ordenados.append((razao if razao is not None else Decimal("0"), grupo, fatias))
    ordenados.sort(reverse=True)

    for razao, grupo, fatias in ordenados:
        celulas = " | ".join(_fmt(fatias[b]) for b in bandas)
        linhas.append(f"| {grupo} | {celulas} | {_fmt(razao)} |")

    linhas.append("")
    linhas.append(
        "A ratio of 1,00 means the age band does not move that group. `NO_FOOD` and "
        "`SIN_BENCHMARK` stay close to 1 by construction: the report does not measure "
        "them, their index is neutral, and each BLOCK's slice is kept constant across "
        "cohorts on purpose. Without that boundary they would absorb the "
        "normalization's residue and the model would end up claiming that elderly "
        "people buy 40% less drugstore items — a number no source in this repo "
        "measures, and bigger than most of the effects that are measured."
    )

    armazens = depois.get("warehouses") or {}
    if armazens:
        linhas.append("")
        linhas.append("### Frequency by autonomous community")
        linhas.append("")
        linhas.append(
            "Before this phase the four warehouses had the SAME order count by "
            "construction. The report measures per capita consumption by community, "
            "and that difference now applies — as frequency, never as basket size, "
            "because the report gives kg per year and does not publish domestic "
            "purchase frequency."
        )
        linhas.append("")
        linhas.append("| warehouse | orders | distinct customers | orders per customer |")
        linhas.append("|---|---:|---:|---:|")
        for wh in sorted(armazens):
            dados = armazens[wh]
            por_cliente = (
                Decimal(dados["pedidos"]) / Decimal(dados["clientes"])
                if dados["clientes"] else Decimal("0")
            )
            linhas.append(
                f"| {wh} | {dados['pedidos']} | {dados['clientes']} | {_fmt(por_cliente)} |"
            )
        antes_wh = (antes or {}).get("warehouses")
        if antes_wh:
            linhas.append("")
            linhas.append(
                "The frozen BEFORE has the count per warehouse alongside; the "
                "difference between them is the regional tilt taking effect."
            )
        else:
            linhas.append("")
            linhas.append(
                "The frozen BEFORE does not record a count per warehouse: it predates "
                "this measurement existing. The useful comparison is among today's "
                "four warehouses, which used to be equal by construction."
            )

    return linhas


def calibration_error(depois: dict, seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR) -> dict:
    """Error of the observed mix against the TARGET, ignoring groups without kg coverage.

    THIS IS NOT A GRADE. The declared objective was never to minimize this distance —
    an error of zero would mean Mercadona's assortment matches the Spanish basket
    perfectly, which would be suspicious and not good. The number exists to catch ONE
    thing: a silently inert calibration.

    And the failure mode is concrete. If the profile stops being applied — a
    `demand.pick` that reverts to uniform, an export that forgets the fifth file, a
    seed that does not reload — nothing breaks loudly. Orders keep going out, totals
    keep closing, and the mix goes back to mirroring assortment size. Before this
    phase, MARISCOS was 11,44 points from the target; inertia announces itself with
    deviations of that order, not with tenths.

    Groups that fall into the line fallback (kg coverage below the minimum) are
    EXCLUDED: their deviation does not measure calibration, it measures conversion
    coverage, and mixing them in would force the threshold to be loosened until it
    stops catching what it came to catch.
    """
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(depois)
    alvos = _targets(benchmark, params)
    pesaveis = list(alvos)
    dep = _block_shares(sd, pesaveis)

    desvios = {}
    for key in pesaveis:
        if sd.get(key, {}).get("linhas_sem_kg", 0):
            continue
        alvo, obtido = alvos[key], dep.get(key)
        if alvo is None or obtido is None:
            continue
        desvios[key] = abs(obtido - alvo)
    if not desvios:
        return {"grupos": 0, "medio": None, "pior": None, "pior_grupo": None}
    pior_grupo = max(desvios, key=desvios.get)
    return {
        "grupos": len(desvios),
        "medio": sum(desvios.values()) / len(desvios),
        "pior": desvios[pior_grupo],
        "pior_grupo": pior_grupo,
        "excluidos": sorted(
            k for k in pesaveis if sd.get(k, {}).get("linhas_sem_kg", 0)
        ),
    }


def write(markdown: str, path: str) -> str:
    if path == "-":
        print(markdown, end="")
        return path
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(markdown)
    return path
