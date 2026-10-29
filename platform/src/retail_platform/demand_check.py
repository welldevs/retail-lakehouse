"""Reality check of demand: BEFORE · MAPA · AFTER, across the three dimensions.

WHY THREE DIMENSIONS, AND NOT JUST REVENUE
-----------------------------------------
Revenue alone is the worst indicator of realism that exists for this problem, because it
mixes two things that need to be judged separately: HOW MUCH is bought and HOW MUCH what
is bought COSTS. The case that opened this phase is exactly that — seafood showed up with
22.8% of revenue and 3.4% of units, and the correct reading ("the price on 10 lines is
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
simulator charged at 1,084.05 would show up with 0.15 kg instead of the 99 kg that price
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

# A single query for both sides. The join carries (wh, price_as_of, product, category,
# subgroup) — the same composite key the generator used to pick the line, and not just the
# product id: a product lives in more than one category, and joining on id alone would
# multiply rows.
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
        -- CONTENT IMPLIED BY THE LINE'S PRICE. Null when reference_format is neither mass
        -- nor volume: converting eggs to kg would need a weight per egg that no source in
        -- this repo measures, and a number invented here would contaminate the EUR/kg
        -- denominator.
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


# THE MIX PER COHORT, the only dimension in which Phase 5 shows itself. The aggregate does
# not move on purpose — the IPF's acceptance criterion — so a report showing only totals
# would conclude the phase did nothing.
#
# `demand_group` is stamped on the LINE and `buyer_age_band` on the ORDER, both from inside
# the event itself. No join with the catalog or the customer registry: what we want is what
# held at the time of the order, and that is exactly what the two stamps keep.
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

# How many orders each warehouse placed. This is where the regional FREQUENCY tilt shows:
# before this phase the four warehouses had the same count by construction.
WAREHOUSE_SQL = """
select wh, count(*) as pedidos, count(distinct customer_id) as clientes
from silver_order
group by 1
order by 1
"""

# THE POPULATION EACH WAREHOUSE SERVES, and the window in days. These are the two numbers
# missing to close the channel: the model produced so much volume; the report says how much
# a population of that size consumes per year; the rate says what fraction of that is
# online. Without measuring them here they would have to be written by hand on this page,
# which is what the page exists to avoid.
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
    result = connection.execute(MIX_SQL)
    columns = [d[0] for d in result.description]
    triples = [dict(zip(columns, values)) for values in result.fetchall()]
    if not triples:
        raise DemandCheckError(
            "no order line to measure. Was `silver_order_line` built?"
        )

    mapping = demand_profile.load_mapping(seeds_dir)
    groups: dict[str, dict] = {}
    for row in triples:
        key = demand_profile.resolve(mapping, row["l1"], row["l2"], row["l3"])
        bucket = groups.setdefault(
            key,
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0"),
             "kg_l": Decimal("0"), "linhas_sem_kg": 0},
        )
        bucket["linhas"] += int(row["linhas"])
        bucket["unidades"] += _decimal(row["unidades"])
        bucket["receita"] += _decimal(row["receita"])
        bucket["kg_l"] += _decimal(row["kg_l"])
        bucket["linhas_sem_kg"] += int(row["linhas_sem_kg"])

    return {
        "measured_at_utc": _utc_now(),
        "groups": {
            key: {
                "linhas": value["linhas"],
                "unidades": str(value["unidades"]),
                "receita": str(value["receita"]),
                "kg_l": str(value["kg_l"]),
                "linhas_sem_kg": value["linhas_sem_kg"],
            }
            for key, value in sorted(groups.items())
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
        result = connection.execute(CHANNEL_SQL)
    except Exception:  # noqa: BLE001 - model absent in a window older than this phase
        return {"channel": None}
    columns = [d[0] for d in result.description]
    lines = [dict(zip(columns, values)) for values in result.fetchall()]
    if not lines:
        return {"channel": None}
    return {
        "channel": {
            "dias": int(lines[0]["dias"]),
            "primeiro_dia": lines[0]["primeiro"],
            "ultimo_dia": lines[0]["ultimo"],
            "warehouses": [
                {
                    "wh": row["wh"],
                    "province_code": row["province_code"],
                    "population_total": int(row["population_total"]),
                }
                for row in lines
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
        result = connection.execute(COHORT_SQL)
    except Exception:  # noqa: BLE001 - column absent in a window older than this phase
        return {"cohorts": None, "warehouses": None}

    columns = [d[0] for d in result.description]
    lines = [dict(zip(columns, values)) for values in result.fetchall()]
    cohorts: dict[str, dict] = {}
    for row in lines:
        band = row["buyer_age_band"]
        if band is None:
            continue
        bucket = cohorts.setdefault(band, {})
        group = bucket.setdefault(
            row["demand_group"],
            {"linhas": 0, "unidades": Decimal("0"), "receita": Decimal("0")},
        )
        group["linhas"] += int(row["linhas"])
        group["unidades"] += _decimal(row["unidades"])
        group["receita"] += _decimal(row["receita"])

    if not cohorts:
        return {"cohorts": None, "warehouses": None}

    result = connection.execute(WAREHOUSE_SQL)
    columns = [d[0] for d in result.description]
    warehouses = {
        row["wh"]: {"pedidos": int(row["pedidos"]), "clientes": int(row["clientes"])}
        for row in (dict(zip(columns, values)) for values in result.fetchall())
    }

    return {
        "cohorts": {
            band: {
                group: {
                    "linhas": value["linhas"],
                    "unidades": str(value["unidades"]),
                    "receita": str(value["receita"]),
                }
                for group, value in sorted(groups.items())
            }
            for band, groups in sorted(cohorts.items())
        },
        "warehouses": warehouses,
    }


def _shares(snapshot: dict) -> dict:
    """Percentage shares per group, across the three dimensions, plus EUR/kg."""
    groups = snapshot["groups"]
    total_un = sum(_decimal(g["unidades"]) for g in groups.values())
    total_kg = sum(_decimal(g["kg_l"]) for g in groups.values())
    total_eur = sum(_decimal(g["receita"]) for g in groups.values())
    out = {}
    for key, g in groups.items():
        un, kg, eur = _decimal(g["unidades"]), _decimal(g["kg_l"]), _decimal(g["receita"])
        out[key] = {
            "unidades": un,
            "kg_l": kg,
            "receita": eur,
            "pct_unidades": (un / total_un * 100) if total_un else None,
            "pct_kg": (kg / total_kg * 100) if total_kg else None,
            "pct_receita": (eur / total_eur * 100) if total_eur else None,
            "eur_por_kg": (eur / kg) if kg else None,
            "linhas_sem_kg": g["linhas_sem_kg"],
        }
    out["__total__"] = {
        "unidades": total_un, "kg_l": total_kg, "receita": total_eur,
        "eur_por_kg": (total_eur / total_kg) if total_kg else None,
    }
    return out


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


def _fmt(value, places: int = 2) -> str:
    if value is None:
        return "—"
    return f"{Decimal(value):,.{places}f}"


def _targets(benchmark: dict, params: dict) -> dict:
    """Volume target per group: MAPA's share tilted by channel, renormalized.

    THIS IS THE COLUMN THE MODEL MUST BE JUDGED AGAINST, and not MAPA's raw share.
    The report measures domestic consumption; the simulator represents e-commerce, and
    e-commerce does NOT consume in the same proportion — 1.1% of fresh volume against
    2.8% of the rest, over an average of 2.2%. Comparing the simulator to the raw share
    would flag as an error precisely the channel correction this entire phase exists to
    apply: fresh fruit MUST show up below the domestic 14.13% in an online basket.
    """
    weighable = [k for k, v in benchmark.items() if v["use_as_weight"]]
    raw = {}
    for key in weighable:
        tilt, _base = demand_profile.channel_tilt(benchmark[key], params)
        raw[key] = benchmark[key]["volume_share_pct"] * tilt
    total = sum(raw.values())
    return {k: (v / total * 100 if total else None) for k, v in raw.items()}


def _block_shares(shares: dict, keys: list) -> dict:
    """Renormalizes shares within a subset of groups.

    Without this, AFTER would be over the total (including non-food) and the target over
    the calibrated block: two different bases in the same table, which is how you end up
    comparing wrong without noticing.
    """
    total = sum(Decimal(shares[k]["kg_l"]) for k in keys if k in shares)
    return {
        k: (Decimal(shares[k]["kg_l"]) / total * 100 if total else None)
        for k in keys if k in shares
    }


def render(
    after: dict,
    before: dict | None,
    seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR,
    antes_nome: str | None = None,
) -> str:
    """Markdown for the reality check. No number written by hand."""
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(after)
    sa = _shares(before) if before else None
    targets = _targets(benchmark, params)
    weighable = sorted(targets, key=lambda k: -(targets[k] or 0))

    after_block = _block_shares(sd, weighable)
    before_block = _block_shares(sa, weighable) if sa else {}

    lines: list[str] = []
    lines.append("# Reality check of synthetic demand")
    lines.append("")
    lines.append(
        "GENERATED by `make demand-reality-check`. No number on this page was written by "
        "hand; regenerate it instead of editing it."
    )
    lines.append("")
    lines.append(f"- demand model: **{params['demand_model_version']}**")
    lines.append(f"- benchmark: MAPA, Informe del Consumo Alimentario en Espana 2025")
    lines.append(f"- measured at: {after['measured_at_utc']}")
    if before:
        label = f" (`{antes_nome}`)" if antes_nome else ""
        lines.append(
            f"- BEFORE frozen at: {before['measured_at_utc']}{label} — the state "
            f"**immediately** prior to this model version, not the oldest one that "
            f"exists. `before_mapa_2025_v1` keeps the uniform mix from before the "
            f"aggregate calibration and stays on disk; mixing the two into one column "
            f"would just make the effects of two phases get read as one."
        )
    else:
        lines.append(
            "- BEFORE: **absent**. Without it this page shows only the current state "
            "against MAPA, and the model-effect column does not exist."
        )
    lines.append("")
    lines.append("## How to read the columns")
    lines.append("")
    lines.append(
        "**MAPA %** is gross domestic consumption. **TARGET %** is the same number "
        "tilted by e-commerce participation and renormalized — it is against this that "
        "the model must be judged. The two differ on purpose: the report measures what "
        "the resident consumes, and the online basket does not have the same "
        "composition (1.1% of fresh volume arrives via e-commerce against 2.8% of the "
        "rest). Fresh fruit **must** show up below the domestic 14.13% in an online store; "
        "if it showed up at 14.13% the model would be confusing total consumption with "
        "channel."
    )
    lines.append("")
    lines.append(
        "Revenue alone does not work as an indicator of realism: it mixes **how much** is "
        "bought with **how much it costs**. That is how 10 catalog lines with an API price "
        "ceiling accounted for 16.6% of all simulated revenue without any total closing "
        "wrong."
    )
    lines.append("")

    # --- totals ------------------------------------------------------------------
    td, ta = sd["__total__"], (sa["__total__"] if sa else None)
    lines.append("## Totals")
    lines.append("")
    lines.append("| dimension | BEFORE | AFTER | change |")
    lines.append("|---|---:|---:|---:|")
    for label, key, places in (
        ("units", "unidades", 0), ("kg or liter", "kg_l", 1),
        ("revenue (EUR)", "receita", 2), ("EUR per kg", "eur_por_kg", 2),
    ):
        current = td[key]
        previous = ta[key] if ta else None
        if previous and current is not None and previous != 0:
            change = f"{(Decimal(current) / Decimal(previous) - 1) * 100:+.1f} %"
        else:
            change = "—"
        lines.append(f"| {label} | {_fmt(previous, places)} | {_fmt(current, places)} | {change} |")
    lines.append("")
    lines.append(
        "MAPA's EUR/kg for total domestic food consumption in 2025 is **3.25**. Ours "
        "also covers the non-food third of the catalog, which the report does not "
        "measure, so the two are not directly comparable — the useful comparison is "
        "group by group, further below."
    )
    lines.append("")

    # --- blocks -------------------------------------------------------------------
    lines.append("## The three blocks")
    lines.append("")
    lines.append(
        "The food share is a declared premise; the split between calibrated and "
        "uncalibrated comes from the benchmark's own coverage, not from a new number."
    )
    lines.append("")
    lines.append("| block | declared weight | % of observed volume | weight origin |")
    lines.append("|---|---:|---:|---|")
    total_kg = td["kg_l"]
    blocks = (
        ("calibrated by MAPA", "benchmark_block", weighable,
         f"food_line_share x benchmark coverage ({params['benchmark_volume_coverage_pct']}%)"),
        ("food without benchmark", "sin_benchmark_block", [demand_profile.SIN_BENCHMARK],
         "complement, divided by assortment size"),
        ("non-food", "no_food_block", [demand_profile.NO_FOOD],
         "1 - food_line_share; outside MAPA's universe"),
    )
    food = Decimal(params["food_line_share"])
    coverage = Decimal(params["benchmark_volume_coverage_pct"]) / 100
    declared = {
        "benchmark_block": food * coverage,
        "sin_benchmark_block": food * (1 - coverage),
        "no_food_block": 1 - food,
    }
    for label, key, members, origin in blocks:
        observed = sum(Decimal(sd[k]["kg_l"]) for k in members if k in sd)
        pct = (observed / total_kg * 100) if total_kg else None
        lines.append(
            f"| {label} | {_fmt(declared[key] * 100)} % (lines) | {_fmt(pct)} % (kg) | {origin} |"
        )
    lines.append("")
    lines.append(
        "Declared weight is a share of LINES; the observed column is a share of "
        "VOLUME. They should not coincide: a pack of water weighs 3.5 kg and a spice "
        "sachet weighs 30 g."
    )
    lines.append("")

    # --- volume, within the calibrated block -----------------------------------
    lines.append("## Volume (kg or liter) — within the calibrated block")
    lines.append("")
    lines.append(
        "All columns sum to 100 over the same groups. This is the dimension MAPA "
        "publishes and where calibration acts."
    )
    lines.append("")
    lines.append("| group | BEFORE % | MAPA % | TARGET % | AFTER % | error | channel |")
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    errors = []
    for key in weighable:
        b = benchmark[key]
        _tilt, base = demand_profile.channel_tilt(b, params)
        target, observed = targets[key], after_block.get(key)
        if target is not None and observed is not None:
            error = observed - target
            errors.append(abs(error))
            error_txt = f"{error:+.2f}"
        else:
            error_txt = "—"
        lines.append(
            f"| {key} | {_fmt(before_block.get(key))} | {_fmt(b['volume_share_pct'])} "
            f"| {_fmt(target)} | {_fmt(observed)} | {error_txt} | {base} |"
        )
    lines.append("")
    if errors:
        mean = sum(errors) / len(errors)
        worst = max(errors)
        lines.append(
            f"Mean absolute error against the TARGET: **{_fmt(mean, 3)}** points; "
            f"largest deviation: **{_fmt(worst, 3)}** points."
        )
        lines.append("")
        lines.append(
            "The goal is **not** to minimize this number. It is here so that a large "
            "deviation is explainable, not to be chased — an error of zero would mean "
            "the catalog's assortment matches the Spanish basket perfectly, which "
            "would be suspicious, not good."
        )
        lines.append("")
    fallback = [
        k for k in weighable
        if sd.get(k, {}).get("linhas_sem_kg", 0) > 0
    ]
    if fallback:
        lines.append(
            "Groups with lines that have no kg conversion (their deviation against the "
            "TARGET does not measure calibration, it measures conversion coverage): "
            + ", ".join(f"`{k}` ({sd[k]['linhas_sem_kg']} lines)" for k in fallback)
        )
        lines.append("")

    # --- the other three views ---------------------------------------------------------
    # Units, revenue and EUR/kg were measured like the shares above, but do not become
    # tables: they would be three more near-identical tables that confirm the calibration
    # argument instead of adding to it. What they caught stays in prose.
    n_categories = sum(1 for k in sd if k != "__total__"
                       and k not in demand_profile.SCOPE_LABELS)
    lines.append("## Other dimensions checked the same way")
    lines.append("")
    lines.append(
        f"The same {n_categories} categories were also measured BEFORE/AFTER across three more views —\n"
        "**units** (share of lines, not comparable to MAPA since the report never counts\n"
        "pieces), **revenue** (share of value, deliberately NOT a target), and **EUR/kg**\n"
        "(the only column needing no conversion at all). They are not reproduced row by row\n"
        f"here — a fourth near-identical {n_categories}-row table would confirm the calibration argument\n"
        "above, not add to it. Two things they caught are worth keeping in prose:"
    )
    lines.append("")
    lines.append(
        "- **Volume and value are expected to diverge, not match.** In the revenue view,\n"
        "  `MARISCOS_MOLUSCOS_CRUSTACEOS` is 0.81% of volume against 2.88% of domestic\n"
        "  value — a simulator that forced the two to be equal would be wrong, not right.\n"
        "- **EUR/kg needs no unit conversion, so it was the column that caught the\n"
        "  bulk-pricing defect before calibration existed** — a healthy observed price\n"
        "  shows up there regardless of how many lines of a group go out."
    )
    lines.append("")

    # --- what the benchmark does not reach -------------------------------------------------
    lines.append("## What the benchmark does not reach")
    lines.append("")
    no_target = sorted(
        k for k, v in benchmark.items()
        if not v["use_as_weight"] and v["provenance"] in ("informe_prose", "none")
        and k not in demand_profile.SCOPE_LABELS
    )
    lines.append(
        f"- **{demand_profile.NO_FOOD}** and **{demand_profile.SIN_BENCHMARK}** have no "
        f"MAPA target and are never added to the calibrated block. The report measures "
        f"food and beverages; drugstore, cleaning, makeup and pet items are outside its "
        f"universe."
    )
    lines.append(
        f"- declared food share: **{params['food_line_share']}** — `synthetic` premise, "
        f"with no benchmark. No source in this repo measures the food composition of an "
        f"online basket, and MAPA does not measure drugstore items."
    )
    lines.append(
        f"- benchmark coverage: **{params['benchmark_volume_coverage_pct']}%** of "
        f"Spanish domestic volume, summing the weighable leaves. The complement goes "
        f"to `SIN_BENCHMARK`, divided by assortment size."
    )
    for key in no_target:
        b = benchmark[key]
        section = b["informe_section"] or "no section"
        lines.append(f"- `{key}` ({b['mapa_label']}): no published share — section {section}")
    lines.append("")
    lines.append("### Seasonality")
    lines.append("")
    lines.append(
        "**Neutral** profile across the 12 months, for lack of numeric evidence: the "
        "report's monthly charts are images, and there are only five monthly numbers "
        "in prose — all for the food **total**, never by category. Besides that, the "
        "simulator's window covers only August, so there is no monthly axis to "
        "exercise. The mechanism exists, applies to the order rate, and has a test "
        "proving that a non-neutral profile changes the output; the trigger for "
        "proposing a profile is the window covering November and December."
    )
    lines.extend(_cohort_section(after, before))
    lines.extend(_channel_section(after, seeds_dir, params))
    lines.append("")
    lines.append("## Boundary the calibration does not cross")
    lines.append("")
    lines.append(
        "MAPA measures the resident's domestic consumption. It does **not** measure "
        "online-store orders, basket, purchase cadence, or ticket per channel. That is "
        "why `daily_order_rate`, `basket_lines_min/mode/max` and `quantity_max` remain "
        "`synthetic` premises in `order_premises_seed.csv` and received **no** calibration "
        "whatsoever in this phase. Calling the benchmark a source for those numbers "
        "would turn it into a false representation of reality."
    )
    lines.append("")
    return "\n".join(lines) + "\n"


def _channel_section(after: dict, seeds_dir: str, params: dict) -> list[str]:
    """The account that only closes once the base was sized by population.

    THE CIRCULAR ARGUMENT THIS BREAKS. The customer base became 2.2% of the adults of
    the four AUFs because 2.2% of food volume goes through e-commerce (MAPA, section
    3). If that transposition — from volume share to people share — were coherent with
    the rest of the model, the volume the orders produce would also have to be 2.2% of
    the domestic consumption of those same AUFs. There is no guarantee that it is:
    `daily_order_rate` and basket size were declared in Phase 3, with no relation to
    the penetration rate, and now the three meet for the first time.

    THIS SECTION IS NOT A TARGET TO CHASE. Closing the gap by tweaking
    `daily_order_rate` would be moving a premise to fit a result — and no source in
    this repository measures purchase cadence or online basket size, so there is
    nothing to consult to decide which of the two is wrong. The number is measured,
    published, and recorded as a gap.
    """
    channel = after.get("channel")
    if not channel:
        return [
            "",
            "## Channel closure",
            "",
            "Not measured: this snapshot was frozen before the served population entered "
            "the measurement.",
        ]

    regions = demand_profile.load_region_reference(seeds_dir)
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)
    rate = Decimal(str(params["channel_reference_pct"]))
    days = Decimal(channel["dias"])

    # FOOD SCOPE, not the model's total. The report's per capita is for "alimentacion y
    # bebidas" and does NOT cover drugstore items; adding NO_FOOD on the model side would
    # compare two different universes and inflate the ratio without anything being wrong.
    # SIN_BENCHMARK STAYS: these are food groups the report simply does not detail, and they
    # belong to the same universe the per capita measures.
    kg_measured = Decimal("0")
    eur_measured = Decimal("0")
    kg_no_food = Decimal("0")
    eur_no_food = Decimal("0")
    for key, value in after["groups"].items():
        if key == demand_profile.NO_FOOD:
            kg_no_food += Decimal(value["kg_l"])
            eur_no_food += Decimal(value["receita"])
            continue
        kg_measured += Decimal(value["kg_l"])
        eur_measured += Decimal(value["receita"])

    lines = ["", "## Channel closure", ""]
    lines.append(
        f"The customer base was sized as **{rate}% of the adults** of the four AUFs, "
        f"because {rate}% of food volume goes through e-commerce. The question this "
        f"section answers is whether the model, after that, produces {rate}% of the "
        f"consumption of those same AUFs — or whether the penetration rate and the "
        f"order cadence, declared in different phases with no relation to each other, "
        f"contradict one another."
    )
    lines.append("")
    lines.append(
        f"Measured window: **{channel['dias']} day(s)**, from {channel['primeiro_dia']} to "
        f"{channel['ultimo_dia']}. The report's consumption is annual and is divided by 365."
    )
    lines.append("")

    lines.append("| warehouse | population served | community | kg-L/capita/yr | EUR/capita/yr |")
    lines.append("|---|---:|---|---:|---:|")
    kg_target = Decimal("0")
    eur_target = Decimal("0")
    population_total = 0
    for entry in channel["warehouses"]:
        ccaa = demand_profile.ccaa_of(province_ccaa, entry["province_code"])
        region = regions[ccaa]
        population = Decimal(entry["population_total"])
        population_total += entry["population_total"]
        kg_target += population * region["per_capita_kg_l"]
        eur_target += population * (region["per_capita_eur"] or Decimal("0"))
        lines.append(
            f"| {entry['wh']} | {entry['population_total']:,} | {region['ccaa_label']} "
            f"| {region['per_capita_kg_l']} | {region['per_capita_eur']} |"
        )
    lines.append(f"| **total** | **{population_total:,}** | | | |")
    lines.append("")

    # From the population's annual consumption to the online slice of the measured window.
    factor = rate / Decimal("100") * days / Decimal("365")
    kg_target_window = kg_target * factor
    eur_target_window = eur_target * factor

    lines.append("| dimension | expected channel | model | ratio |")
    lines.append("|---|---:|---:|---:|")
    for label, measured, target in (
        ("kg or liter", kg_measured, kg_target_window),
        ("revenue (EUR)", eur_measured, eur_target_window),
    ):
        if measured is None or not target:
            lines.append(f"| {label} | — | — | — |")
            continue
        ratio = measured / target
        lines.append(
            f"| {label} | {_fmt(target, 0)} | {_fmt(measured, 0)} | **{_fmt(ratio, 2)}x** |"
        )
    lines.append("")
    lines.append(
        f"The scope on both sides is **food**. `{demand_profile.NO_FOOD}` is left out of "
        f"the numerator — {_fmt(kg_no_food, 0)} kg-L and {_fmt(eur_no_food, 2)} EUR in "
        f"the window — because the report's per capita is for food and beverages and "
        f"does not cover drugstore items. Adding it would compare two different "
        f"universes and inflate the ratio without anything being wrong. "
        f"`SIN_BENCHMARK` **stays**: these are food groups the report does not detail, "
        f"but that belong to the same universe the per capita measures."
    )
    lines.append("")
    lines.append(
        "**None of these numbers was adjusted to get closer to the other, and that is "
        "what makes them interesting.** The penetration rate entered in Phase 6, "
        "anchored on the report. `daily_order_rate`, `basket_lines_*` and "
        "`quantity_max` entered in Phase 3, chosen with no relation to it and with no "
        "source that measured them. The two halves only meet in this table, and the "
        "rest between them is what you see above."
    )
    lines.append("")
    lines.append(
        "The distance that remains must **not** be closed by tweaking `daily_order_rate` "
        "until the ratio becomes 1.00: that would make a premise fit a result without "
        "anything having been measured, and no source in this repository measures "
        "domestic purchase cadence or online basket — there is no criterion to decide "
        "which side is wrong. As long as that is so, this ratio is an **observation**, not "
        "a target. **Trigger:** a source that measures domestic purchase frequency or "
        "average ticket per channel turns this line into a test."
    )
    lines.append("")
    lines.append(
        "A caveat about the denominator, for the same reason it already exists for the "
        "cohort: the report's per capita consumption is for the community's **entire** "
        "population, children included, while the customers are adults. That is "
        "correct here — a household's consumption shows up in the per capita of all "
        "its members — but it means the ratio above cannot be read as 'each customer "
        "buys X% of what they should'."
    )
    return lines


def _cohort_section(after: dict, before: dict | None) -> list[str]:
    """The only dimension in which the cohort layer shows itself.

    THE AGGREGATE DOES NOT MOVE ON PURPOSE — that is the IPF's acceptance criterion —
    so a page that only showed totals would conclude the phase did nothing. This
    section exists because the effect is entirely conditional, and something that
    only shows up in the conditional needs a place of its own to be seen.
    """
    cohorts = after.get("cohorts")
    lines: list[str] = ["", "## Propensity by buyer cohort", ""]

    if not cohorts:
        lines.append(
            "**Not present in this window.** `silver_order.buyer_age_band` is not "
            "populated — the window was generated by a model prior to the cohort "
            "layer. This is not a measurement of zero, it is the absence of the stamp."
        )
        return lines

    lines.append(
        "The cohort has two dimensions, and they are the two **only** ones in which an "
        "observed customer attribute coincides with a cut published by the report: "
        "**age** (`birth_year`, from the INE's provincial distribution) and "
        "**autonomous community** (`province_code`, from the Callejero). Household "
        "life cycle and socioeconomic level are MAPA's richest cuts and were left out: "
        "the customer has no household composition or income, and assigning them "
        "would mean inventing the attribute."
    )
    lines.append("")
    lines.append(
        "**The aggregate does not move, and that is the acceptance criterion.** The "
        "per-cohort weights, weighted by the real distribution of cohorts among "
        "orders, reproduce the weights of the aggregate calibration — that is what "
        "the IPF exists for. Anyone who looks for this layer's effect in a total will "
        "not find it: it lives entirely in the columns below."
    )

    bands = [b for b in ("LT35", "35_49", "50_64", "GE65") if b in cohorts]
    totals = {
        b: sum(int(v["linhas"]) for v in cohorts[b].values()) for b in bands
    }

    lines.append("")
    lines.append("### Each group's slice WITHIN the cohort (% of lines)")
    lines.append("")
    lines.append(
        "The denominator is the cohort itself, not the total: that is how the "
        "comparison between bands isolates propensity from cohort size. The `x` "
        "column is the ratio between the oldest and the youngest band — the summary "
        "of an entire row."
    )
    lines.append("")
    header = "| group | " + " | ".join(f"{b} %" for b in bands) + " | x GE65/LT35 |"
    lines.append(header)
    lines.append("|---|" + "---:|" * (len(bands) + 1))

    groups = sorted({g for b in bands for g in cohorts[b]})
    ranked = []
    for group in groups:
        slices = {}
        for b in bands:
            total = totals[b] or 1
            slices[b] = Decimal(cohorts[b].get(group, {}).get("linhas", 0)) * 100 / total
        ratio = None
        if "GE65" in slices and "LT35" in slices and slices["LT35"] > 0:
            ratio = slices["GE65"] / slices["LT35"]
        ranked.append((ratio if ratio is not None else Decimal("0"), group, slices))
    ranked.sort(reverse=True)

    for ratio, group, slices in ranked:
        cells = " | ".join(_fmt(slices[b]) for b in bands)
        lines.append(f"| {group} | {cells} | {_fmt(ratio)} |")

    lines.append("")
    lines.append(
        "A ratio of 1.00 means the age band does not move that group. `NO_FOOD` and "
        "`SIN_BENCHMARK` stay close to 1 by construction: the report does not measure "
        "them, their index is neutral, and each **block's** slice is kept constant across "
        "cohorts on purpose. Without that boundary they would absorb the "
        "normalization's residue and the model would end up claiming that elderly "
        "people buy 40% less drugstore items — a number no source in this repo "
        "measures, and bigger than most of the effects that are measured."
    )

    warehouses = after.get("warehouses") or {}
    if warehouses:
        lines.append("")
        lines.append("### Frequency by autonomous community")
        lines.append("")
        lines.append(
            "Before this phase the four warehouses had the **same** order count by "
            "construction. The report measures per capita consumption by community, "
            "and that difference now applies — as frequency, never as basket size, "
            "because the report gives kg per year and does not publish domestic "
            "purchase frequency."
        )
        lines.append("")
        lines.append("| warehouse | orders | distinct customers | orders per customer |")
        lines.append("|---|---:|---:|---:|")
        for wh in sorted(warehouses):
            counts = warehouses[wh]
            per_customer = (
                Decimal(counts["pedidos"]) / Decimal(counts["clientes"])
                if counts["clientes"] else Decimal("0")
            )
            lines.append(
                f"| {wh} | {counts['pedidos']} | {counts['clientes']} | {_fmt(per_customer)} |"
            )
        before_wh = (before or {}).get("warehouses")
        if before_wh:
            lines.append("")
            lines.append(
                "The frozen BEFORE has the count per warehouse alongside; the "
                "difference between them is the regional tilt taking effect."
            )
        else:
            lines.append("")
            lines.append(
                "The frozen BEFORE does not record a count per warehouse: it predates "
                "this measurement existing. The useful comparison is among today's "
                "four warehouses, which used to be equal by construction."
            )

    return lines


def calibration_error(after: dict, seeds_dir: str = demand_profile.DEFAULT_SEEDS_DIR) -> dict:
    """Error of the observed mix against the TARGET, ignoring groups without kg coverage.

    THIS IS NOT A GRADE. The declared objective was never to minimize this distance —
    an error of zero would mean Mercadona's assortment matches the Spanish basket
    perfectly, which would be suspicious and not good. The number exists to catch ONE
    thing: a silently inert calibration.

    And the failure mode is concrete. If the profile stops being applied — a
    `demand.pick` that reverts to uniform, an export that forgets the fifth file, a
    seed that does not reload — nothing breaks loudly. Orders keep going out, totals
    keep closing, and the mix goes back to mirroring assortment size. Before this
    phase, MARISCOS was 11.44 points from the target; inertia announces itself with
    deviations of that order, not with tenths.

    Groups that fall into the line fallback (kg coverage below the minimum) are
    EXCLUDED: their deviation does not measure calibration, it measures conversion
    coverage, and mixing them in would force the threshold to be loosened until it
    stops catching what it came to catch.
    """
    benchmark = demand_profile.load_benchmark(seeds_dir)
    params = demand_profile.load_params(seeds_dir)
    sd = _shares(after)
    targets = _targets(benchmark, params)
    weighable = list(targets)
    after_shares = _block_shares(sd, weighable)

    deviations = {}
    for key in weighable:
        if sd.get(key, {}).get("linhas_sem_kg", 0):
            continue
        target, observed = targets[key], after_shares.get(key)
        if target is None or observed is None:
            continue
        deviations[key] = abs(observed - target)
    if not deviations:
        return {"grupos": 0, "medio": None, "pior": None, "pior_grupo": None}
    worst_group = max(deviations, key=deviations.get)
    return {
        "grupos": len(deviations),
        "medio": sum(deviations.values()) / len(deviations),
        "pior": deviations[worst_group],
        "pior_grupo": worst_group,
        "excluidos": sorted(
            k for k in weighable if sd.get(k, {}).get("linhas_sem_kg", 0)
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
