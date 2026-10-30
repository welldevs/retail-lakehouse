"""Exports, from Silver, the flat references that the simulated Orders Source consumes.

WHY THIS MODULE EXISTS, AND WHY IT LIVES IN THE PLATFORM
--------------------------------------------------------
Same mechanism as `customers_reference.py`, one level further down the chain.
`simulated-orders-source` needs REAL customer, product and price so that the order is the only
invented thing — but every Source in this repo is FROZEN (`dependencies = []`, checked by AST)
and `duckdb`/`boto3` are platform-only dependencies by design. The PLATFORM queries Silver
and writes four flat JSON files; the Source's `extract` reads them with the stdlib `json`.
Neither side imports the other's code.

THE GOLDEN RULE, APPLIED TO ORDERS
----------------------------------
The order is invented; who buys, what is bought, how much it costs and where they live are
not. Customer comes from `silver_customer`, product and price come from `silver_product_price`
of the SAME warehouse on the SAME date. Nothing here fabricates product, price or customer.

WHERE EACH THING COMES FROM (and what was measured before writing this)
----------------------------------------------------------------------
  * `silver_product_price` has MORE ROWS than distinct products per partition (e.g.: 4,581
    rows for 4,311 products in mad1/2026-08-24): a product appears in more than one
    category, and that is source semantics, not a defect. The dedup by
    `(warehouse, ingestion_date, source_product_id)` is SAFE because the price is identical
    across appearances — a guarantee that already has its own test in the repo
    (`assert_price_is_consistent_across_appearances`). The chosen category is the smallest
    `(category_id, subgroup_id)`: a deterministic TIE-BREAK rule, not a business one.
  * `unit_price` travels as a STRING, never float. It is obligation 4.4 of the Mercadona
    contract ("converting to float loses decimal precision in currency"), and the generator
    does the arithmetic with `decimal.Decimal`, which is stdlib and so does not break the
    FROZEN boundary.
  * PRODUCT CHOICE IS UNIFORM, on purpose, and that is not a "realistic basket". No source
    in this repo measures sales, turnover or baskets. Weighting products would invent a
    distribution nobody measured — the same prohibition Phase 1 applied to the tramo choice.
    The declared consequence: the mix per category mirrors the SIZE of the assortment, and
    that is a consequence of a premise, not a claim about the market.
  * THE WINDOW IS DERIVED, NEVER ASSUMED. For each `(wh, order_date)` the price comes from
    that warehouse's latest catalog snapshot on a date <= order_date. Equal: `price_source =
    'observed'`. Earlier: `'carried_forward'`, explicit. Measured on disk on 2026-08-28: the
    4 warehouses have catalog from 08-24 to 08-27; mad1 also has 08-15/08-16, with a gap from
    08-17 to 08-23. A retailer sells every day; the source was only observed on some.
  * NO ORDER BEFORE THE CUSTOMER EXISTS. `first_ingestion_date` per customer is exported so
    the generator can refuse. The base is append-only (growing preserves the first N
    byte for byte), so a customer present on one date is present on all later ones.
  * The PREMISES are read from the seed CSV, not from the Lakehouse, for the same reason
    already measured in `customers_reference.py`: dbt seeds have no `location =` and never
    become parquet under `silver/`, which is all `connect_lakehouse()` sees.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone

from . import demand_profile, product_choice

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

# The only label allowed in the premises table. There is no `observed` or `proxy` premise
# here: no source in this repo measures baskets, cadence or availability. A different
# label is an export error, not a warning.
PREMISE_LABEL = "synthetic"

# Keys the generator requires. A truncated or renamed seed must fail the export, never
# produce orders with a default hidden in the generator.
REQUIRED_PREMISES = (
    "daily_order_rate",
    "min_buyer_age",
    "basket_lines_min",
    "basket_lines_mode",
    "basket_lines_max",
    "min_order_amount_eur",
    "quantity_max",
    "quantity_max_multi",
    "quantity_multi_price_eur",
    "quantity_max_mid",
    "quantity_mid_price_eur",
    "quantity_max_high",
    "quantity_high_price_eur",
    "own_brand_weight",
    "substitution_rate_fresh",
    "substitution_rate_other",
    "removal_rate_fresh",
    "removal_rate_other",
    "payment_failure_rate",
    "cancellation_rate",
    "delivery_failure_rate",
    "return_rate",
    "cadence_weight_7d",
    "cadence_weight_14d",
    "cadence_weight_21d",
    "cadence_weight_28d",
    "order_skip_rate",
    *(f"order_weekday_weight_{day}" for day in range(1, 8)),
    *(f"order_hour_weight_{hour:02d}" for hour in range(24)),
    "delivery_closed_isoweekday",
    "delivery_open_hour",
    "delivery_close_hour",
    "slot_hours",
    "slot_lead_day_weight_1",
    "slot_lead_day_weight_2",
    "slot_lead_day_weight_3",
    *(f"slot_hour_weight_{hour:02d}" for hour in range(7, 22)),
    "delivery_on_time_rate",
    "delivery_early_rate",
    "delivery_early_max_minutes",
    "delivery_late_max_minutes",
    "minutes_dispatch_before_slot_min",
    "minutes_dispatch_before_slot_max",
    "minutes_picked_before_dispatch_min",
    "minutes_picked_before_dispatch_max",
    "minutes_to_payment_min",
    "minutes_to_payment_max",
    "minutes_to_cancel_min",
    "minutes_to_cancel_max",
    "minutes_per_line_picked",
    "minutes_to_return_min",
    "minutes_to_return_max",
    "sla_minutes_picking",
)


class OrdersReferenceError(Exception):
    """Silver does not have what this export needs, or coverage regressed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sql_literal(value: str) -> str:
    """SQL literal. A single quote would break the query silently: better to refuse."""
    if "'" in value:
        raise OrdersReferenceError(f"value with a single quote is not supported in SQL: {value}")
    return f"'{value}'"


def _seed(seeds_dir: str, name: str) -> str:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise OrdersReferenceError(
            f"seed not found: {path}. Run from the repo root, or pass "
            f"--seeds-dir pointing to the dbt seeds directory."
        )
    return path


def _rows(connection, sql: str) -> list[dict]:
    """Executes and returns rows as dictionaries, in the order the SQL determined."""
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _write_json(path: str, payload, indent: int | None) -> int:
    """Writes JSON atomically (temporary file in the same directory + os.replace)."""
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
        raise OrdersReferenceError(f"invalid date: {value!r}. Expected format YYYY-MM-DD.") from exc


def _date_range(window_from: str, window_to: str) -> list[str]:
    start, end = _parse_date(window_from), _parse_date(window_to)
    if end < start:
        raise OrdersReferenceError(f"inverted window: --from {window_from} > --to {window_to}")
    days = (end - start).days + 1
    return [(start + timedelta(days=offset)).isoformat() for offset in range(days)]


def _warehouses(seeds_dir: str) -> list[str]:
    """Warehouses in scope. They come from the seed, not from whatever happens to have a catalog.

    The warehouse->province link is a decision of THIS platform, not a property of INE or
    Mercadona — already recorded in ARCHITECTURE.md. Deriving the list from the available data
    would make a warehouse without a catalog disappear silently instead of failing.
    """
    path = _seed(seeds_dir, PROVINCE_MAP_SEED)
    with open(path, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise OrdersReferenceError(f"{path}: empty seed")
    if "wh" not in rows[0]:
        raise OrdersReferenceError(f"{path}: seed without the 'wh' column")
    return sorted({row["wh"] for row in rows})


# --------------------------------------------------------------------------------
# 1. Declared premises
# --------------------------------------------------------------------------------

def _build_premises(seeds_dir: str) -> dict:
    """Reads the premises table from the CSV and labels it with its own sha256.

    The digest travels all the way to the partition manifest. Without it, changing a rate and
    regenerating would produce different orders under the same seed without leaving a trace —
    the same class of problem Phase 1 solved by keeping each run's seed in `history`.
    """
    path = _seed(seeds_dir, PREMISES_SEED)
    with open(path, "rb") as handle:
        blob = handle.read()
    digest = hashlib.sha256(blob).hexdigest()

    rows = list(csv.DictReader(blob.decode("utf-8").splitlines()))
    if not rows:
        raise OrdersReferenceError(f"{path}: empty premises seed")

    values: dict[str, str] = {}
    for position, row in enumerate(rows):
        key = (row.get("premise_key") or "").strip()
        if not key:
            raise OrdersReferenceError(f"{path}: row {position} without premise_key")
        if key in values:
            raise OrdersReferenceError(f"{path}: duplicated premise_key: {key!r}")
        label = (row.get("label") or "").strip()
        if label != PREMISE_LABEL:
            raise OrdersReferenceError(
                f"{path}: premise {key!r} labeled {label!r}. The only accepted label is "
                f"{PREMISE_LABEL!r}: no source in this repo measures baskets, cadence or "
                f"availability, so calling any of these 'observed' or 'proxy' would "
                f"promise data that does not exist."
            )
        try:
            float(row.get("value"))
        except (TypeError, ValueError) as exc:
            raise OrdersReferenceError(
                f"{path}: premise {key!r} with a non-numeric value: {row.get('value')!r}"
            ) from exc
        values[key] = str(row["value"]).strip()

    missing = sorted(set(REQUIRED_PREMISES) - set(values))
    if missing:
        raise OrdersReferenceError(
            f"{path}: required premise(s) missing: {missing}. The generator has no "
            f"default for any of them, on purpose."
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
# 2. Price calendar: which snapshot backs each (wh, day)
# --------------------------------------------------------------------------------

def _calendar_sql(warehouses: list[str], days: list[str]) -> str:
    wh_values = ", ".join(f"({_sql_literal(w)})" for w in warehouses)
    day_values = ", ".join(f"(date {_sql_literal(d)})" for d in days)
    return f"""
    with wh_list(wh) as (values {wh_values}),
    day_list(order_date) as (values {day_values}),
    catalog_dates as (
        select distinct warehouse as wh, ingestion_date
        from silver_product_price
    ),
    grid as (
        select w.wh, d.order_date from wh_list w cross join day_list d
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

    without_price = [(r["wh"], r["order_date"]) for r in rows if r["price_as_of"] is None]
    if without_price:
        raise OrdersReferenceError(
            f"{len(without_price)} (warehouse, day) pair(s) without ANY catalog snapshot on an "
            f"earlier or equal date: {without_price[:10]}. An order on that day would have to "
            f"invent a price. Narrow the window with --from, or extract that warehouse's catalog."
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
# 3. Catalog: real product and price, per warehouse and per snapshot
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
            -- THE PRICE THE GENERATOR USES. It differs from unit_price only on `bunch` rows,
            -- where the source returns reference_price * 99 — the ceiling of the weight
            -- selector, not the price of anything a household buys (obligation 5 of the Mercadona contract).
            purchasable_unit_price,
            price_basis,
            net_content_kg_l,
            tax_percentage,
            -- GRAIN dedup, not a business rule: a product appears in more than one
            -- category (source semantics). The price is identical across appearances, and that
            -- already has its own test in the repo. Picking the smallest (category_id, subgroup_id)
            -- is a deterministic tie-break.
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
        -- String, never float: obligation 4.4 of the Mercadona contract. The generator does
        -- the arithmetic with decimal.Decimal, which is stdlib.
        --
        -- `unit_price` HERE IS THE PRICE OF THE PURCHASABLE PORTION. The raw value the API
        -- returned travels alongside in `source_unit_price`, so the difference is auditable
        -- instead of living only in Silver: on the 10 bulk rows it is three orders of magnitude.
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
    """Every (level 1, category, subgroup) triple of the tree, BEFORE the dedup.

    MEASURED, and the reason this query exists: the catalog cut keeps ONE row per
    (warehouse, date, product), tie-broken by the smallest (category_id, subgroup_id). A
    product living in `Congelados > Carne` and also in `Carne > Carne congelada` disappears
    from the first after the dedup — and three perfectly correct mapping rules looked
    dead. Checking coverage against the cut would measure the tie-break, not the mapping.
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
    """Stamps `demand_group` on every catalog row and returns the count per group.

    THE RESOLUTION HAPPENS HERE, IN THE PLATFORM, and not in the generator. It is deliberate:
    the Source is FROZEN and cannot carry the mapping table or the category tree; and, more
    importantly, the cut that crosses the boundary stays DUMB — an already resolved column,
    with no business rule on the other side.
    """
    mapping = demand_profile.load_mapping(seeds_dir)
    counts: dict[str, int] = {}
    cache: dict[tuple, str] = {}
    for row in rows:
        key = (row["l1"], row["category_name"], row["subgroup_name"])
        group = cache.get(key)
        if group is None:
            group = demand_profile.resolve(mapping, *key)
            cache[key] = group
        row["demand_group"] = group
        counts[group] = counts.get(group, 0) + 1

    # Coverage against the TREE, not against the cut. Every triple needs a rule even if
    # the dedup hides it today: the tie-break may change when the source reorders a
    # product's categories, and then the hidden triple becomes the chosen one.
    tree = [
        (row["l1"], row["l2"], row["l3"])
        for row in _rows(connection, _tree_triples_sql(price_dates))
    ]
    for l1, l2, l3 in tree:
        demand_profile.resolve(mapping, l1, l2, l3)

    dead = demand_profile.unused_rules(mapping, tree)
    if dead:
        raise OrdersReferenceError(
            f"{len(dead)} rule(s) of {demand_profile.MAPPING_SEED} match no triple "
            f"of the category tree: "
            f"{[(m['l1'], m['l2'], m['l3']) for m in dead[:5]]}. A dead rule documents a "
            f"decision that is not in force, and whoever reads the seed will believe it."
        )
    return counts


def _build_catalog(connection, price_dates: list[str], seeds_dir: str) -> dict:
    rows = _rows(connection, _catalog_sql(price_dates))
    if not rows:
        raise OrdersReferenceError(
            f"no catalog rows for dates {price_dates}. "
            f"Has `silver_product_price` been built?"
        )
    groups = _stamp_demand_group(connection, rows, price_dates, seeds_dir)
    return {
        "demand_groups": dict(sorted(groups.items())),
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
# 4. Customers: who can order, from which warehouse, starting when
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
    -- `birth_year` and not the age band: the band depends on the ORDER DAY, which only the
    -- generator knows. Stamping it here would freeze the customer's age at the export date,
    -- and a birthday inside the window would go unnoticed.
    v.birth_year,
    cast(f.first_ingestion_date as varchar) as first_ingestion_date
from versions v
join first_seen f on v.customer_id = f.customer_id
where v.rn = 1
order by v.wh, v.customer_id
"""

# A customer that shows up with two warehouses would break the central constraint ("a customer
# of W only orders from W") without any downstream test noticing: the order would be coherent
# with one of the two rows. Re-checked here, and the export REFUSES instead of choosing.
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
            f"customer(s) with more than one warehouse in silver_customer: {drift}. The "
            f"constraint 'a customer of W only orders from W' would no longer be verifiable."
        )

    rows = _rows(connection, _CUSTOMERS_SQL)
    if not rows:
        raise OrdersReferenceError(
            "silver_customer is empty. Run `make oltp-refresh-all` before this export."
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
# 5. Customer cohort: the bridge between an observed attribute and a MAPA cut
# --------------------------------------------------------------------------------

def _warehouse_regions(seeds_dir: str) -> dict:
    """warehouse -> autonomous community, via the province that was already declared.

    Two chained seeds and not one: `warehouse_province_map_seed` is a decision of THIS
    platform (where the warehouses are), and `ine_ccaa_map_seed` is INE administrative
    geography (which community a province belongs to). Merging the two into a single file
    would make a decision of ours look like an official fact.
    """
    path = _seed(seeds_dir, PROVINCE_MAP_SEED)
    with open(path, "r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)

    regions: dict[str, str] = {}
    for row in rows:
        wh = (row.get("wh") or "").strip()
        ccaa = demand_profile.ccaa_of(province_ccaa, row.get("province_code"))
        previous = regions.setdefault(wh, ccaa)
        if previous != ccaa:
            raise OrdersReferenceError(
                f"warehouse {wh!r} appears in two communities ({previous}, {ccaa}). The "
                f"regional tilt would no longer be defined for it."
            )
    return regions


def _cohort_counts(
    customer_rows: list[dict],
    seeds_dir: str,
    min_buyer_age: int,
    reference_date: str,
) -> tuple[dict, dict]:
    """(band, ccaa) -> how many ELIGIBLE customers, plus a summary of what was left out.

    AGE IS COMPUTED ON THE WINDOW'S REFERENCE DATE, not on the clock: the export must be
    reproducible next year. A customer may cross a band boundary within a long window; the
    cohort mass is a property of the WINDOW, and the date used travels in the payload so
    the choice is visible instead of implicit.

    THE MINIMUM AGE CUT COMES IN HERE, and not only in the generator. If the mass were
    counted over the whole base, IPF would balance the books against a cohort distribution
    that includes people who will never place an order — and the aggregate would miss the
    target by a path no generator test reaches.
    """
    province_ccaa = demand_profile.load_province_ccaa(seeds_dir)
    year = int(reference_date[:4])

    counts: dict[tuple, int] = {}
    underage = 0
    without_year = 0
    for row in customer_rows:
        birth_year = row.get("birth_year")
        if birth_year is None:
            without_year += 1
            continue
        age = year - int(birth_year)
        if age < min_buyer_age:
            underage += 1
            continue
        band = demand_profile.age_band_of(age)
        ccaa = demand_profile.ccaa_of(province_ccaa, row.get("province_code"))
        key = (band, ccaa)
        counts[key] = counts.get(key, 0) + 1

    if without_year:
        raise OrdersReferenceError(
            f"{without_year} customer(s) without birth_year in silver_customer. Without a "
            f"birth year there is no age band, and a customer without a band has no defined propensity."
        )
    if not counts:
        raise OrdersReferenceError(
            f"no customer aged {min_buyer_age} or more on {reference_date}. With "
            f"min_buyer_age={min_buyer_age} the whole base would be ineligible."
        )

    summary = {
        "reference_date": reference_date,
        "min_buyer_age": min_buyer_age,
        "eligible": sum(counts.values()),
        "below_min_age": underage,
        "note": (
            "Medido em 2026-08-31, antes desta fase: 18,01% da base tinha menos de 18 anos "
            "(3.602 de 20.000), com idades a partir de zero. Isso NAO e defeito da Source de "
            "OLTP — o contrato dela declara que a idade vem da distribuicao POPULACIONAL do "
            "INE, e e isso que ela entrega. O que faltava declarado era a diferenca entre "
            "residente e quem coloca um pedido, e ela so passou a importar quando a idade "
            "comecou a governar a demanda."
        ),
    }
    return counts, summary


# --------------------------------------------------------------------------------
# Coherence across the four files
# --------------------------------------------------------------------------------

def _assert_coverage(
    customers: dict,
    catalog: dict,
    calendar: dict,
    premises: dict,
    demand: dict,
    warehouse_regions: dict,
) -> None:
    """Coverage checked at run time, never assumed to be permanent."""
    warehouses = set(calendar["warehouses"])

    with_customer = {row["wh"] for row in customers["rows"]}
    without_customer = sorted(warehouses - with_customer)
    if without_customer:
        raise OrdersReferenceError(
            f"warehouse(s) in scope without any customer: {without_customer}. An order without "
            f"a customer is not an order."
        )
    out_of_scope = sorted(with_customer - warehouses)
    if out_of_scope:
        raise OrdersReferenceError(
            f"customer(s) in a warehouse outside the scope seed: {out_of_scope}"
        )

    # Every (wh, price_as_of) pair the calendar points to must exist in the catalog.
    catalog_by_pair: dict[tuple, int] = {}
    for row in catalog["rows"]:
        key = (row["wh"], row["price_as_of"])
        catalog_by_pair[key] = catalog_by_pair.get(key, 0) + 1

    required = {(row["wh"], row["price_as_of"]) for row in calendar["rows"]}
    absent = sorted(required - set(catalog_by_pair))
    if absent:
        raise OrdersReferenceError(
            f"{len(absent)} (warehouse, price_as_of) pair(s) pointed to by the calendar and "
            f"absent from the catalog: {absent[:10]}"
        )

    # A basket is drawn WITHOUT REPLACEMENT: if a pair's catalog had fewer products than the
    # largest possible basket, the generator could not build the basket and the defect would
    # show up as a smaller order, not as an error.
    largest_basket = int(float(premises["values"]["basket_lines_max"]))
    thin = sorted(
        (pair, total) for pair, total in catalog_by_pair.items() if total < largest_basket
    )
    if thin:
        raise OrdersReferenceError(
            f"(warehouse, price_as_of) pair(s) with fewer than basket_lines_max={largest_basket} "
            f"products: {thin[:10]}"
        )

    # EVERY WEIGHTED GROUP MUST HAVE A PRODUCT IN EVERY (warehouse, price_as_of) PAIR.
    # Without this check, an empty group in a warehouse would be silently renormalized in the
    # draw and that warehouse would end up with a mix different from the others — plausible,
    # error-free, and impossible to notice in a total. It is the same class of defect that
    # `_duplicates_sql` catches in customers_reference.py.
    weighted = {
        g["demand_group"] for g in demand["groups"]
        if float(g["line_weight"]) > 0
    }
    by_pair: dict[tuple, set] = {}
    for row in catalog["rows"]:
        by_pair.setdefault((row["wh"], row["price_as_of"]), set()).add(row["demand_group"])
    holes = sorted(
        (pair, sorted(weighted - present)[:5])
        for pair, present in by_pair.items()
        if weighted - present
    )
    if holes:
        raise OrdersReferenceError(
            f"{len(holes)} (warehouse, price_as_of) pair(s) without a product in some weighted "
            f"group: {holes[:5]}. The draw would renormalize silently and that warehouse's mix "
            f"would diverge from the others without any error."
        )

    # EVERY WAREHOUSE NEEDS A COMMUNITY WITH A FREQUENCY INDEX, and every cohort needs a
    # weight vector covering the same groups as the aggregate profile. A short vector would
    # make that cohort's draw ignore a whole group — no error, no null, and with a mix that
    # still sums to 1.
    cohorts = demand.get("cohorts")
    if not cohorts:
        raise OrdersReferenceError(
            "the demand profile came out without the `cohorts` section. A profile without the "
            "cohort layer stamped with the version that promises it would be a silent no-op."
        )
    with_index = {r["ccaa_code"] for r in cohorts["regions"]}
    without_region = sorted(
        wh for wh in warehouses if warehouse_regions.get(wh) not in with_index
    )
    if without_region:
        raise OrdersReferenceError(
            f"warehouse(s) without a community with a frequency index: {without_region}"
        )

    all_groups = {g["demand_group"] for g in demand["groups"]}
    for vector in cohorts["weights"]:
        present = {g["demand_group"] for g in vector["groups"]}
        missing = sorted(all_groups - present)
        if missing:
            raise OrdersReferenceError(
                f"cohort {vector['cohort']!r} has no weight for {len(missing)} group(s): "
                f"{missing[:5]}. That cohort's draw would ignore the whole group."
            )
        total = sum(float(g["line_weight"]) for g in vector["groups"])
        if abs(total - 1.0) > 1e-6:
            raise OrdersReferenceError(
                f"the weights of cohort {vector['cohort']!r} sum to {total}, not 1"
            )

def build(connection, window_from: str, window_to: str, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Builds the four payloads from an already open DuckDB connection.

    Separate from `export` so the suite exercises the queries against real DuckDB fixtures,
    without object storage and without network.
    """
    days = _date_range(window_from, window_to)
    warehouses = _warehouses(seeds_dir)

    premises = _build_premises(seeds_dir)
    calendar = _build_calendar(connection, warehouses, days)
    price_dates = sorted({row["price_as_of"] for row in calendar["rows"]})
    catalog = _build_catalog(connection, price_dates, seeds_dir)
    # BEFORE the demand profile: its kg-per-line must be computed with the same weights the
    # generator will draw with, or the MAPA calibration measures a basket nobody buys.
    catalog["product_choice"] = product_choice.stamp(
        catalog["rows"], premises["values"], demand_profile.load_benchmark(seeds_dir)
    )
    customers = _build_customers(connection, warehouses)

    # The customer cohort is resolved HERE, in a single place, for the same reason
    # `_stamp_demand_group` resolves the mapping in a single place: two resolutions diverge at
    # the first tweak and the divergence would be invisible, because both would give a plausible mix.
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
    """Writes the four files. Returns the size of each.

    `catalog.json` comes out compact for the same reason as `address_candidates.json`: it is
    an intermediate input of tens of thousands of rows, not a landed, hash-verified partition.
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
        # Indented: it is dozens of groups, not tens of thousands of products, and this is the
        # file someone will open to check where a weight came from.
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
    """Writes the four reference files into out_dir. Returns a summary."""
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
