"""Exports, from Silver, the flat references that the simulated OLTP Source consumes.

WHY THIS MODULE EXISTS, AND WHY IT LIVES IN THE PLATFORM
--------------------------------------------------------
`simulated-oltp-source` generates synthetic customers anchored in REAL address and
demographics. For that it needs to read Silver — but every Source in this repo is FROZEN
(`dependencies = []`, checked by AST) and `duckdb`/`boto3` are platform-only dependencies
by design (`platform/pyproject.toml`: "the two sets never meet"). A Source cannot open a
connection to the Lakehouse without breaking that boundary.

The way out is the same pattern `ine-callejero-source` already uses: there, `extract` does not
hit the network, it incorporates files already prepared in `--in`. Here, one step earlier: the
PLATFORM queries Silver and writes three flat JSON files; the Source's `extract` reads them with
the stdlib `json`. Neither side imports the other's code — they share only a physical contract,
the same trick `land.py`/`manifest.py` already use with any Source's manifest.

WHERE EACH THING COMES FROM (and what was measured before writing this)
----------------------------------------------------------------------
  * The dbt SEEDS are NOT reachable through `connect_lakehouse()`: they have no
    `location =` in dbt_project.yml, so dbt-duckdb materializes them inside the local
    `retail.duckdb` and they never become parquet under `s3://.../silver/`, which is all
    that function sees. That is why the two seeds are read straight from the CSV, with
    `read_csv` over a path this module controls.
  * None of the Silver models in this join has an `ORDER BY` in the final SELECT — row
    order comes from DuckDB's scan/glob and is not stable across runs. All three queries
    here order explicitly by a canonical key: the row's POSITION in
    `address_candidates.json` is the `candidate_index` each customer carries, so the order
    is not cosmetic, it is what makes the lineage auditable.
  * The municipality name MUST come from the aggregate row matched by
    `(province_code, municipality_code)`. Matching by `unit_code` returns 0 of 7,194 tramos
    in Valencia, because Valencia is the only one of the 4 seat municipalities with real
    nucleos/pedanias and no tramo hangs off the aggregate row.
  * The 103 `age_label`s of table 31304 do NOT form a partition: alongside the 101 single
    ages live two overlapping aggregates, 'Total' and '85 y mas anos'. Summing the 103
    naively gives 2.03x the correct value (measured in Madrid/2022: 27,724,421 against
    13,650,010, which is exactly the 'Total' label). Besides, '85 anos' and '85 y mas
    anos' would collide in the same bucket when turned into an integer. Hence EXCLUDED_AGE_LABELS.
  * Silver stacks ALL `ingestion_date`s on purpose (the history is deliberate), and a
    re-extraction of the same INE publication duplicates equivalent rows — measured in
    `silver_ine_population_series`, with 1,547,496 rows in each of two dates. All the
    queries here filter by `is_latest_ingestion`, the column the models expose precisely
    so that reading the current state does not depend on the consumer remembering a
    `max(ingestion_date)`.
  * THE DELIVERED AGE DISTRIBUTION IS NOT THE POPULATION'S. It is truncated at
    `customer_premises.min_customer_age` and renormalized per province. This is not a new
    liberty of this file: it already was a derived sampling distribution, it already
    excluded two aggregate labels and already renormalized, and it already declared the
    exclusions in the header. The cut comes in through the same mechanism. The reason is
    measured: until 2026-08-31 the base had 18.01% of customers under 18 (3,602 of 20,000),
    with ages starting at zero — the Source faithfully delivered the RESIDENT population,
    and a customer registry is not a census. The adult share is measured BEFORE truncation
    and survives in `adult_share_by_province`: measuring it afterwards would return 100% in
    every province.
  * HOW MANY CUSTOMERS EACH WAREHOUSE HAS IS NO LONGER AN ARGUMENT. Until Phase 5 it was
    5,000 per warehouse — the same number for AUFs that differ by 4.6x in population (mad1
    has 7.10 million inhabitants, svq1 has 1.59). The header of
    `municipality_population_weights` now carries `customer_allocation`, derived from each
    warehouse's adult population times the penetration rate, and the Source's `extract`
    reads it when `--count` is omitted. The TOTAL is a consequence, not a quota: adding a
    municipality to the service area adds customers instead of taking them from the other
    warehouses.
  * One population row per municipality is a guarantee of the Silver model, not of this
    export. Until 2026-08-27 it was not: the by-name join of
    `silver_ine_population_by_municipality` also matched the national homonym
    (Torrent/Girona=182 next to Torrent/Valencia=90,928, and the same for Arroyomolinos and
    El Molar), because the RAW side is national. Fixed in the model with the official INE
    code (`ine_ambiguous_series_seed`, derived from VALORES_SERIE/{COD}). Here the guarantee
    is only RE-CHECKED, and the export REFUSES if it breaks — picking a value on its own
    would silently bias the warehouse weight.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
from datetime import datetime, timezone

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")
SERVICE_AREA_SEED = "warehouse_service_area_seed.csv"
PROVINCE_MAP_SEED = "warehouse_province_map_seed.csv"
CUSTOMER_PREMISES_SEED = "customer_premises_seed.csv"
DEMAND_PROFILE_SEED = "demand_profile_seed.csv"

# Premises THIS export requires from the customer registry seed. Missing any of them fails: a
# default here would produce a base sized by a number nobody declared.
REQUIRED_PREMISES = (
    "min_customer_age",
    "customer_penetration_source",
    "customer_population_basis",
    "customer_allocation",
)

# The ONLY pointer this module knows how to follow. `customer_penetration_source` exists so
# the rate is not copied into two places, but following an arbitrary pointer would make the
# export read any number from any seed. It points here or it fails.
PENETRATION_POINTER = "demand_profile.channel_reference_pct"
PENETRATION_PARAM = "channel_reference_pct"

ADDRESS_CANDIDATES_FILE = "address_candidates.json"
POPULATION_WEIGHTS_FILE = "municipality_population_weights.json"
AGE_DISTRIBUTION_FILE = "province_age_distribution.json"

POPULATION_TABLE_ID = "31304"

# Labels that are NOT single ages: they are aggregates overlapping the 101 ages from 0 to
# "100 y mas anos". Including any of them inflates the pyramid (see the module docstring).
# '100 y mas anos' is NOT here: it is the legitimate terminal bucket of the single ages.
EXCLUDED_AGE_LABELS = ("Total", "85 y más años")

# Sex: each table uses its own source's vocabulary and this module does not normalize it.
SEX_TOTAL_29005 = "Total"
SEX_MALE_29005 = "Hombres"
SEX_FEMALE_29005 = "Mujeres"
SEX_BOTH_31304 = "Ambos sexos"


class ReferenceExportError(Exception):
    """Silver does not have what this export needs, or coverage regressed."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sql_literal(path: str) -> str:
    """Seed path as an SQL literal.

    The path is built by this module from --seeds-dir, never from source data, but a
    single quote would still break the query silently: better to refuse.
    """
    if "'" in path:
        raise ReferenceExportError(f"seed path with a single quote is not supported: {path}")
    return f"'{path}'"


def _seed(seeds_dir: str, name: str) -> str:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise ReferenceExportError(
            f"seed not found: {path}. Run from the repo root, or pass "
            f"--seeds-dir pointing to the dbt seeds directory."
        )
    return _sql_literal(path)


def _rows(connection, sql: str) -> list[dict]:
    """Executes and returns rows as dictionaries, in the order the SQL determined."""
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _write_json(path: str, payload, indent: int | None) -> int:
    """Writes JSON atomically (temporary file in the same directory + os.replace).

    `indent=2` for the small files, in the same canonical format as the rest of the repo.
    `indent=None` (compact) for `address_candidates.json`: it is 216 thousand rows, and the
    indented canonical form exists to stabilize the SHA-256 of files INSIDE a Source's
    partition — this is an intermediate input, not a landed partition, and it is not hash-verified.
    """
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


# --------------------------------------------------------------------------------
# 0. Customer registry premises, and the rate they point to
# --------------------------------------------------------------------------------

def _premises(connection, seeds_dir: str) -> dict:
    """The four premises of `customer_premises_seed`, as a dictionary of strings.

    Read from the CSV for the same reason as the other seeds: `connect_lakehouse()` only sees
    parquet under `silver/`, and the `customer_premises` model lives there — but it only exists
    AFTER a `dbt build`, and this export must run before anything else. The CSV is the
    source, the model is its projection.
    """
    rows = _rows(
        connection,
        f"select premise_key, value from read_csv("
        f"{_seed(seeds_dir, CUSTOMER_PREMISES_SEED)}, header = true, all_varchar = true)",
    )
    premises = {row["premise_key"]: row["value"] for row in rows}
    missing = [key for key in REQUIRED_PREMISES if key not in premises]
    if missing:
        raise ReferenceExportError(
            f"{CUSTOMER_PREMISES_SEED} is missing premise(s) {missing}. Without them the base "
            f"would be sized by a number nobody declared."
        )
    return premises


def _min_customer_age(premises: dict) -> int:
    raw = premises["min_customer_age"]
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ReferenceExportError(
            f"min_customer_age is not an integer: {raw!r}"
        ) from exc
    if value < 0 or value > 120:
        raise ReferenceExportError(f"min_customer_age outside any useful range: {value}")
    return value


def _penetration_pct(connection, seeds_dir: str, premises: dict) -> float:
    """The penetration rate, FOLLOWING the pointer instead of copying the number.

    The rate is e-commerce's share of total food volume (MAPA 2025, section 3), and it
    already lives in `demand_profile_seed` labeled `observed`. Two DECLARED premises turn it
    into a share of people, and neither is measured: that the online shopper consumes like
    the average, and that these four warehouses model the AUF's whole channel and not one
    operator within it. See the `customer_penetration_source` row of the seed.
    """
    pointer = premises["customer_penetration_source"]
    if pointer != PENETRATION_POINTER:
        raise ReferenceExportError(
            f"customer_penetration_source points to {pointer!r}, and this export only knows "
            f"how to follow {PENETRATION_POINTER!r}. Following an arbitrary pointer would size "
            f"the base by any number from any seed."
        )
    rows = _rows(
        connection,
        f"select value from read_csv({_seed(seeds_dir, DEMAND_PROFILE_SEED)}, "
        f"header = true, all_varchar = true) where param_key = '{PENETRATION_PARAM}'",
    )
    if len(rows) != 1:
        raise ReferenceExportError(
            f"{DEMAND_PROFILE_SEED} has {len(rows)} row(s) for "
            f"param_key='{PENETRATION_PARAM}', expected exactly 1."
        )
    try:
        value = float(rows[0]["value"])
    except (TypeError, ValueError) as exc:
        raise ReferenceExportError(
            f"{PENETRATION_PARAM} is not a number: {rows[0]['value']!r}"
        ) from exc
    if not 0 < value <= 100:
        raise ReferenceExportError(f"{PENETRATION_PARAM} outside (0, 100]: {value}")
    return value


# --------------------------------------------------------------------------------
# 1. Address candidates
# --------------------------------------------------------------------------------

def _address_candidates_sql(service_area: str) -> str:
    return f"""
    with service_area as (
        select distinct wh, province_code, municipality_code
        from read_csv({service_area}, header = true, all_varchar = true)
    ),
    tramos as (
        select * from silver_callejero_tramos
        where is_latest_ingestion
    ),
    units as (
        select * from silver_callejero_population_units
        where is_latest_ingestion
    ),
    -- Municipality name: AGGREGATE row, matched by (province, municipality). Matching by
    -- unit_code zeroes Valencia (0 of 7,194 tramos) — see the module docstring.
    municipality as (
        select province_code, municipality_code,
               max(municipality_name) as municipality_name
        from units
        where is_municipality_aggregate
        group by 1, 2
    ),
    -- The tramo's own nucleo/pedania, by unit_code and WITHOUT the aggregate filter.
    neighborhood as (
        select unit_code, max(population_unit_name) as population_unit_name
        from units
        group by 1
    ),
    streets as (
        select street_code, max(street_name_full) as street_name_full
        from silver_callejero_streets
        where is_latest_ingestion
        group by 1
    ),
    pseudo as (
        select pseudo_address_code, max(name_full) as name_full
        from silver_callejero_pseudo_addresses
        where is_latest_ingestion
        group by 1
    )
    select
        a.wh                                              as wh,
        t.province_code                                   as province_code,
        t.municipality_code                               as municipality_code,
        -- _callejero suffix on purpose: the two INE products render the SAME
        -- municipality differently in 370 of 370 cases in scope — the Callejero uses
        -- upper case with the article in parentheses ("BRUC (EL)") and table 29005 uses
        -- mixed case with a postponed article ("Bruc, El"). The code is the key; the name is not.
        -- Giving both the same label in different files would be a trap.
        m.municipality_name                               as municipality_name_callejero,
        nullif(n.population_unit_name, '')                as neighborhood_name,
        case when t.street_id = '00000' then p.name_full
             else s.street_name_full end                  as street_name,
        (t.street_id = '00000')                           as is_pseudo_address,
        t.postal_code                                     as postal_code,
        t.numbering_type                                  as numbering_type,
        try_cast(t.number_from as integer)                as number_from,
        try_cast(t.number_to as integer)                  as number_to
    from tramos t
    join service_area a
      on t.province_code = a.province_code
     and t.municipality_code = a.municipality_code
    left join municipality m
      on t.province_code = m.province_code
     and t.municipality_code = m.municipality_code
    left join neighborhood n on t.unit_code = n.unit_code
    left join streets s      on t.street_code = s.street_code
    left join pseudo p       on t.pseudo_address_code = p.pseudo_address_code
    -- Total order over the tramo grain: nothing here depends on scan order, because the
    -- row's position becomes the candidate_index the customer carries.
    order by a.wh, t.province_code, t.municipality_code, t.section_code, t.entity_suffix,
             t.street_id, t.pseudo_address_id, t.postal_code, t.numbering_type,
             t.number_from, t.number_from_qualifier, t.number_to, t.number_to_qualifier
    """


def _build_address_candidates(connection, seeds_dir: str) -> dict:
    rows = _rows(connection, _address_candidates_sql(_seed(seeds_dir, SERVICE_AREA_SEED)))
    if not rows:
        raise ReferenceExportError(
            "silver_callejero_tramos returned no rows within the AUFs' scope. "
            "Has the Callejero been landed and Silver rebuilt?"
        )

    # Municipality name coverage: today it is 100%, but that is checked, not assumed.
    without_municipality = sorted(
        {
            f"{row['province_code']}/{row['municipality_code']}"
            for row in rows
            if not row["municipality_name_callejero"]
        }
    )
    if without_municipality:
        raise ReferenceExportError(
            f"{len(without_municipality)} service-area municipality(ies) without an aggregate "
            f"row in silver_callejero_population_units: {without_municipality[:10]}"
        )

    kept = [row for row in rows if row["street_name"]]
    orphans = len(rows) - len(kept)

    return {
        "generated_at_utc": _utc_now(),
        "callejero_ingestion_date": str(
            connection.execute(
                "select max(ingestion_date) from silver_callejero_tramos"
            ).fetchone()[0]
        ),
        "excluded_rows": {"no_street_or_pseudo_match": orphans},
        "rows": kept,
    }


# --------------------------------------------------------------------------------
# 2. Population weight per municipality
# --------------------------------------------------------------------------------

def _population_sql(service_area: str, province_map: str) -> str:
    return f"""
    with service_area as (
        select distinct wh, province_code, municipality_code
        from read_csv({service_area}, header = true, all_varchar = true)
    ),
    province as (
        select distinct wh, province_name
        from read_csv({province_map}, header = true, all_varchar = true)
    ),
    scoped as (
        select p.*
        from silver_ine_population_by_municipality p
        join service_area a
          on p.province_code = a.province_code
         and p.municipality_code = a.municipality_code
        where p.is_latest_ingestion
          and p.year = (
                select max(year) from silver_ine_population_by_municipality
                where is_latest_ingestion
              )
    ),
    -- Sex pivot to one row per municipality. It is NOT deduplication: the Silver model
    -- guarantees one series per (municipality, sex, year) since the national homonym started
    -- being resolved by the official INE code (ine_ambiguous_series_seed). That guarantee
    -- is RE-CHECKED before this query — see _duplicates_sql.
    pivoted as (
        select
            s.province_code,
            s.municipality_code,
            max(s.municipality_name) as municipality_name,
            max(case when s.sex_label = '{SEX_TOTAL_29005}'  then s.population_value end) as population_total,
            max(case when s.sex_label = '{SEX_MALE_29005}'   then s.population_value end) as population_male,
            max(case when s.sex_label = '{SEX_FEMALE_29005}' then s.population_value end) as population_female
        from scoped s
        group by 1, 2
    )
    select
        a.wh                                                as wh,
        pv.province_name                                    as province_name,
        v.province_code                                     as province_code,
        v.municipality_code                                 as municipality_code,
        v.municipality_name                                 as municipality_name,
        cast(v.population_total as bigint)                  as population_total,
        v.population_total
            / sum(v.population_total) over (partition by a.wh) as proportion_within_wh,
        v.population_male   / (v.population_male + v.population_female) as sex_hombres_proportion,
        v.population_female / (v.population_male + v.population_female) as sex_mujeres_proportion
    from pivoted v
    join service_area a
      on v.province_code = a.province_code
     and v.municipality_code = a.municipality_code
    join province pv on pv.wh = a.wh
    where v.population_total is not null
      and coalesce(v.population_male, 0) + coalesce(v.population_female, 0) > 0
    order by a.wh, v.province_code, v.municipality_code
    """


def _duplicates_sql(service_area: str) -> str:
    """Municipalities with more than one series for the same sex — there must be none.

    Until 2026-08-27 there were three (Arroyomolinos, El Molar, Torrent): the by-name join of
    `silver_ine_population_by_municipality` also matched the homonym from another province,
    because the RAW side is national. Fixed in the model with the official INE code. This
    stopped being a deduplication and became a CHECK: if it shows up again, the export
    refuses instead of picking a value on its own.
    """
    return f"""
    with service_area as (
        select distinct wh, province_code, municipality_code
        from read_csv({service_area}, header = true, all_varchar = true)
    )
    select
        a.wh                          as wh,
        p.province_code               as province_code,
        p.municipality_code           as municipality_code,
        max(p.municipality_name)      as municipality_name,
        p.sex_label                   as sex_label,
        count(*)                      as series_rows,
        string_agg(p.series_code, ',' order by p.series_code) as series_codes
    from silver_ine_population_by_municipality p
    join service_area a
      on p.province_code = a.province_code
     and p.municipality_code = a.municipality_code
    where p.is_latest_ingestion
      and p.year = (
            select max(year) from silver_ine_population_by_municipality
            where is_latest_ingestion
          )
    group by 1, 2, 3, 5
    having count(*) > 1
    order by 1, 2, 3, 5
    """


def _build_population_weights(connection, seeds_dir: str) -> dict:
    service_area = _seed(seeds_dir, SERVICE_AREA_SEED)

    # Check BEFORE any proportion: if a municipality had two series, the warehouse weight
    # would come out normalized over a phantom row and the bias would be silent.
    duplicates = _rows(connection, _duplicates_sql(service_area))
    if duplicates:
        detail = ", ".join(
            f"{d['municipality_name']} ({d['province_code']}/{d['municipality_code']}, "
            f"{d['sex_label']}: {d['series_codes']})"
            for d in duplicates[:5]
        )
        raise ReferenceExportError(
            f"{len(duplicates)} municipality(ies) with more than one population series for "
            f"the same sex: {detail}. silver_ine_population_by_municipality should "
            f"guarantee a single one (see ine_ambiguous_series_seed and "
            f"assert_ine_population_by_municipality_has_one_series_per_municipality). "
            f"I will not pick a value on my own."
        )

    rows = _rows(connection, _population_sql(service_area, _seed(seeds_dir, PROVINCE_MAP_SEED)))
    if not rows:
        raise ReferenceExportError(
            "silver_ine_population_by_municipality returned no rows within the AUFs' "
            "scope. Has table 29005 been extracted and landed?"
        )

    header = connection.execute(
        """
        select max(ingestion_date), max(year)
        from silver_ine_population_by_municipality
        """
    ).fetchone()
    reference_date = connection.execute(
        """
        select max(reference_date) from silver_ine_population_by_municipality
        where is_latest_ingestion
          and year = (
            select max(year) from silver_ine_population_by_municipality
            where is_latest_ingestion
          )
        """
    ).fetchone()[0]

    return {
        "generated_at_utc": _utc_now(),
        "population_ingestion_date": str(header[0]),
        "population_year": int(header[1]),
        "population_reference_date": str(reference_date),
        "population_table_id": "29005",
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# 3. Age distribution per province
# --------------------------------------------------------------------------------

def _scoped_ages_cte(province_map: str, fk_periodo: int, year: int) -> str:
    """CTE shared by both age queries: the WHOLE provincial pyramid, uncut.

    Shared on purpose. The adult share must be measured over the whole pyramid, and the
    distribution delivered to the Source must be its adult slice renormalized — if each
    query built its own cut, one day they would diverge and the numerator of one would no
    longer be the same universe as the denominator of the other.
    """
    excluded = ", ".join(f"'{label}'" for label in EXCLUDED_AGE_LABELS)
    return f"""
    with province as (
        select distinct wh, province_code, province_name
        from read_csv({province_map}, header = true, all_varchar = true)
    ),
    scoped as (
        select
            s.province_name,
            cast(regexp_extract(s.age_label, '^(\\d+)', 1) as integer) as age,
            s.population_value
        from silver_ine_population_series s
        where cast(s.table_id as varchar) = '{POPULATION_TABLE_ID}'
          and s.is_latest_ingestion
          and s.year = {year}
          and s.fk_periodo = {fk_periodo}
          and s.sex_label = '{SEX_BOTH_31304}'
          and s.age_label not in ({excluded})
          and s.population_value is not null
    )"""


def _adult_share_sql(province_map: str, fk_periodo: int, year: int, min_age: int) -> str:
    """Fraction of the provincial population aged >= min_age, MEASURED BEFORE ANY CUT.

    The order matters and is the obvious defect of this calculation: measuring the share after
    truncating the pyramid would return 100% in every province — a plausible number that would
    fail nothing and would size the whole base by the total population as if it were adult.
    """
    return f"""
    {_scoped_ages_cte(province_map, fk_periodo, year)}
    select
        p.province_code                                as province_code,
        p.province_name                                as province_name,
        sum(sc.population_value)                       as population_value,
        sum(case when sc.age >= {min_age} then sc.population_value else 0 end)
                                                       as adult_population_value,
        sum(case when sc.age >= {min_age} then sc.population_value else 0 end)
            / sum(sc.population_value)                 as adult_share
    from scoped sc
    join province p on sc.province_name = p.province_name
    group by 1, 2
    order by 1
    """


def _age_sql(province_map: str, fk_periodo: int, year: int, min_age: int) -> str:
    """The distribution delivered to the Source: only ages >= min_age, RENORMALIZED.

    Truncating without renormalizing would deliver a vector that sums to ~0.82 instead of 1, and
    `rng.choices` with `cum_weights` does not complain — it would simply never draw the tail.
    The cut is a premise of this platform (`customer_premises.min_customer_age`) and is declared
    in the file header, next to the two aggregate labels that were already excluded.
    """
    return f"""
    {_scoped_ages_cte(province_map, fk_periodo, year)},
    adult as (
        select * from scoped where age >= {min_age}
    )
    select
        p.province_code as province_code,
        a.age           as age,
        a.population_value
            / sum(a.population_value) over (partition by p.province_code) as proportion
    from adult a
    join province p on a.province_name = p.province_name
    order by p.province_code, a.age
    """


def _build_age_distribution(connection, seeds_dir: str, min_age: int) -> dict:
    province_map = _seed(seeds_dir, PROVINCE_MAP_SEED)

    chosen = connection.execute(
        f"""
        select max(year) from silver_ine_population_series
        where cast(table_id as varchar) = '{POPULATION_TABLE_ID}'
          and is_latest_ingestion
        """
    ).fetchone()[0]
    if chosen is None:
        raise ReferenceExportError(
            "silver_ine_population_series has no row from table 31304. Has the "
            "population-by-province extraction run?"
        )
    year = int(chosen)
    # Most recent fk_periodo of the chosen year: pinning is mandatory, otherwise two periods
    # of the same year would be summed. Which one was pinned goes in the file header.
    fk_periodo = int(
        connection.execute(
            f"""
            select max(fk_periodo) from silver_ine_population_series
            where cast(table_id as varchar) = '{POPULATION_TABLE_ID}'
              and is_latest_ingestion
              and year = {year}
            """
        ).fetchone()[0]
    )

    # THE ADULT SHARE FIRST, over the whole pyramid, and only then the truncation.
    shares = _rows(connection, _adult_share_sql(province_map, fk_periodo, year, min_age))
    if not shares:
        raise ReferenceExportError(
            f"no age rows for year={year} fk_periodo={fk_periodo}. Does the seed's "
            f"province_name spelling match silver_ine_population_series?"
        )
    degenerate = [s for s in shares if not 0 < float(s["adult_share"]) < 1]
    if degenerate:
        raise ReferenceExportError(
            f"adult share outside (0, 1) in {[s['province_code'] for s in degenerate]}: "
            f"the provincial pyramid already came truncated, or min_customer_age={min_age} "
            f"left nobody out. Measuring the share after the cut always returns 100%."
        )

    rows = _rows(connection, _age_sql(province_map, fk_periodo, year, min_age))
    if not rows:
        raise ReferenceExportError(
            f"no age >= {min_age} for year={year} fk_periodo={fk_periodo}."
        )
    below = [r for r in rows if int(r["age"]) < min_age]
    if below:
        raise ReferenceExportError(
            f"{len(below)} row(s) below min_customer_age={min_age} survived the "
            f"cut: {[r['province_code'] + '/' + str(r['age']) for r in below[:5]]}"
        )

    header = connection.execute(
        f"""
        select max(reference_date), max(fk_tipo_dato)
        from silver_ine_population_series
        where cast(table_id as varchar) = '{POPULATION_TABLE_ID}'
          and is_latest_ingestion
          and year = {year} and fk_periodo = {fk_periodo}
        """
    ).fetchone()

    return {
        "generated_at_utc": _utc_now(),
        "population_series_ingestion_date": str(
            connection.execute(
                "select max(ingestion_date) from silver_ine_population_series"
            ).fetchone()[0]
        ),
        "population_table_id": POPULATION_TABLE_ID,
        "year": year,
        "reference_date": str(header[0]),
        "fk_periodo": fk_periodo,
        "fk_tipo_dato": header[1],
        "sex_label": SEX_BOTH_31304,
        "excluded_age_labels": list(EXCLUDED_AGE_LABELS),
        # THE CUT PREMISE, in the header and not implicit in the data. Whoever reads this file
        # finds out the distribution is adult without having to inspect the lowest age
        # present, and the Source REFUSES the reference if the two disagree.
        "min_customer_age": min_age,
        "adult_share_by_province": [
            {
                "province_code": row["province_code"],
                "province_name": row["province_name"],
                "population_value": float(row["population_value"]),
                "adult_population_value": float(row["adult_population_value"]),
                "adult_share": float(row["adult_share"]),
            }
            for row in shares
        ],
        "note": (
            "Idade por PROVINCIA usada como proxy: nao existe faixa etaria por municipio "
            "nas tabelas ingeridas por esta plataforma (29005 nao tem coluna de idade e "
            "31304 so tem provincia). Os rotulos agregados 'Total' e '85 y mas anos' sao "
            "excluidos por se sobreporem as 101 idades simples. As linhas abaixo de "
            "min_customer_age tambem sao excluidas, e o que resta e RENORMALIZADO para somar "
            "1 por provincia: este arquivo e a distribuicao de amostragem do CADASTRO, nao "
            "uma copia da piramide do INE. A piramide inteira sobrevive em "
            "adult_share_by_province, que e onde o corte foi medido antes de ser aplicado."
        ),
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# 4. How many customers each warehouse has — and why the total is a consequence
# --------------------------------------------------------------------------------

def _round_half_up(value: float) -> int:
    """Rounding reproducible in SQL, which is why it is not `round()`.

    Python's `round()` rounds 0.5 to the nearest even number and DuckDB's does not. The dbt test
    `assert_customer_base_follows_the_declared_population_allocation` redoes this calculation
    in SQL and compares; with two different tie-breaking conventions it would flag a
    one-customer divergence that is nobody's defect. `floor(x + 0.5)` is the same on both sides.
    """
    return int(math.floor(value + 0.5))


def _build_customer_allocation(
    weights: dict, ages: dict, min_age: int, penetration_pct: float, pointer: str
) -> dict:
    """Customer target per warehouse, derived from the ADULT population it serves.

        observed municipal population (29005)
          x adult share of that municipality's province (31304, >= min_age)
          x penetration rate (MAPA 2025, section 3)
          = customers of that warehouse

    THE TOTAL IS A CONSEQUENCE, NOT A QUOTA. The difference only shows when the service area
    changes: with a quota of 20,000 split up, adding a municipality WOULD TAKE customers from
    the other warehouses; this way, it adds customers. Replaces the previous allocation, which
    was 5,000 per warehouse — the same number for AUFs that differ by 4.6x in population.

    The calculation is per MUNICIPALITY and not per warehouse, even though today each warehouse
    falls in a single province: multiplying the warehouse's whole population by a single adult
    share would presume that. Today both forms give the same integer; if a warehouse starts
    crossing provinces, this one stays right and the other becomes silently wrong.
    """
    share_by_province = {
        row["province_code"]: float(row["adult_share"])
        for row in ages["adult_share_by_province"]
    }

    by_wh: dict = {}
    for row in weights["rows"]:
        province = row["province_code"]
        if province not in share_by_province:
            raise ReferenceExportError(
                f"municipality {province}/{row['municipality_code']} without an adult share "
                f"for province {province}: the allocation would be silently biased."
            )
        entry = by_wh.setdefault(
            row["wh"], {"population_total": 0, "adult_population": 0.0}
        )
        population = int(row["population_total"])
        entry["population_total"] += population
        entry["adult_population"] += population * share_by_province[province]

    # An ORDERED list and not a dict: this file's header is read by a Source that cannot
    # depend on dictionary insertion order for anything, for the usual reason.
    entries = []
    for wh in sorted(by_wh):
        entry = by_wh[wh]
        customers = _round_half_up(entry["adult_population"] * penetration_pct / 100.0)
        if customers < 1:
            raise ReferenceExportError(
                f"allocation of {customers} customer(s) for wh={wh!r}: adult population "
                f"{entry['adult_population']:.0f} times rate {penetration_pct}% does not "
                f"sustain a base."
            )
        entries.append(
            {
                "wh": wh,
                "population_total": entry["population_total"],
                "adult_population": entry["adult_population"],
                "customers": customers,
            }
        )

    return {
        "rule": "per_warehouse_population",
        "population_basis": "adult_resident_population",
        "min_customer_age": min_age,
        "penetration_pct": penetration_pct,
        "penetration_source": pointer,
        "penetration_note": (
            "Participacao do e-commerce no volume total de alimentacao em 2025 (MAPA, secao "
            "3), usada como taxa de CLIENTES sob duas premissas declaradas e nao medidas: o "
            "comprador online consome como a media, e estes armazens modelam o canal inteiro "
            "da AUF e nao um operador dentro dele."
        ),
        "served_population": sum(e["population_total"] for e in entries),
        "served_adult_population": sum(e["adult_population"] for e in entries),
        "total_customers": sum(e["customers"] for e in entries),
        "by_warehouse": entries,
    }


# --------------------------------------------------------------------------------
# Coherence across the three files
# --------------------------------------------------------------------------------

def _assert_coverage(candidates: dict, weights: dict, ages: dict) -> None:
    """Coverage checked at run time, never assumed to be permanent.

    Today it is 100%, but a new municipality in the service area, or a province without an
    age series, would make generation silently biased instead of failing.
    """
    with_address = {(r["wh"], r["province_code"], r["municipality_code"]) for r in candidates["rows"]}
    with_weight = {(r["wh"], r["province_code"], r["municipality_code"]) for r in weights["rows"]}

    without_address = sorted(with_weight - with_address)
    if without_address:
        raise ReferenceExportError(
            f"{len(without_address)} municipality(ies) with a population weight but no "
            f"address candidate: {without_address[:10]}"
        )
    without_weight = sorted(with_address - with_weight)
    if without_weight:
        raise ReferenceExportError(
            f"{len(without_weight)} municipality(ies) with an address but no population row: "
            f"{without_weight[:10]}"
        )

    provinces_needed = {r["province_code"] for r in weights["rows"]}
    provinces_with_age = {r["province_code"] for r in ages["rows"]}
    missing = sorted(provinces_needed - provinces_with_age)
    if missing:
        raise ReferenceExportError(
            f"province(s) without an age distribution: {missing}"
        )

    # The delivered distribution cannot contain an age below the declared cut. It is the same
    # check the Source does when reading the file; done here, it fails the EXPORT instead of
    # failing generation four commands later.
    declared_min = ages.get("min_customer_age")
    if declared_min is None:
        raise ReferenceExportError(
            "province_age_distribution without min_customer_age in the header: the Source "
            "would have no way to know whether the distribution is the registry's or the whole "
            "population's."
        )
    lowest = min(int(row["age"]) for row in ages["rows"])
    if lowest < int(declared_min):
        raise ReferenceExportError(
            f"age distribution with age {lowest}, below the min_customer_age={declared_min} "
            f"declared in its own header."
        )


def _assert_allocation(weights: dict) -> None:
    """The allocation covers every warehouse with a population weight, and only those.

    Separate from `_assert_coverage` on purpose, and run AFTER it. Coverage of the three
    files is a precondition of the allocation — a province without an age distribution makes
    both fail, and whoever reads the error needs the cause, not the consequence.
    """
    allocation = weights.get("customer_allocation") or {}
    with_target = {row["wh"] for row in allocation.get("by_warehouse", [])}
    with_weight = {row["wh"] for row in weights["rows"]}
    without_target = sorted(with_weight - with_target)
    if without_target:
        raise ReferenceExportError(
            f"warehouse(s) with a population weight but no customer target: {without_target}. "
            f"`extract` without --count would fall back to a default for them."
        )
    orphan_target = sorted(with_target - with_weight)
    if orphan_target:
        raise ReferenceExportError(
            f"customer target for warehouse(s) without any municipality: {orphan_target}"
        )


def build(connection, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Builds the three payloads from an already open DuckDB connection.

    Separate from `export` so the suite exercises the three queries against real DuckDB
    fixtures, without object storage and without network — it is the only way the
    municipality join bug (which zeroed Valencia) gets caught before running against the Lakehouse.
    """
    premises = _premises(connection, seeds_dir)
    min_age = _min_customer_age(premises)
    penetration = _penetration_pct(connection, seeds_dir, premises)

    candidates = _build_address_candidates(connection, seeds_dir)
    weights = _build_population_weights(connection, seeds_dir)
    ages = _build_age_distribution(connection, seeds_dir, min_age)
    # COVERAGE BEFORE ALLOCATION. An allocation derived from a leaky coverage would be a
    # plausible number computed over an incomplete universe — and the error would point at the symptom.
    _assert_coverage(candidates, weights, ages)

    # The allocation lives in the header of the WEIGHTS file, and not in a fourth file: it is
    # the same municipal population as that file, summed per warehouse and multiplied by two
    # things. A new file would force the Source to match two sources for the same truth.
    weights["customer_allocation"] = _build_customer_allocation(
        weights, ages, min_age, penetration, premises["customer_penetration_source"]
    )
    _assert_allocation(weights)
    return {"candidates": candidates, "weights": weights, "ages": ages}


def write(payloads: dict, out_dir: str) -> dict:
    """Writes the three files. Returns the size of each."""
    return {
        ADDRESS_CANDIDATES_FILE: _write_json(
            os.path.join(out_dir, ADDRESS_CANDIDATES_FILE), payloads["candidates"], indent=None
        ),
        POPULATION_WEIGHTS_FILE: _write_json(
            os.path.join(out_dir, POPULATION_WEIGHTS_FILE), payloads["weights"], indent=2
        ),
        AGE_DISTRIBUTION_FILE: _write_json(
            os.path.join(out_dir, AGE_DISTRIBUTION_FILE), payloads["ages"], indent=2
        ),
    }


def export(config, out_dir: str, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Writes the three reference files into out_dir. Returns a summary."""
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    try:
        payloads = build(connection, seeds_dir)
    finally:
        connection.close()

    candidates, weights, ages = payloads["candidates"], payloads["weights"], payloads["ages"]
    written = write(payloads, out_dir)
    warehouses = sorted({row["wh"] for row in weights["rows"]})
    return {
        "out_dir": out_dir,
        "warehouses": warehouses,
        "address_candidates": len(candidates["rows"]),
        "orphan_tramos_excluded": candidates["excluded_rows"]["no_street_or_pseudo_match"],
        "municipalities": len(weights["rows"]),
        "age_rows": len(ages["rows"]),
        "age_year": ages["year"],
        "age_fk_periodo": ages["fk_periodo"],
        "population_year": weights["population_year"],
        "customer_allocation": weights["customer_allocation"],
        "bytes": written,
    }
