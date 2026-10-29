-- PRODUCT, SCD2 — and the dimension NAMES an ambiguity instead of hiding it.
--
-- GRAIN: one product version, (source_product_id, valid_from).
-- KEY: product_sk. Natural key: source_product_id.
-- TYPE: observed.
--
-- WHAT THIS SCD2 REALLY MODELS. Keying on `source_product_id` models the life cycle OF THE
-- SOURCE KEY, not of the commercial item — CONTRACT.md 4.5 and ARCHITECTURE.md already
-- record that the source offers no business identity. In the measured `name_seen_before`
-- cases (a new id whose display_name already existed in the previous partition), the SCD2
-- produces "one product died, another was born". It may be right, but it is a CHOICE, and
-- that is why the identity_ambiguous flag is carried into the dimension: whoever queries
-- sees the case, instead of inheriting the decision without knowing.
--
-- SCD2 DERIVED FROM HISTORY, NOT ACCUMULATED BY MERGE — and this is the underlying decision.
-- STAGE brings ALL partitions, and RAW keeps all snapshots: the dimension can be rebuilt
-- from scratch at any time, always the same. A `dbt snapshot` would do the opposite — it
-- would create state that accumulates outside RAW, that cannot be rebuilt and that can
-- silently diverge from it. The whole repo rests on "RAW is the point of no return,
-- everything downstream is rebuildable"; deriving from history is the only way consistent
-- with that.
--
-- HONEST CONSEQUENCE TO RECORD: ARCHITECTURE.md said Iceberg would come in "when Gold
-- dim_product needs SCD2 via MERGE into an existing table". That trigger did NOT fire —
-- the complete history made MERGE unnecessary. Snowflake remains justified by governance
-- (RBAC) and by being the served layer, not by MERGE.
--
-- TRACKED ATTRIBUTES (Type 2): display_name and the primary category. Price is NOT — price
-- is a fact, and lives in FACT_PRICE_SNAPSHOT. Tracking price here would create a new
-- product version at every fluctuation and the dimension would become the fact.
--
-- GLOBAL DIMENSION, with no warehouse axis: measured that display_name does not vary across
-- the 4 warehouses (0 divergences). What varies across warehouses is price and assortment,
-- and both are facts.
{{ config(materialized = 'table') }}

with por_data as (

    -- One product per date, across warehouses. `max` is safe here per the measurement
    -- above; if display_name starts diverging across warehouses, the grain-uniqueness test
    -- does not catch it — but the version-count one does, because the dimension would start
    -- to oscillate. So noted.
    select
        source_product_id,
        ingestion_date,
        max(display_name)               as display_name,
        max(primary_category_id)        as primary_category_id,
        max(category_appearances)       as category_appearances,
        max(pack_size)                  as pack_size,
        max(unit_size)                  as unit_size,
        max(unit_name)                  as unit_name,
        max(size_format)                as size_format,
        count(distinct warehouse)       as warehouses_listing
    from {{ source('stage', 'STG_PRODUCT_PRICE') }}
    group by 1, 2

),

-- The window specification is repeated instead of named via a `window` clause:
-- Snowflake does not support `WINDOW <name> AS (...)`. Repeating is ugly and it is what
-- compiles.
marcado as (

    select
        *,
        case
            when lag(ingestion_date) over (
                partition by source_product_id order by ingestion_date) is null then 1
            when display_name is distinct from lag(display_name) over (
                partition by source_product_id order by ingestion_date) then 1
            when primary_category_id is distinct from lag(primary_category_id) over (
                partition by source_product_id order by ingestion_date) then 1
            else 0
        end as inicia_versao
    from por_data

),

agrupado as (

    select
        *,
        sum(inicia_versao) over (
            partition by source_product_id
            order by ingestion_date
            rows between unbounded preceding and current row
        ) as version_number
    from marcado

),

versoes as (

    select
        source_product_id,
        version_number,
        min(ingestion_date)             as valid_from,
        max(ingestion_date)             as last_seen_date,
        max(display_name)               as display_name,
        max(primary_category_id)        as primary_category_id,
        max(category_appearances)       as category_appearances,
        max(pack_size)                  as pack_size,
        max(unit_size)                  as unit_size,
        max(unit_name)                  as unit_name,
        max(size_format)                as size_format,
        max(warehouses_listing)         as warehouses_listing
    from agrupado
    group by 1, 2

),

-- A new id whose display_name already existed in the previous partition: the source does
-- not say whether it is the same item re-keyed or an item withdrawn and another launched.
-- silver_price_change already flags these cases; here the flag just travels to the
-- dimension.
ambiguidade as (

    select distinct source_product_id
    from {{ source('stage', 'STG_PRICE_CHANGE') }}
    where name_seen_before

)

select
    md5(concat_ws('|', v.source_product_id, cast(v.valid_from as varchar))) as product_sk,

    v.source_product_id,
    v.version_number,

    v.valid_from,
    -- valid_to is the valid_from of the NEXT version, not "the day before it": the partitions
    -- have gaps (08-17 to 08-23 do not exist), so subtracting a day would invent a date on
    -- which nothing was observed. Closed-open interval [valid_from, valid_to).
    lead(v.valid_from) over (
        partition by v.source_product_id order by v.version_number
    )                                                       as valid_to,
    lead(v.valid_from) over (
        partition by v.source_product_id order by v.version_number
    ) is null                                               as is_current,
    v.last_seen_date,

    v.display_name,
    v.primary_category_id,
    v.category_appearances,
    (v.category_appearances > 1)                            as is_multi_category,

    v.pack_size,
    v.unit_size,
    v.unit_name,
    v.size_format,
    v.warehouses_listing,

    coalesce(a.source_product_id is not null, false)        as identity_ambiguous
from versoes v
left join ambiguidade a on a.source_product_id = v.source_product_id
