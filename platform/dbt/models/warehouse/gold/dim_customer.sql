-- CUSTOMER, SCD2 — and the SCD2 here exists for a different reason than DIM_PRODUCT's.
--
-- GRAIN: one customer version, (customer_id, valid_from).
-- KEY: customer_sk. Natural key: customer_id.
-- TYPE: the PERSON is synthetic; the ADDRESS is observed. Both live in the same row and the
-- distinction matters: municipality, street, postcode and numbering range always come from
-- a real Callejero row. The customer is invented; the place where they live is not.
--
-- WHY SCD2 AND NOT A FIXED TABLE. The customer base is RELOADABLE on purpose — it will
-- grow to provide real density. Each regeneration creates a new ingestion_date, and the
-- generator's behavior has two consequences the dimension needs to represent:
--
--   * GROWING THE BASE IS ADDITIVE. The generator consumes a single random.Random(seed) in
--     fixed order and nothing before the loop depends on `count`, so the first N customers
--     of a larger generation are byte for byte the same as before. Verified end to end:
--     200 -> 5,000 preserved the 200 (test_aumentar_count_e_aditivo_nao_reembaralha, and
--     the sha256s checked on the real data). Growing does NOT invalidate who already exists.
--   * CHANGING THE DATE OR THE SEED IS NOT ADDITIVE. Another ingestion_date preserves the
--     AGE and shifts birth_year — the same person, older. Another seed swaps the people
--     behind the same ids. The manifest records both in `history`.
--
-- A fixed table would have no way to represent that; SCD2 does. valid_from = the
-- generation's ingestion_date. Order facts must point to customer_sk (the version in
-- effect on the order date) and carry customer_id as the business key.
--
-- THE ADDRESS IS AN ASSIGNMENT, NOT AN IDENTITY. Two customers can receive the same
-- segment AND the same number — an identical address — and that is neither a collision
-- nor a bug: it is the consequence of the address being drawn among the municipality's
-- real candidates. That is why the address is an ATTRIBUTE of this dimension and not a
-- DIM_ADDRESS: there is no "address" entity with a life of its own in the model. Trigger to
-- create one: an order being deliverable to an address different from the registered one.
--
-- age_at_ingestion, NOT age computed against today. Age is the attribute the generator
-- actually drew; computing it against the current date would make the dimension change on
-- its own at every birthday without any new observation.
{{ config(materialized = 'table') }}

with versoes as (

    select
        customer_id,
        ingestion_date                                      as valid_from,
        wh,
        province_code,
        province_name,
        municipality_code,
        municipality_name,
        postal_code,
        street_name,
        numbering_type,
        house_number,
        candidate_index,
        first_name,
        last_name,
        sex_label,
        birth_year,
        age_at_ingestion,
        is_latest_ingestion
    from {{ source('stage', 'STG_CUSTOMER') }}

)

select
    md5(concat_ws('|', customer_id, cast(valid_from as varchar))) as customer_sk,

    customer_id,
    valid_from,
    -- Closed-open [valid_from, valid_to), for the same reason as DIM_PRODUCT: the
    -- generations are not daily and subtracting a day would invent a date with no observation.
    --
    -- The window is repeated instead of named via `window w as (...)`: Snowflake does not
    -- support the WINDOW clause. DuckDB does, and that is how this passed the local parse and
    -- only failed on the real engine.
    lead(valid_from) over (partition by customer_id order by valid_from) as valid_to,
    lead(valid_from) over (partition by customer_id order by valid_from) is null
                                                            as is_current,

    wh,

    -- Geography. Joins DIM_GEOGRAPHY by the same natural hash, and not by name: the two INE
    -- products spell the municipality differently in 370 of 370 cases.
    md5(concat_ws('|', province_code, municipality_code, postal_code)) as geography_sk,
    province_code,
    province_name,
    municipality_code,
    municipality_name,
    postal_code,

    -- Address: synthetic assignment derived from a real segment.
    street_name,
    numbering_type,
    house_number,
    (house_number is null)                                  as address_is_street_level,
    -- Audit: points to the exact row of the address_candidates.json used in the generation.
    -- It only has meaning against the reference identified in that partition's manifest.
    candidate_index,

    first_name,
    last_name,
    first_name || ' ' || last_name                          as full_name,
    sex_label,
    birth_year,
    age_at_ingestion,

    -- Age bands for slicing. Labels declared here and not inherited from any source: they are
    -- a decision of this model, not INE vocabulary.
    case
        when age_at_ingestion < 18 then '00-17'
        when age_at_ingestion < 30 then '18-29'
        when age_at_ingestion < 45 then '30-44'
        when age_at_ingestion < 65 then '45-64'
        else '65+'
    end                                                     as age_band,

    is_latest_ingestion                                     as is_current_generation
from versoes
