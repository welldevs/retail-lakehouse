-- Uniqueness of the COMPOSITE GRAIN. There is no single "segment id" column in the
-- TRAM bytes (see the model comment) — verified without exception against 304,952 real rows
-- of the 4 provinces before assuming this key.
select
    ingestion_date,
    section_code,
    entity_suffix,
    street_id,
    pseudo_address_id,
    postal_code,
    numbering_type,
    number_from,
    number_from_qualifier,
    number_to,
    number_to_qualifier,
    count(*) as n
from {{ ref('silver_callejero_tramos') }}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
having count(*) > 1
