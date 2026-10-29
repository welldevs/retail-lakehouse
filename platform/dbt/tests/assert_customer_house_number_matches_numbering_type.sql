-- NEVER FABRICATE A HOUSE NUMBER. The Callejero declares in TINUM (here, numbering_type) whether the
-- street has numbering and which parity; the generator respects it, and this test re-checks by looking
-- only at Silver — without needing the reference, which is an ephemeral input.
--
-- Three STRICT conditions, all in one direction only:
--   1. number present  =>  numbering_type <> "0"  (the source says there is no numbering)
--   2. number present  =>  parity matches ("1" odd, "2" even)
--   3. number present  =>  >= 1  (0 is not a house number; 14 real street segments have range
--      0000..0000 and drawing from them would give exactly that)
--
-- The converse is NOT testable here, on purpose: a missing number with numbering_type "1"
-- or "2" is legitimate when the segment's range contains no number of the declared
-- parity (e.g. evens between 3 and 3). Checking that direction would require the range, which only exists
-- in address_candidates.json. Asserting an "if and only if" the data does not support would be
-- worse than not testing.
--
-- Fails with one row per inconsistent customer.
select
    ingestion_date,
    wh,
    customer_id,
    numbering_type,
    house_number,
    case
        when numbering_type = '0'                       then 'numero em via sem numeracao'
        when house_number < 1                           then 'numero menor que 1'
        when numbering_type = '1' and house_number % 2 = 0 then 'par em via impar'
        when numbering_type = '2' and house_number % 2 = 1 then 'impar em via par'
    end                                                 as motivo
from {{ ref('silver_customer') }}
where house_number is not null
  and (
        numbering_type = '0'
     or house_number < 1
     or (numbering_type = '1' and house_number % 2 = 0)
     or (numbering_type = '2' and house_number % 2 = 1)
  )
