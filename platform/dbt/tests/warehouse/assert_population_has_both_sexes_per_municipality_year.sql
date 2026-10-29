-- Every (municipality, year) has EXACTLY the two sex labels.
--
-- It is the test that makes it safe to have excluded the aggregate. The cut drops `sex_label = 'Total'`
-- because summing the three labels would give double the population — but dropping the aggregate is only
-- legitimate if the detail rebuilds it. If a municipality shows up with a single label (INE's
-- statistical confidentiality suppressing one of the sexes, or a row lost along the way), that
-- municipality's sum is halved and the denominator of every per-capita figure is wrong — without
-- anything else in the model noticing.
--
-- Why this is a test and not a premise: an aggregate mixed with detail does not FAIL, it produces a
-- plausible number. That is how Phase 1's age pyramid came out 2.03x inflated with
-- age_label, and no test at the time caught it.
--
-- Fails with one row per incomplete (municipality, year).
select
    province_code,
    municipality_code,
    municipality_name,
    year,
    count(distinct sex_label)                   as rotulos,
    {{ listagg('distinct sex_label', "', '") }} as quais
from {{ ref('fact_population_municipality') }}
group by 1, 2, 3, 4
having count(distinct sex_label) <> 2
