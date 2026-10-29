-- PASS-THROUGH of the capture seal to object storage — same mechanism as order_premises.
--
-- IT MAY BE EMPTY, AND EMPTY MEANS SOMETHING. Zero rows means "the capture has not been
-- sealed yet", which is the legitimate state of a repository before `make freeze` and of a
-- fresh clone. `assert_raw_matches_the_frozen_capture` treats that case as "nothing to
-- guard" and passes; from the first seal on, it starts enforcing.
--
-- IT IS A TWO-STATE MACHINE, declared: unsealed -> sealed. What does NOT exist is a third
-- state where the seal exists and does not hold.
{{ config(
    location = 's3://retail-lakehouse/silver/frozen_capture.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select * from {{ ref('frozen_capture_seed') }}
