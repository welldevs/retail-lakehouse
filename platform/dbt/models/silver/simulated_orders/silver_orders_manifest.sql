-- O MANIFESTO COMO TABELA, para que o Silver seja reconciliavel contra o que a Source
-- declarou. Irmao de raw_manifest.sql e de silver_oltp_manifest.sql.
--
-- GRAO: (ingestion_date, wh). Uma particao, uma linha.
--
-- POR QUE ISTO IMPORTA MAIS AQUI DO QUE NAS OUTRAS SOURCES. Nas outras o manifesto declara
-- contagem de linhas de um arquivo; aqui ele declara o FOLD — order_rows, net_amount_picked,
-- substituted_lines. Sao numeros que so existem depois de dobrar o log, e a Source os calculou
-- em Python enquanto o Silver os recalcula em SQL, por caminhos independentes. O teste que
-- cruza os dois fecha manifesto -> RAW -> parquet, e uma divergencia significa que o fold de
-- um dos dois lados esta errado.
--
-- `declared_` como prefixo em tudo que veio de `totals`: o nome diz que aquilo e uma
-- AFIRMACAO da Source, e nao uma medida deste modelo. Mesma convencao de silver_oltp_manifest.
{{ config(
    location = 's3://retail-lakehouse/silver/silver_orders_manifest.parquet',
    options = {'overwrite_or_ignore': 1}
) }}

select
    ingestion_date,
    wh,
    regexp_extract(filename, 'wh=([^/]+)', 1)                as partition_wh,

    run_id,
    cast(started_at_utc as timestamp)                        as started_at_utc,
    cast(finished_at_utc as timestamp)                       as finished_at_utc,
    duration_seconds,
    complete,
    manifest_version,

    source.name                                              as source_name,
    source.wh                                                as source_warehouse,
    source.generator_version                                 as generator_version,

    config.seed                                              as seed,
    config.orders                                            as declared_orders,
    config.premises_sha256                                   as premises_sha256,
    config.reference                                         as reference_path,

    reference.roster_ingestion_date                          as customer_roster_ingestion_date,
    reference.price_as_of                                    as price_as_of,
    reference.price_source                                   as price_source,
    reference.window_from                                    as window_from,
    reference.window_to                                      as window_to,
    len(coalesce(reference.customer_ingestion_dates, []))    as customer_generations,
    len(coalesce(reference.catalog_ingestion_dates, []))     as catalog_snapshots,

    totals.event_rows                                        as declared_event_rows,
    totals.order_rows                                        as declared_order_rows,
    totals.line_rows                                         as declared_line_rows,
    totals.customers_used                                    as declared_customers_used,
    totals.substituted_lines                                 as declared_substituted_lines,
    totals.removed_lines                                     as declared_removed_lines,
    totals.carried_forward_orders                            as declared_carried_forward_orders,
    totals.orders_terminal                                   as declared_orders_terminal,
    cast(totals.gross_amount_placed as decimal(14, 2))       as declared_gross_amount_placed,
    cast(totals.net_amount_picked as decimal(14, 2))         as declared_net_amount_picked,
    totals.bytes                                             as declared_bytes,

    schema_fingerprint.sha256                                as schema_fingerprint,

    len(coalesce(files, []))                                 as declared_files,
    len(coalesce(failures, []))                              as failure_count,
    len(coalesce(anomalies, []))                             as anomaly_count,
    len(coalesce(history, []))                               as previous_runs

from read_json(
    '{{ var("orders_raw_prefix") }}/ingestion_date=*/wh=*/_manifest.json',
    hive_partitioning = 1,
    union_by_name = true,
    filename = true
)
