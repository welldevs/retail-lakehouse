"""Recorta o Silver para o Snowflake: parquet local, PUT em stage interno, COPY INTO.

POR QUE ESTE MODULO EXISTE
--------------------------
O Snowflake e o QUARTO consumidor que nao alcanca o Lakehouse — os outros tres sao as
Sources FROZEN. Duas razoes independentes, e ambas ja tem precedente no repo:

  1. Rede: um Snowflake gerenciado nao enxerga um MinIO em localhost. `COPY INTO` a partir
     de um external stage exigiria S3 real e uma storage integration; o stage INTERNO
     (`PUT file://...`) resolve sem nada disso, porque quem empurra os bytes e este
     processo, que enxerga os dois lados.
  2. Volume: nao faz sentido copiar o Silver inteiro. Medido em 2026-08-27 —
     3.796.213 linhas no Silver, das quais 3.094.992 (81,5%) sao
     `silver_ine_population_series`, nacional, com 57.072 linhas (1,8%) no escopo real das
     4 provincias. Carregar isso seria pagar armazenamento por 27x o dado util.

Por isso este modulo e irmao direto de `oltp_reference.py`: materializa um RECORTE do
Silver para quem nao pode falar DuckDB. A diferenca e so o formato de saida (parquet em
vez de JSON) e o destino (uma tabela em vez de um arquivo lido pela stdlib).

O RECORTE E DELIBERADAMENTE BURRO
----------------------------------
Filtro de escopo geografico, filtro de ultima ingestao, deduplicacao de grao. NENHUMA
regra de negocio. Se aparecer um `case when` de dominio aqui, esta no lugar errado: a
modelagem e do dbt, do lado do Snowflake. E o que impede a mesma logica existir em dois
motores e divergir em silencio.

A UNICA EXCECAO DECLARADA e excluir agregados que a fonte mistura com o detalhe
(`sex_label = 'Total'`). Nao e regra de negocio, e evitar dupla contagem: somar os tres
rotulos da o dobro da populacao. E exatamente a armadilha que ja custou 2,03x de inflacao
na piramide etaria da Fase 1, com `age_label`. Um agregado carregado junto do detalhe nao
e "dado a mais": e um fato errado esperando alguem somar.

O SCHEMA DAS TABELAS STAGE VEM DA PROPRIA QUERY
------------------------------------------------
O DDL e gerado a partir do tipo que o DuckDB devolve, nao escrito a mao num arquivo ao
lado. Um DDL manual e um segundo lugar onde o schema vive, e os dois divergem no primeiro
dia em que alguem acrescenta uma coluna ao recorte. Como STAGE e espelho 1:1, seu schema E
o resultado da query — deriva-lo e o que mantem os dois em sincronia por construcao. O DDL
gerado pode ser inspecionado sem conexao nenhuma (`dump_ddl`), entao continua revisavel.

STAGE E FULL REFRESH (`create or replace` + `COPY INTO`). Nao ha merge nem incremental
aqui de proposito: sao ~130 mil linhas, e historico/versionamento sao responsabilidade das
dimensoes SCD2 do GOLD, nao do espelho.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone

# Schemas do lado do Snowflake. STAGE e o unico que este modulo escreve.
STAGE_SCHEMA = "STAGE"

DEFAULT_OUT = os.path.join("data", "snowflake-stage")


class SnowflakeExportError(Exception):
    """O recorte nao pode ser produzido, ou o destino recusou."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------------
# O recorte, tabela a tabela
# --------------------------------------------------------------------------------
# Cada entrada e (nome da tabela STAGE, o que o recorte faz e por que, SQL). Os
# comentarios de "por que" ficam no proprio SQL, junto da linha que impoem.

SPECS: list[dict] = [
    {
        "name": "STG_PRODUCT_PRICE",
        "grain": "(ingestion_date, warehouse, source_product_id)",
        "sql": """
            -- DEDUP DO EIXO DE CATEGORIA. O grao do Silver e
            -- (data, wh, category_id, subgroup_id, produto): um produto aparece em mais de
            -- uma categoria e a fonte repete a linha inteira. Medido: o PRECO nao diverge
            -- entre essas aparicoes (0 casos), entao reduzir por produto e seguro e nao
            -- esconde conflito — a mesma premissa que silver_price_change ja assume e que
            -- assert_price_is_consistent_across_appearances protege.
            --
            -- O eixo `warehouse` FICA no grao, e isso e medido, nao convencao: 187 produtos
            -- tem unit_price diferente entre os 4 armazens no mesmo dia, e 0 divergem
            -- dentro do mesmo armazem. Um fato de preco sem `wh` mediria uma media que nao
            -- existe em lugar nenhum.
            select
                ingestion_date,
                warehouse,
                source_product_id,
                any_value(display_name)                     as display_name,

                -- Categoria primaria escolhida por MIN, que e determinista, e a contagem ao
                -- lado para que o caso multi-categoria seja um numero visivel e nao um
                -- detalhe apagado pela reducao (medido: max 2, media 1,06).
                min(category_id)                            as primary_category_id,
                count(distinct category_id)                 as category_appearances,
                any_value(product_level1_category_id)       as product_level1_category_id,

                any_value(unit_price)                       as unit_price,
                any_value(bulk_price)                       as bulk_price,
                any_value(reference_price)                  as reference_price,
                any_value(reference_format)                 as reference_format,
                any_value(previous_unit_price)              as previous_unit_price,
                any_value(tax_percentage)                   as tax_percentage,

                any_value(is_pack)                          as is_pack,
                any_value(pack_size)                        as pack_size,
                any_value(unit_size)                        as unit_size,
                any_value(size_format)                      as size_format,
                any_value(unit_name)                        as unit_name,
                any_value(total_units)                      as total_units,
                any_value(selling_method)                   as selling_method,
                any_value(published)                        as published,
                any_value(is_new_arrival)                   as is_new_arrival
            from silver_product_price
            group by 1, 2, 3
            order by 1, 2, 3
        """,
    },
    {
        "name": "STG_PRICE_CHANGE",
        "grain": "(ingestion_date, previous_ingestion_date, warehouse, source_product_id)",
        "sql": """
            -- CARREGADO, NAO REDERIVADO. Seria derivavel de STG_PRODUCT_PRICE com uma window
            -- function, mas isso criaria uma segunda implementacao do mesmo lag() ciente-de-
            -- lacuna que o Silver ja tem testado — e as duas divergiriam no primeiro ajuste.
            -- Duplicar 43 mil linhas custa menos que duplicar logica.
            --
            -- name_seen_before vem junto porque e o insumo da decisao de SCD2 do
            -- DIM_PRODUCT: id novo com nome que ja existia e ambiguidade de identidade, nao
            -- "produto novo".
            select *
            from silver_price_change
            order by ingestion_date, warehouse, source_product_id
        """,
    },
    {
        "name": "STG_POPULATION_MUNICIPALITY",
        "grain": "(province_code, municipality_code, sex_label, year)",
        "sql": """
            -- SEM sex_label = 'Total'. A fonte mistura o agregado com o detalhe na mesma
            -- coluna: carregar os tres rotulos faz qualquer soma dar o dobro da populacao.
            -- Mesma armadilha do age_label na Fase 1 (2,03x de inflacao, medida). Um teste
            -- do lado do Snowflake reconfere Hombres + Mujeres = Total da fonte.
            --
            -- Escopo: so os 370 municipios das AUFs. O Silver e nacional (74.898 linhas);
            -- fora das AUFs nao ha cliente, nem armazem, nem pergunta.
            select
                p.province_code,
                p.municipality_code,
                p.municipality_name,
                a.wh,
                p.sex_label,
                p.year,
                p.reference_date,
                p.population_value,
                p.is_secret
            from silver_ine_population_by_municipality p
            join warehouse_service_area a
              on  a.province_code     = p.province_code
             and  a.municipality_code = p.municipality_code
            where p.is_latest_ingestion
              and p.sex_label <> 'Total'
            order by 1, 2, 5, 6
        """,
    },
    {
        "name": "STG_CATEGORY",
        "grain": "(category_id)",
        "sql": """
            -- GLOBAL, sem eixo de armazem nem de data. Medido: a arvore tem 151 categorias
            -- de nivel 2 em CADA um dos 4 armazens, identicas. Carregar 2.114 linhas
            -- (14 particoes x 151) para servir uma dimensao de 151 seria duplicacao pura.
            -- Recorte pela ultima ingestao para que a escolha seja determinista, nao um
            -- any_value sobre datas diferentes.
            select
                category_id,
                any_value(category_name)            as category_name,
                any_value(category_order)           as category_order,
                any_value(parent_category_id)       as parent_category_id,
                any_value(parent_category_name)     as parent_category_name,
                any_value(parent_category_order)    as parent_category_order
            from silver_category
            where ingestion_date = (select max(ingestion_date) from silver_category)
            group by 1
            order by 1
        """,
    },
    {
        "name": "STG_CUSTOMER",
        "grain": "(ingestion_date, customer_id)",
        "sql": """
            -- TODAS as ingestion_date, nao so a corrente: DIM_CUSTOMER e SCD2 e precisa do
            -- historico para versionar. `is_latest_ingestion` viaja junto para que o lado
            -- do Snowflake saiba qual e a base vigente sem recalcular um max().
            select *
            from silver_customer
            order by ingestion_date, customer_id
        """,
    },
    {
        "name": "STG_GEOGRAPHY",
        "grain": "(province_code, municipality_code, postal_code)",
        "sql": """
            -- O ROLLUP QUE SOBREVIVE DOS 304.952 TRAMOS: 699 linhas.
            --
            -- postal_code SOZINHO NAO E CHAVE — medido: 29 CEPs cruzam fronteira de
            -- municipio. O par (municipio, CEP) e o menor grao que responde tanto "onde o
            -- cliente mora" quanto "para onde se entrega". Medido tambem: 0 CEPs cruzam
            -- armazem, entao `wh` e funcao do par e a roteirizacao por CEP e inequivoca.
            --
            -- OS DOIS NOMES DE MUNICIPIO viajam juntos, com rotulos distintos, porque os
            -- dois produtos do INE grafam o mesmo municipio de forma diferente em 370 de
            -- 370 casos ("BRUC (EL)" contra "Bruc, El"). Deixar so um esconderia a
            -- divergencia; deixar os dois com o mesmo rotulo seria uma armadilha. O que se
            -- compara entre fontes e sempre o CODIGO.
            with tramos as (
                select * from silver_callejero_tramos where is_latest_ingestion
            ),
            municipality_callejero as (
                select province_code, municipality_code,
                       max(municipality_name) as municipality_name
                from silver_callejero_population_units
                where is_latest_ingestion and is_municipality_aggregate
                group by 1, 2
            ),
            -- Populacao do ANO CORRENTE como atributo de tamanho da dimensao. A serie
            -- inteira (1996-2025) vive em STG_POPULATION_MUNICIPALITY, que e fato: isto
            -- aqui e conveniencia de recorte, nao a serie.
            population as (
                select province_code, municipality_code,
                       max(municipality_name) as municipality_name,
                       sum(population_value)  as population_total
                from silver_ine_population_by_municipality
                where is_latest_ingestion
                  and sex_label <> 'Total'
                  and year = (select max(year) from silver_ine_population_by_municipality
                              where is_latest_ingestion)
                group by 1, 2
            )
            select
                t.province_code,
                t.municipality_code,
                t.postal_code,
                a.wh,
                a.is_home_municipality,
                mc.municipality_name                        as municipality_name_callejero,
                pp.municipality_name                        as municipality_name_ine,
                pp.population_total                         as municipality_population,
                count(*)                                    as tramo_count,
                count(distinct t.section_code)              as section_count,
                count(distinct t.street_code)
                    filter (where t.street_id <> '00000')   as street_count
            from tramos t
            join warehouse_service_area a
              on  a.province_code     = t.province_code
             and  a.municipality_code = t.municipality_code
            left join municipality_callejero mc
              on  mc.province_code     = t.province_code
             and  mc.municipality_code = t.municipality_code
            left join population pp
              on  pp.province_code     = t.province_code
             and  pp.municipality_code = t.municipality_code
            group by 1, 2, 3, 4, 5, 6, 7, 8
            order by 1, 2, 3
        """,
    },
    {
        "name": "STG_SERVICE_AREA",
        "grain": "(wh, province_code, municipality_code)",
        "sql": """
            -- 370 linhas. A AUF (Area Urbana Funcional do INE) de cada armazem: quais
            -- municipios ele atende. E o que impede um pedido futuro nascer fora da area.
            select * from warehouse_service_area
            order by wh, province_code, municipality_code
        """,
    },
    {
        "name": "STG_WAREHOUSE",
        "grain": "(wh)",
        "sql": """
            select * from warehouse_province_map order by wh
        """,
    },
    {
        "name": "STG_INGESTION_RUN",
        "grain": "(source_name, ingestion_date, wh)",
        "sql": """
            -- 18 LINHAS QUE IMPEDEM UM MART MENTIR. Distinguem "nao houve preco" de "nao
            -- houve observacao": os dias 2026-08-17 a 08-23 nao existem no catalogo e NAO
            -- podem ser recuperados (a API so serve o preco de hoje). Sem esta tabela, um
            -- fato de preco com lacuna e indistinguivel de um produto que sumiu, e qualquer
            -- serie temporal interpola em silencio.
            --
            -- As duas sources com eixo de armazem entram na mesma forma; o union e por
            -- COLUNA NOMEADA, nao por posicao.
            select
                source_name,
                ingestion_date,
                warehouse                       as wh,
                run_id,
                complete,
                declared_product_rows           as declared_rows,
                failure_count,
                anomaly_count
            from raw_manifest

            union all by name

            select
                source_name,
                ingestion_date,
                wh,
                run_id,
                complete,
                declared_customer_rows          as declared_rows,
                failure_count,
                anomaly_count
            from silver_oltp_manifest

            union all by name

            -- ORDERS DECLARA EVENTOS, NAO LINHAS, e o mapeamento e para `declared_rows`
            -- porque e isso que o arquivo da particao contem: uma linha NDJSON por evento.
            -- Mapear `declared_order_rows` aqui faria a reconciliacao "linhas do arquivo x
            -- linhas declaradas" comparar duas grandezas diferentes e passar por engano
            -- em 6.400 contra 44.456.
            select
                source_name,
                ingestion_date,
                wh,
                run_id,
                complete,
                declared_event_rows             as declared_rows,
                failure_count,
                anomaly_count
            from silver_orders_manifest

            order by source_name, ingestion_date, wh
        """,
    },
    {
        "name": "STG_ORDER",
        "grain": "(order_id)",
        "sql": """
            -- O FOLD JA DOBRADO, carregado inteiro. `select *` de proposito: silver_order
            -- e o unico modelo do Silver cujo grao ja e exatamente o que o warehouse quer,
            -- entao projetar coluna a coluna aqui so criaria um segundo lugar para
            -- esquecer de acrescentar uma.
            --
            -- SEM `is_latest_ingestion`, e nao por esquecimento: cada ingestion_date e um
            -- DIA DE OPERACAO, nao uma reingestao do mesmo dia. Filtrar pela ultima
            -- deixaria 4.800 dos 6.400 pedidos fora do warehouse.
            select *
            from silver_order
            order by ingestion_date, wh, order_id
        """,
    },
    {
        "name": "STG_ORDER_LINE",
        "grain": "(order_id, line_no)",
        "sql": """
            -- A LINHA COLOCADA E A LINHA CUMPRIDA NA MESMA LINHA FISICA. `source_product_id`
            -- e o que o cliente pediu, `fulfilled_source_product_id` e o que ele recebeu, e
            -- os dois atravessam a fronteira: carregar so o segundo apagaria a substituicao,
            -- que e o unico fato que o modelo de eventos existe para registrar.
            select *
            from silver_order_line
            order by order_id, line_no
        """,
    },
    {
        "name": "STG_ORDER_EVENT",
        "grain": "(event_id)",
        "sql": """
            -- O LOG INTEIRO, 44.456 linhas. E a unica tabela do STAGE que carrega um corpo
            -- semiestruturado, e a decisao merece estar escrita.
            --
            -- POR QUE O PAYLOAD ATRAVESSA. Todo campo tipado dele ja foi dobrado em
            -- STG_ORDER e STG_ORDER_LINE — decline_reason, cancelled_by, picked_amount.
            -- Sem o payload, este fato responde apenas "um evento do tipo X ocorreu no
            -- instante T", e nao ha pergunta que so ele responda. Com o payload, ele e o
            -- log auditavel dentro do warehouse: quem quiser conferir o fold contra a
            -- origem consegue, sem voltar ao RAW. Custa 17,5 MB de texto antes da
            -- compressao. NAO e uma segunda derivacao do fold — e a ORIGEM dele, o que e
            -- exatamente a diferenca entre auditoria e duplicacao de verdade.
            --
            -- CAST EXPLICITO PARA VARCHAR, e nao um JSON caindo no mapa de tipos. O
            -- _TYPE_MAP recusa o que nao conhece justamente para nao adivinhar VARCHAR em
            -- silencio; aqui a escolha e visivel na propria consulta, e o nome da coluna
            -- carrega o tipo. Quem quiser VARIANT faz o parse do lado do Snowflake, onde
            -- ha um motor que o entende.
            --
            -- `partition_wh` FICA DE FORA. E coluna de auditoria do Silver — existe para
            -- que assert_order_partition_axis_matches_record possa comparar o caminho do
            -- arquivo com o conteudo do registro. Atravessar a fronteira com ela seria
            -- carregar uma PROVA, nao um fato, e a prova ja foi executada do outro lado.
            select
                ingestion_date,
                wh,
                event_id,
                event_type,
                event_version,
                order_id,
                sequence_no,
                producer,
                occurred_at,
                event_date,
                crosses_order_date,
                cast(payload as varchar)        as payload_json
            from silver_order_event
            order by ingestion_date, wh, order_id, sequence_no
        """,
    },
    {
        "name": "STG_ORDER_PREMISE",
        "grain": "(premise_key)",
        "sql": """
            -- 30 LINHAS QUE IMPEDEM UM MART MEDIR CONTRA UM LIMIAR QUE NINGUEM USOU.
            -- Irma direta de STG_INGESTION_RUN: metadado, nao medida, promovido porque sem
            -- ele o mart a jusante nao tem como ser honesto.
            --
            -- MART_FULFILLMENT_SLA conta violacoes de `sla_minutes_picking`. Esse numero
            -- tem dono: o seed que o gerador leu, cujo sha256 esta no manifesto de cada
            -- particao. Reescreve-lo como var do dbt criaria a segunda copia que diverge
            -- na primeira edicao — e nada reprovaria, porque contar zero violacao contra o
            -- limiar errado tem exatamente a aparencia de contar zero contra o certo.
            select *
            from order_premises
            order by premise_key
        """,
    },
]


# --------------------------------------------------------------------------------
# DuckDB -> parquet local
# --------------------------------------------------------------------------------

# Mapeamento de tipo. Deliberadamente curto: se o recorte produzir um tipo que nao esta
# aqui, e para FALHAR, nao para adivinhar um VARCHAR e perder precisao em silencio.
_TYPE_MAP = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "NUMBER(38,0)",
    "SMALLINT": "NUMBER(38,0)",
    "INTEGER": "NUMBER(38,0)",
    "BIGINT": "NUMBER(38,0)",
    "HUGEINT": "NUMBER(38,0)",
    "UBIGINT": "NUMBER(38,0)",
    "UINTEGER": "NUMBER(38,0)",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "VARCHAR": "VARCHAR",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP_NTZ",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ",
}


def _snowflake_type(duckdb_type: str) -> str:
    tipo = duckdb_type.upper().strip()
    if tipo.startswith("DECIMAL"):
        # DECIMAL(10,2) -> NUMBER(10,2). Precisao decimal em moeda nao vira FLOAT.
        return "NUMBER" + tipo[len("DECIMAL"):]
    if tipo in _TYPE_MAP:
        return _TYPE_MAP[tipo]
    raise SnowflakeExportError(
        f"tipo do DuckDB sem mapeamento para Snowflake: {duckdb_type!r}. "
        f"Acrescente-o a _TYPE_MAP em vez de deixar o recorte cair num VARCHAR."
    )


def _describe(connection, sql: str) -> list[tuple[str, str]]:
    """Colunas e tipos do recorte, sem materializar nenhuma linha."""
    rows = connection.execute(f"describe {sql}").fetchall()
    return [(r[0], r[1]) for r in rows]


def ddl_for(spec: dict, columns: list[tuple[str, str]], database: str) -> str:
    """`create or replace table` derivado do schema do proprio recorte."""
    corpo = ",\n".join(
        f"    {nome} {_snowflake_type(tipo)}" for nome, tipo in columns
    )
    return (
        f"create or replace table {database}.{STAGE_SCHEMA}.{spec['name']} (\n"
        f"{corpo}\n"
        f")\ncomment = 'Espelho 1:1 do recorte do Silver. Grao {spec['grain']}.'"
    )


def build(connection, out_dir: str) -> dict:
    """Escreve um parquet por spec em out_dir. Devolve o resumo de cada um.

    Separado de `load` para que a suite exercite todos os recortes contra fixtures DuckDB
    reais, sem Snowflake e sem rede — as consultas sao onde os erros silenciosos moram
    (o agregado 'Total' somado junto do detalhe, o escopo geografico esquecido), nao no
    transporte.
    """
    os.makedirs(out_dir, exist_ok=True)
    resumo = {}
    for spec in SPECS:
        sql = spec["sql"].strip()
        columns = _describe(connection, sql)
        destino = os.path.join(out_dir, f"{spec['name']}.parquet")

        # Escrita atomica pelo mesmo motivo do resto do repo: um arquivo truncado por
        # interrupcao nao pode parecer completo para o PUT que vem depois.
        handle, temp_path = tempfile.mkstemp(dir=out_dir, prefix=".tmp-", suffix=".parquet")
        os.close(handle)
        try:
            connection.execute(
                f"copy ({sql}) to '{temp_path}' (format parquet, compression zstd)"
            )
            os.replace(temp_path, destino)
        except BaseException:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise

        linhas = connection.execute(f"select count(*) from ({sql})").fetchone()[0]
        resumo[spec["name"]] = {
            "rows": linhas,
            "columns": len(columns),
            "bytes": os.path.getsize(destino),
            "path": destino,
            "grain": spec["grain"],
        }
    return resumo


def dump_ddl(connection, database: str) -> str:
    """Todo o DDL do STAGE, sem conexao com o Snowflake. Para revisao e versionamento."""
    partes = [
        f"-- Gerado por retail_platform.snowflake_export em {_utc_now()}.",
        "-- NAO editar a mao: o schema do STAGE e derivado do recorte em SPECS, e um DDL",
        "-- editado passa a ser um segundo lugar onde o schema vive.",
        "",
        f"create database if not exists {database};",
        f"create schema if not exists {database}.{STAGE_SCHEMA};",
        "",
    ]
    for spec in SPECS:
        columns = _describe(connection, spec["sql"].strip())
        partes.append(ddl_for(spec, columns, database) + ";")
        partes.append("")
    return "\n".join(partes)


def export(config, out_dir: str) -> dict:
    """Recorta o Silver para parquet local. Nao fala com o Snowflake."""
    from .query import connect_lakehouse

    connection = connect_lakehouse(config)
    try:
        return build(connection, out_dir)
    finally:
        connection.close()
