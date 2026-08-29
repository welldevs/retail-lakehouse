"""Exporta, do Silver, as referencias planas que a Source de OLTP simulado consome.

POR QUE ESTE MODULO EXISTE, E POR QUE ELE FICA NA PLATAFORMA
------------------------------------------------------------
`simulated-oltp-source` gera clientes sinteticos ancorados em endereco e demografia
REAIS. Para isso precisa ler o Silver — mas toda Source deste repo e FROZEN
(`dependencies = []`, verificado por AST) e `duckdb`/`boto3` sao dependencias exclusivas
da plataforma por design (`platform/pyproject.toml`: "os dois conjuntos nunca se
encontram"). Uma Source nao pode abrir conexao com o Lakehouse sem quebrar essa fronteira.

A saida e o mesmo padrao que `ine-callejero-source` ja usa: la, `extract` nao busca rede,
incorpora arquivos ja preparados em `--in`. Aqui, um passo antes: a PLATAFORMA consulta o
Silver e escreve tres arquivos JSON planos; o `extract` da Source os le com `json` da
stdlib. Nenhum lado importa o codigo do outro — compartilham so um contrato fisico, o
mesmo truque que `land.py`/`manifest.py` ja usam com o manifesto de qualquer Source.

DE ONDE VEM CADA COISA (e o que foi medido antes de escrever isto)
------------------------------------------------------------------
  * Os SEEDS do dbt NAO sao alcancaveis por `connect_lakehouse()`: eles nao tem
    `location =` no dbt_project.yml, entao o dbt-duckdb os materializa dentro do
    `retail.duckdb` local e nunca viram parquet em `s3://.../silver/`, que e tudo que
    aquela funcao enxerga. Por isso os dois seeds sao lidos direto do CSV, com
    `read_csv` sobre um caminho que este modulo controla.
  * Nenhum dos models Silver desta juncao tem `ORDER BY` no SELECT final — a ordem de
    linha vem do scan/glob do DuckDB e nao e estavel entre execucoes. Todas as tres
    queries aqui ordenam explicitamente por chave canonica: a POSICAO da linha em
    `address_candidates.json` e o `candidate_index` que cada cliente carrega, entao a
    ordem nao e cosmetica, e o que torna a linhagem auditavel.
  * O nome do municipio TEM de vir da linha agregada casada por
    `(province_code, municipality_code)`. Casar por `unit_code` devolve 0 de 7.194 tramos
    de Valencia, porque Valencia e o unico dos 4 municipios-sede com nucleos/pedanias
    reais e nenhum tramo pendura na linha agregada.
  * Os 103 `age_label` da tabela 31304 NAO formam uma particao: junto das 101 idades
    simples convivem dois agregados sobrepostos, 'Total' e '85 y mas anos'. Somar os 103
    ingenuamente da 2,03x o valor correto (medido em Madrid/2022: 27.724.421 contra
    13.650.010, que e exatamente o rotulo 'Total'). Alem disso '85 anos' e '85 y mas
    anos' colidiriam no mesmo balde ao virar inteiro. Dai EXCLUDED_AGE_LABELS.
  * O Silver empilha TODAS as `ingestion_date` de proposito (o historico e deliberado), e
    uma reextracao da mesma publicacao do INE duplica linhas equivalentes — medido em
    `silver_ine_population_series`, com 1.547.496 linhas em cada uma de duas datas. Todas
    as queries aqui filtram por `is_latest_ingestion`, a coluna que os modelos expoem
    justamente para que ler o estado atual nao dependa de o consumidor lembrar de um
    `max(ingestion_date)`.
  * Uma linha de populacao por municipio e garantia do modelo Silver, nao deste export.
    Ate 2026-08-27 nao era: o join por nome de `silver_ine_population_by_municipality`
    casava tambem o homonimo nacional (Torrent/Girona=182 junto de Torrent/Valencia=90.928,
    e o mesmo em Arroyomolinos e El Molar), porque o lado RAW e nacional. Corrigido no
    modelo com o codigo oficial do INE (`ine_ambiguous_series_seed`, derivado de
    VALORES_SERIE/{COD}). Aqui a garantia e apenas RECONFERIDA, e o export RECUSA se ela
    cair — escolher um valor por conta propria enviesaria o peso do warehouse em silencio.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")
SERVICE_AREA_SEED = "warehouse_service_area_seed.csv"
PROVINCE_MAP_SEED = "warehouse_province_map_seed.csv"

ADDRESS_CANDIDATES_FILE = "address_candidates.json"
POPULATION_WEIGHTS_FILE = "municipality_population_weights.json"
AGE_DISTRIBUTION_FILE = "province_age_distribution.json"

POPULATION_TABLE_ID = "31304"

# Rotulos que NAO sao idade simples: sao agregados sobrepostos as 101 idades de 0 a
# "100 y mas anos". Incluir qualquer um deles infla a piramide (ver docstring do modulo).
# '100 y mas anos' NAO entra aqui: e o balde terminal legitimo das idades simples.
EXCLUDED_AGE_LABELS = ("Total", "85 y más años")

# Sexo: cada tabela usa o vocabulario da propria fonte e este modulo nao normaliza.
SEX_TOTAL_29005 = "Total"
SEX_MALE_29005 = "Hombres"
SEX_FEMALE_29005 = "Mujeres"
SEX_BOTH_31304 = "Ambos sexos"


class ReferenceExportError(Exception):
    """O Silver nao tem o que este export precisa, ou a cobertura regrediu."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sql_literal(path: str) -> str:
    """Caminho de seed como literal SQL.

    O caminho e montado por este modulo a partir de --seeds-dir, nunca de dado da fonte,
    mas uma aspa simples ainda quebraria a query em silencio: melhor recusar.
    """
    if "'" in path:
        raise ReferenceExportError(f"caminho de seed com aspa simples nao suportado: {path}")
    return f"'{path}'"


def _seed(seeds_dir: str, name: str) -> str:
    path = os.path.join(seeds_dir, name)
    if not os.path.exists(path):
        raise ReferenceExportError(
            f"seed nao encontrado: {path}. Rode a partir da raiz do repo, ou passe "
            f"--seeds-dir apontando para o diretorio de seeds do dbt."
        )
    return _sql_literal(path)


def _rows(connection, sql: str) -> list[dict]:
    """Executa e devolve linhas como dicionarios, na ordem que o SQL determinou."""
    result = connection.execute(sql)
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _write_json(path: str, payload, indent: int | None) -> int:
    """Grava JSON atomicamente (temporario no mesmo diretorio + os.replace).

    `indent=2` para os arquivos pequenos, no mesmo formato canonico do resto do repo.
    `indent=None` (compacto) para `address_candidates.json`: sao 216 mil linhas, e a forma
    canonica indentada existe para estabilizar o SHA-256 de arquivos DENTRO da particao de
    uma Source — este e insumo intermediario, nao particao pousada, e nao e hash-verificado.
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
# 1. Candidatos de endereco
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
    -- Nome do municipio: linha AGREGADA, casada por (provincia, municipio). Casar por
    -- unit_code zera Valencia (0 de 7.194 tramos) — ver docstring do modulo.
    municipality as (
        select province_code, municipality_code,
               max(municipality_name) as municipality_name
        from units
        where is_municipality_aggregate
        group by 1, 2
    ),
    -- Nucleo/pedania do proprio tramo, por unit_code e SEM o filtro de agregado.
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
        -- Sufixo _callejero de proposito: os dois produtos do INE renderizam o MESMO
        -- municipio de forma diferente em 370 de 370 casos no escopo — o Callejero usa
        -- caixa alta com artigo entre parenteses ("BRUC (EL)") e a tabela 29005 usa caixa
        -- mista com artigo posposto ("Bruc, El"). O codigo e que e a chave; o nome nao.
        -- Deixar os dois com o mesmo rotulo em arquivos diferentes seria uma armadilha.
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
    -- Ordem total sobre o grao do tramo: nada aqui depende da ordem do scan, porque a
    -- posicao da linha vira o candidate_index que o cliente carrega.
    order by a.wh, t.province_code, t.municipality_code, t.section_code, t.entity_suffix,
             t.street_id, t.pseudo_address_id, t.postal_code, t.numbering_type,
             t.number_from, t.number_from_qualifier, t.number_to, t.number_to_qualifier
    """


def _build_address_candidates(connection, seeds_dir: str) -> dict:
    rows = _rows(connection, _address_candidates_sql(_seed(seeds_dir, SERVICE_AREA_SEED)))
    if not rows:
        raise ReferenceExportError(
            "silver_callejero_tramos nao devolveu nenhuma linha no escopo das AUFs. "
            "O Callejero ja foi aterrissado e o Silver reconstruido?"
        )

    # Cobertura de nome de municipio: hoje e 100%, mas isso e verificado, nao presumido.
    sem_municipio = sorted(
        {
            f"{row['province_code']}/{row['municipality_code']}"
            for row in rows
            if not row["municipality_name_callejero"]
        }
    )
    if sem_municipio:
        raise ReferenceExportError(
            f"{len(sem_municipio)} municipio(s) da service area sem linha agregada em "
            f"silver_callejero_population_units: {sem_municipio[:10]}"
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
# 2. Peso populacional por municipio
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
    -- Pivot de sexo para uma linha por municipio. NAO e deduplicacao: o modelo Silver
    -- garante uma serie por (municipio, sexo, ano) desde que o homonimo nacional passou a
    -- ser resolvido pelo codigo oficial do INE (ine_ambiguous_series_seed). Essa garantia
    -- e RECONFERIDA antes desta query — ver _assert_one_series_per_municipality.
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
    """Municipios com mais de uma serie para o mesmo sexo — nao deve haver nenhum.

    Ate 2026-08-27 havia tres (Arroyomolinos, El Molar, Torrent): o join por nome de
    `silver_ine_population_by_municipality` casava tambem o homonimo de outra provincia,
    porque o lado RAW e nacional. Corrigido no modelo com o codigo oficial do INE. Isto
    aqui deixou de ser uma deduplicacao e virou uma VERIFICACAO: se voltar a aparecer, o
    export recusa em vez de escolher um valor por conta propria.
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

    # Verificacao ANTES de qualquer proporcao: se um municipio tivesse duas series, o peso
    # do warehouse sairia normalizado sobre linha fantasma e o vies seria silencioso.
    duplicates = _rows(connection, _duplicates_sql(service_area))
    if duplicates:
        detalhe = ", ".join(
            f"{d['municipality_name']} ({d['province_code']}/{d['municipality_code']}, "
            f"{d['sex_label']}: {d['series_codes']})"
            for d in duplicates[:5]
        )
        raise ReferenceExportError(
            f"{len(duplicates)} municipio(s) com mais de uma serie de populacao para o "
            f"mesmo sexo: {detalhe}. silver_ine_population_by_municipality deveria "
            f"garantir uma so (ver ine_ambiguous_series_seed e "
            f"assert_ine_population_by_municipality_has_one_series_per_municipality). "
            f"Nao vou escolher um valor por conta propria."
        )

    rows = _rows(connection, _population_sql(service_area, _seed(seeds_dir, PROVINCE_MAP_SEED)))
    if not rows:
        raise ReferenceExportError(
            "silver_ine_population_by_municipality nao devolveu nenhuma linha no escopo "
            "das AUFs. A tabela 29005 ja foi extraida e aterrissada?"
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
# 3. Distribuicao etaria por provincia
# --------------------------------------------------------------------------------

def _age_sql(province_map: str, fk_periodo: int, year: int) -> str:
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
    )
    select
        p.province_code as province_code,
        sc.age          as age,
        sc.population_value
            / sum(sc.population_value) over (partition by p.province_code) as proportion
    from scoped sc
    join province p on sc.province_name = p.province_name
    order by p.province_code, sc.age
    """


def _build_age_distribution(connection, seeds_dir: str) -> dict:
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
            "silver_ine_population_series nao tem linha da tabela 31304. A extracao de "
            "populacao por provincia ja rodou?"
        )
    year = int(chosen)
    # fk_periodo mais recente do ano escolhido: pinar e obrigatorio, senao dois periodos
    # do mesmo ano entrariam somados. Qual foi pinado vai no cabecalho do arquivo.
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

    rows = _rows(connection, _age_sql(province_map, fk_periodo, year))
    if not rows:
        raise ReferenceExportError(
            f"nenhuma linha de idade para year={year} fk_periodo={fk_periodo}. A grafia de "
            f"province_name do seed bate com a de silver_ine_population_series?"
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
        "note": (
            "Idade por PROVINCIA usada como proxy: nao existe faixa etaria por municipio "
            "nas tabelas ingeridas por esta plataforma (29005 nao tem coluna de idade e "
            "31304 so tem provincia). Os rotulos agregados 'Total' e '85 y mas anos' sao "
            "excluidos por se sobreporem as 101 idades simples."
        ),
        "rows": rows,
    }


# --------------------------------------------------------------------------------
# Coerencia entre os tres arquivos
# --------------------------------------------------------------------------------

def _assert_coverage(candidates: dict, weights: dict, ages: dict) -> None:
    """Cobertura verificada em tempo de execucao, nunca presumida como permanente.

    Hoje e 100%, mas um municipio novo na service area, ou uma provincia sem serie de
    idade, tornaria a geracao silenciosamente enviesada em vez de falhar.
    """
    with_address = {(r["wh"], r["province_code"], r["municipality_code"]) for r in candidates["rows"]}
    with_weight = {(r["wh"], r["province_code"], r["municipality_code"]) for r in weights["rows"]}

    sem_endereco = sorted(with_weight - with_address)
    if sem_endereco:
        raise ReferenceExportError(
            f"{len(sem_endereco)} municipio(s) com peso populacional mas sem nenhum "
            f"candidato de endereco: {sem_endereco[:10]}"
        )
    sem_peso = sorted(with_address - with_weight)
    if sem_peso:
        raise ReferenceExportError(
            f"{len(sem_peso)} municipio(s) com endereco mas sem linha de populacao: "
            f"{sem_peso[:10]}"
        )

    provinces_needed = {r["province_code"] for r in weights["rows"]}
    provinces_with_age = {r["province_code"] for r in ages["rows"]}
    faltando = sorted(provinces_needed - provinces_with_age)
    if faltando:
        raise ReferenceExportError(
            f"provincia(s) sem distribuicao etaria: {faltando}"
        )


def build(connection, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Monta os tres payloads a partir de uma conexao DuckDB ja aberta.

    Separado de `export` para que a suite exercite as tres queries contra fixtures
    DuckDB reais, sem object storage e sem rede — e o unico jeito de o bug do join de
    municipio (que zerava Valencia) ser pego antes de rodar contra o Lakehouse.
    """
    candidates = _build_address_candidates(connection, seeds_dir)
    weights = _build_population_weights(connection, seeds_dir)
    ages = _build_age_distribution(connection, seeds_dir)
    _assert_coverage(candidates, weights, ages)
    return {"candidates": candidates, "weights": weights, "ages": ages}


def write(payloads: dict, out_dir: str) -> dict:
    """Grava os tres arquivos. Devolve o tamanho de cada um."""
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
    """Escreve os tres arquivos de referencia em out_dir. Devolve um resumo."""
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
        "bytes": written,
    }
