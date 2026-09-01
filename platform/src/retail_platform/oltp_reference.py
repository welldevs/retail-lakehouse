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
  * A DISTRIBUICAO ETARIA ENTREGUE NAO E A DA POPULACAO. Ela e truncada em
    `customer_premises.min_customer_age` e renormalizada por provincia. Isto nao e uma
    liberdade nova deste arquivo: ele ja era uma distribuicao de amostragem derivada, ja
    excluia dois rotulos agregados e ja renormalizava, e ja declarava as exclusoes no
    cabecalho. O corte entra pelo mesmo mecanismo. O motivo esta medido: ate 2026-08-31 a
    base tinha 18,01% de clientes com menos de 18 anos (3.602 de 20.000), com idade a partir
    de zero — a Source entregava fielmente a populacao RESIDENTE, e um cadastro nao e um
    censo. O share adulto e medido ANTES da truncagem e sobrevive em
    `adult_share_by_province`: medi-lo depois devolveria 100% em toda provincia.
  * QUANTOS CLIENTES CADA ARMAZEM TEM DEIXOU DE SER UM ARGUMENTO. Ate a Fase 5 eram 5.000
    por armazem — o mesmo numero para AUFs que diferem por 4,6x em populacao (mad1 tem 7,10
    milhoes de habitantes, svq1 tem 1,59). O cabecalho de `municipality_population_weights`
    passa a trazer `customer_allocation`, derivada da populacao adulta de cada armazem vezes
    a taxa de penetracao, e o `extract` da Source a le quando `--count` e omitido. O TOTAL e
    consequencia, nao cota: acrescentar um municipio a area de servico acrescenta clientes em
    vez de tira-los dos outros armazens.
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
import math
import os
import tempfile
from datetime import datetime, timezone

DEFAULT_SEEDS_DIR = os.path.join("platform", "dbt", "seeds")
SERVICE_AREA_SEED = "warehouse_service_area_seed.csv"
PROVINCE_MAP_SEED = "warehouse_province_map_seed.csv"
CUSTOMER_PREMISES_SEED = "customer_premises_seed.csv"
DEMAND_PROFILE_SEED = "demand_profile_seed.csv"

# Premissas que ESTE export exige do seed de cadastro. Ausencia de qualquer uma reprova: um
# default aqui produziria uma base dimensionada por um numero que ninguem declarou.
REQUIRED_PREMISES = (
    "min_customer_age",
    "customer_penetration_source",
    "customer_population_basis",
    "customer_allocation",
)

# O UNICO ponteiro que este modulo sabe seguir. `customer_penetration_source` existe para que
# a taxa nao seja copiada para dois lugares, mas seguir um ponteiro arbitrario faria o export
# ler qualquer numero de qualquer seed. Ele aponta para aqui ou reprova.
PENETRATION_POINTER = "demand_profile.channel_reference_pct"
PENETRATION_PARAM = "channel_reference_pct"

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
# 0. Premissas do cadastro, e a taxa que elas apontam
# --------------------------------------------------------------------------------

def _premises(connection, seeds_dir: str) -> dict:
    """As quatro premissas de `customer_premises_seed`, como dicionario de strings.

    Lido do CSV pelo mesmo motivo que os outros seeds: `connect_lakehouse()` so enxerga
    parquet sob `silver/`, e o modelo `customer_premises` mora la — mas ele so existe DEPOIS
    de um `dbt build`, e este export precisa rodar antes de qualquer coisa. O CSV e a
    fonte, o modelo e a projecao dela.
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
            f"{CUSTOMER_PREMISES_SEED} sem a(s) premissa(s) {missing}. Sem elas a base seria "
            f"dimensionada por um numero que ninguem declarou."
        )
    return premises


def _min_customer_age(premises: dict) -> int:
    raw = premises["min_customer_age"]
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ReferenceExportError(
            f"min_customer_age nao e inteiro: {raw!r}"
        ) from exc
    if value < 0 or value > 120:
        raise ReferenceExportError(f"min_customer_age fora de qualquer faixa util: {value}")
    return value


def _penetration_pct(connection, seeds_dir: str, premises: dict) -> float:
    """A taxa de penetracao, SEGUINDO o ponteiro em vez de copiar o numero.

    A taxa e a participacao do e-commerce no volume total de alimentacao (MAPA 2025, secao
    3), e ela ja mora em `demand_profile_seed` com rotulo `observed`. Duas premissas
    DECLARADAS a transformam num share de gente, e nenhuma e medida: que o comprador online
    consome como a media, e que estes quatro armazens modelam o canal inteiro da AUF e nao um
    operador dentro dele. Ver a linha `customer_penetration_source` do seed.
    """
    pointer = premises["customer_penetration_source"]
    if pointer != PENETRATION_POINTER:
        raise ReferenceExportError(
            f"customer_penetration_source aponta para {pointer!r}, e este export so sabe "
            f"seguir {PENETRATION_POINTER!r}. Seguir um ponteiro arbitrario faria a base ser "
            f"dimensionada por qualquer numero de qualquer seed."
        )
    rows = _rows(
        connection,
        f"select value from read_csv({_seed(seeds_dir, DEMAND_PROFILE_SEED)}, "
        f"header = true, all_varchar = true) where param_key = '{PENETRATION_PARAM}'",
    )
    if len(rows) != 1:
        raise ReferenceExportError(
            f"{DEMAND_PROFILE_SEED} tem {len(rows)} linha(s) para "
            f"param_key='{PENETRATION_PARAM}', esperava exatamente 1."
        )
    try:
        value = float(rows[0]["value"])
    except (TypeError, ValueError) as exc:
        raise ReferenceExportError(
            f"{PENETRATION_PARAM} nao e numero: {rows[0]['value']!r}"
        ) from exc
    if not 0 < value <= 100:
        raise ReferenceExportError(f"{PENETRATION_PARAM} fora de (0, 100]: {value}")
    return value


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

def _scoped_ages_cte(province_map: str, fk_periodo: int, year: int) -> str:
    """CTE comum as duas consultas de idade: a piramide provincial INTEIRA, sem corte.

    Compartilhada de proposito. O share adulto tem de ser medido sobre a piramide inteira, e a
    distribuicao entregue a Source tem de ser a fatia adulta dela renormalizada — se cada
    consulta montasse o proprio recorte, um dia elas divergiriam e o numerador de uma nao
    seria mais o mesmo universo do denominador da outra.
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
    """Fracao da populacao provincial com idade >= min_age, MEDIDA ANTES DE QUALQUER CORTE.

    A ordem importa e e o defeito obvio deste calculo: medir o share depois de truncar a
    piramide devolveria 100% em toda provincia — um numero plausivel, que nao reprovaria nada
    e faria a base inteira ser dimensionada pela populacao total como se fosse adulta.
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
    """A distribuicao entregue a Source: so as idades >= min_age, RENORMALIZADAS.

    Truncar sem renormalizar entregaria um vetor que soma ~0,82 em vez de 1, e `rng.choices`
    com `cum_weights` nao reclama disso — ele simplesmente nunca sortearia a cauda. O corte e
    premissa desta plataforma (`customer_premises.min_customer_age`) e vai declarado no
    cabecalho do arquivo, ao lado dos dois rotulos agregados que ja eram excluidos.
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

    # O SHARE ADULTO PRIMEIRO, sobre a piramide inteira, e so depois a truncagem.
    shares = _rows(connection, _adult_share_sql(province_map, fk_periodo, year, min_age))
    if not shares:
        raise ReferenceExportError(
            f"nenhuma linha de idade para year={year} fk_periodo={fk_periodo}. A grafia de "
            f"province_name do seed bate com a de silver_ine_population_series?"
        )
    degenerada = [s for s in shares if not 0 < float(s["adult_share"]) < 1]
    if degenerada:
        raise ReferenceExportError(
            f"share adulto fora de (0, 1) em {[s['province_code'] for s in degenerada]}: "
            f"a piramide provincial ja veio truncada, ou min_customer_age={min_age} nao "
            f"deixou ninguem de fora. Medir o share depois do corte devolve sempre 100%."
        )

    rows = _rows(connection, _age_sql(province_map, fk_periodo, year, min_age))
    if not rows:
        raise ReferenceExportError(
            f"nenhuma idade >= {min_age} para year={year} fk_periodo={fk_periodo}."
        )
    abaixo = [r for r in rows if int(r["age"]) < min_age]
    if abaixo:
        raise ReferenceExportError(
            f"{len(abaixo)} linha(s) abaixo de min_customer_age={min_age} sobreviveram ao "
            f"corte: {[r['province_code'] + '/' + str(r['age']) for r in abaixo[:5]]}"
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
        # A PREMISSA DO CORTE, no cabecalho e nao implicita nos dados. Quem ler este arquivo
        # descobre que a distribuicao e adulta sem precisar inspecionar a menor idade
        # presente, e a Source RECUSA a referencia se as duas coisas discordarem.
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
# 4. Quantos clientes cada armazem tem — e por que o total e consequencia
# --------------------------------------------------------------------------------

def _round_half_up(value: float) -> int:
    """Arredondamento reproduzivel em SQL, e por isso nao e `round()`.

    `round()` do Python arredonda 0,5 para o par mais proximo e o do DuckDB nao. O teste dbt
    `assert_customer_base_follows_the_declared_population_allocation` refaz esta conta em SQL
    e compara; com duas convencoes de desempate diferentes ele acusaria uma divergencia de um
    cliente que nao e defeito de ninguem. `floor(x + 0.5)` e a mesma coisa dos dois lados.
    """
    return int(math.floor(value + 0.5))


def _build_customer_allocation(
    weights: dict, ages: dict, min_age: int, penetration_pct: float, pointer: str
) -> dict:
    """Alvo de clientes por armazem, derivado da populacao ADULTA que ele serve.

        populacao municipal observada (29005)
          x share adulto da provincia daquele municipio (31304, >= min_age)
          x taxa de penetracao (MAPA 2025, secao 3)
          = clientes daquele armazem

    O TOTAL E CONSEQUENCIA, NAO COTA. A diferenca so aparece quando a area de servico muda:
    com uma cota de 20.000 repartida, acrescentar um municipio TIRARIA clientes dos outros
    armazens; assim, ele acrescenta clientes. Substitui a alocacao anterior, que era 5.000
    por armazem — o mesmo numero para AUFs que diferem por 4,6x em populacao.

    A conta e por MUNICIPIO e nao por armazem, ainda que hoje cada armazem caia numa provincia
    so: multiplicar a populacao inteira do armazem por um unico share adulto presumiria isso.
    Hoje as duas formas dao o mesmo inteiro; se um armazem passar a cruzar provincia, esta
    continua certa e a outra passa a estar errada em silencio.
    """
    share_por_provincia = {
        row["province_code"]: float(row["adult_share"])
        for row in ages["adult_share_by_province"]
    }

    por_wh: dict = {}
    for row in weights["rows"]:
        provincia = row["province_code"]
        if provincia not in share_por_provincia:
            raise ReferenceExportError(
                f"municipio {provincia}/{row['municipality_code']} sem share adulto da "
                f"provincia {provincia}: a alocacao ficaria enviesada em silencio."
            )
        entrada = por_wh.setdefault(
            row["wh"], {"population_total": 0, "adult_population": 0.0}
        )
        populacao = int(row["population_total"])
        entrada["population_total"] += populacao
        entrada["adult_population"] += populacao * share_por_provincia[provincia]

    # Lista ORDENADA e nao dict: o cabecalho deste arquivo e lido por uma Source que nao pode
    # depender de ordem de insercao de dicionario para nada, pela mesma razao de sempre.
    linhas = []
    for wh in sorted(por_wh):
        entrada = por_wh[wh]
        clientes = _round_half_up(entrada["adult_population"] * penetration_pct / 100.0)
        if clientes < 1:
            raise ReferenceExportError(
                f"alocacao de {clientes} cliente(s) para wh={wh!r}: populacao adulta "
                f"{entrada['adult_population']:.0f} vezes taxa {penetration_pct}% nao "
                f"sustenta uma base."
            )
        linhas.append(
            {
                "wh": wh,
                "population_total": entrada["population_total"],
                "adult_population": entrada["adult_population"],
                "customers": clientes,
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
        "served_population": sum(linha["population_total"] for linha in linhas),
        "served_adult_population": sum(linha["adult_population"] for linha in linhas),
        "total_customers": sum(linha["customers"] for linha in linhas),
        "by_warehouse": linhas,
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

    # A distribuicao entregue nao pode conter idade abaixo do corte declarado. E a mesma
    # verificacao que a Source faz ao ler o arquivo; feita aqui, ela reprova o EXPORT em vez
    # de reprovar a geracao quatro comandos depois.
    minima = ages.get("min_customer_age")
    if minima is None:
        raise ReferenceExportError(
            "province_age_distribution sem min_customer_age no cabecalho: a Source nao teria "
            "como saber se a distribuicao ja e a do cadastro ou a da populacao inteira."
        )
    menor = min(int(row["age"]) for row in ages["rows"])
    if menor < int(minima):
        raise ReferenceExportError(
            f"distribuicao etaria com idade {menor}, abaixo do min_customer_age={minima} "
            f"declarado no proprio cabecalho."
        )


def _assert_allocation(weights: dict) -> None:
    """A alocacao cobre todo armazem com peso populacional, e so eles.

    Separada de `_assert_coverage` de proposito, e rodada DEPOIS dela. A cobertura dos tres
    arquivos e pre-condicao da alocacao — uma provincia sem distribuicao etaria faz as duas
    coisas falharem, e quem le o erro precisa da causa, nao da consequencia.
    """
    alocacao = weights.get("customer_allocation") or {}
    com_alvo = {row["wh"] for row in alocacao.get("by_warehouse", [])}
    com_peso = {row["wh"] for row in weights["rows"]}
    sem_alvo = sorted(com_peso - com_alvo)
    if sem_alvo:
        raise ReferenceExportError(
            f"armazem(ns) com peso populacional mas sem alvo de clientes: {sem_alvo}. O "
            f"`extract` sem --count cairia num default para eles."
        )
    alvo_orfao = sorted(com_alvo - com_peso)
    if alvo_orfao:
        raise ReferenceExportError(
            f"alvo de clientes para armazem(ns) sem nenhum municipio: {alvo_orfao}"
        )


def build(connection, seeds_dir: str = DEFAULT_SEEDS_DIR) -> dict:
    """Monta os tres payloads a partir de uma conexao DuckDB ja aberta.

    Separado de `export` para que a suite exercite as tres queries contra fixtures
    DuckDB reais, sem object storage e sem rede — e o unico jeito de o bug do join de
    municipio (que zerava Valencia) ser pego antes de rodar contra o Lakehouse.
    """
    premises = _premises(connection, seeds_dir)
    min_age = _min_customer_age(premises)
    penetration = _penetration_pct(connection, seeds_dir, premises)

    candidates = _build_address_candidates(connection, seeds_dir)
    weights = _build_population_weights(connection, seeds_dir)
    ages = _build_age_distribution(connection, seeds_dir, min_age)
    # COBERTURA ANTES DA ALOCACAO. A alocacao derivada de uma cobertura furada seria um numero
    # plausivel calculado sobre um universo incompleto — e o erro apontaria para o sintoma.
    _assert_coverage(candidates, weights, ages)

    # A alocacao mora no cabecalho do arquivo de PESOS, e nao num quarto arquivo: ela e a
    # mesma populacao municipal daquele arquivo, somada por armazem e multiplicada por duas
    # coisas. Um arquivo novo obrigaria a Source a casar duas fontes para a mesma verdade.
    weights["customer_allocation"] = _build_customer_allocation(
        weights, ages, min_age, penetration, premises["customer_penetration_source"]
    )
    _assert_allocation(weights)
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
        "customer_allocation": weights["customer_allocation"],
        "bytes": written,
    }
