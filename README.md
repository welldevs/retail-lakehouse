# Retail Lakehouse — Mercadona

Plataforma de dados sobre a **Mercadona Catalog Source**: preserva o snapshot RAW em object
storage e produz o Silver tipado em parquet, orquestrado por Airflow.

A Source é um componente **congelado e independente**, em
[sources/mercadona-catalog-source/](sources/mercadona-catalog-source/), com seu próprio
contrato ([CONTRACT.md](sources/mercadona-catalog-source/CONTRACT.md)) e **zero dependências
de runtime**. Esta plataforma a consome pelo contrato físico — nunca importando seu código.

As decisões de arquitetura, e o gatilho de cada tecnologia ainda ausente
(Iceberg, Kafka, Spark, Snowflake), estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Camadas

```
L0  Source     API -> partição canônica, manifesto com sha256, validate --strict
L1  RAW        partição byte-idêntica no MinIO/S3, checksum conferido pós-PUT
L2  Silver     parquet tipado + modelo de variação de preço
```

## Requisitos

Python 3.12, Docker com Compose v2. `make venv` cria o ambiente da plataforma.

## Uso

```bash
make up            # sobe o MinIO e cria os buckets
make venv          # cria platform/.venv e instala a plataforma
make daily         # extract -> validate -> land -> verify-landing -> silver
make test          # 145 testes da Source + 19 da plataforma, ambos sem rede
make status        # containers e contagem de objetos nos buckets
```

`make daily` é **idempotente**: uma partição já completa não é reextraída (é imutável), e
objetos já aterrissados com o checksum esperado são pulados, não reenviados.

Alvos individuais aceitam `DATE=` e `WH=`:

```bash
make land DATE=2026-08-16 WH=mad1
make verify-landing DATE=2026-08-16
```

## Estrutura

```
.
├── ARCHITECTURE.md                     # ADR: o que não entrou, e o gatilho de cada um
├── Makefile                            # ponto de entrada da plataforma
├── sources/mercadona-catalog-source/   # a Source, FROZEN, dependencies = []
├── platform/
│   ├── pyproject.toml                  # boto3, duckdb, dbt-core, dbt-duckdb
│   ├── src/retail_platform/
│   │   ├── manifest.py                 # o contrato do consumidor, em código
│   │   ├── land.py                     # partição -> object storage, verificado
│   │   ├── verify.py                   # releitura e reconferência independentes
│   │   └── cli.py                      # land / verify-landing
│   ├── dbt/models/silver/              # 4 modelos
│   ├── dbt/tests/                      # 7 testes singulares
│   └── tests/                          # 19 testes, sem rede
├── orchestration/airflow/dags/         # o DAG diário
├── infra/docker-compose.yml            # MinIO + criação de buckets
└── data/                               # scratch da extração, fora do versionamento
```

## L1 — RAW no object storage

```
s3://retail-raw/mercadona_catalog_api/ingestion_date=YYYY-MM-DD/wh=<wh>/
    categories/categories.json
    catalog/category_id=<id>.json      (151 objetos)
    _manifest.json  _run.log
    _SUCCESS                            <- último objeto gravado
```

A partição sobe **byte a byte idêntica**: é a canonicalização feita pela Source que torna o
sha256 do manifesto verificável ponta a ponta, e um `.tar` destruiria a conferência por
objeto e a leitura direta pelo DuckDB.

- `_SUCCESS` por último — um upload interrompido nunca parece completo.
- sha256 do arquivo local conferido **antes** do PUT: a plataforma não propaga corrupção,
  e não assume que `validate` rodou.
- `ChecksumSHA256` declarado no PUT: o servidor recusa o objeto se os bytes divergirem.
- Versioning ligado no bucket RAW.

## L2 — Silver

| Modelo | Grão | Linhas/partição |
|---|---|---|
| `raw_manifest` | (ingestion_date, warehouse) | 1 |
| `silver_category` | (…, category_id) | 151 |
| `silver_product_price` | (…, category_id, subgroup_id, source_product_id) | ~4.600 |
| `silver_price_change` | (…, source_product_id) vs partição anterior | ~4.330 |

`raw_manifest` existe para que **"o Silver perdeu linha?" seja um teste**, não um script
solto: a contagem derivada é reconciliada contra `totals` declarado pela Source.

`silver_price_change` é o único modelo que cruza datas. Existe porque `price_decreased` é
**falso em 100% das linhas** nas três partições, enquanto 152 preços mudaram entre 08-16 e
08-24 — o campo que a fonte oferece para sinalizar variação não sinaliza nada. Ele compara
cada partição com a **anterior existente** (`lag` sobre a sequência de partições, não
aritmética de data), o que o mantém correto no vão de 8 dias.

### Grão: por que não é `source_product_id`

Um produto aparece em mais de uma categoria e em mais de um subgrupo — ~270 linhas
repetidas por partição. É semântica da fonte, preservada de propósito. Um teste `unique` no
id isolado falharia por desenho, não por defeito, então a unicidade é testada na chave
composta.

`source_product_id` é a **chave da fonte**, não identidade de negócio. A coluna
`name_seen_before` marca os ids novos cujo `display_name` já existia na partição anterior
(6 casos medidos) como fila de revisão, em vez de tratá-los em silêncio como produto novo.

## Verificação

```bash
make test          # 164 testes sem rede (145 Source + 19 plataforma)
make silver        # dbt build: 4 modelos + 40 testes
```

Números conhecidos, que servem de critério de aceitação:

| | 2026-08-15 | 2026-08-16 | 2026-08-24 |
|---|---|---|---|
| linhas | 4.600 | 4.599 | 4.581 |
| produtos únicos | 4.329 | 4.328 | 4.311 |
| objetos no RAW | 155 | 155 | 155 |

| Janela | Preços alterados | Ids fora | Ids dentro | Nome já existia |
|---|---|---|---|---|
| 08-15 → 08-16 | 17 | 2 | 1 | 1 |
| 08-16 → 08-24 | 152 | 33 | 16 | 5 |

Cada verificação foi provada **capaz de falhar**: adulterar um byte no destino reprova o
`verify-landing` com exit 1; remover um objeto de catálogo reprova o teste de reconciliação.

## Notas operacionais

**Backfill não existe.** A API serve apenas o preço de hoje. Extrair para uma data passada
gravaria os preços de hoje sob a chave daquela data — dado silenciosamente errado. Os dias
08-17 a 08-23 estão perdidos de forma irrecuperável. Ver
[ARCHITECTURE.md](ARCHITECTURE.md).

**Uma extração por dia, por armazém.** A partição é imutável e o `robots.txt` do host
declara `Disallow: /api`. O pool `mercadona_api` com 1 slot serializa as requisições porque
o throttle da Source é por processo — dois `extract` concorrentes dobram a taxa real.

**Airflow.** O DAG está em
[orchestration/airflow/dags/](orchestration/airflow/dags/mercadona_catalog_daily.py) e não
sobe no compose. Para exercitá-lo:

```bash
export AIRFLOW_HOME=/tmp/airflow RETAIL_REPO_ROOT=$PWD
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/orchestration/airflow/dags
orchestration/.venv/bin/airflow db migrate
orchestration/.venv/bin/airflow pools set mercadona_api 1 "throttle por processo"
orchestration/.venv/bin/airflow dags test mercadona_catalog_daily $(date -u +%F)
```
