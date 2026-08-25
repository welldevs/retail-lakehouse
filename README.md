# Retail Lakehouse — Mercadona + INE

Plataforma de dados sobre a **Mercadona Catalog Source** (catálogo de retail), a **INE
Population Source** (população por província) e a **INE Callejero Source** (geografia
oficial — seções censitárias, ruas, núcleos populacionais): preserva o snapshot RAW em
object storage e produz o Silver tipado em parquet, orquestrado por Airflow.

Cada Source é um componente **congelado e independente** — a da Mercadona em
[sources/mercadona-catalog-source/](sources/mercadona-catalog-source/), a de população
em [sources/ine-population-source/](sources/ine-population-source/), a do Callejero em
[sources/ine-callejero-source/](sources/ine-callejero-source/) — com seu próprio
contrato físico (`CONTRACT.md`) e **zero dependências de runtime**. Esta plataforma as
consome pelo contrato físico — nunca importando o código de nenhuma delas. Ver
[ARCHITECTURE.md § "Segunda source: população do INE"](ARCHITECTURE.md) para por que são
pacotes irmãos, não uma abstração compartilhada.

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
cp .env.example .env && make secrets   # chaves aleatórias; o compose recusa subir sem elas

make up            # sobe o MinIO e cria os buckets (só o plano de dados)
make venv          # cria platform/.venv e instala a plataforma
make daily         # Mercadona: extract -> validate -> land -> verify-landing -> silver
make ine-refresh   # INE população: mesma cadeia, sob demanda — ver "Segunda source" abaixo
make callejero-refresh  # INE Callejero: sem API, incorpora arquivos já baixados — ver "Terceira source"
make test          # suite de cada Source + da plataforma, tudo sem rede
make status        # containers e contagem de objetos nos buckets

make airflow       # Postgres + scheduler + webserver em :8080 (admin/admin)
make query         # consulta o Silver
```

`make up` sobe **apenas** o MinIO: o Airflow custa ~2 GB de RAM e não é necessário para
iterar num modelo dbt ou rodar `make daily`/`make ine-refresh` à mão.

`make daily` e `make ine-refresh` são **idempotentes**: uma partição já completa não é
reextraída (é imutável), e objetos já aterrissados com o checksum esperado são pulados,
não reenviados.

Alvos individuais aceitam `DATE=` e `WH=` (Mercadona) ou `DATE=` e `TABLES=` (INE):

```bash
make land DATE=2026-08-16 WH=mad1
make verify-landing DATE=2026-08-16
make ine-land DATE=2026-08-16
```

## Estrutura

```
.
├── ARCHITECTURE.md                     # ADR: o que não entrou, e o gatilho de cada um
├── Makefile                            # ponto de entrada da plataforma
├── sources/
│   ├── mercadona-catalog-source/       # Source do catálogo, FROZEN, dependencies = []
│   ├── ine-population-source/          # Source de população, FROZEN, dependencies = []
│   └── ine-callejero-source/           # Source do Callejero, FROZEN, dependencies = [] — sem API
├── .env.example                        # copie para .env; credenciais só de desenvolvimento
├── platform/
│   ├── pyproject.toml                  # boto3, duckdb, dbt-core, dbt-duckdb
│   ├── src/retail_platform/
│   │   ├── config.py                   # endpoint e credenciais, do ambiente
│   │   ├── manifest.py                 # o contrato do consumidor, em código — genérico por source
│   │   ├── land.py                     # partição -> object storage, verificado
│   │   ├── verify.py                   # releitura e reconferência independentes
│   │   ├── query.py                    # conexão configurada + secret do DuckDB
│   │   └── cli.py                      # land / verify-landing / query / duckdb-secret / has-data
│   ├── dbt/seeds/
│   │   └── warehouse_province_map_seed.csv  # wh -> província/município, códigos oficiais do INE
│   ├── dbt/models/silver/
│   │   ├── warehouse_province_map.sql   # passagem do seed para o object storage
│   │   ├── mercadona/                   # 4 modelos, grão por wh
│   │   ├── ine_population/              # série de população por província
│   │   └── ine_callejero/               # seções, núcleos, ruas — geografia oficial
│   ├── dbt/tests/                      # testes singulares
│   └── tests/                          # sem rede (duplo de S3 em memória)
├── orchestration/airflow/dags/
│   ├── mercadona_catalog_daily.py      # cron diário
│   ├── ine_population_on_demand.py     # sem cron — disparo manual
│   └── ine_callejero_on_demand.py      # sem cron — disparo manual, sem API
├── infra/
│   ├── docker-compose.yml              # MinIO + mc + Postgres + scheduler + webserver
│   └── Dockerfile.airflow              # imagem do orquestrador: duas runtimes
└── data/                               # scratch da extração, fora do versionamento
    ├── mercadona/                       # ingestion_date=…/wh=…/
    ├── ine/                             # ingestion_date=…/
    └── callejero/                       # ingestion_date=…/
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

## Segunda source: população do INE

[sources/ine-population-source/](sources/ine-population-source/) extrai séries de
população por província da API pública Tempus3 do INE — pensado para eventualmente cruzar
com os dados de retail por armazém (mad1/bcn1/vlc1/svq1 = Madrid/Barcelona/Valência/
Sevilha), embora esse cruzamento (Gold) ainda não exista.

Pacote irmão da Mercadona Catalog Source, não uma extensão dela — mesmo padrão (frozen,
`dependencies = []`, contrato físico próprio), estruturalmente independente porque as duas
fontes não têm nada em comum além de serem sources deste monorepo. Ver
[CONTRACT.md](sources/ine-population-source/CONTRACT.md) e
[README.md](sources/ine-population-source/README.md) da source para os detalhes.

**Sem cron.** Ao contrário do DAG diário da Mercadona, `ine_population_on_demand` tem
`schedule=None` — o INE publica de forma irregular (às vezes meses entre atualizações), e
um cron fixo daria uma garantia de frescor que a fonte não tem. Dispare com
`make ine-trigger` (Airflow) ou `make ine-refresh` (direto, sem orquestrador).

Aterrissa nos **mesmos buckets** `retail-raw`/`retail-lakehouse`, com seu próprio prefixo
(`ine_population_api/`) — nenhum bucket novo foi necessário. O modelo Silver
(`silver_ine_population_series`) fica de fora do `dbt build` automaticamente enquanto essa
source não tiver aterrissado nada (`make silver` confere com `retail-platform has-data`
antes de decidir), para que um clone novo do repositório — ou o dia a dia de quem só opera
a Mercadona — não quebre por causa de uma source que ainda não rodou.

## Terceira source: Callejero do INE

[sources/ine-callejero-source/](sources/ine-callejero-source/) incorpora **geografia
oficial do INE** — seções censitárias, unidades populacionais (núcleos) e ruas — para os
municípios dos 4 warehouses. Junto com `warehouse_province_map` (seed, abaixo), é o que
permite ir de `warehouse → província → município` (já existia) até
`warehouse → município → distrito/seção → rua`.

**Sem API.** Diferente das outras duas sources, o Callejero só é distribuído pelo INE
para download manual, semestral. `extract` não faz nenhuma requisição de rede — incorpora
arquivos que já foram baixados e colocados num diretório local (`--in`), preservando os
bytes originais (ISO-8859-1, sem conversão). Mesmo assim é um pacote irmão completo:
`dependencies = []`, contrato físico próprio, particionado só por `ingestion_date` (sem
eixo de warehouse nem de província). Ver
[CONTRACT.md](sources/ine-callejero-source/CONTRACT.md) — inclui o layout de coluna de
cada arquivo, medido contra os dados reais, já que o INE não anexa documentação de layout
ao download.

**`TRAM` fica de fora.** Os downloads trazem 5 arquivos por província; só `SECC`, `UP`,
`VIAS` e `PSEU` são incorporados. `TRAM` (trechos de rua com faixa de numeração — o mais
pesado e menos necessário até aqui) pode entrar numa rodada futura se a granularidade de
número de porta vier a ser necessária.

Dispare com `make callejero-trigger` (Airflow) ou `make callejero-refresh` (direto), com
os arquivos já baixados em `CALLEJERO_IN` (default: `temp/` na raiz). Mesma lógica de
`has-data` das outras sources: os 4 modelos `silver_callejero_*` ficam de fora do
`dbt build` até que algo tenha sido aterrissado.

**`warehouse_province_map`** (`platform/dbt/seeds/warehouse_province_map_seed.csv`) é o
seed que resolve `wh → província/município` com códigos oficiais do INE — derivado do
Callejero durante o desenvolvimento (ver
[scripts/derive_warehouse_province_map.py](scripts/derive_warehouse_province_map.py)),
não gerado por esta source em tempo de execução. A source do Callejero **não sabe que
warehouses existem** — produz geografia pura do INE; a união com `warehouse_province_map`
acontece via `JOIN` no Silver/Gold, nunca dentro da source.

## Consultar o Silver

O dado real é o **parquet no object storage**. O arquivo `platform/dbt/retail.duckdb`
guarda apenas **views** apontando para ele — uma por modelo materializado, nenhum dado
próprio — está fora do versionamento, e `make clean-duckdb` o apaga sem perda.

Por isso abrir o arquivo com um cliente DuckDB qualquer **falha** com `NoSuchBucket`: a
sessão nova não conhece o endpoint nem a credencial, e o DuckDB tenta a AWS de verdade.
Duas saídas:

```bash
make query                                    # resumo por partição
make query SQL="select * from silver_price_change where name_seen_before"

make duckdb-secret                            # grava o secret uma vez...
duckdb platform/dbt/retail.duckdb             # ...e daí qualquer cliente funciona
```

`make duckdb-secret` grava um secret do DuckDB em `~/.duckdb/stored_secrets` a partir do
`.env`. Depois disso, qualquer cliente — CLI, DBeaver, notebook — abre o arquivo e consulta
as views sem configurar nada.

> **O DuckDB é single-writer.** Um cliente com o arquivo aberto em leitura-escrita (o
> DBeaver faz isso por padrão) **bloqueia o `make silver`**. O alvo detecta isso e falha com
> uma mensagem acionável em vez de um traceback. Três saídas:
>
> ```bash
> # 1. fechar a conexão no cliente, ou abri-la em modo somente-leitura
> # 2. escrever o estado em outro lugar:
> make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb
> ```
>
> **`make query` não é afetado:** ele monta as views em memória diretamente sobre o parquet
> e não abre o arquivo. Um *writer* bloqueia leitores também, então depender do arquivo
> para consultar seria depender de ninguém ter esquecido uma janela aberta.
>
> O DAG também não é afetado: o `DUCKDB_PATH` do orquestrador vive dentro do container,
> não no repositório montado.
>
> Isto não põe dado em risco: o arquivo só guarda views — o dado é o parquet no object
> storage — e `make clean-duckdb` o recria.

## Verificação

```bash
make test          # 189 testes sem rede (145 Source + 44 plataforma)
make silver        # dbt build: 4 modelos + 36 testes (40 nós)
```

Números conhecidos, que servem de critério de aceitação:

| partição | linhas | produtos únicos | objetos no RAW |
|---|---|---|---|
| `mad1` 08-15 | 4.600 | 4.329 | 155 |
| `mad1` 08-16 | 4.599 | 4.328 | 155 |
| `mad1` 08-24 | 4.581 | 4.311 | 155 |
| `bcn1` 08-24 | 4.587 | 4.320 | 155 |
| `vlc1` 08-24 | 4.599 | 4.328 | 155 |
| `svq1` 08-24 | 4.558 | 4.287 | 155 |

Todas com o mesmo `schema_fingerprint` (`37a3d95d…`): a forma da resposta não varia por
armazém nem por data. `silver_price_change` tem linhas **apenas** para `mad1` — os armazéns
com uma só partição entram com zero, porque não há anterior com que comparar.

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

**O container roda com o seu UID, não com o do Airflow.** A Source grava os arquivos com
modo `600` (consequência de `tempfile.mkstemp()` na escrita atômica), então um container
rodando como o usuário `airflow` (50000) padrão da imagem **não consegue ler a partição**.
`AIRFLOW_UID` no `.env` resolve — gere com `id -u`. Não há fallback por grupo.

**Quatro armazéns.** O DAG cobre `mad1`, `bcn1`, `vlc1` e `svq1` — quatro cidades, ~608
requisições/dia, ~15 min. `wh` altera sortimento **e** preço: entre `mad1` e `bcn1`, dos
4.040 produtos comuns, **124 (3,1%) têm preço diferente**, até ±24%; e 551 produtos (12,0%)
existem só num dos dois. A fonte serve 7 armazéns — a escolha destes quatro é por divergência
de sortimento medida par a par, e `alc1` ficou de fora por duplicar `vlc1`. Critério e matriz
completa em [ARCHITECTURE.md](ARCHITECTURE.md).

**`wh` inválido não falha — cai em `vlc1`.** A fonte devolve `200` para qualquer código
desconhecido. Um erro de digitação em `WAREHOUSES` produz uma partição rotulada com o código
errado contendo dados de Valência, internamente consistente e invisível para os testes.
Confira o código antes de adicionar um armazém.

**Uma extração por dia, por armazém.** A partição é imutável e o `robots.txt` do host
declara `Disallow: /api`. O pool `mercadona_api` com 1 slot serializa as requisições porque
o throttle da Source é por processo — dois `extract` concorrentes dobram a taxa real.

**Airflow.** `make airflow` sobe o stack completo — Postgres para o metadata, scheduler
com **LocalExecutor** e webserver em `:8080` (`admin`/`admin`).

```bash
make airflow           # builda a imagem e sobe o stack
make airflow-trigger   # despausa e dispara o DAG de hoje
make airflow-logs      # acompanha o scheduler
make airflow-down      # derruba só o Airflow, mantendo o MinIO de pé
```

O `LocalExecutor` é escolha deliberada, e não conveniência: é o único executor em que o
pool `mercadona_api` de 1 slot significa algo. Com `SequentialExecutor` a serialização
aconteceria por acidente, não pelo mecanismo — e o mecanismo é o que protege o throttle da
fonte.

### Duas runtimes na imagem, de propósito

O Airflow e o `dbt-core` fixam versões incompatíveis de `jinja2`, `click` e `pydantic`.
Em vez de brigar com isso, a imagem tem duas:

| Runtime | O que roda | Por que cabe ali |
|---|---|---|
| `/usr/local/bin/python` | Airflow + **a Source** | A Source tem `dependencies = []`: não existe pacote de terceiros para conflitar |
| `/opt/platform-venv` | `boto3`, `duckdb`, `dbt-duckdb` | Isolado dos pins do Airflow |

**Nenhum código vai para a imagem.** O repositório é montado em `/opt/retail-lakehouse` e
alcançado por `PYTHONPATH`, então editar um modelo dbt ou um módulo da plataforma não exige
rebuild — só as dependências ficam na imagem.

Dentro da rede do compose o endpoint do MinIO é `minio:9000`, não `localhost:9000`. O
compose sobrescreve `S3_ENDPOINT` para os serviços do Airflow, e isso funciona porque
`config.load_dotenv()` **não** sobrepõe variável já presente no ambiente — o `.env` é
default de desenvolvimento, não autoridade.
