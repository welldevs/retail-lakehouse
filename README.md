# Retail Lakehouse — Mercadona + INE

Plataforma de dados sobre a **Mercadona Catalog Source** (catálogo de retail), a **INE
Population Source** (população por província e por município) e a **INE Callejero
Source** (geografia oficial — seções censitárias, ruas, núcleos populacionais): preserva
o snapshot RAW em object storage e produz o Silver tipado em parquet, orquestrado por
Airflow. Sobre essa base, a **Simulated OLTP Source** gera clientes sintéticos
geograficamente coerentes — o cliente é inventado, o endereço onde ele mora não — e a
**Simulated Orders Source** gera pedidos como **log de eventos**: o pedido é inventado; quem
compra, o que se compra e quanto custa não.

Cada Source é um componente **congelado e independente** — a da Mercadona em
[sources/mercadona-catalog-source/](sources/mercadona-catalog-source/), a de população
em [sources/ine-population-source/](sources/ine-population-source/), a do Callejero em
[sources/ine-callejero-source/](sources/ine-callejero-source/), a de OLTP simulado em
[sources/simulated-oltp-source/](sources/simulated-oltp-source/), a de pedidos em
[sources/simulated-orders-source/](sources/simulated-orders-source/) — com seu próprio
contrato físico (`CONTRACT.md`) e **zero dependências de runtime**. Esta plataforma as
consome pelo contrato físico — nunca importando o código de nenhuma delas. Ver
[ARCHITECTURE.md § "Segunda source: população do INE"](ARCHITECTURE.md) para por que são
pacotes irmãos, não uma abstração compartilhada.

As decisões de arquitetura, e o gatilho de cada tecnologia ainda ausente
(Iceberg, Kafka, Spark, Snowflake), estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Camadas

```
L0  Source     API -> partição canônica, manifesto com sha256, validate --strict
               5 pacotes FROZEN; 4 entregam fotografia, simulated_orders entrega LOG
L1  RAW        partição byte-idêntica no MinIO/S3, checksum conferido pós-PUT
    ┌──── plano operacional, sob demanda (make stream-up) ──────────────────┐
    │ OLTP   Postgres: orders, order_line, outbox                           │
    │        estado + evento NA MESMA transação, uma transação por evento   │
    │ Broker Kafka retail.orders.events.v1 — 4 partições, key = order_id    │
    │ Read   Postgres OU Iceberg: live_order_state                          │
    │ model  dedup por (order_id, sequence_no); DOIS escritores no Iceberg,  │
    │        com fusão monotônica — o que não avança a sequência é descartado│
    └───────────────────────────────────────────────────────────────────────┘
L2  Silver     parquet tipado + fold de pedidos + variação de preço · DuckDB · 4,1 M linhas
────────────── fronteira física: COPY INTO, nunca ref() ──────────────
L3  Stage      espelho 1:1 de um RECORTE do Silver               · Snowflake · 407 k linhas
L4  Gold       DIM_* / FACT_* conformados, SCD2 da história
L5  Mart       MART_*, com o grão declarado em cada tabela
```

O Snowflake **não** recebe o Silver inteiro: atravessam 406.855 das 4.087.507 linhas
(**9,95%**). A razão não é propriedade do pipeline — é função de quanto de cada source cai
dentro do escopo. A população do INE é nacional e entrega 1,8%; os pedidos nascem dentro das
quatro AUFs e entregam ~100%. Ver [ARCHITECTURE.md § "Fase 2"](ARCHITECTURE.md).

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
make oltp-export-reference && make oltp-refresh-all  # clientes sintéticos — ver "Quarta source"
make stream-up && make orders-apply-all  # log -> OLTP + outbox — ver "OLTP e outbox" abaixo
make orders-publish && make orders-project  # outbox -> Kafka -> read model
make orders-rebuild-projection && make orders-reconcile  # o 2o escritor, e os 3 folds
make warehouse-refresh  # recorte -> Snowflake -> DIM/FACT/MART — ver "Warehouse analítico"
make test          # suite de cada Source + da plataforma, tudo sem rede
make status        # containers e contagem de objetos nos buckets

make airflow       # Postgres + scheduler + webserver em :8080 (admin/admin)
make query         # consulta o Silver
```

`make up` sobe **apenas** o MinIO: o Airflow custa ~2 GB de RAM e não é necessário para
iterar num modelo dbt ou rodar `make daily`/`make ine-refresh`/`make callejero-refresh` à
mão. O OLTP de pedidos também não sobe aí — vive sob o profile `stream` do compose e só
existe depois de `make stream-up`.

`make daily`, `make ine-refresh` e `make callejero-refresh` são **idempotentes**: uma
partição já completa não é reextraída (é imutável, mesma guarda `_SUCCESS` nas três), e
objetos já aterrissados com o checksum esperado são pulados, não reenviados.

Alvos individuais aceitam `DATE=` e `WH=` (Mercadona), `DATE=` e `TABLES=` (INE população),
ou `DATE=`, `CALLEJERO_IN=` e `CALLEJERO_PROVINCES=` (INE Callejero):

```bash
make land DATE=2026-08-16 WH=mad1
make verify-landing DATE=2026-08-16
make ine-land DATE=2026-08-16
make callejero-land DATE=2026-08-16 CALLEJERO_IN=temp CALLEJERO_PROVINCES=08,28,41,46
```

**Espaço em disco.** `data/` é scratch de extração e nada nunca é apagado sozinho — uma
partição do INE ocupa entre 264 e 384 MB. Depois de `land` + `verify-landing`, o object
storage é a verdade e a cópia local é redundante:

```bash
make data-usage                                          # quanto cada source ocupa
make prune-local PARTITION=data/ine/ingestion_date=2026-08-25
```

`prune-local` faz **duas** conferências antes de remover — a cópia local contra o próprio
manifesto, e o destino contra esse mesmo manifesto — e recusa se qualquer uma falhar. Não
há `--force`, e ele nunca é encadeado num `*-refresh`: apagar dado é decisão de quem opera,
não efeito colateral de pipeline.

## Estrutura

```
.
├── ARCHITECTURE.md                     # ADR: o que não entrou, e o gatilho de cada um
├── Makefile                            # ponto de entrada da plataforma
├── sources/
│   ├── mercadona-catalog-source/       # Source do catálogo, FROZEN, dependencies = []
│   ├── ine-population-source/          # Source de população, FROZEN, dependencies = []
│   ├── ine-callejero-source/           # Source do Callejero, FROZEN, dependencies = [] — sem API
│   ├── simulated-oltp-source/          # Clientes sintéticos, FROZEN, dependencies = [] — derivada do Silver
│   └── simulated-orders-source/        # Pedidos como LOG DE EVENTOS, FROZEN — derivada do Silver
├── .env.example                        # copie para .env; credenciais só de desenvolvimento
├── .env.snowflake.example              # copie para .env.snowflake; identidade da conta, sem segredo
├── docs/warehouse-evidence/            # a execução real no Snowflake, datada — gerada, não escrita
├── docs/stream-evidence/               # OLTP, broker e projeção vivos, datado — gerado, não escrito
├── platform/
│   ├── pyproject.toml                  # boto3, duckdb, dbt-core, dbt-duckdb, dbt-snowflake
│   ├── src/retail_platform/
│   │   ├── config.py                   # endpoint e credenciais, do ambiente
│   │   ├── manifest.py                 # o contrato do consumidor, em código — genérico por source
│   │   ├── land.py                     # partição -> object storage, verificado
│   │   ├── verify.py                   # releitura e reconferência independentes
│   │   ├── query.py                    # conexão configurada + secret do DuckDB
│   │   ├── oltp_reference.py           # Silver -> 3 JSON planos para a source de OLTP simulado
│   │   ├── orders_reference.py         # Silver -> 5 JSON planos para a source de pedidos
│   │   ├── demand_profile.py           # de-para + benchmark do MAPA -> pesos de demanda
│   │   ├── demand_check.py             # reality check ANTES | MAPA | ALVO | DEPOIS
│   │   ├── orders_oltp.py              # o OLTP de pedidos: estado + outbox NA MESMA transacao
│   │   ├── orders_stream.py            # produtor, consumidor e read model; a semantica de entrega
│   │   ├── orders_projection.py        # a projecao em Iceberg: dois escritores, fusao monotonica
│   │   ├── snowflake_export.py         # o RECORTE: Silver -> parquet (10%), sem regra de negócio
│   │   ├── snowflake_load.py           # transporte: DDL, PUT em stage interno, COPY INTO, papéis
│   │   ├── snowflake_evidence.py       # observa o destino e escreve a evidência datada
│   │   ├── stream_evidence.py          # observa os TRÊS planos vivos; ausência é declarada
│   │   ├── silver_gate.py              # o que excluir do dbt build do Silver — UM lugar só
│   │   └── cli.py                      # land / verify-landing / query / prune-local / has-data
│   │                                   # / export-oltp-reference / export-orders-reference
│   │                                   # / orders-oltp-ddl / orders-oltp-init
│   │                                   # / orders-apply / orders-outbox
│   │                                   # / export-snowflake / snowflake-ddl
│   │                                   # / snowflake-bootstrap / load-snowflake / snowflake-evidence
│   │                                   # / stream-evidence / silver-build
│   ├── dbt/seeds/
│   │   ├── warehouse_province_map_seed.csv  # wh -> província/município (sede), códigos do INE
│   │   ├── warehouse_service_area_seed.csv  # wh -> N municípios da mesma AUF (INE)
│   │   ├── order_premises_seed.csv          # premissas do gerador de pedidos, TODAS `synthetic`
│   │   ├── ine_municipality_codes_seed.csv  # nome (Tempus3) -> código de município, 08/28/41/46
│   │   └── ine_ambiguous_series_seed.csv    # série -> código oficial, para nomes homônimos na Espanha
│   ├── dbt/macros/                     # generate_schema_name: GOLD/MART absolutos, sem prefixo
│   ├── dbt/models/silver/              # target dev (duckdb) — 21 modelos
│   │   ├── warehouse_province_map.sql   # passagem do seed para o object storage
│   │   ├── warehouse_service_area.sql   # idem, para a área de atendimento
│   │   ├── order_premises.sql           # idem, para as premissas — atravessa até o warehouse
│   │   ├── mercadona/                   # 4 modelos, grão por wh
│   │   ├── ine_population/              # série de população por província E por município
│   │   ├── ine_callejero/               # seções, núcleos, ruas — geografia oficial
│   │   ├── simulated_oltp/              # clientes sintéticos + manifesto com a linhagem
│   │   └── simulated_orders/            # o fold do log: evento, pedido, linha, manifesto
│   ├── dbt/models/warehouse/           # target snowflake — 21 modelos, ligados por source()
│   │   ├── sources.yml                  # as 13 tabelas STAGE: a fronteira, declarada
│   │   ├── gold/                        # 6 DIM + 8 FACT, SCD2 derivado da história
│   │   └── mart/                        # 7 marts, grão no cabeçalho de cada um
│   ├── dbt/tests/                      # testes singulares do Silver
│   ├── dbt/tests/warehouse/            # idem do Gold/Mart (separados: ref() cruzado não compila)
│   └── tests/                          # sem rede (duplos de S3 e de Postgres em memória)
│       ├── fake_s3.py                   # valida o ChecksumSHA256 como o servidor validaria
│       ├── fake_pg.py                   # GRAVA a fronteira da transação: o que só o motor
│       │                                # prova (que rollback desfaz) fica fora, de propósito
│       ├── fake_kafka.py                # diário COMPARTILHADO: a ordem entre escrever e
│       │                                # commitar o offset é a semântica de entrega
│       └── fake_iceberg.py              # conflito sob demanda: exercita a fusão monotônica
├── orchestration/airflow/dags/         # 6 DAGs: 5 sources + warehouse_load
│   ├── mercadona_catalog_daily.py      # cron diário
│   ├── ine_population_on_demand.py     # sem cron — disparo manual
│   ├── ine_callejero_on_demand.py      # sem cron — disparo manual, sem API
│   ├── simulated_oltp_customers.py     # sem cron — a base muda quando alguém decide
│   ├── simulated_orders_events.py      # sem cron — o streaming NÃO é tarefa de DAG
│   └── warehouse_load.py               # a única que atravessa a fronteira entre dois motores
├── streamlit/                          # painel de CONFERÊNCIA sobre o MART (RETAIL_READER)
│   ├── indicators.py                   # a FONTE ÚNICA: SQL e explicação juntos
│   ├── CONTRACT.md                     # GERADO de indicators.py — o doc de conferência
│   ├── contract.py                     # o gerador; não importa nada que conecte
│   ├── connection.py                   # sessão RETAIL_READER + `use secondary roles none`
│   ├── smoke.py                        # roda o app de verdade e exige zero exceção
│   └── app.py                          # a interface, 6 grupos + "Fora de alcance"
├── scripts/
│   ├── prove_oltp_atomicity.py         # injeta falha no BANCO e prova que os dois lados caem
│   ├── prove_stream_semantics.py       # reproduz a janela de duplicação e prova o replay
│   ├── spike_iceberg_duckdb.py         # o experimento FECHADO, rodado antes do Marco 6
│   ├── prove_iceberg_projection.py     # concorrência, fusão monotônica, snapshot isolation
│   └── prove_warehouse_orders_tests.py # injeta o defeito que cada teste diz pegar, no dado real
├── infra/
│   ├── docker-compose.yml              # MinIO + mc + Postgres + scheduler + webserver
│   │                                   # + oltp-postgres e kafka (profile `stream`); o
│   │                                   # catalogo Iceberg mora no proprio oltp-postgres
│   └── Dockerfile.airflow              # imagem do orquestrador: duas runtimes
└── data/                               # scratch da extração, fora do versionamento
    ├── mercadona/                       # ingestion_date=…/wh=…/
    ├── ine/                             # ingestion_date=…/
    ├── callejero/                       # ingestion_date=…/
    ├── oltp-reference/                  # ingestion_date=…/ (insumo efêmero, não é partição)
    └── oltp/                            # ingestion_date=…/wh=…/
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
população da API pública Tempus3 do INE — hoje **duas granularidades**, mesmo mecanismo
genérico de fetch (só muda o `table_id`, configurado fora da Source): **por província**
(`31304`, com idade+sexo) e **por município** (`29005`, só sexo, sem idade — extensão
adicionada para dar densidade real por município, já que "Valencia" em `31304` é a
província inteira, 2,6 milhões de habitantes, não a cidade). Pensado para eventualmente
cruzar com os dados de retail por armazém (mad1/bcn1/vlc1/svq1 =
Madrid/Barcelona/Valência/Sevilha), embora esse cruzamento (Gold) ainda não exista. Ver
[ARCHITECTURE.md § "Extensão: população por município (Fase A)"](ARCHITECTURE.md) para o porquê
de estender esta Source em vez de criar uma quarta, e por que faixa etária por município
ficou de fora desta rodada.

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
(`ine_population_api/`) — nenhum bucket novo foi necessário. Os modelos Silver
(`silver_ine_population_series` e `silver_ine_population_by_municipality`) ficam de fora
do `dbt build` automaticamente enquanto essa source não tiver aterrissado nada (`make
silver` confere com `retail-platform has-data` antes de decidir), para que um clone novo
do repositório — ou o dia a dia de quem só opera a Mercadona — não quebre por causa de
uma source que ainda não rodou.

**O nome do município não é chave.** A tabela 29005 traz só o nome por extenso, e o
payload é nacional (~8.200 municípios): 18 nomes se repetem entre províncias diferentes,
com texto idêntico. Três afetam as 4 províncias desta plataforma — Arroyomolinos, El Molar
e Torrent — e um join por nome traria junto a série homônima de Cáceres, Tarragona e
Girona. A desambiguação vem do **código oficial do INE**, obtido série a série via
`VALORES_SERIE/{COD}` e materializado em `ine_ambiguous_series_seed`
([scripts/derive_ambiguous_series.py](scripts/derive_ambiguous_series.py)); o invariante
"uma série por município, sexo e ano" é garantido por teste dbt. Ver
[ARCHITECTURE.md § "Fanout de homônimo no Silver de população"](ARCHITECTURE.md).

## Terceira source: Callejero do INE

[sources/ine-callejero-source/](sources/ine-callejero-source/) incorpora **geografia
oficial do INE** — seções censitárias, unidades populacionais (núcleos), ruas, pseudovias
e tramos de via com **código postal** — para os municípios dos 4 warehouses. Junto com
`warehouse_province_map` (seed, abaixo), é o que permite ir de
`warehouse → província → município` (já existia) até
`warehouse → município → distrito/seção → rua → CEP`.

**Sem API.** Diferente das outras duas sources, o Callejero só é distribuído pelo INE
para download manual, semestral. `extract` não faz nenhuma requisição de rede — incorpora
arquivos que já foram baixados e colocados num diretório local (`--in`), preservando os
bytes originais (ISO-8859-1, sem conversão). Mesmo assim é um pacote irmão completo:
`dependencies = []`, contrato físico próprio, particionado só por `ingestion_date` (sem
eixo de warehouse nem de província). Ver
[CONTRACT.md](sources/ine-callejero-source/CONTRACT.md) — inclui o layout de coluna de
cada arquivo, medido contra os dados reais, já que o INE não anexa documentação de layout
ao download.

**Os 5 arquivos do download são incorporados**, inclusive `TRAM` (tramos de via) — a
única das 5 tabelas que carrega **código postal**, ligando num só registro seção
censitária + entidade/núcleo + via ou pseudovia + CEP + faixa de numeração. É o que
fecha `warehouse → município → rua → CEP` e, onde o núcleo do INE tiver granularidade
(só em Valencia, entre os 4 warehouses — Madrid/Barcelona/Sevilla capital são uma
entidade única sem subdivisão), também `→ bairro/pedania`.

Dispare com `make callejero-trigger` (Airflow) ou `make callejero-refresh` (direto), com
os arquivos já baixados em `CALLEJERO_IN` (default: `temp/` na raiz). Mesma lógica de
`has-data` das outras sources: os 5 modelos `silver_callejero_*` ficam de fora do
`dbt build` até que algo tenha sido aterrissado.

**`warehouse_province_map`** (`platform/dbt/seeds/warehouse_province_map_seed.csv`) é o
seed que resolve `wh → província/município` com códigos oficiais do INE — derivado do
Callejero durante o desenvolvimento (ver
[scripts/derive_warehouse_province_map.py](scripts/derive_warehouse_province_map.py)),
não gerado por esta source em tempo de execução. A source do Callejero **não sabe que
warehouses existem** — produz geografia pura do INE; a união com `warehouse_province_map`
acontece via `JOIN` no Silver/Gold, nunca dentro da source.

**`warehouse_service_area`** (`platform/dbt/seeds/warehouse_service_area_seed.csv`)
responde uma pergunta diferente: não "onde o armazém fica" (1 município), mas "quais
municípios vizinhos fazem parte da mesma região funcional" — ex. Albal, Alaquàs, Mislata
para `vlc1`. Fonte: [Áreas Urbanas Funcionais do
INE](https://www.ine.es/ss/Satellite?L=es_ES&c=INESeccion_C&p=1254735110672&pagename=ProductosYServicios/PYSLayout&param1=PYSDetalleFichaSeccionUA&param3=1259944561392&cid=1259947044694)
(AUF, metodologia oficial única — ≥15% da população empregada comuta pra cidade-núcleo),
não uma lista inventada. Derivado e cross-validado município a município contra o
Callejero real (cada código confirmado contra `SECC`, cada nome vindo do `UP`) em
[scripts/derive_warehouse_service_area.py](scripts/derive_warehouse_service_area.py).
**Limitação conhecida**: a AUF oficial de Madrid tem 38 municípios fora das províncias
já baixadas (Ávila/Guadalajara/Toledo) e a de Barcelona tem 2 (Tarragona) — ficam de
fora da área derivada aqui, porque não há Callejero landado pra cruzar. Sevilla e
Valencia estão 100% contidas na própria província, sem essa lacuna.

## Quarta source: OLTP simulado (Customers)

[sources/simulated-oltp-source/](sources/simulated-oltp-source/) é a primeira source
**derivada**: em vez de trazer dado de fora, consome o Silver que as três anteriores
produziram e gera **clientes sintéticos com endereço real**. Cada cliente nasce numa via
real de um município real, com o CEP real daquele tramo, num município que pertence de
fato à Área Urbana Funcional do seu armazém — nenhum CEP, município ou via é inventado.

```
warehouse → município ponderado pela população municipal observada (INE 29005)
          → tramo UNIFORME entre os candidatos válidos daquele município
          → número da casa dentro da faixa real, respeitando a paridade
          → sexo pela proporção municipal observada; idade pela provincial (proxy)
```

A escolha do tramo é uniforme de propósito: **não existe população por rua** em nenhuma
fonte ingerida aqui, e ponderar tramos inventaria uma distribuição que ninguém mediu.

**Como uma Source FROZEN lê o Lakehouse sem quebrar a fronteira.** Ela não lê. A
plataforma materializa antes o que a Source precisa em três JSON planos
(`make oltp-export-reference`), e a Source os consome só com a stdlib — mesmo precedente
do `extract --in <dir>` do Callejero, um nível antes na cadeia. Nenhum lado importa o
código do outro.

```bash
make oltp-export-reference     # uma vez, depois de callejero-refresh e ine-refresh
make oltp-refresh-all          # mad1, bcn1, svq1, vlc1 (ou oltp-refresh WH=mad1)
```

**Reprodutível por seed**: mesma referência + mesma seed + mesma data produzem
`customers.json` byte a byte idêntico — inclusive sob `PYTHONHASHSEED` diferente, o que é
verificado em subprocesso. `oltp-validate` não confere só checksum: relê a referência e
prova, cliente a cliente, que o endereço bate com a linha de origem e que o município
está na AUF certa. Base atual: **20.000 clientes** (5.000 por armazém), **5.000/5.000
coerentes em cada um**, cobrindo 125 dos 128 municípios da AUF de `mad1`.

**Crescer a base é aditivo.** O gerador consome uma única `random.Random(seed)` em ordem
fixa e nada antes do laço depende de `count`, então os primeiros N clientes de uma geração
maior são byte a byte os mesmos de antes — verificado ponta a ponta (200 → 5.000 preservou
os 200, sha256 conferidos):

```bash
make oltp-refresh-all OLTP_CUSTOMERS_PER_WH=20000 OLTP_OVERWRITE=1
```

Vale com a **mesma seed, mesma referência e mesma data**. Trocar a data preserva a idade e
desloca `birth_year`; trocar a seed troca as pessoas por trás dos mesmos ids. O manifesto
registra as duas coisas em `history`, e é por isso que `DIM_CUSTOMER` é SCD2.

Modelo Silver (`silver_customer`, `silver_oltp_manifest`) e DAG entraram na Fase 2. Ver
[CONTRACT.md](sources/simulated-oltp-source/CONTRACT.md) e
[ARCHITECTURE.md § "Quarta source"](ARCHITECTURE.md).

## Quinta source: pedidos simulados (log de eventos)

```bash
make orders-export-reference ORDERS_FROM=2026-08-24 ORDERS_TO=2026-08-27
make orders-refresh-all      ORDERS_FROM=2026-08-24 ORDERS_TO=2026-08-27
```

Segunda source derivada, e a **primeira que entrega um log em vez de uma fotografia**. A
regra de ouro, deslocada um nível: **o pedido é inventado; quem compra, o que se compra,
quanto custa e onde mora não.** Cliente vem de `silver_customer`, produto e preço vêm de
`silver_product_price` do mesmo armazém na mesma data.

### A partição não contém estado, e isso é deliberado

```
data/orders/ingestion_date=2026-08-24/wh=mad1/
├── order_events.jsonl      NDJSON, uma linha por evento
├── _manifest.json
└── _SUCCESS
```

Não existe `orders.json` ao lado: duas representações da mesma verdade divergem. O estado do
pedido é o **fold** dos seus eventos, e o fold mora no Silver (`silver_order`).

### Por que o fold não é trivial

Substituição e remoção de linha alteram a cesta **depois** da colocação, então o valor final
não é derivável do evento `order_placed`. Medido na janela de 2026-08-24 a 08-27, sobre
120.693 linhas:

| Mecanismo | Linhas | Efeito no valor |
|---|---|---|
| cumprida sem alteração | 108.194 | 0,00 |
| substituída | 4.670 | +10.318,99 |
| removida | 2.321 | −15.153,72 |
| nunca separada (pedido morreu antes) | 5.508 | não entra: `net_amount` é nulo |

Se o fold fosse trivial, o log seria um carimbo de data. Um teste dbt **invertido**
(`assert_order_fold_is_not_trivial`) reprova quando nenhuma cesta muda.

### `ingestion_date` é a data do PEDIDO, não a do evento

Todo evento de um pedido fica na partição do dia em que ele foi colocado, mesmo atravessando
a meia-noite — medido: **10,3% dos eventos**. Particionar por data do evento deixaria a
partição impossível de fechar. O Silver expõe as duas colunas (`ingestion_date` e
`event_date`), porque as duas perguntas são legítimas.

### Acrescentar um dia é aditivo

Cada `(armazém, dia)` deriva a própria semente de `sha256("<seed>|<wh>|<dia>")`, então gerar
`D+1` deixa a partição de `D` **byte a byte idêntica**. O que **não** é aditivo: outra seed,
outra referência, ou outra tabela de premissas — as três estão em `history`.

### Premissas: sintéticas, declaradas, sem default

Nenhuma fonte deste repo mede venda, cesta, cadência ou disponibilidade. Toda premissa vive
em `platform/dbt/seeds/order_premises_seed.csv`, é rotulada `synthetic` — outro rótulo
**reprova o export** — e seu `sha256` viaja até o manifesto. Uma chave ausente reprova a
geração: um default escondido no gerador seria uma premissa não declarada.

### Ao medir valor, olhe a cauda

O `unit_price` da fonte cobre quatro ordens de grandeza (mediana 2,25; máximo 3.663,00 —
marisco congelado e presunto ibérico vendidos por peso). Medido: **9 das 4.670 substituições
respondem por 42% do valor substituído**. Média aritmética de cesta é dominada por punhado de
linha; use mediana ou percentil.

Modelos Silver: `silver_order_event`, `silver_order`, `silver_order_line`,
`silver_orders_manifest`. DAG: `simulated_orders_events`. Ver
[CONTRACT.md](sources/simulated-orders-source/CONTRACT.md) e
[ARCHITECTURE.md § "Fase 3"](ARCHITECTURE.md).

## OLTP e outbox: o evento nasce na transação

```bash
make stream-up                    # sobe o oltp-postgres e cria as três tabelas
make orders-apply-all             # replica o log da janela: 44.456 eventos
make orders-outbox PARTITION=data/orders/ingestion_date=2026-08-27/wh=mad1
make orders-prove-atomicity       # injeta falha e prova que os dois lados caem juntos
```

O log de eventos já existe em disco e no RAW. O que este plano acrescenta não é transporte:
é **o evento passar a nascer dentro da transação que muda o pedido**. Essa é a diferença
entre um outbox e um *dual-write* — duas escritas separadas podem discordar, uma transação
não pode.

### A propriedade, dita com precisão

Para todo evento: **ou a mudança de estado e a linha do outbox são visíveis, ou nenhuma das
duas é.** Nunca uma sem a outra.

`make orders-prove-atomicity` prova isso **nas duas direções**, e a injeção é no banco — um
trigger que levanta exceção no `insert` — não no código:

| Injeção | O que não pode sobreviver | Por que a direção importa |
|---|---|---|
| o `insert` no `outbox` explode | nenhum pedido, nenhuma linha | o estado avançaria sem ninguém saber |
| o `insert` em `orders` explode | **nenhuma linha de outbox** | senão o broker publicaria um evento que nunca aconteceu |

A segunda é a que se esquece. Verificado injetando o dual-write no applier: a primeira prova
continua passando, a segunda reprova.

### O outbox reconstitui o log byte a byte

`outbox.event_json` guarda a **linha canônica do log, verbatim**. As colunas do envelope
existem para rotear, e quatro `CHECK` amarram cada uma ao próprio JSON — uma linha não
consegue ser roteada sob uma chave que discorda do payload que carrega.

Reordenando as linhas do outbox pela ordem canônica e recompondo o arquivo, o `sha256` bate
com o manifesto da partição. **16 de 16.** Contar linhas não provaria isso; reproduzir os
bytes prova.

### Três guardas independentes

| Guarda | Recusa |
|---|---|
| `outbox.event_id` UNIQUE | reaplicar o mesmo evento — o replay **pula**, não falha |
| `orders.last_sequence_no` | evento fora de ordem |
| `orders.status` em `from_states` | transição inválida |

A segunda é o que torna `key = order_id` uma exigência do broker, e não uma preferência: se
o OLTP aceitasse evento fora de ordem, preservar ordem por pedido não compraria nada.

**Uma transação por evento**, não por pedido nem por partição — é o único recorte que
corresponde ao que um OLTP de verdade faz. 44.456 transações em 85 s.

### O que dois folds independentes acharam um no outro

Replicar o mesmo log por um caminho completamente diferente — incremental e transacional,
em vez de window function sobre o log inteiro — e comparar os dois estados achou **dois
defeitos no Silver** que nenhum teste pegava, porque os dois eram internamente coerentes:

1. **`net_amount` respondia duas perguntas com o mesmo nome.** 298 pedidos morrem antes da
   separação; o Silver dizia nulo (*"não houve separação"*), o OLTP dizia o valor colocado
   (*"quanto ainda vale"*). Corrigido no OLTP.
2. **O Silver afirmava separação que o log nunca declarou.** `line_status` era
   `else 'fulfilled'` — inclusive nas **5.508 linhas** dos 298 pedidos cancelados ou com
   pagamento recusado. `order_picked` é o único evento que declara separação, e ele não
   ocorre neles. O vocabulário passou a ser
   `placed | fulfilled | substituted | removed | not_picked`, idêntico nos dois lados.

Depois da correção os dois folds concordam em tudo: **6.400 pedidos × 7 atributos** e
**120.693 linhas × 6 atributos**, zero divergências.

Nenhum dos dois apareceria com mais um teste no Silver — o teste que os pegaria teria de
conhecer a resposta certa. O que os achou foi uma segunda implementação independente do
mesmo fold.

### O que isto habilitou

O gatilho literal do Kafka — *"CDC de um OLTP"* — passou a existir aqui. O transporte está
logo abaixo.

## Transporte: Kafka, replay e consumo idempotente

```bash
make stream-up                    # OLTP + broker + read model
make orders-apply-all             # log -> OLTP + outbox
make orders-publish               # outbox -> topico (at-least-once, por desenho)
make orders-project               # topico -> live_order_state (idempotente)
make orders-replay                # rebobina o grupo; NAO apaga a projecao
make orders-prove-stream          # as seis provas
```

Isto é *transporte + replay + semântica de entrega + consumo idempotente*, e não "subir um
broker e publicar mensagens". Contar mensagens prova que algo trafegou; não prova que
trafegou intacto, nem o que acontece quando alguém morre no meio, nem que reprocessar é
seguro.

### A semântica é at-least-once, e o lado em que se erra foi escolhido

Marcar `published_at` no Postgres e receber o ack do Kafka são duas escritas em dois
sistemas, e não existe transação entre eles:

| Ordem | Morrer no meio produz | |
|---|---|---|
| publicar → ack → marcar | **duplicata** | escolhido |
| marcar → publicar | **perda** | recusado |

Perder é irreversível; duplicar é absorvível. Por isso o consumidor é idempotente **por
obrigação, não por elegância**.

`enable.idempotence=true` **não** cobre isso — ele elimina duplicata de *retry dentro da
sessão do produtor*. A duplicata de o processo morrer entre o ack e o commit do outbox é do
desenho, não do transporte. `make orders-prove-stream` **reproduz essa janela**: devolve 500
linhas do outbox para a fila, republica, e o tópico passa a ter mais mensagens que o log tem
eventos (44.456 → 44.956).

### Deduplicação sem conjunto que cresce

O consumidor não guarda um conjunto de `event_id`. Compara `sequence_no` com o que já está
no read model:

| Comparação | Verdito |
|---|---|
| `seq <= last` | duplicata — descarta |
| `seq == last + 1` | aplica |
| `seq > last + 1` | **buraco — para** |

É **limitado por construção** (um inteiro por pedido), sem política de expiração — e toda
política de expiração é uma janela em que a duplicata volta a passar. E só funciona porque a
ordem por chave é garantida: uma duplicata sempre chega *depois* do original. É isso que faz
`key = order_id` virar peça de carga em vez de configuração.

**Buraco é perda.** Avançar o offset por cima tornaria a perda permanente e invisível.

### O offset é commitado depois da escrita

`enable.auto.commit` é **false**: o commit automático anda no timer, não na escrita, e
entrega at-most-once sem ninguém escolher. A ordem é escrever → commitar a projeção →
commitar o offset. **At-least-once na entrega, efeito exactly-once na projeção.**

Nenhum teste de contagem enxerga essa ordem — ela é asserida contra duplos que gravam um
**diário compartilhado**.

### As seis provas

| # | Prova | Resultado |
|---|---|---|
| 1 | Transporte fiel — o tópico relido reproduz o sha256 dos manifestos | **16/16** |
| 2 | Ordem por chave — 1 partição por pedido, repetição sempre depois do original | 6.400 pedidos |
| 3 | At-least-once é real — a janela de duplicação é reproduzida | 44.456 → 44.956 |
| 4 | Consumo idempotente — as duplicatas não mexem na projeção | digest igual |
| 5 | Replay — rebobinar e reprocessar o tópico inteiro | 0 aplicados, digest igual |
| 6 | Buraco é recusado, duplicata é descartada | as duas |

Mais: os três folds independentes — Silver (window function), OLTP (transacional) e projeção
(streaming) — concordam em **6.400 pedidos, zero divergências**. E o plano inteiro
reconstruído de volumes vazios produz o **mesmo digest**.

### Dois achados que ficaram registrados em vez de corrigidos

**A premissa `sla_minutes_picking = 90` não pode disparar.** `basket_lines_max ×
minutes_per_line_picked = 40 × 2 = 80 min`, e a maior separação em 6.400 pedidos foi
exatamente 80,00. O mecanismo do alerta funciona e está testado; o limiar não é alcançável.
Não foi ajustado: adaptar uma premissa declarada até a verificação acender é o oposto de
verificar.

**A distribuição uniforme entre partições é artefato da chave.** 1.600 pedidos em cada
partição, exatamente 100 dentro de cada (armazém, dia). Não é mérito do particionador: o
índice sequencial denso de `order_id` faz os bits baixos do murmur2 formarem um sistema
completo de resíduos. Com `order_id` esparso o equilíbrio viraria estatístico.

### O que isto habilitou

`live_order_state` passou a existir, e é ela que ganha um **segundo escritor** logo abaixo —
o gatilho do Iceberg, literal.

## Projeção viva em Iceberg: dois escritores, um leitor

```bash
make spike-iceberg                # o experimento fechado, ANTES de tudo isto existir
make iceberg-init                 # catalogo SQL no Postgres + live_order_state
make orders-rebuild-projection --through 2026-08-26   # escritor 2: o lote
make orders-project-iceberg       # escritor 1: o streaming, na MESMA tabela
make orders-reconcile             # Iceberg x Silver x OLTP; sai 1 se divergirem
make orders-prove-projection      # concorrencia, fusao monotonica, snapshot isolation
```

O gatilho do Iceberg era *"um segundo engine precisar **escrever** a mesma tabela"*, e agora
`live_order_state` tem dois escritores por desenho, com o DuckDB lendo enquanto os dois
escrevem. **Disparou por concorrência, não por volume** — neste volume um parquet reescrito
com `os.replace` atômico serviria.

### O experimento fechado veio primeiro

O plano registrou "o DuckDB pode não ler o catálogo SQL do pyiceberg" como a premissa mais
frágil. `make spike-iceberg` respondeu nove perguntas contra o stack de verdade **antes de
uma linha da projeção existir**, e duas respostas mudaram o desenho:

**O DuckDB lê pelo `metadata_location`, e só por ele.** Ele recusa descobrir sozinho qual é o
metadado corrente — *"globbing the filesystem ... is considered unsafe and could result in
reading uncommitted data"*. O atalho (`unsafe_enable_version_guessing`) foi medido, funciona,
e foi recusado. Quem sabe é o **catálogo**: `make silver` pergunta a ele e passa a resposta
como var.

**O experimento reprovou a minha asserção, não o Iceberg.** Exigi que duas escritas
concorrentes "sobrevivessem" e recebi `CommitFailedException` — que é o controle otimista
funcionando. Se passasse calado, seria lost update.

### Retry não basta: a fusão é monotônica

Recarregar e tentar de novo resolve o conflito de **commit** e ainda assim perde dado: se o
outro escritor já gravou o pedido no `sequence_no` 7 e a nossa tentativa carrega o 5, o retry
cego escreve o 5 por cima. **O commit passa, a tabela regride, nada reprova.**

Por isso cada tentativa relê o estado das chaves afetadas e descarta as próprias linhas que
não avançam — a mesma guarda de `last_sequence_no` que protege o OLTP e o consumidor, agora
protegendo a escrita concorrente. É a terceira vez que o mesmo invariante paga.

### O par lambda, medido

| | Pedidos | Tempo |
|---|---|---|
| Lote — 12 partições, 33.349 eventos | 4.800 | **3,2 s** |
| Streaming — o tópico inteiro por cima | 1.600 | 60 s |

`written_by` na tabela: **`{rebuild: 4800, stream: 1600}`** — proveniência consultável, não
afirmação sobre log. O streaming descartou 33.849 eventos como duplicata porque o lote já os
tinha trazido ao estado final, e a dedup do consumidor reconheceu isso lendo o estado que o
**outro** escritor gravou.

### Quatro caminhos, um digest

O read model tem quatro produções independentes, todas com o mesmo `d769f727f805736a…`:
Postgres; Iceberg só streaming; Iceberg lote + streaming; e a reconstrução do zero de volumes
vazios. **Dois motores de armazenamento diferentes com digest byte a byte igual.**

### O que o acordo entre os dois escritores não prova

`orders-rebuild-projection` e `orders-project` compartilham o fold. Concordarem mostra que não
se atropelam — não que estão certos. A correção vem de `orders-reconcile`, que compara com
`silver_order`: window function em SQL sobre o log inteiro, sem uma linha em comum com as
outras duas. **Três folds independentes, 6.400 pedidos, zero divergências** — e o mesmo
invariante virou teste dbt, porque verificação que só roda quando alguém lembra é hábito, não
verificação.

### `make stream-evidence`

A metade em streaming é dívida declarada: `make test` roda sem rede, então broker, OLTP e
Iceberg só existem enquanto `make stream-up` estiver de pé. Os duplos em memória cobrem a
**forma** do código — a ordem da transação, o protocolo de dedup, a construção do SQL; o que
eles não podem cobrir é a **semântica** dos motores reais.

[`docs/stream-evidence/README.md`](docs/stream-evidence/README.md) registra os três planos e
os três folds concordando em 6.400 pedidos, com data e nenhum número escrito à mão. É
tolerante a plano desligado de propósito: cada seção ausente aparece como **ausência
declarada**, nunca como zero — *"o outbox tem 0 eventos"* e *"o OLTP não respondeu"* cabem na
mesma célula de tabela e significam coisas opostas.

```bash
make stream-evidence
```

### O custo do copy-on-write, medido

O sink Iceberg processou o tópico em **4min48s** contra **14s** do Postgres. O `upsert` do
pyiceberg é copy-on-write: cada lote reescreve os arquivos de dados, e a guarda monotônica lê
a tabela antes de cada tentativa.

Não foi otimizado, e o motivo é que os dois sinks respondem perguntas diferentes: o Postgres é
o read model de baixa latência, o Iceberg é o que aceita dois escritores e guarda história.
**Gatilho para mexer**: a projeção sair da ordem de 10⁴ linhas.

## Pedidos no warehouse: o fato transacional que faltava

```bash
make warehouse-refresh        # export -> load -> dbt (agora com 4 STAGE e 4 FACT novos)
make warehouse-prove-tests    # injeta o defeito que cada teste diz pegar e exige o vermelho
```

| Camada | Objetos novos |
|---|---|
| STAGE | `STG_ORDER` (6.400) · `STG_ORDER_LINE` (120.693) · `STG_ORDER_EVENT` (44.456) · `STG_ORDER_PREMISE` (30) |
| GOLD | `FACT_ORDER` · `FACT_ORDER_ITEM` · `FACT_ORDER_EVENT` · `FACT_ORDER_PREMISE` |
| MART | `MART_ORDER_FUNNEL` · `MART_FULFILLMENT_SLA` · `MART_BASKET_DAILY` |

`FACT_ORDER` é **accumulating snapshot** — uma linha por pedido que se preenche conforme ele
avança, com onze marcos e as durações entre eles. O padrão só existe porque há eventos: uma
fotografia de estado diria *onde* o pedido está, nunca *quanto tempo levou para chegar lá*.

### Um timestamp 56 milhões de anos no futuro, com 166 nós verdes por cima

A primeira carga pôs **todo** timestamp no ano **56.648.666**: o DuckDB anota a unidade só no
`LogicalType` moderno do parquet e deixa o `ConvertedType` legado em `NONE`; o Snowflake cai
no legado e assume milissegundos onde havia microssegundos.

**Nada reprovou.** A reconferência da carga compara contagem de linhas, e ela estava certa.
Os 166 nós do dbt construíram em verde — as durações viraram números grandes, não erros. O
grão continuou único. E o teste de funil passou, porque um funil é feito de
`count_if(marco is not null)` e "não nulo" continua exato com o instante deslocado. Quem
apontou foi ler **80.000.060 minutos de separação** num mart.

Só apareceu agora porque era a primeira vez que um `TIMESTAMP` cruzava a fronteira — até
então o recorte só tinha `DATE`, que viaja como `date32` sem ambiguidade de unidade.

A correção é `use_logical_type = true`. O que ficou é o teste que ancora cada marco contra
`order_date`, que chegou por outro caminho: comparar marcos **entre si** passaria alegremente,
porque todos estavam deslocados pelo mesmo fator.

### O SCD2 finalmente paga por si

Até aqui `DIM_CUSTOMER` e `DIM_PRODUCT` eram SCD2 sem nenhum fato apontando para uma versão.
`FACT_ORDER` resolve a versão de cliente vigente na data do pedido; `FACT_ORDER_ITEM` resolve
**duas** versões de produto — a do pedido e a do cumprido, que diferem nas 4.670 linhas
substituídas.

E a versão resolvida pelo *range join* coincide com a que a Source gravou no evento
`order_placed` nos **6.400** pedidos — dois caminhos que não se tocam. Virou teste.

### O funil se apoia em marco, e 61 pedidos provam por quê

Um pedido devolvido tem status `RETURNED` — **e foi entregue**. Contar
`order_status = 'DELIVERED'` dá **5.985**; contar `delivered_at is not null` dá **6.046**.
Marco é monotônico, status não é.

### Dois achados registrados em vez de corrigidos

**`sla_minutes_picking = 90` é inalcançável por construção**: `basket_lines_max` (40) ×
`minutes_per_line_picked` (2) dá teto de 80, e o máximo medido é exatamente 80. Zero
violações — não porque a operação seja boa, mas porque as premissas não se cruzam. O mart
publica `sla_minutes`, `max_picking_minutes` e `orders_breaching_sla` lado a lado, para que o
zero seja legível.

**A janela de entrega quase nunca é cumprida, e o desvio é para CEDO**: das 6.046 entregas,
**5.166 chegam antes de a janela abrir**, 471 dentro, 409 depois. O mart separa
`orders_delivered_before_slot` de `orders_delivered_after_slot`, porque chegar cedo e chegar
tarde são problemas **opostos** e "fora da janela" não diz qual dos dois é.

Nos dois casos, mexer no seed até o número melhorar seria ajustar a entrada até a saída
agradar. As premissas atravessam a fronteira em `FACT_ORDER_PREMISE` justamente para que o
mart meça contra **o mesmo número** que gerou as durações, e não contra uma cópia.

### Cada teste foi visto vermelho

`make warehouse-prove-tests` injeta, no dado real, o defeito que cada um dos cinco testes diz
pegar; exige o vermelho; desfaz; exige o verde de volta; e confere uma sentinela no fim. Só
toca GOLD e MART, que são inteiramente reconstruíveis a partir do STAGE.

Depois do que aconteceu com os timestamps, um teste verde que nunca foi visto vermelho não é
evidência de nada.

## Calibração da demanda contra o MAPA 2025

`docs/Informe comsumo 2025_.pdf` — o Informe del Consumo Alimentario en España do Ministerio
de Agricultura, Pesca y Alimentación — passou a servir de **benchmark** para a distribuição de
demanda da cesta sintética. Não é uma fonte que a plataforma ingere: é referência externa,
usada só como alvo de distribuição.

```bash
make demand-check-mapping     # 444 trincas do catalogo, uma regra cada, zero default
make demand-reality-check SNAPSHOT=before_mapa_2025_v1   # congela o ANTES
make demand-reality-check     # ANTES | MAPA | ALVO | DEPOIS nas tres dimensoes
```

### O achado que abriu a fase não era de demanda

"Marisco y pescado" tinha 3,38% das unidades e **22,82% da receita**, com preço médio pago de
27,09 € num catálogo cujo produto mais caro custava 24,05 €. O RAW explicou: quando
`selling_method = 1` e `unit_size` é nulo, a API devolve `unit_price = reference_price × 99`
— o teto do seletor de peso, não um preço de consumo. **12 produtos em 4.939 produziam 23% da
receita**, e nenhum dos 947 testes reprovava.

O campo que corrige — `min_bunch_amount` — sempre esteve no RAW e o Silver o descartava.
`silver_product_price` ganhou `purchasable_unit_price` ao lado do valor cru, que permanece
intacto.

### A cadeia, com preço fora do caminho da demanda

```
grupo de demanda   <- alvo de VOLUME (kg/L) do MAPA, inclinado pelo canal e-commerce
produto no grupo   <- UNIFORME (nenhuma fonte mede giro por SKU)
quantidade / preço <- inalterado / observado
valor do pedido    <- consequência, nunca objetivo
```

**Volume e valor divergem de propósito**: no MAPA, mariscos são 0,81% do volume e 2,88% do
valor. Um simulador que os igualasse estaria errado.

**O que o benchmark NÃO calibra:** `daily_order_rate`, `basket_lines_*`, `quantity_max`. O
MAPA mede consumo doméstico do residente, não pedido de loja online. Essas continuam
`synthetic`.

### Configuração versionada, zero hardcode

| Seed | Papel |
|---|---|
| `mapa_2025_benchmark_seed.csv` | 64 linhas do informe, com seção citada em cada uma; 39 pesáveis cobrindo 86,12% do volume doméstico |
| `demand_category_mapping_seed.csv` | 128 regras `(l1, l2, l3)` com `*` como coringa; a mais específica vence |
| `demand_profile_seed.csv` | `demand_model_version`, share alimentar, limiar de cobertura, bases de canal |
| `demand_seasonality_seed.csv` | 12 meses, **neutros** — e o motivo escrito em cada linha |

O perfil resolvido viaja como quinto arquivo de referência (`demand_profile.json`) para a
Source, que continua **FROZEN**: ela recebe pesos, não regras.

### Resultado medido na mesma janela

| dimensão | ANTES | DEPOIS |
|---|---:|---:|
| receita | 821.121,93 | 583.154,43 |
| EUR/kg | 6,49 | 4,04 |
| MARISCOS, % do volume | 11,44 | 0,43 (alvo 0,43) |
| FRUTAS_FRESCAS, % do volume | 3,11 | 9,51 (alvo 9,17) |

Erro absoluto médio contra o alvo: **0,098 ponto**. A queda de 29% na receita é a correção
funcionando — 23% dela eram os 12 produtos com preço de teto de API.

### O que o informe não sustenta, e ficou registrado

Sazonalidade mensal por categoria **não é extraível**: os gráficos mensais são imagens. O
perfil sazonal é neutro por ausência de evidência, aplica-se à taxa de pedidos, e tem um par
de testes que prova que o mecanismo funciona *e* que o perfil entregue está neutro. O gatilho
para propor um perfil é a janela cobrir novembro e dezembro.

## Warehouse analítico (Snowflake)

```bash
cp .env.snowflake.example .env.snowflake   # conta, usuário, papel — nenhum segredo
make warehouse-bootstrap                   # 1x por conta, exige ACCOUNTADMIN
make warehouse-refresh                     # export -> load -> dbt
make warehouse-evidence                    # registra o que ficou no destino, datado
```

**Três verbos separados**, pelo mesmo motivo que `land` e `verify-landing` são separados:
`warehouse-export` lê o Silver e escreve parquet local sem falar com o Snowflake;
`warehouse-load` faz `PUT` num stage interno mais `COPY INTO` e reconfere contagem a
contagem; `warehouse` roda `dbt build --target snowflake`. Falha de recorte é falha de
dado e não é retentável; falha de carga é rede e é.

**Stage interno, não external.** Um Snowflake gerenciado não enxerga um MinIO em
`localhost`; `PUT file://` inverte o sentido e dispensa S3 real e storage integration.

**Credencial**: par de chaves RSA, nunca senha — é o método recomendado para acesso
programático e o único que serve para uma DAG. A chave privada vive em
`~/.snowflake/keys/` com modo 600, fora do repositório; o `config.toml` guarda só o
caminho.

**Governança usada, não só verificada.** `warehouse-bootstrap` cria três papéis, aplica os
grants **e prova a matriz de isolamento** antes de retornar sucesso — com
`use secondary roles none`, sem o qual a verificação passaria por engano. E o pipeline
**veste** os papéis: a carga roda como `RETAIL_LOADER` e o dbt como `RETAIL_TRANSFORMER`,
nunca como `ACCOUNTADMIN`.

| Papel | Pode | Não pode |
|---|---|---|
| `RETAIL_LOADER` | escrever `STAGE` (é dono das 13 tabelas) | ler `GOLD` ou `MART` |
| `RETAIL_TRANSFORMER` | ler `STAGE`, escrever `GOLD` e `MART` (dono das 21) | — |
| `RETAIL_READER` | ler `MART` | ler `STAGE` ou `GOLD` |

Parar de rodar como administrador expôs quatro defeitos que nenhum teste pegaria antes —
`usage` faltando no warehouse, posse confundida com privilégio, e um em que o
administrador simplesmente **deixa de enxergar** os objetos sem erro nenhum. Estão
descritos em [ARCHITECTURE.md](ARCHITECTURE.md), e viraram teste.

**Trocar de conta Snowflake** é editar `.env.snowflake` e o bloco correspondente de
`~/.snowflake/config.toml`, e rodar `make warehouse-bootstrap`. Nenhum modelo, nenhum SQL e
nenhum teste muda: a fronteira L2→L3 é física. A metade Lakehouse não depende disso —
`make silver` e as 992 checagens de `make test` rodam sem nenhuma variável de Snowflake
definida.

**Evidência datada.** A metade Snowflake não é reproduzível offline como o Lakehouse, e a
conta usada aqui é um trial. [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
registra posse, volume, matriz de isolamento e amostra de cada mart, com data e identidade
da conta — para que os modelos continuem tendo prova depois que ela expirar. Os prints que
completam isso estão listados em
[docs/warehouse-evidence/PRINTS.md](docs/warehouse-evidence/PRINTS.md).

```bash
make warehouse-ddl   # imprime o DDL do STAGE sem conectar em nada (derivado do recorte)
```

## Painel estratégico (Streamlit sobre o MART)

Bancada de **conferência** dos indicadores antes de reconstruí-los no Power BI. 16
indicadores em 6 grupos, lendo só o `MART`.

```bash
make dashboard-venv       # 1x: streamlit/pandas/altair (extra, fora da imagem do Airflow)
make dashboard            # http://localhost:8501
make dashboard-contract   # regenera streamlit/CONTRACT.md, sem conectar em nada
make dashboard-check      # roda o painel de verdade e exige zero exceção (exige conta)
```

**Veste `RETAIL_READER`, e prova isso na tela.** É o primeiro consumidor a vestir o papel de
BI — a carga já vestia `RETAIL_LOADER` e o dbt `RETAIL_TRANSFORMER`. O painel roda uma sonda
ao vivo que confirma a recusa em `GOLD` e `STAGE`, com `use secondary roles none`. Um painel
que afirma respeitar um limite sem demonstrar está pedindo confiança.

**Lê ao vivo, com o relógio à mostra.** Cache de 60 s e um botão que o limpa. A barra lateral
mostra a contagem e a janela de **cada** mart, para que uma carga nova apareça como *mudança
de base* e não como número diferente sem explicação. Verificado: um `make warehouse-refresh`
levou `MART_PRICE_EVOLUTION` de 112.061 para 129.275 linhas e a janela de 08-29 para 08-31, e
o painel viu — enquanto os marts de pedido ficaram parados, porque não houve pedido novo.

**As armadilhas não ficam em rodapé.** [`streamlit/CONTRACT.md`](streamlit/CONTRACT.md) é
**gerado** de `indicators.py`, onde a consulta e a explicação moram juntas — e um teste
offline reprova se os dois saírem de sincronia. Três exemplos do que ele registra:

| Armadilha | O erro que ela evita |
|---|---|
| Perda de valor tem **duas** causas | `SUM(gross) − SUM(net)` = 58.327,81 mistura cesta que encolheu (4.834,73) com pedido que morreu antes da separação (53.493,08) |
| Ticket médio tem **dois** denominadores | receita/separados = 134,57; receita/colocados = 128,30 — o segundo mede algo que não existe |
| `orders_touching_category` **não é aditivo** | somar as 151 categorias de um dia dá muito mais que os 1.600 pedidos daquele dia |

E a aba *Fora de alcance* declara o que o painel **não** exibe, com o gatilho de cada item:
margem, estoque, recompra/LTV/coorte, rota, penetração de mercado, tendência. A lacuna mais
acionável: **nenhum mart junta cliente com pedido** — o elo existe em
`FACT_ORDER.customer_sk`, no GOLD, fora do alcance de `RETAIL_READER` por desenho.

## O portão do `dbt build` do Silver

Nem todos os 21 modelos podem ser construídos sempre, e os dois motivos são legítimos: uma
source que ainda não aterrissou nada faz `read_json` **falhar** (não devolver zero linhas), e
`silver_live_order_state` só pode ser lido quando o catálogo Iceberg responde.

`make silver` e as cinco DAGs chamam **o mesmo verbo**, e ele diz o que decidiu:

```
mercadona_catalog_api....... aterrissado
ine_population_api.......... aterrissado
ine_callejero............... aterrissado
simulated_oltp.............. aterrissado
simulated_orders............ aterrissado
projecao iceberg............ CATALOGO INDISPONIVEL (excluida)
argumentos ................. --exclude silver_live_order_state assert_live_projection_matches_batch_fold
```

**Isto já morou em seis arquivos**, e o custo apareceu: a DAG da Mercadona não tinha portão
nenhum — ela sempre tem dado, então ninguém sentiu falta — e passou a reprovar todo dia
assim que o Marco 6 criou um modelo que não tem nada a ver com a source dela. `make silver`
passava, `make test` passava, e só a execução real reprovava, um dia depois.

`plan()` é **pura**, então a decisão inteira é testável sem MinIO e sem catálogo. E uma
checagem de fonte exige que nenhuma DAG monte o próprio `dbt build`, porque **um portão
único só vale enquanto for o único**. Ver [ARCHITECTURE.md](ARCHITECTURE.md).

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

### `where is_latest_ingestion` nos modelos de referência

Os modelos do INE (população e Callejero) **empilham todas as `ingestion_date`** — o
histórico é deliberado. A consequência é que reextrair a *mesma* publicação duplica linhas
equivalentes, e já duplicou: `silver_ine_population_series` ficou com **1.547.496 linhas em
cada uma de duas datas**. Quem consultasse sem filtrar contaria em dobro, sem nenhum erro
visível.

Os sete modelos de referência expõem `is_latest_ingestion` justamente para que ler o estado
atual não dependa de o consumidor lembrar de um `max(ingestion_date)`:

```sql
-- estado atual (o que quase sempre se quer)
select count(*) from silver_ine_population_series where is_latest_ingestion;

-- histórico completo — agora é uma escolha explícita, não um acidente
select ingestion_date, count(*) from silver_ine_population_series group by 1;
```

Um teste dbt garante que a flag marca **exatamente uma** data por modelo. Os modelos da
Mercadona **não** têm a coluna: lá as várias `ingestion_date` são o produto (histórico de
preço), não um efeito colateral.

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
make test          # 992 testes sem rede: 145 Mercadona + 136 INE população + 95 Callejero
                   #                    + 125 OLTP simulado + 148 pedidos + 343 plataforma
make silver        # dbt build no DuckDB: 21 modelos + 9 seeds + 298 testes de dados
make warehouse     # dbt build no Snowflake: 21 modelos + 149 testes de dados
```

`make test` e `make silver` não leem nenhuma variável do Snowflake — é o que mantém a
metade Lakehouse reproduzível quando a conta não existir.

As duas árvores do dbt são **mutuamente exclusivas por target** (guard `+enabled` no
`dbt_project.yml`): `--target dev` enxerga só o Silver, `--target snowflake` só o
warehouse. Confirmado com `dbt list`: zero sobreposição.

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
