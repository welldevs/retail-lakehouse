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
[DECISIONS.md § "Segunda source: população do INE"](DECISIONS.md) para por que são
pacotes irmãos, não uma abstração compartilhada.

As decisões de arquitetura estão em [ARCHITECTURE.md](ARCHITECTURE.md), com a data de
adoção de Snowflake, Kafka, Iceberg e **Spark**. A história de cada uma — decisão, razão,
evidência, trade-off — está em [DECISIONS.md](DECISIONS.md), o que não entra em
[BACKLOG.md](BACKLOG.md), e as restrições que governam qualquer agente que continue o trabalho
em [AI_ENGINEERING_CONSTRAINTS.md](AI_ENGINEERING_CONSTRAINTS.md). O caso do Spark é o mais instrutivo dos
quatro: o gatilho declarado para ele — *"partição que o DuckDB não segura"* — **nunca
disparou, e isso está medido** (37,9 M pares de cesta em ~1,5 s e ~2,4 GB num nó, sobre a
janela final). Ele entrou por duas
outras razões: é o primeiro escritor do catálogo Iceberg fora do Python, e a forma do job
que ele carrega — uma soma corrida realimentada pelo próprio estado — não é expressável em
SQL. [`make spark-evidence`](docs/spark-evidence/README.md) publica o mesmo job nos dois
motores, **inclusive quando o Python puro ganha**.

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

O Snowflake **não** recebe o Silver inteiro: atravessam 3.327.809 das 7.098.881 linhas
(**46,9%** em 2026-09-01). A razão **não é propriedade do pipeline** — é função de quanto de
cada source cai dentro do escopo, e por isso ela se move sozinha: era 3,85% na Fase 2 e
9,95% na Fase 3, sem ninguém afrouxar o recorte. A população do INE é nacional e entrega
1,8%; os pedidos e os clientes nascem dentro das quatro AUFs e entregam ~100%, e a Fase 6
multiplicou os dois por 14× e 14×. Sem Orders, o recorte é 19,2%. Ver
[DECISIONS.md § "Fase 2"](DECISIONS.md).

## Requisitos

Python 3.12, Docker com Compose v2. `make venv` cria o ambiente da plataforma.

## Uso

```bash
cp .env.example .env && make secrets   # chaves aleatórias; o compose recusa subir sem elas
make venv                              # cria platform/.venv e instala a plataforma
make up                                # MinIO + buckets. NÃO sobe Kafka, Spark nem Airflow
make daily                             # extract -> validate -> land -> verify -> silver
make test                              # todas as suítes, sem rede
make help                              # todos os alvos, um por linha
```

A sequência completa, do zero até o painel, está em **[Do zero até o painel](#do-zero-até-o-painel)**.

`make up` sobe **apenas** o plano de dados. O Airflow custa ~2 GB de RAM e não é necessário
para iterar num modelo dbt; o OLTP e o Kafka vivem sob o profile `stream`, e o Spark sob o
profile `spark`. Nenhum dos três sobe sozinho, e **nada do caminho padrão depende deles** —
`silver_gate.py` tira do build o que depende de uma tabela que não existe, e há teste
provando que `make silver` fica verde numa árvore onde o Spark nunca rodou.

`make daily`, `make ine-refresh` e `make callejero-refresh` são **idempotentes**: uma
partição já completa não é reextraída (é imutável, mesma guarda `_SUCCESS` nas três), e
objetos já aterrissados com o checksum esperado são pulados, não reenviados.

Alvos individuais aceitam `DATE=` e `WH=` (Mercadona), `DATE=` e `TABLES=` (INE população),
ou `DATE=`, `CALLEJERO_IN=` e `CALLEJERO_PROVINCES=` (INE Callejero).

**Espaço em disco.** `data/` é scratch de extração e nada nunca é apagado sozinho — uma
partição do INE ocupa entre 264 e 384 MB. Depois de `land` + `verify-landing`, o object
storage é a verdade:

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
├── DECISIONS.md                        # a história: decisão -> razão -> evidência -> trade-off
├── BACKLOG.md                          # o que NÃO entra, e o gatilho de cada item
├── AI_ENGINEERING_CONSTRAINTS.md       # as restrições de engenharia para IA/agentes
├── Makefile                            # ponto de entrada da plataforma
├── sources/
│   ├── mercadona-catalog-source/       # Source do catálogo, FROZEN, dependencies = []
│   ├── ine-population-source/          # Source de população, FROZEN, dependencies = []
│   ├── ine-callejero-source/           # Source do Callejero, FROZEN, dependencies = [] — sem API
│   ├── simulated-oltp-source/          # Clientes sintéticos, FROZEN, dependencies = [] — derivada do Silver
│   └── simulated-orders-source/        # Pedidos como LOG DE EVENTOS, FROZEN — derivada do Silver
├── .env.example                        # copie para .env; credenciais só de desenvolvimento
├── .env.snowflake.example              # copie para .env.snowflake; identidade da conta, sem segredo
├── docs/README.md                      # o benchmark do MAPA: URL, sha256, como reextrair
├── docs/demand-evidence/               # ANTES | MAPA | ALVO | DEPOIS — gerada, mais os ANTES congelados
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
│   │   ├── demand_profile.py           # de-para + benchmark do MAPA -> pesos por coorte (IPF)
│   │   ├── demand_check.py             # reality check ANTES | MAPA | ALVO | DEPOIS + coorte
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
│   ├── dbt/seeds/                      # 16 seeds, todos com coluna de proveniência
│   │   ├── warehouse_province_map_seed.csv  # wh -> província/município (sede), códigos do INE
│   │   ├── warehouse_service_area_seed.csv  # wh -> N municípios da mesma AUF (INE)
│   │   ├── order_premises_seed.csv          # premissas do gerador de pedidos, TODAS `synthetic`
│   │   ├── customer_premises_seed.csv       # quem EXISTE: idade mínima, denominador, alocação
│   │   ├── ine_municipality_codes_seed.csv  # nome (Tempus3) -> código de município, 08/28/41/46
│   │   ├── ine_ambiguous_series_seed.csv    # série -> código oficial, para nomes homônimos na Espanha
│   │   ├── ine_ccaa_map_seed.csv            # província -> comunidade autónoma; sem destino padrão
│   │   ├── demand_profile_seed.csv          # a configuração do modelo de demanda, versionada
│   │   ├── demand_category_mapping_seed.csv # categoria da Mercadona -> grupo do MAPA (444 trincas)
│   │   ├── demand_seasonality_seed.csv      # o que o informe NÃO publica por categoria, declarado
│   │   ├── mapa_2025_benchmark_seed.csv     # 64 linhas do informe, cada uma citando a seção
│   │   ├── mapa_2025_region_seed.csv        # consumo per cápita por comunidade autónoma
│   │   ├── demand_cohort_age_seed.csv       # volume x população por faixa etária, `benchmark`
│   │   └── demand_cohort_region_seed.csv    # idem por comunidade; a página do PDF em cada linha
│   ├── dbt/macros/                     # generate_schema_name: GOLD/MART absolutos, sem prefixo
│   ├── dbt/models/silver/              # target dev (duckdb) — 25 modelos
│   │   ├── warehouse_province_map.sql   # passagem do seed para o object storage
│   │   ├── warehouse_service_area.sql   # idem, para a área de atendimento
│   │   ├── order_premises.sql           # idem, para as premissas — atravessa até o warehouse
│   │   ├── customer_premises.sql        # idem, para as premissas de CADASTRO (outro domínio)
│   │   ├── mercadona/                   # 4 modelos, grão por wh
│   │   ├── ine_population/              # série de população por província E por município
│   │   ├── ine_callejero/               # seções, núcleos, ruas — geografia oficial
│   │   ├── simulated_oltp/              # clientes sintéticos + manifesto com a linhagem
│   │   └── simulated_orders/            # o fold do log: evento, pedido, linha, manifesto
│   ├── dbt/models/warehouse/           # target snowflake — 24 modelos, ligados por source()
│   │   ├── sources.yml                  # as 15 tabelas STAGE: a fronteira, declarada
│   │   ├── gold/                        # 6 DIM + 8 FACT, SCD2 derivado da história
│   │   └── mart/                        # 9 marts, grão no cabeçalho de cada um
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
│   └── app.py                          # a interface, 7 grupos + "Fora de alcance"
├── scripts/                            # cada um tem alvo no Makefile; nenhum roda sozinho
│   ├── derive_warehouse_province_map.py   # deriva o seed de província/município do Callejero
│   ├── derive_warehouse_service_area.py   # deriva a AUF de cada armazém do AUF_mun.xlsx do INE
│   ├── derive_municipality_codes.py       # deriva nome (Tempus3) -> código oficial de município
│   ├── derive_ambiguous_series.py         # resolve homônimo nacional pelo código do VALORES_SERIE
│   ├── gen-secrets.py                     # gera as chaves do Airflow no .env (modo 600)
│   ├── prove_oltp_atomicity.py            # injeta falha no BANCO e prova que os dois lados caem
│   ├── prove_stream_semantics.py          # reproduz a janela de duplicação e prova o replay
│   ├── spike_iceberg_duckdb.py            # o experimento FECHADO, rodado antes do Marco 6
│   ├── prove_iceberg_projection.py        # concorrência, fusão monotônica, snapshot isolation
│   ├── spike_spark_iceberg.py             # o PORTÃO da Fase 7: o Spark lê o catálogo do pyiceberg?
│   └── prove_warehouse_orders_tests.py    # injeta o defeito que cada teste diz pegar, no dado real
├── jobs/spark/                         # o único código que roda fora do venv da plataforma
│   ├── session.py                      # a sessão com o catálogo Iceberg; o spike importa daqui
│   └── stock_ledger.py                 # saldo, ruptura e reposição — a forma que o SQL não expressa
├── infra/
│   ├── docker-compose.yml              # MinIO + mc + Postgres + scheduler + webserver
│   │                                   # + oltp-postgres e kafka (profile `stream`), e
│   │                                   # spark (profile `spark`); o catalogo Iceberg
│   │                                   # mora no proprio oltp-postgres
│   ├── Dockerfile.airflow              # imagem do orquestrador: duas runtimes
│   └── Dockerfile.spark                # imagem do Spark: Iceberg + JDBC + S3FileIO, versões cravadas
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

## As cinco Sources

Cada uma é um pacote Python **congelado**, com `dependencies = []`, contrato físico próprio
(`CONTRACT.md`) e README próprio. A plataforma as consome pelo contrato em disco — nunca
importando o código de nenhuma delas. **O porquê de cada decisão está em
[DECISIONS.md](DECISIONS.md);** aqui está o que cada uma entrega e como rodar.

| Source | O que entrega | Real ou sintético | Como rodar |
|---|---|---|---|
| [`mercadona-catalog-source`](sources/mercadona-catalog-source/) | Catálogo e preço por armazém e dia, da API pública da Mercadona | **observado** | `make extract validate land verify-landing WH=mad1` |
| [`ine-population-source`](sources/ine-population-source/) | População por província (com idade e sexo) e por município (só sexo), da API Tempus3 do INE | **observado** | `make ine-refresh` |
| [`ine-callejero-source`](sources/ine-callejero-source/) | Seções censitárias, unidades populacionais, ruas e tramos — com CEP e faixa de numeração | **observado** (download manual semestral) | `make callejero-refresh` |
| [`simulated-oltp-source`](sources/simulated-oltp-source/) | Base de clientes, ancorada na população real do município e no endereço real do tramo | **sintético sobre geografia observada** | `make oltp-export-reference && make oltp-refresh-all` |
| [`simulated-orders-source`](sources/simulated-orders-source/) | **Log de eventos** de pedido — não fotografia de estado | **sintético sobre catálogo e clientes observados** | `make orders-export-reference && make orders-refresh-all` |

**A distinção real/sintético não é rodapé.** Ela viaja no dado: `label = 'synthetic'` em toda
premissa, `stock_label` em cada linha do mart de estoque, e a lista *Fora de alcance* do
painel diz o que **não** dá para perguntar. Uma plataforma que mistura os dois sem rótulo
convida a ler densidade de simulação como penetração de mercado.

**Os quatro seeds derivados** têm alvo no Makefile e reproduzem byte a byte — um artefato
versionado sem comando que o gere é indistinguível de um número digitado:

```bash
make seed-province-map         # wh -> província/município, reconferido contra o Callejero
make seed-service-area         # wh -> municípios da AUF (AUF_XLSX=temp/AUF_mun.xlsx)
make seed-municipality-codes   # nome (Tempus3) -> código oficial    [rede: API do INE]
make seed-ambiguous-series     # série -> código, para homônimo      [rede: API do INE]
```

## Do zero até o painel

O caminho padrão **não** exige Kafka, Iceberg nem Spark. Os dois planos opcionais sobem sob
demanda, e o que depende deles sai do build sozinho — `silver_gate.py` decide isso num lugar
só, e há teste provando que `make silver` fica verde sem nenhum dos dois.

```bash
# 1. plano de dados
make up                       # MinIO + Postgres + Airflow. NÃO sobe Kafka nem Spark
make daily                    # extract -> validate -> land -> verify -> silver
make ine-refresh callejero-refresh
make oltp-export-reference && make oltp-refresh-all
make orders-export-reference && make orders-refresh-all
make silver && make test

# 2. calibração da demanda, contra o MAPA 2025
make demand-check-mapping     # 444 trincas do catálogo, uma regra cada, zero default
make demand-reality-check     # ANTES | MAPA | ALVO | DEPOIS + propensão por coorte

# 3. plano de stream (opcional) — OLTP, outbox, Kafka, projeção Iceberg
make stream-up
make orders-apply-all         # log -> OLTP + outbox, na MESMA transação
make orders-publish           # outbox -> tópico, at-least-once por desenho
make orders-project           # tópico -> live_order_state, idempotente
make orders-rebuild-projection PROJECTION_RESET=1   # o SEGUNDO escritor, em lote
make orders-reconcile         # três folds independentes; sai 1 se divergirem
make orders-prove-atomicity orders-prove-stream orders-prove-projection

# 4. plano de estoque (opcional) — o job Spark
make spike-spark-iceberg      # o PORTÃO: o Spark lê o catálogo do pyiceberg?
make stock-ledger             # consumo observado -> saldo, ruptura e reposição
make spark-evidence           # os dois motores, e os dois tempos

# 5. warehouse e painel
make warehouse-refresh        # export -> load -> dbt no Snowflake
make warehouse-prove-tests    # injeta o defeito que cada teste diz pegar e exige o vermelho
make dashboard                # http://localhost:8501

# 6. fechar
make freeze                   # sela a captura do RAW
make freeze-check             # e confere que ela não mudou
```


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
`make silver` e as 1.068 checagens de `make test` rodam sem nenhuma variável de Snowflake
definida.

**Evidência datada.** A metade Snowflake não é reproduzível offline como o Lakehouse, e a
conta usada aqui é um trial. [`make warehouse-evidence`](docs/warehouse-evidence/README.md)
registra posse, volume, matriz de isolamento, **papéis em execução vistos pelo verbo** e
amostra de cada mart, com data e identidade da conta — para que os modelos continuem tendo
prova depois que ela expirar. A tabela de papéis é a que separa governança verificada de
governança adotada: mostra que `RETAIL_READER` só executou `SELECT`, e que quem escreveu
GOLD foi `RETAIL_TRANSFORMER` — nunca o administrador. A captura do console em
[docs/warehouse-evidence/screens/query-history.png](docs/warehouse-evidence/screens/query-history.png)
é a mesma separação vista pela interface do fornecedor, que é a única coisa aqui que o
repositório não consegue produzir sozinho.

```bash
make warehouse-ddl   # imprime o DDL do STAGE sem conectar em nada (derivado do recorte)
```

## Painel estratégico (Streamlit sobre o MART)

Bancada de **conferência** dos indicadores antes de reconstruí-los no Power BI. 22
indicadores em 7 grupos, lendo só o `MART`.

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

Nem todos os 25 modelos do Silver podem ser construídos sempre, e os dois motivos são legítimos: uma
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
make test          # 1.095 testes sem rede: 145 Mercadona + 136 INE população + 95 Callejero
                   #                      + 140 OLTP simulado + 163 pedidos + 416 plataforma
make silver        # dbt build no DuckDB: 25 modelos + 16 seeds + os testes de dados
make warehouse     # dbt build no Snowflake: 24 modelos + os testes de dados
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

## O que muda numa máquina nova

**O RAW deste projeto não é reproduzível, e prometer que fosse seria falso.** A API da
Mercadona é viva, o Callejero é um download manual semestral, e a URL do MAPA aponta para
"últimos datos". Rodar a extração amanhã produz outra captura — e isso não é defeito, é a
natureza de fontes públicas.

O que **é** garantido: tudo a jusante é determinístico **dada a mesma RAW**. Os mesmos
manifestos produzem o mesmo Silver, o mesmo warehouse e os mesmos marts.

Daí sai a regra de onde cada número pode morar, e ela vale para quem for editar a
documentação:

| Natureza do número | Onde pode morar |
|---|---|
| Estrutural — grão, invariante, razão por construção | README, ARCHITECTURE |
| Propriedade **desta captura** — contagens, percentuais medidos | página gerada, ou datado explicitamente |

Numa máquina nova o operador roda `make freeze`, que sela **a captura dele** em
[`docs/FREEZE.md`](docs/FREEZE.md) com um `capture_id`. A partir daí `make freeze-check`
reprova se qualquer partição selada mudar — e o teste passa a guardar a captura dele, não a
que gerou os números publicados aqui.

**Três coisas exigem download ou credencial e não sobem sozinhas:** os arquivos do Callejero
(`temp/`), a conta Snowflake (`.env.snowflake`, chave RSA fora do repositório) e o informe do
MAPA em PDF. Sem eles o caminho padrão ainda roda — o que some é a camada analítica e a
calibração, e cada ausência é declarada onde apareceria.

## As quatorze perguntas do fechamento

Este é um **índice**, não uma explicação nova: cada resposta cabe numa linha e aponta para
onde a evidência mora. Ele existe porque o projeto fechou e um leitor tem direito de checar,
sem ler 3.000 linhas, se a documentação sustenta o que afirma.

| | Pergunta | Resposta curta | Onde a evidência mora |
|---|---|---|---|
| 1 | O que o projeto faz? | Ingere 5 sources, preserva o RAW, produz Silver tipado, serve um modelo dimensional e mantém um plano de stream que converge para o mesmo estado que o lote | [Camadas](#camadas) |
| 2 | Quais dados são **reais**? | Catálogo e preço da Mercadona; população do INE; geografia do INE (seções, ruas, núcleos); o informe do MAPA 2025, usado como **benchmark** | [As cinco Sources](#as-cinco-sources) · [`docs/README.md`](docs/README.md) |
| 3 | Quais dados são **sintéticos**? | Clientes e pedidos — inventados sobre atributos reais (o endereço existe, a pessoa não); e o **ledger de estoque**, calculado a partir do consumo observado mais uma política em seed. Nenhuma fonte deste repositório mede estoque | seeds `*_premises_seed.csv`, todos com coluna de proveniência · `stock_label = 'synthetic'` em `MART_STOCK_HEALTH` |
| 4 | Por que cada tecnologia existe? | Cada uma tem data de adoção, o gatilho que disparou e o que **não** ficou provado | [ARCHITECTURE.md § "O que não entrou, e quando entra"](ARCHITECTURE.md) |
| 5 | Por que Spark existe **sendo mais lento**? | Três razões, e desempenho não é nenhuma: estado cumulativo cuja saída depende do estado anterior; interop Iceberg demonstrada por spike; e a necessidade de validar mais de um engine escrevendo o mesmo catálogo. Neste volume o Python puro é ~3× mais rápido, e o número está publicado | [`docs/spark-evidence/`](docs/spark-evidence/README.md) · [DECISIONS.md § "Fase 7"](DECISIONS.md) |
| 6 | Qual é o papel do **Kafka**? | **Transporte**, nunca a fonte canônica. `publish → ack do broker → marca o outbox`, at-least-once, com a janela de duplicação reproduzida em teste | [`docs/stream-evidence/`](docs/stream-evidence/README.md) |
| 7 | Qual é o papel do **Iceberg**? | Commit atômico com concorrência otimista entre escritores, e **interop entre engines** — afirmada na Fase 3, demonstrada na Fase 7 com três escritores no catálogo (`platform`, `rebuild`, `spark`) | `make spike-iceberg` · `make spike-spark-iceberg` |
| 8 | Qual é o **sistema de registro** em cada etapa? | RAW no object storage é o ponto de não-retorno; o OLTP é a origem do evento (estado + outbox na mesma transação); o Kafka é transporte; a projeção e o Silver são **derivados** e descartáveis | [L1 — RAW](#l1--raw-no-object-storage) · [ARCHITECTURE.md](ARCHITECTURE.md) |
| 9 | Como trata **duplicação**? | Dedup no consumidor por `(order_id, sequence_no)`: `seq <= last` descarta. Duplicação na janela entre publicar e marcar o outbox é **aceita e declarada** — exactly-once ponta a ponta não é prometido | `make orders-prove-stream` · `fake_kafka.py` |
| 10 | Como trata **gaps**? | `seq == last + 1` aplica; `seq > last + 1` é buraco e **interrompe** em vez de aplicar fora de ordem | `make orders-prove-stream` |
| 11 | Como trata **concorrência**? | Conflito otimista no Iceberg com o ciclo completo: detectar → recarregar → reaplicar → retry, e `seq` velho **não** sobrescreve `seq` novo. Demonstrado inclusive **entre motores diferentes** | `make orders-prove-projection` · `make spike-spark-iceberg` |
| 12 | Como sabe que os **folds concordam**? | `make orders-reconcile` fecha nos três caminhos — lote, projeção Iceberg e sink Postgres — sobre 206.523 pedidos. Foi discordância entre folds que achou **três** defeitos reais neste projeto | [Verificação](#verificação) |
| 13 | Como sabe que a **RAW não mudou**? | `make freeze` sela `(path, sha256, bytes, records)` de 81 partições num `capture_id`; `make freeze-check` relê o RAW e sai 1 em qualquer diferença | [`docs/FREEZE.md`](docs/FREEZE.md) |
| 14 | Quais **limitações** permanecem? | Oito itens de dívida, cada um com problema, impacto, status e próximo passo; mais o escopo que exige fonte nova | [ARCHITECTURE.md § "Dívida técnica"](ARCHITECTURE.md) · [BACKLOG.md](BACKLOG.md) |

### O que **não** está demonstrado neste projeto

Escrito com estas palavras de propósito. A ausência de prova é informação, e apagá-la seria a
única forma de esta lista ficar bonita:

- **Que o Spark escala.** Ele roda `local[*]` — driver e executor no mesmo JVM, sem shuffle
  entre nós. **Não demonstrado neste projeto.**
- **Que alguém precisa da latência do Kafka, ou que o broker seja a origem.** O log canônico
  continua nascendo em disco. **Não demonstrado neste projeto.**
- **Exactly-once ponta a ponta.** Não é prometido, e a janela onde a duplicação acontece está
  reproduzida em teste em vez de escondida.
- **Que o RAW é reproduzível.** Não é, por natureza das fontes. O que é garantido é
  `mesmo RAW congelado → downstream reproduzível`.
- **Que a operação é production-grade.** Não há tráfego real, SLO, plantão nem incidente. O que
  existe é comportamento verificado em teste e evidência datada de execução.
- **Que o comportamento de compra é realista.** Cliente e pedido são sintéticos, calibrados
  contra um **benchmark** que nunca é tratado como ground truth.
