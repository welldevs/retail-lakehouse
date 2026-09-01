# Plataforma Retail: catalogo da Mercadona + populacao e Callejero do INE.
#
# Este Makefile e o ponto de entrada da PLATAFORMA. Cada Source tem o seu proprio
# Makefile (sources/mercadona-catalog-source/, sources/ine-population-source/,
# sources/ine-callejero-source/), e continua utilizavel de forma isolada — e isso e a
# fronteira: nada aqui altera arquivo delas.
#
#   make up             sobe o object storage
#   make daily          Mercadona: extract -> validate -> land -> verify -> silver
#   make ine-refresh    INE populacao: mesma cadeia, sob demanda (sem cron)
#   make callejero-refresh  INE Callejero: sem API, incorpora arquivos ja baixados
#   make test           suite de cada Source (sem rede) + suite da plataforma (sem rede)

-include .env
# Identidade do Snowflake em arquivo PROPRIO, e opcional: toda a metade Lakehouse (up,
# daily, silver, test) roda sem ele. Separado do .env porque tem ciclo de vida diferente —
# trocar de conta Snowflake nao deveria exigir tocar na configuracao do MinIO. Copie de
# .env.snowflake.example. Nao guarda segredo: a chave privada mora fora do repo e o que
# viaja aqui e o CAMINHO dela.
-include .env.snowflake
export

PYTHON        ?= python3
MERCADONA_SOURCE_DIR ?= sources/mercadona-catalog-source
MERCADONA_SOURCE_SRC  = $(MERCADONA_SOURCE_DIR)/src
PLATFORM_PY   ?= platform/.venv/bin/python
DBT           ?= platform/.venv/bin/dbt
# --env-file e OBRIGATORIO: o project dir do compose e infra/, entao sem isto o .env da
# raiz nao e lido e ${AIRFLOW_UID} cai no default 50000 — o container roda como outro
# usuario e nao consegue ler os arquivos da particao, que a Source grava com modo 600.
COMPOSE       ?= docker compose --env-file .env -f infra/docker-compose.yml

MERCADONA_DATA_ROOT ?= data/mercadona
WH            ?= mad1
DATE          ?= $(shell date -u +%F)
MERCADONA_PARTITION = $(MERCADONA_DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)

# Segunda source, independente da Mercadona: sem eixo de armazem, particao so por
# ingestion_date. Ver sources/ine-population-source/.
# 31304 = populacao por provincia+idade+sexo (Fase inicial); 29005 = populacao por
# municipio+sexo, sem idade (Fase A do plano de granularidade municipal) — mesmo
# mecanismo generico de fetch, so muda o table_id. Ver CONTRACT.md da source, secao 2.
INE_SOURCE_DIR ?= sources/ine-population-source
INE_SOURCE_SRC  = $(INE_SOURCE_DIR)/src
INE_DATA_ROOT  ?= data/ine
INE_TABLES     ?= 31304,29005
INE_PARTITION   = $(INE_DATA_ROOT)/ingestion_date=$(DATE)
# O default da Source e 30s por requisicao, dimensionado para uma chamada de API comum.
# Estas duas tabelas nao sao comuns: MEDIDO em disco, 31304 devolve 264 MB e 29005 outros
# 125 MB num unico GET. Com 30s a extracao estola sem nunca completar; com 240s ela passa,
# e ainda assim ja foi observada uma resposta truncar no meio (JSON corrompido por volta do
# byte 154.000.000), que so o retry resolveu. A Source continua com o default generico —
# quem sabe o tamanho da tabela e quem a pede, e e aqui que isso fica escrito.
INE_TIMEOUT     ?= 240
INE_MAX_RETRIES ?= 3

# Terceira source: Callejero do INE. Sem API — "extract" incorpora arquivos ja
# baixados manualmente do site do INE em CALLEJERO_IN. Ver sources/ine-callejero-source/.
CALLEJERO_SOURCE_DIR ?= sources/ine-callejero-source
CALLEJERO_SOURCE_SRC  = $(CALLEJERO_SOURCE_DIR)/src
CALLEJERO_IN         ?= temp
CALLEJERO_DATA_ROOT  ?= data/callejero
CALLEJERO_PROVINCES  ?= 08,28,41,46
CALLEJERO_PARTITION   = $(CALLEJERO_DATA_ROOT)/ingestion_date=$(DATE)

# Quarta source: OLTP simulado. A PRIMEIRA source derivada — as outras tres sao upstream
# de dado externo, esta consome o Silver que elas produziram. Como toda Source e FROZEN
# (dependencies = []) e nao pode falar com o Lakehouse, a plataforma materializa antes o
# que ela precisa em tres JSON planos (`oltp-export-reference`), e a Source os le so com
# a stdlib. Ver sources/simulated-oltp-source/CONTRACT.md secao 2.
OLTP_SOURCE_DIR      ?= sources/simulated-oltp-source
OLTP_SOURCE_SRC       = $(OLTP_SOURCE_DIR)/src
OLTP_DATA_ROOT       ?= data/oltp
OLTP_REFERENCE_ROOT  ?= data/oltp-reference
OLTP_REFERENCE        = $(OLTP_REFERENCE_ROOT)/ingestion_date=$(DATE)
# VAZIO POR PADRAO, e isso e deliberado. Ate a Fase 5 este numero era 5.000 para os quatro
# armazens — o mesmo para AUFs que diferem por 4,6x em populacao. Vazio, o `extract` usa o
# alvo que `oltp-export-reference` derivou da populacao ADULTA de cada armazem vezes a taxa de
# penetracao (customer_premises_seed). Definir a variavel continua funcionando e vira override
# explicito, registrado como `count_source: cli` no manifesto.
OLTP_CUSTOMERS_PER_WH ?=
# Seed fixa por padrao, e nao aleatoria: a mesma data com a mesma referencia tem de
# reproduzir a mesma particao. Passe SEED=... para gerar outra populacao sintetica.
OLTP_SEED            ?= 20260827
OLTP_PARTITION        = $(OLTP_DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)
# Regerar a base de clientes deliberadamente. Sem isto, uma particao completa e imutavel
# como nas outras tres sources. Crescer a base e ADITIVO: o gerador consome uma unica
# random.Random(seed) em ordem fixa e nada antes do laco depende de count, entao os
# primeiros N clientes de uma geracao maior sao byte a byte os mesmos de antes (provado em
# tests/test_customers_generator.py::test_aumentar_count_e_aditivo_nao_reembaralha). O
# manifesto guarda a seed e o count de cada execucao anterior em `history`.
OLTP_OVERWRITE       ?=

# Quinta source: pedidos simulados como LOG DE EVENTOS. Segunda source derivada, como a de
# clientes — e pelo mesmo mecanismo: a plataforma materializa cliente, catalogo, calendario de
# preco e premissas em quatro JSON planos (`orders-export-reference`), e a Source os le so com
# a stdlib. Ver sources/simulated-orders-source/CONTRACT.md.
ORDERS_SOURCE_DIR      ?= sources/simulated-orders-source
ORDERS_SOURCE_SRC       = $(ORDERS_SOURCE_DIR)/src
ORDERS_DATA_ROOT       ?= data/orders
ORDERS_REFERENCE_ROOT  ?= data/orders-reference
ORDERS_REFERENCE        = $(ORDERS_REFERENCE_ROOT)/ingestion_date=$(ORDERS_TO)
# A JANELA E DECLARADA, e nao derivada de "hoje". Quem sabe de quando ate quando os pedidos
# existem e quem opera; adivinhar produziria uma base de fatos diferente a cada execucao.
# Medido em 2026-08-28: os 4 armazens tem catalogo de 08-24 a 08-27.
ORDERS_FROM            ?= 2026-08-24
ORDERS_TO              ?= 2026-08-27
ORDERS_SEED            ?= 20260828
# AQUI ingestion_date E A DATA DO PEDIDO, nao o dia da extracao.
ORDERS_PARTITION        = $(ORDERS_DATA_ROOT)/ingestion_date=$(ORDERS_DATE)/wh=$(WH)
ORDERS_DATE            ?= $(ORDERS_FROM)
ORDERS_OVERWRITE       ?=

.PHONY: help up down logs status venv secrets test source-test platform-test \
        extract validate land verify-landing silver daily query duckdb-secret \
        airflow airflow-down airflow-logs airflow-trigger clean-duckdb \
        ine-extract ine-validate ine-land ine-verify-landing ine-refresh ine-trigger \
        callejero-extract callejero-validate callejero-land callejero-verify-landing \
        callejero-refresh callejero-trigger \
        oltp-export-reference oltp-extract oltp-validate oltp-land oltp-verify-landing \
        oltp-refresh oltp-refresh-all oltp-trigger prune-local data-usage \
        orders-export-reference orders-extract orders-validate orders-land \
        orders-verify-landing orders-refresh orders-refresh-all orders-trigger \
        stream-up stream-down stream-logs orders-oltp-ddl orders-oltp-init \
        orders-apply orders-apply-all orders-outbox orders-prove-atomicity \
        orders-projection-init orders-publish orders-project orders-lag \
        orders-replay orders-topic orders-prove-stream spike-iceberg \
        iceberg-init iceberg-metadata orders-project-iceberg \
        orders-rebuild-projection orders-reconcile orders-prove-projection \
        stream-evidence \
        warehouse-bootstrap warehouse-export warehouse-ddl warehouse-load warehouse \
        warehouse-refresh warehouse-evidence warehouse-trigger warehouse-prove-tests \
        dashboard dashboard-venv dashboard-contract dashboard-check \
        demand-reality-check demand-check-mapping

help:
	@echo "infra"
	@echo "  up              sobe o MinIO e cria os buckets (so o plano de dados)"
	@echo "  down            derruba tudo (mantem os volumes)"
	@echo "  status          estado dos containers e conteudo dos buckets"
	@echo ""
	@echo "orquestracao"
	@echo "  airflow         builda e sobe Postgres + scheduler + webserver (:8080)"
	@echo "  airflow-trigger dispara o DAG de hoje e acompanha"
	@echo "  airflow-logs    logs do scheduler"
	@echo "  airflow-down    derruba so o Airflow, mantendo o MinIO de pe"
	@echo ""
	@echo "pipeline (DATE=$(DATE) WH=$(WH))"
	@echo "  extract         extrai um snapshot para $(MERCADONA_PARTITION)"
	@echo "  validate        valida a particao em modo --strict"
	@echo "  land            sobe a particao para o object storage, com sha256 conferido"
	@echo "  verify-landing  rele do object storage e reconfere"
	@echo "  silver          dbt build (modelos + testes)"
	@echo "  daily           os cinco acima, em ordem"
	@echo ""
	@echo "populacao do INE (DATE=$(DATE) TABLES=$(INE_TABLES)) — sem cadencia fixa,"
	@echo "roda quando alguem decide rodar, nao num cron (publicacao do INE e irregular)"
	@echo "  ine-extract         extrai um snapshot para $(INE_PARTITION)"
	@echo "  ine-validate        valida a particao (sem --strict — ver comentario do alvo)"
	@echo "  ine-land            sobe a particao para o object storage"
	@echo "  ine-verify-landing  rele do object storage e reconfere"
	@echo "  ine-refresh         os quatro acima + silver, em ordem"
	@echo "  ine-trigger         dispara a DAG sob demanda no Airflow e acompanha"
	@echo ""
	@echo "callejero do INE (CALLEJERO_IN=$(CALLEJERO_IN) PROVINCES=$(CALLEJERO_PROVINCES)) —"
	@echo "sem API: extract incorpora arquivos ja baixados manualmente, sem cadencia fixa"
	@echo "  callejero-extract         incorpora CALLEJERO_IN para $(CALLEJERO_PARTITION)"
	@echo "  callejero-validate        valida a particao em modo --strict"
	@echo "  callejero-land            sobe a particao para o object storage"
	@echo "  callejero-verify-landing  rele do object storage e reconfere"
	@echo "  callejero-refresh         os quatro acima + silver, em ordem"
	@echo "  callejero-trigger         dispara a DAG sob demanda no Airflow e acompanha"
	@echo ""
	@echo "OLTP simulado (WH=$(WH) COUNT=$(or $(OLTP_CUSTOMERS_PER_WH),da referencia) SEED=$(OLTP_SEED)) —"
	@echo "clientes sinteticos com endereco real; sem rede, derivado do Silver:"
	@echo "  oltp-export-reference     materializa o Silver em $(OLTP_REFERENCE) (roda 1x, cobre os 4 wh)"
	@echo "  oltp-extract              gera os clientes de $(WH) em $(OLTP_PARTITION)"
	@echo "  oltp-validate             valida a particao e a coerencia geografica"
	@echo "  oltp-land                 sobe a particao para o object storage"
	@echo "  oltp-verify-landing       rele do object storage e reconfere"
	@echo "  oltp-refresh              os quatro acima + silver, em ordem"
	@echo "  oltp-refresh-all          os quatro wh, e silver uma vez no fim"
	@echo "  oltp-trigger              dispara a DAG sob demanda no Airflow e acompanha"
	@echo "  ...o tamanho da base:     vem da referencia (populacao adulta x taxa), nao daqui."
	@echo "                            OLTP_CUSTOMERS_PER_WH=N so para override explicito."
	@echo "  ...aumentar a base:       OLTP_CUSTOMERS_PER_WH=N OLTP_OVERWRITE=1 (aditivo:"
	@echo "                            os clientes que ja existem sao preservados)"
	@echo ""
	@echo "pedidos simulados (ORDERS_FROM=$(ORDERS_FROM) ORDERS_TO=$(ORDERS_TO) SEED=$(ORDERS_SEED)) —"
	@echo "log de eventos com cliente, produto e preco reais; sem rede, derivado do Silver:"
	@echo "  orders-export-reference   cliente + catalogo + calendario de preco + premissas (1x)"
	@echo "  orders-extract            gera o log de $(WH) no dia $(ORDERS_DATE)"
	@echo "  orders-validate           valida a particao e a coerencia com catalogo e cliente"
	@echo "  orders-land               sobe a particao para o object storage"
	@echo "  orders-verify-landing     rele do object storage e reconfere"
	@echo "  orders-refresh            os quatro acima + silver, para um (armazem, dia)"
	@echo "  orders-refresh-all        a janela inteira: 4 armazens x N dias, silver no fim"
	@echo "  orders-trigger            dispara a DAG simulated_orders_events"
	@echo "  ...a janela e limitada pelo catalogo: um dia sem snapshot anterior ou igual"
	@echo "     para aquele armazem RECUSA, em vez de inventar preco."
	@echo ""
	@echo "plano de stream (nao sobe com \`make up\`) — OLTP de pedidos e outbox transacional:"
	@echo "  stream-up                 sobe o oltp-postgres e cria orders/order_line/outbox"
	@echo "  stream-down               derruba so o plano de stream (mantem o volume)"
	@echo "  orders-oltp-ddl           imprime a DDL do OLTP (sem conectar em nada)"
	@echo "  orders-apply              replica o log de $(WH)/$(ORDERS_DATE) no OLTP"
	@echo "  orders-apply-all          a janela inteira: 4 armazens x N dias"
	@echo "  orders-outbox             estado do outbox; PARTITION=... reconstitui o log"
	@echo "  orders-prove-atomicity    injeta falha e prova que estado e evento caem juntos"
	@echo ""
	@echo "transporte (Kafka) — at-least-once no produtor, idempotente no consumidor:"
	@echo "  orders-publish            drena o outbox para $(KAFKA_TOPIC)"
	@echo "  orders-project            consome e mantem live_order_state"
	@echo "  orders-lag                lag do grupo, particao a particao"
	@echo "  orders-replay             rebobina o grupo (NAO apaga a projecao)"
	@echo "  orders-topic              descreve o topico no broker"
	@echo "  orders-prove-stream       prova transporte, duplicata, replay e buraco"
	@echo "  spike-iceberg             experimento fechado: Iceberg + catalogo + DuckDB"
	@echo ""
	@echo "projecao viva (Iceberg) — dois escritores na mesma tabela, um leitor:"
	@echo "  iceberg-init              catalogo SQL + tabela live_order_state"
	@echo "  orders-project-iceberg    o consumidor escrevendo no Iceberg (escritor 1)"
	@echo "  orders-rebuild-projection reconstroi do RAW na MESMA tabela (escritor 2)"
	@echo "  orders-reconcile          Iceberg x Silver x OLTP; sai 1 se divergirem"
	@echo "  orders-prove-projection   concorrencia, fusao monotonica e snapshot isolation"
	@echo "  stream-evidence           registra OLTP, broker, projecao e os tres folds, datado"
	@echo ""
	@echo "calibracao da demanda (MAPA 2025):"
	@echo "  demand-reality-check      ANTES | MAPA | DEPOIS nas tres dimensoes"
	@echo "  demand-reality-check SNAPSHOT=before_mapa_2025_v1   congela o ANTES"
	@echo "  demand-check-mapping      confere as 444 trincas do catalogo contra o de-para"
	@echo ""
	@echo "warehouse analitico (Snowflake, DB=$(SNOWFLAKE_DATABASE)) —"
	@echo "recebe um recorte do Silver (~5% das linhas), nunca o Silver inteiro:"
	@echo "  warehouse-bootstrap       database, schemas, papeis e grants (1x, ACCOUNTADMIN)"
	@echo "  warehouse-export          recorta o Silver para parquet em $(SNOWFLAKE_STAGE_DIR)"
	@echo "  warehouse-ddl             imprime o DDL do STAGE (sem conectar em nada)"
	@echo "  warehouse-load            PUT em stage interno + COPY INTO + reconferencia"
	@echo "  warehouse                 dbt build --target snowflake (STAGE -> GOLD -> MART)"
	@echo "  warehouse-refresh         os tres acima, em ordem"
	@echo "  warehouse-prove-tests     injeta o defeito que cada teste diz pegar e exige o vermelho"
	@echo "  warehouse-evidence        registra posse, volume e isolamento do destino, datado"
	@echo ""
	@echo "painel estrategico (Streamlit sobre o MART, papel RETAIL_READER) —"
	@echo "  dashboard-venv            instala streamlit/pandas/altair (extra, fora da imagem)"
	@echo "  dashboard                 sobe o painel em http://localhost:$(DASHBOARD_PORT)"
	@echo "  dashboard-contract        regenera streamlit/CONTRACT.md (sem conectar em nada)"
	@echo "  dashboard-check           roda o painel de verdade e exige zero excecao (exige conta)"
	@echo "  warehouse-trigger         dispara a DAG do warehouse no Airflow e acompanha"
	@echo "  ...trocar de conta:       edite .env.snowflake (veja .env.snowflake.example) e"
	@echo "                            ~/.snowflake/config.toml; nenhum modelo ou teste muda"
	@echo ""
	@echo "espaco em disco —"
	@echo "  data-usage                quanto o scratch local (data/) esta ocupando"
	@echo "  prune-local PARTITION=…   apaga a copia local, so se ela estiver integra no destino"
	@echo ""
	@echo "consulta"
	@echo "  query           consulta o Silver.  make query SQL=\"select ...\""
	@echo "  duckdb-secret   grava o secret para abrir o .duckdb em qualquer cliente"
	@echo "  clean-duckdb    apaga o .duckdb e os artefatos do dbt (só estado local; o dado vive no S3)"
	@echo ""
	@echo "testes"
	@echo "  test            source-test + platform-test"
	@echo "  source-test     suite das cinco Sources (sem rede, sem dependencias)"
	@echo "  platform-test   testes da plataforma, sem rede"
	@echo ""
	@echo "  venv            cria platform/.venv e instala a plataforma"
	@echo "  secrets         gera AIRFLOW_SECRET_KEY e AIRFLOW_FERNET_KEY no .env"

# ---- infra -----------------------------------------------------------------
# So o plano de dados. O Airflow e ~2 GB de RAM e nao e necessario para iterar num
# modelo dbt ou rodar `make daily` a mao.
up:
	$(COMPOSE) up -d minio mc
	@echo "MinIO: http://localhost:$(or $(MINIO_CONSOLE_PORT),9001) (console)"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=50

# ---- orquestracao ----------------------------------------------------------
# airflow-init roda antes e cria o pool mercadona_api com 1 slot. Sem ele o
# LocalExecutor rodaria varios extract em paralelo e dobraria a taxa contra a fonte.
airflow: up
	$(COMPOSE) up -d --build airflow-scheduler airflow-webserver
	@echo ""
	@echo "Airflow UI: http://localhost:$(or $(AIRFLOW_PORT),8080)   (admin / admin)"

airflow-down:
	$(COMPOSE) stop airflow-scheduler airflow-webserver
	$(COMPOSE) rm -f airflow-scheduler airflow-webserver airflow-init

airflow-logs:
	$(COMPOSE) logs --tail=60 airflow-scheduler

airflow-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause mercadona_catalog_daily
	$(COMPOSE) exec airflow-scheduler airflow dags trigger mercadona_catalog_daily
	@echo "disparado. acompanhe com: make airflow-logs"

# Sem schedule: a DAG so roda quando disparada, nunca sozinha (ver docstring da DAG).
# unpause continua necessario para o trigger executar de fato, nao so entrar na fila.
ine-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause ine_population_on_demand
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ine_population_on_demand
	@echo "disparado. acompanhe com: make airflow-logs"

callejero-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause ine_callejero_on_demand
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ine_callejero_on_demand
	@echo "disparado. acompanhe com: make airflow-logs"

# A base de clientes muda quando alguem decide muda-la, nunca sozinha. Para REGERAR
# (mais clientes, ou outra seed), passe os parametros no disparo:
#   make oltp-trigger OLTP_DAG_CONF='{"customers_per_wh":20000,"overwrite":true}'
OLTP_DAG_CONF ?=
oltp-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause simulated_oltp_customers
	$(COMPOSE) exec airflow-scheduler airflow dags trigger simulated_oltp_customers \
	  $(if $(OLTP_DAG_CONF),--conf '$(OLTP_DAG_CONF)',)
	@echo "disparado. acompanhe com: make airflow-logs"

ORDERS_DAG_CONF ?=
orders-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause simulated_orders_events
	$(COMPOSE) exec airflow-scheduler airflow dags trigger simulated_orders_events \
	  $(if $(ORDERS_DAG_CONF),--conf '$(ORDERS_DAG_CONF)',)

status:
	@$(COMPOSE) ps
	@echo ""
	@$(COMPOSE) run --rm --entrypoint sh mc -c \
	  'mc alias set local http://minio:9000 "$$MINIO_ROOT_USER" "$$MINIO_ROOT_PASSWORD" >/dev/null && \
	   echo "objetos em retail-raw:       $$(mc ls -r local/$$RAW_BUCKET | wc -l)" && \
	   echo "objetos em retail-lakehouse: $$(mc ls -r local/$$LAKEHOUSE_BUCKET | wc -l)"'

# ---- pipeline ---------------------------------------------------------------
# A particao completa e IMUTAVEL: reexecutar extract sobre ela retorna exit 2
# (partition.py -> PartitionError). Um `make daily` que nao checasse _SUCCESS primeiro
# abortaria num dia que deu certo. Esta guarda e a mesma logica do short-circuit do DAG.
extract:
	@if [ -f "$(MERCADONA_PARTITION)/_SUCCESS" ]; then \
	  echo "particao ja completa e imutavel: $(MERCADONA_PARTITION)"; \
	  echo "extract pulado (nao e falha)."; \
	else \
	  PYTHONPATH=$(MERCADONA_SOURCE_SRC) $(PYTHON) -m mercadona_catalog_source extract \
	    --out $(MERCADONA_DATA_ROOT) --wh $(WH) --date $(DATE); \
	fi

validate:
	PYTHONPATH=$(MERCADONA_SOURCE_SRC) $(PYTHON) -m mercadona_catalog_source validate $(MERCADONA_PARTITION) --strict

land:
	$(PLATFORM_PY) -m retail_platform land $(MERCADONA_PARTITION)

verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(MERCADONA_PARTITION)

# O DuckDB e single-writer. Um cliente com o arquivo aberto em leitura-escrita (DBeaver
# faz isso por padrao) faz o dbt abortar com 20 linhas de traceback para um problema que
# se resolve fechando uma conexao. A guarda troca isso por uma linha acionavel.
DUCKDB_PATH   ?= platform/dbt/retail.duckdb
# `dbt build` compila o projeto INTEIRO de uma vez: se uma source recem-adicionada ainda
# nao aterrissou nada (clone novo, ou INE antes do primeiro `make ine-refresh`), o
# read_json dela falha com "no files found" e derruba o build de TODAS as sources, nao so
# a vazia. A guarda exclui do build o modelo de uma source sem dado ainda, em vez de
# ensinar o SQL do modelo a fingir que existe uma particao vazia (medido: o proprio
# materialization `external` do dbt-duckdb, ao lidar com relacao vazia, grava uma linha
# sentinela que reaparece como fantasma na primeira execucao com dado real).
silver:
	@$(PLATFORM_PY) -c "import duckdb; duckdb.connect('$(DUCKDB_PATH)').close()" 2>/dev/null || { \
	  echo "ERRO: outro processo tem $(DUCKDB_PATH) aberto em escrita."; \
	  echo "      O DuckDB e single-writer. Feche a conexao (DBeaver, notebook, CLI) e"; \
	  echo "      tente de novo, ou use: make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb"; \
	  echo "      O arquivo so guarda views: nada se perde ao recria-lo."; \
	  exit 2; }
# O PORTAO MORA EM UM LUGAR SO: retail_platform/silver_gate.py. Ele decidia o que excluir
# aqui E, em copia parcial, dentro de cada uma das cinco DAGs — seis lugares, nenhum igual
# ao outro. A DAG da Mercadona nao tinha portao nenhum e reprovava todo dia desde que
# `silver_live_order_state` nasceu. Um `--exclude` a mais neste arquivo nao chega no Airflow.
	RETAIL_DBT=$(DBT) $(PLATFORM_PY) -m retail_platform silver-build \
	  --project-dir platform/dbt --profiles-dir platform/dbt

daily: extract validate land verify-landing silver
	@echo ""
	@echo "daily OK para ingestion_date=$(DATE) wh=$(WH)"

# ---- populacao do INE --------------------------------------------------------
# Mesma imutabilidade de particao da Mercadona (extract.py -> PartitionError), mesma
# guarda. Sem eixo de armazem: um nivel de particao a menos que $(MERCADONA_PARTITION) acima.
ine-extract:
	@if [ -f "$(INE_PARTITION)/_SUCCESS" ]; then \
	  echo "particao ja completa e imutavel: $(INE_PARTITION)"; \
	  echo "extract pulado (nao e falha)."; \
	else \
	  PYTHONPATH=$(INE_SOURCE_SRC) $(PYTHON) -m ine_population_source extract \
	    --out $(INE_DATA_ROOT) --tables $(INE_TABLES) --date $(DATE) \
	    --timeout $(INE_TIMEOUT) --max-retries $(INE_MAX_RETRIES); \
	fi

# Sem --strict: medido que table_id=29005 tem 6 series (de ~8.200 municipios da
# Espanha inteira) sem NENHUM ponto de dado — "Gatova" (Castellon, 12) e "Palmerola"
# (Girona, 17), nenhum dos dois dentro do escopo desta plataforma (08/28/41/46; nao
# aparecem em ine_municipality_codes_seed). --strict e opcional por contrato (CONTRACT.md
# da source, "Qualidade"); reprovar a particao por 2 municipios fora do escopo, sempre
# que a extracao rodar, nao protegeria nada real. A contagem continua REPORTADA no
# output do validate mesmo sem --strict (nao fica silenciosa).
ine-validate:
	PYTHONPATH=$(INE_SOURCE_SRC) $(PYTHON) -m ine_population_source validate $(INE_PARTITION)

ine-land:
	$(PLATFORM_PY) -m retail_platform land $(INE_PARTITION)

ine-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(INE_PARTITION)

# Nome deliberadamente sem "daily": isto roda quando alguem decide rodar (make
# ine-refresh, ou a DAG sob demanda), nunca numa cadencia fixa — ver Makefile help e a
# docstring de orchestration/airflow/dags/ine_population_on_demand.py.
ine-refresh: ine-extract ine-validate ine-land ine-verify-landing silver
	@echo ""
	@echo "ine-refresh OK para ingestion_date=$(DATE)"

# ---- callejero do INE ---------------------------------------------------------
# Sem API: "extract" nao busca rede, incorpora arquivos de CALLEJERO_IN (baixados
# manualmente do site do INE). Mesma imutabilidade de particao das outras duas
# sources, mesma guarda. Particao so por ingestion_date (decisao do plano: o artefato
# e um intake do dataset oficial inteiro, nao um recorte por warehouse/provincia).
callejero-extract:
	@if [ -f "$(CALLEJERO_PARTITION)/_SUCCESS" ]; then \
	  echo "particao ja completa e imutavel: $(CALLEJERO_PARTITION)"; \
	  echo "extract pulado (nao e falha)."; \
	else \
	  PYTHONPATH=$(CALLEJERO_SOURCE_SRC) $(PYTHON) -m ine_callejero_source extract \
	    --in $(CALLEJERO_IN) --out $(CALLEJERO_DATA_ROOT) --provinces $(CALLEJERO_PROVINCES) --date $(DATE); \
	fi

callejero-validate:
	PYTHONPATH=$(CALLEJERO_SOURCE_SRC) $(PYTHON) -m ine_callejero_source validate $(CALLEJERO_PARTITION) --strict

callejero-land:
	$(PLATFORM_PY) -m retail_platform land $(CALLEJERO_PARTITION)

callejero-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(CALLEJERO_PARTITION)

# Nome deliberadamente sem "daily": roda quando alguem decide rodar (o Callejero e
# publicado semestralmente, sem API para verificar se ha algo novo) — ver Makefile
# help e a docstring de orchestration/airflow/dags/ine_callejero_on_demand.py.
callejero-refresh: callejero-extract callejero-validate callejero-land callejero-verify-landing silver
	@echo ""
	@echo "callejero-refresh OK para ingestion_date=$(DATE)"

# ---- OLTP simulado ----------------------------------------------------------
# Sem rede em nenhum passo. `oltp-export-reference` roda UMA vez e cobre os quatro
# warehouses de uma so consulta ao Silver: por isso ele NAO entra em `oltp-refresh`,
# que e por warehouse — encadea-lo faria a mesma consulta pesada quatro vezes.
#
#   make oltp-export-reference     (uma vez, depois de callejero-refresh e ine-refresh)
#   make oltp-refresh WH=mad1      (repetir para bcn1, svq1, vlc1 — ou oltp-refresh-all)
oltp-export-reference:
	$(PLATFORM_PY) -m retail_platform export-oltp-reference \
	  --out $(OLTP_REFERENCE_ROOT) --date $(DATE)

oltp-extract:
	@if [ -f "$(OLTP_PARTITION)/_SUCCESS" ] && [ -z "$(OLTP_OVERWRITE)" ]; then \
	  echo "particao ja completa e imutavel: $(OLTP_PARTITION)"; \
	  echo "extract pulado (nao e falha). Use OLTP_OVERWRITE=1 para regerar a base."; \
	else \
	  PYTHONPATH=$(OLTP_SOURCE_SRC) $(PYTHON) -m simulated_oltp_source extract \
	    --reference $(OLTP_REFERENCE) --out $(OLTP_DATA_ROOT) --wh $(WH) --date $(DATE) \
	    $(if $(OLTP_CUSTOMERS_PER_WH),--count $(OLTP_CUSTOMERS_PER_WH),) --seed $(OLTP_SEED) \
	    $(if $(OLTP_OVERWRITE),--overwrite,); \
	fi

# --reference nao e opcional aqui, diferente das outras tres sources: a garantia central
# desta Source — todo cliente mora num endereco real da AUF do seu warehouse — so pode
# ser reconferida relendo a mesma referencia que gerou a particao.
oltp-validate:
	PYTHONPATH=$(OLTP_SOURCE_SRC) $(PYTHON) -m simulated_oltp_source validate \
	  $(OLTP_PARTITION) --reference $(OLTP_REFERENCE) --strict

oltp-land:
	$(PLATFORM_PY) -m retail_platform land $(OLTP_PARTITION)

oltp-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(OLTP_PARTITION)

oltp-refresh: oltp-extract oltp-validate oltp-land oltp-verify-landing silver
	@echo ""
	@echo "oltp-refresh OK para ingestion_date=$(DATE) wh=$(WH)"

# `silver` UMA vez no fim, nao dentro do laco: `dbt build` cobre o projeto inteiro, entao
# encadea-lo por armazem repetiria os mesmos 215 modelos e testes quatro vezes para chegar
# ao mesmo resultado. Por isso o laco chama a cadeia de aterrissagem, nao `oltp-refresh`.
oltp-refresh-all:
	@for wh in mad1 bcn1 svq1 vlc1; do \
	  $(MAKE) --no-print-directory oltp-extract oltp-validate oltp-land oltp-verify-landing \
	    WH=$$wh || exit 1; \
	done
	@$(MAKE) --no-print-directory silver

# ---- quinta source: pedidos simulados (log de eventos) ----------------------
# Roda UMA vez e cobre os quatro armazens e a janela inteira. Encadea-lo no orders-refresh
# (que e por armazem e por dia) repetiria a mesma consulta pesada 16 vezes. Mesmo motivo pelo
# qual oltp-export-reference nao entra em oltp-refresh.
# CONGELA o ANTES. Passo separado de proposito: depois de `orders-refresh-all --overwrite`
# o estado anterior nao existe mais em lugar nenhum, e um reality check sem ANTES so
# consegue dizer "e assim hoje", que e metade da pergunta.
demand-reality-check:
	$(PLATFORM_PY) -m retail_platform demand-reality-check \
	  $(if $(SNAPSHOT),--snapshot $(SNAPSHOT),)

# Confere o de-para contra a ARVORE de categorias, nao contra o recorte: o dedup do catalogo
# esconde trincas que existem na fonte, e conferir contra ele mediria o desempate.
demand-check-mapping:
	$(PLATFORM_PY) -m retail_platform export-orders-reference \
	  --out $(ORDERS_REFERENCE_ROOT) --date $(ORDERS_TO) \
	  --from $(ORDERS_FROM) --to $(ORDERS_TO)

orders-export-reference:
	$(PLATFORM_PY) -m retail_platform export-orders-reference \
	  --out $(ORDERS_REFERENCE_ROOT) --date $(ORDERS_TO) \
	  --from $(ORDERS_FROM) --to $(ORDERS_TO)

orders-extract:
	@if [ -f "$(ORDERS_PARTITION)/_SUCCESS" ] && [ -z "$(ORDERS_OVERWRITE)" ]; then \
	  echo "particao ja completa e imutavel: $(ORDERS_PARTITION)"; \
	  echo "extract pulado (nao e falha). Use ORDERS_OVERWRITE=1 para regerar."; \
	else \
	  PYTHONPATH=$(ORDERS_SOURCE_SRC) $(PYTHON) -m simulated_orders_source extract \
	    --reference $(ORDERS_REFERENCE) --out $(ORDERS_DATA_ROOT) --wh $(WH) \
	    --date $(ORDERS_DATE) --seed $(ORDERS_SEED) \
	    $(if $(ORDERS_OVERWRITE),--overwrite,); \
	fi

orders-validate:
	PYTHONPATH=$(ORDERS_SOURCE_SRC) $(PYTHON) -m simulated_orders_source validate \
	  $(ORDERS_PARTITION) --reference $(ORDERS_REFERENCE) --strict

orders-land:
	$(PLATFORM_PY) -m retail_platform land $(ORDERS_PARTITION)

orders-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(ORDERS_PARTITION)

orders-refresh: orders-extract orders-validate orders-land orders-verify-landing silver
	@echo ""
	@echo "orders-refresh OK para ingestion_date=$(ORDERS_DATE) wh=$(WH)"

# A janela inteira: 4 armazens x N dias, com o silver UMA vez no fim. O laco de datas usa
# `date -d`, que aceita a aritmetica de dia sem precisar de Python aqui.
orders-refresh-all:
	@dia="$(ORDERS_FROM)"; \
	while [ "$$dia" != "$$(date -u -d "$(ORDERS_TO) +1 day" +%F)" ]; do \
	  for wh in mad1 bcn1 svq1 vlc1; do \
	    $(MAKE) --no-print-directory orders-extract orders-validate orders-land \
	      orders-verify-landing WH=$$wh ORDERS_DATE=$$dia || exit 1; \
	  done; \
	  dia="$$(date -u -d "$$dia +1 day" +%F)"; \
	done
	@$(MAKE) --no-print-directory silver

# ---- plano de stream: OLTP de pedidos e outbox --------------------------------
# NAO SOBE COM `make up`, e isso e promessa do README: o plano de dados continua sendo so
# o MinIO. O `--profile stream` do compose e o que mantem essa separacao no arquivo, em vez
# de na memoria de quem opera.
#
# O QUE ESTE BLOCO PROVA, e por que ele precede qualquer linha de Kafka: que o evento nasce
# DENTRO da transacao que muda o pedido. Sem isso, um topico seria o output do lote com
# passos a mais — que e exatamente o que ARCHITECTURE.md ja recusou por escrito.
OLTP_DSN  ?= postgresql://oltp:oltp@localhost:$(or $(OLTP_PORT),5433)/oltp
# OLTP_RESET=--reset derruba as tres tabelas antes de criar. Diferente de `prune-local`,
# que nao tem `--force`: la o alvo e uma particao aterrissada, que pode ser a unica copia;
# aqui e uma REPLICA do log, que `orders-apply` reconstroi em minutos.
OLTP_RESET ?=

stream-up:
	$(COMPOSE) --profile stream up -d oltp-postgres kafka
	@echo "aguardando o OLTP e o broker ficarem saudaveis..."
	@for i in $$(seq 1 60); do \
	  pg=$$($(COMPOSE) ps --format '{{.Health}}' oltp-postgres 2>/dev/null); \
	  kf=$$($(COMPOSE) ps --format '{{.Health}}' kafka 2>/dev/null); \
	  [ "$$pg" = "healthy" ] && [ "$$kf" = "healthy" ] && break; sleep 2; \
	done
	$(COMPOSE) --profile stream up kafka-init
	@$(MAKE) --no-print-directory orders-oltp-init orders-projection-init iceberg-init
	@echo ""
	@echo "OLTP ......... $(OLTP_DSN)"
	@echo "projecao ..... $(PROJECTION_DSN)   (banco separado: e um read model)"
	@echo "broker ....... $(KAFKA_BOOTSTRAP)  topico $(KAFKA_TOPIC)"
	@echo "iceberg ...... $(ICEBERG_WAREHOUSE)  catalogo SQL no proprio OLTP"

stream-down:
	$(COMPOSE) --profile stream stop oltp-postgres kafka
	$(COMPOSE) --profile stream rm -f oltp-postgres kafka kafka-init

stream-logs:
	$(COMPOSE) --profile stream logs --tail=60 oltp-postgres kafka

orders-oltp-ddl:
	@$(PLATFORM_PY) -m retail_platform.cli orders-oltp-ddl

orders-oltp-init:
	$(PLATFORM_PY) -m retail_platform.cli orders-oltp-init --dsn "$(OLTP_DSN)" $(OLTP_RESET)

orders-apply:
	$(PLATFORM_PY) -m retail_platform.cli orders-apply "$(ORDERS_PARTITION)" \
	  --dsn "$(OLTP_DSN)"

orders-apply-all:
	@dia="$(ORDERS_FROM)"; \
	while [ "$$dia" != "$$(date -u -d "$(ORDERS_TO) +1 day" +%F)" ]; do \
	  for wh in mad1 bcn1 svq1 vlc1; do \
	    $(MAKE) --no-print-directory orders-apply WH=$$wh ORDERS_DATE=$$dia || exit 1; \
	  done; \
	  dia="$$(date -u -d "$$dia +1 day" +%F)"; \
	done

# Com PARTITION=..., reconstitui o log a partir das linhas do outbox e compara o sha256
# com o manifesto. Contar linhas nao provaria fidelidade; reproduzir os bytes prova.
orders-outbox:
	$(PLATFORM_PY) -m retail_platform.cli orders-outbox --dsn "$(OLTP_DSN)" \
	  $(if $(PARTITION),--verify "$(PARTITION)",)

# A PROVA DO MARCO 4. Injeta um trigger que faz o insert explodir — no outbox e depois em
# orders — e confere que o outro lado tambem nao sobreviveu. A injecao e no BANCO, nao no
# codigo: e uma falha que o applier nao pode prever nem tratar, que e o tipo de falha
# contra a qual a transacao existe. Ver o cabecalho do script.
orders-prove-atomicity:
	@$(PLATFORM_PY) scripts/prove_oltp_atomicity.py "$(ORDERS_PARTITION)"

# ---- transporte: Kafka, replay e consumo idempotente --------------------------
# ISTO NAO E "INSTALAR KAFKA E PUBLICAR MENSAGEM". As quatro propriedades que este bloco
# existe para tornar demonstraveis, e onde cada uma e provada:
#
#   transporte fiel        o topico relido reproduz o sha256 dos 16 manifestos
#   semantica de entrega   at-least-once do outbox ao broker, DEMONSTRADO (nao afirmado):
#                          matar o publisher no meio do drain produz duplicata de verdade
#   consumo idempotente    dedup por (order_id, sequence_no) contra o estado — sem conjunto
#                          de event_id crescendo sem limite; buraco vira RECUSA, nao no-op
#   replay                 rebobinar o grupo e reprocessar deixa o digest identico
KAFKA_BOOTSTRAP ?= localhost:$(or $(KAFKA_PORT),9092)
KAFKA_TOPIC     ?= retail.orders.events.v1
KAFKA_GROUP     ?= orders-projector
PROJECTION_DSN  ?= postgresql://oltp:oltp@localhost:$(or $(OLTP_PORT),5433)/projection

orders-projection-init:
	$(PLATFORM_PY) -m retail_platform.cli orders-projection-init \
	  --projection-dsn "$(PROJECTION_DSN)"

orders-publish:
	$(PLATFORM_PY) -m retail_platform.cli orders-publish --dsn "$(OLTP_DSN)" \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" $(PUBLISH_ARGS)

orders-project:
	$(PLATFORM_PY) -m retail_platform.cli orders-project \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)" \
	  --projection-dsn "$(PROJECTION_DSN)" --from-beginning $(PROJECT_ARGS)

orders-lag:
	$(PLATFORM_PY) -m retail_platform.cli orders-lag \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)"

orders-replay:
	$(PLATFORM_PY) -m retail_platform.cli orders-replay \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)"

orders-topic:
	$(COMPOSE) --profile stream exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka:19092 --describe --topic "$(KAFKA_TOPIC)"

# EXPERIMENTO FECHADO, rodado ANTES de construir a projecao concorrente. O plano da Fase 3
# registrou "o DuckDB pode nao ler o catalogo SQL do pyiceberg" como a premissa mais fragil;
# isto responde nove perguntas contra o stack de verdade, limpa o que criou, e devolve um
# veredito. Se reprovar, nao se constroi a projecao — se contorna ou se muda de plano.
spike-iceberg:
	@$(PLATFORM_PY) scripts/spike_iceberg_duckdb.py

orders-prove-stream:
	@$(PLATFORM_PY) scripts/prove_stream_semantics.py

# ---- projecao viva em Iceberg: dois escritores, um leitor ---------------------
# O GATILHO DO ICEBERG DISPAROU POR CONCORRENCIA, NAO POR VOLUME. Neste volume um parquet
# reescrito com `os.replace` atomico funcionaria; o que o Iceberg compra e isolamento de
# snapshot entre DOIS ESCRITORES (o consumidor em streaming e a reconstrucao em lote) e um
# leitor concorrente (o DuckDB), mais time travel na projecao.
#
# `make spike-iceberg` mediu isso antes de qualquer linha deste bloco existir.
ICEBERG_WAREHOUSE ?= s3://retail-lakehouse/iceberg

iceberg-init:
	$(PLATFORM_PY) -m retail_platform.cli iceberg-init --warehouse "$(ICEBERG_WAREHOUSE)"

# O caminho do metadado CORRENTE, perguntado ao catalogo. O `silver` chama isto sozinho.
iceberg-metadata:
	@$(PLATFORM_PY) -m retail_platform.cli iceberg-metadata

# O consumidor escrevendo no Iceberg em vez do Postgres: a costura do Marco 5, exercida.
#
# GRUPO PROPRIO, e nao o mesmo do sink Postgres. Nao e detalhe de configuracao: dois grupos
# lendo o MESMO topico, cada um no proprio offset e no proprio ritmo, e exatamente o ponto de
# desacoplamento que o Kafka comprou. Compartilhar o grupo faria os dois competirem pelas
# particoes e cada evento chegaria a um so — que e o oposto do que se quer aqui.
KAFKA_GROUP_ICEBERG ?= orders-projector-iceberg

orders-project-iceberg:
	$(PLATFORM_PY) -m retail_platform.cli orders-project --sink iceberg \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" \
	  --group "$(KAFKA_GROUP_ICEBERG)" --from-beginning $(PROJECT_ARGS)

# O SEGUNDO escritor da mesma tabela.
# PROJECTION_RESET=1 apaga e recria a tabela antes de reconstruir. Necessario quando a Source
# foi regerada com outra seed, referencia, premissas ou demand_model_version: o merge da
# projecao e monotonico e assume que um order_id sempre e o mesmo pedido — e depois de uma
# regeracao nao e. Sem o reset, o rebuild descarta as linhas novas como "velhas" e a projecao
# fica com dois universos misturados (medido: 4.028 linhas descartadas em 2026-08-31).
orders-rebuild-projection:
	$(PLATFORM_PY) -m retail_platform.cli orders-rebuild-projection --root "$(ORDERS_DATA_ROOT)" \
	  $(if $(PROJECTION_RESET),--reset,)

# Tres folds, uma comparacao. Sai 1 em qualquer divergencia.
orders-reconcile:
	$(PLATFORM_PY) -m retail_platform.cli orders-reconcile --dsn "$(OLTP_DSN)"

# A PROVA DO MARCO 6: dois escritores concorrentes com leitor ativo, fusao monotonica,
# isolamento de snapshot e time travel — contra o Iceberg de verdade.
orders-prove-projection:
	@$(PLATFORM_PY) scripts/prove_iceberg_projection.py

# A METADE EM STREAMING NAO E COBERTA OFFLINE, e isso e divida declarada, nao descuido:
# `make test` roda sem rede — invariante do repo — entao broker, OLTP e Iceberg so existem
# enquanto `make stream-up` estiver de pe. Os duplos em memoria cobrem a FORMA do codigo
# (ordem da transacao, protocolo de dedup, construcao do SQL); a SEMANTICA dos motores
# reais so pode ser exercida contra eles. Este alvo registra que foi.
#
# Nao reprova com o plano meio de pe: cada secao ausente aparece como ausencia DECLARADA,
# nunca como numero inventado. Reprova so quando nenhum dos tres responde.
STREAM_EVIDENCE ?= docs/stream-evidence/README.md
stream-evidence:
	@$(PLATFORM_PY) -m retail_platform stream-evidence \
	  --dsn "$(OLTP_DSN)" --out $(STREAM_EVIDENCE)

# ---- warehouse analitico (Snowflake) -----------------------------------------
# O Snowflake e o QUARTO consumidor que nao alcanca o Lakehouse — os outros tres sao as
# Sources FROZEN. Nao recebe o Silver inteiro: recebe um RECORTE de ~5% das linhas
# (cresce a cada ingestao; o numero do momento sai de `make warehouse-evidence`), porque
# 81,5% do Silver e populacao NACIONAL com 1,8% de conteudo
# no escopo das 4 provincias. Ver platform/src/retail_platform/snowflake_export.py.
#
#   make warehouse-export   (Silver -> parquet local; nao fala com o Snowflake)
#   make warehouse-load     (PUT em stage interno + COPY INTO + reconferencia)
#   make warehouse          (dbt build --target snowflake: STAGE -> GOLD -> MART)
SNOWFLAKE_STAGE_DIR ?= data/snowflake-stage
SNOWFLAKE_CONNECTION ?= spark_retail
# Deliberadamente com o MESMO nome que profiles.yml le: por causa do `export` da linha 15,
# mudar isto aqui muda o destino do COPY INTO e o target do dbt de uma vez so. E o jeito de
# isolar uma execucao (CI, uma segunda pessoa) — por DATABASE, nao por prefixo de schema.
SNOWFLAKE_DATABASE  ?= RETAIL

# Uma vez por conta, exige ACCOUNTADMIN. Cria database, os tres schemas e os tres papeis,
# aplica os grants e PROVA o isolamento papel a papel — com `use secondary roles none`, sem
# o qual a verificacao passaria por engano num usuario que tambem tem ACCOUNTADMIN.
#
# CUIDADO AO NOMEAR VARIAVEL AQUI. A linha 15 deste arquivo tem um `export` nu: TODA
# variavel do make vira variavel de ambiente dos subprocessos. Um `SNOWFLAKE_USER ?=`
# (vazio) chegava ao dbt como variavel PRESENTE E VAZIA, e `env_var('SNOWFLAKE_USER',
# 'default')` devolve o vazio nesse caso, nao o default — o dbt morria com "'user' is a
# required property" enquanto `dbt debug` fora do make passava. Dai o prefixo proprio:
# nenhum nome daqui pode colidir com os que profiles.yml le.
SNOWFLAKE_GRANT_USER ?=
warehouse-bootstrap:
	$(PLATFORM_PY) -m retail_platform snowflake-bootstrap \
	  --database $(SNOWFLAKE_DATABASE) --connection $(SNOWFLAKE_CONNECTION) \
	  $(if $(SNOWFLAKE_GRANT_USER),--grant-to-user $(SNOWFLAKE_GRANT_USER),)

warehouse-export:
	$(PLATFORM_PY) -m retail_platform export-snowflake --out $(SNOWFLAKE_STAGE_DIR)

# Sem conexao nenhuma: o DDL e derivado do proprio recorte, entao pode ser revisado antes
# de qualquer coisa tocar o Snowflake.
warehouse-ddl:
	@$(PLATFORM_PY) -m retail_platform snowflake-ddl --database $(SNOWFLAKE_DATABASE)

warehouse-load:
	$(PLATFORM_PY) -m retail_platform load-snowflake \
	  --stage-dir $(SNOWFLAKE_STAGE_DIR) --database $(SNOWFLAKE_DATABASE) \
	  --connection $(SNOWFLAKE_CONNECTION)

# `--target snowflake` faz o guard `+enabled` do dbt_project.yml desligar a arvore inteira
# do Silver: nenhum read_json sobre s3:// e tentado dentro do Snowflake.
warehouse:
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt --target snowflake

# A metade Snowflake nao e reproduzivel offline como a metade Lakehouse: depende de uma
# conta viva, e a usada aqui e um trial. Este alvo registra o resultado da execucao REAL —
# posse, volume, isolamento e amostra — com data e identidade da conta, para que os modelos
# do warehouse continuem tendo prova depois que a conta expirar. Regeneravel: vincular
# outra conta e rodar isto produz a evidencia daquela conta.
SNOWFLAKE_EVIDENCE ?= docs/warehouse-evidence/README.md
warehouse-evidence:
	@$(PLATFORM_PY) -m retail_platform snowflake-evidence \
	  --database $(SNOWFLAKE_DATABASE) --connection $(SNOWFLAKE_CONNECTION) \
	  --out $(SNOWFLAKE_EVIDENCE)

# "Verificacao que nunca falhou nao e verificacao" — e o Marco 7 mostrou por que a regra
# existe: a primeira carga do STAGE pos TODO timestamp no ano 56.648.666 e os 166 nos do
# dbt construiram em VERDE (contagem certa, tipo certo, grao unico, e um funil feito de
# `count_if(marco is not null)` continua exato com o instante 56 milhoes de anos deslocado).
# Este alvo injeta, no dado REAL do warehouse, o defeito que cada teste diz pegar, exige o
# vermelho, desfaz e exige o verde de volta. So mexe em GOLD e MART, que sao inteiramente
# reconstruiveis por `dbt build`.
warehouse-prove-tests:
	@$(PLATFORM_PY) scripts/prove_warehouse_orders_tests.py

warehouse-refresh: warehouse-export warehouse-load warehouse
	@echo ""
	@echo "warehouse-refresh OK: STAGE carregado e GOLD/MART reconstruidos"

# ---- painel estrategico (Streamlit sobre o MART) -----------------------------
# Bancada de CONFERENCIA dos indicadores antes de reconstrui-los no Power BI. Le so o MART, e
# veste `RETAIL_READER` — o papel de BI, que este painel e o primeiro consumidor a vestir de
# verdade (a carga ja vestia RETAIL_LOADER e o dbt RETAIL_TRANSFORMER).
#
# As dependencias vivem em [project.optional-dependencies] de platform/pyproject.toml, FORA de
# `dependencies`: o Dockerfile.airflow instala exatamente aquela lista, e ~150 MB de UI nao tem
# o que fazer numa imagem que nao renderiza dashboard.
DASHBOARD_PORT ?= 8501
dashboard-venv:
	$(PLATFORM_PY) -m pip install --quiet -e "platform[dashboard]"
	@$(PLATFORM_PY) -c "import streamlit, pandas, altair; \
	  print(f'dashboard venv OK: streamlit {streamlit.__version__}, pandas {pandas.__version__}, altair {altair.__version__}')"

dashboard:
	$(PLATFORM_PY) -m streamlit run streamlit/app.py \
	  --server.port $(DASHBOARD_PORT) --server.headless true

# Sem conexao nenhuma: o CONTRACT e derivado de indicators.py, entao pode ser revisado antes
# de qualquer coisa tocar o Snowflake — mesma propriedade de `make warehouse-ddl`.
dashboard-contract:
	@$(PLATFORM_PY) streamlit/contract.py

# EXIGE CONTA VIVA, e por isso fica fora de `make test`. Roda o script do Streamlit de verdade
# e exige zero excecao: um `curl` no /health nao serve, porque o Streamlit devolve HTTP 200 com
# o esqueleto da pagina mesmo quando o script morre no primeiro `select` — a renderizacao e no
# cliente. As 19 consultas so sao exercitadas assim.
dashboard-check:
	@$(PLATFORM_PY) streamlit/smoke.py

warehouse-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause warehouse_load
	$(COMPOSE) exec airflow-scheduler airflow dags trigger warehouse_load
	@echo "disparado. acompanhe com: make airflow-logs"

# ---- consulta ---------------------------------------------------------------
# O retail.duckdb guarda apenas VIEWs sobre o parquet do object storage — nao contem
# dado. Por isso abri-lo com um cliente qualquer falha com NoSuchBucket: a sessao nova
# nao sabe o endpoint. Este alvo abre com tudo configurado; `duckdb-secret` resolve de
# vez, para qualquer cliente.
SQL ?=
query:
	@$(PLATFORM_PY) -m retail_platform query $(if $(SQL),"$(SQL)",)

duckdb-secret:
	@$(PLATFORM_PY) -m retail_platform duckdb-secret

# ---- espaco em disco ---------------------------------------------------------
# `data/` e scratch de extracao: depois de land + verify-landing, o object storage e a
# verdade e a copia local e redundante. Ela nao e pequena — uma particao do INE ocupa
# entre 264 e 384 MB — e nada nunca era apagado.
#
# Deliberadamente NAO encadeado em nenhum *-refresh: apagar dado e decisao de quem opera,
# nao efeito colateral de um pipeline. A verificacao roda ANTES da remocao e, se reprovar,
# nada e apagado — nao ha --force.
#
#   make prune-local PARTITION=data/ine/ingestion_date=2026-08-25
PARTITION ?=
prune-local:
	@if [ -z "$(PARTITION)" ]; then \
	  echo "uso: make prune-local PARTITION=<caminho da particao>"; \
	  echo "ex.: make prune-local PARTITION=$(INE_DATA_ROOT)/ingestion_date=$(DATE)"; \
	  exit 2; \
	fi
	$(PLATFORM_PY) -m retail_platform prune-local $(PARTITION)

data-usage:
	@echo "espaco ocupado pelo scratch local (data/):"
	@du -sh data/* 2>/dev/null | sort -h || echo "  (data/ vazio)"
	@echo ""
	@echo "libere uma particao ja aterrissada com: make prune-local PARTITION=<caminho>"

# ---- testes ----------------------------------------------------------------
test: source-test platform-test

# Roda no Python do sistema, sem venv: se este alvo passar, cada Source continua sem
# nenhuma dependencia de terceiros. E a fronteira sendo verificada, nao afirmada. Duas
# chamadas explicitas, nao um loop sobre sources/*: a mesma preferencia por repeticao
# clara em vez de abstracao prematura que ja existia neste Makefile antes do INE.
source-test:
	@echo "--- Mercadona Catalog Source (sem dependencias) ---"
	$(MAKE) -C $(MERCADONA_SOURCE_DIR) test
	@echo "--- INE Population Source (sem dependencias) ---"
	$(MAKE) -C $(INE_SOURCE_DIR) test
	@echo "--- INE Callejero Source (sem dependencias) ---"
	$(MAKE) -C $(CALLEJERO_SOURCE_DIR) test
	@echo "--- Simulated OLTP Source (sem dependencias) ---"
	$(MAKE) -C $(OLTP_SOURCE_DIR) test
	@echo "--- Simulated Orders Source (sem dependencias) ---"
	$(MAKE) -C $(ORDERS_SOURCE_DIR) test

platform-test:
	@echo "--- Plataforma ---"
	cd platform && ../$(PLATFORM_PY) -m unittest discover -s tests -t .

# ---- setup -----------------------------------------------------------------
venv:
	$(PYTHON) -m venv platform/.venv
	$(PLATFORM_PY) -m pip install --quiet --upgrade pip
	$(PLATFORM_PY) -m pip install --quiet -e platform/
	@$(PLATFORM_PY) -c "import boto3, duckdb; print('plataforma pronta')"

# Chaves aleatorias no .env (que esta no .gitignore). O compose recusa subir sem elas,
# entao nao existe caminho em que um valor de exemplo vire a chave real por esquecimento.
secrets:
	@$(PYTHON) scripts/gen-secrets.py

clean-duckdb:
	rm -f platform/dbt/retail.duckdb
	rm -rf platform/dbt/target platform/dbt/logs
