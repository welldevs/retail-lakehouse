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
INE_SOURCE_DIR ?= sources/ine-population-source
INE_SOURCE_SRC  = $(INE_SOURCE_DIR)/src
INE_DATA_ROOT  ?= data/ine
INE_TABLES     ?= 31304
INE_PARTITION   = $(INE_DATA_ROOT)/ingestion_date=$(DATE)

# Terceira source: Callejero do INE. Sem API — "extract" incorpora arquivos ja
# baixados manualmente do site do INE em CALLEJERO_IN. Ver sources/ine-callejero-source/.
CALLEJERO_SOURCE_DIR ?= sources/ine-callejero-source
CALLEJERO_SOURCE_SRC  = $(CALLEJERO_SOURCE_DIR)/src
CALLEJERO_IN         ?= temp
CALLEJERO_DATA_ROOT  ?= data/callejero
CALLEJERO_PROVINCES  ?= 08,28,41,46
CALLEJERO_PARTITION   = $(CALLEJERO_DATA_ROOT)/ingestion_date=$(DATE)

.PHONY: help up down logs status venv secrets test source-test platform-test \
        extract validate land verify-landing silver daily query duckdb-secret \
        airflow airflow-down airflow-logs airflow-trigger clean-duckdb \
        ine-extract ine-validate ine-land ine-verify-landing ine-refresh ine-trigger \
        callejero-extract callejero-validate callejero-land callejero-verify-landing \
        callejero-refresh callejero-trigger

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
	@echo "  ine-validate        valida a particao em modo --strict"
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
	@echo "consulta"
	@echo "  query           consulta o Silver.  make query SQL=\"select ...\""
	@echo "  duckdb-secret   grava o secret para abrir o .duckdb em qualquer cliente"
	@echo ""
	@echo "testes"
	@echo "  test            source-test + platform-test"
	@echo "  source-test     suite de cada Source (Mercadona + INE populacao + Callejero), sem rede"
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
	$(eval SILVER_EXCLUDE := $(shell $(PLATFORM_PY) -m retail_platform has-data ine_population_api >/dev/null 2>&1 || echo "--exclude silver_ine_population_series"))
	$(eval SILVER_EXCLUDE += $(shell $(PLATFORM_PY) -m retail_platform has-data ine_callejero >/dev/null 2>&1 || echo "--exclude silver_callejero_sections silver_callejero_population_units silver_callejero_streets silver_callejero_pseudo_addresses"))
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt $(SILVER_EXCLUDE)

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
	    --out $(INE_DATA_ROOT) --tables $(INE_TABLES) --date $(DATE); \
	fi

ine-validate:
	PYTHONPATH=$(INE_SOURCE_SRC) $(PYTHON) -m ine_population_source validate $(INE_PARTITION) --strict

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
