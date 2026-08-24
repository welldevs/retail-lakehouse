# Plataforma Retail sobre a Mercadona Catalog Source.
#
# Este Makefile e o ponto de entrada da PLATAFORMA. A Source tem o seu proprio, em
# sources/mercadona-catalog-source/Makefile, e continua utilizavel de forma isolada —
# e isso e a fronteira: nada aqui altera arquivo dela.
#
#   make up        sobe o object storage
#   make daily     extract -> validate -> land -> verify -> silver
#   make test      suite da Source (145, sem rede) + suite da plataforma (19, sem rede)

-include .env
export

PYTHON        ?= python3
SOURCE_DIR    ?= sources/mercadona-catalog-source
SOURCE_SRC     = $(SOURCE_DIR)/src
PLATFORM_PY   ?= platform/.venv/bin/python
DBT           ?= platform/.venv/bin/dbt
# --env-file e OBRIGATORIO: o project dir do compose e infra/, entao sem isto o .env da
# raiz nao e lido e ${AIRFLOW_UID} cai no default 50000 — o container roda como outro
# usuario e nao consegue ler os arquivos da particao, que a Source grava com modo 600.
COMPOSE       ?= docker compose --env-file .env -f infra/docker-compose.yml

DATA_ROOT     ?= data/source
WH            ?= mad1
DATE          ?= $(shell date -u +%F)
PARTITION      = $(DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)

.PHONY: help up down logs status venv secrets test source-test platform-test \
        extract validate land verify-landing silver daily query duckdb-secret \
        airflow airflow-down airflow-logs airflow-trigger clean-duckdb

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
	@echo "  extract         extrai um snapshot para $(PARTITION)"
	@echo "  validate        valida a particao em modo --strict"
	@echo "  land            sobe a particao para o object storage, com sha256 conferido"
	@echo "  verify-landing  rele do object storage e reconfere"
	@echo "  silver          dbt build (modelos + testes)"
	@echo "  daily           os cinco acima, em ordem"
	@echo ""
	@echo "consulta"
	@echo "  query           consulta o Silver.  make query SQL=\"select ...\""
	@echo "  duckdb-secret   grava o secret para abrir o .duckdb em qualquer cliente"
	@echo ""
	@echo "testes"
	@echo "  test            source-test + platform-test"
	@echo "  source-test     145 testes da Source, sem rede e sem dependencias"
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
	@if [ -f "$(PARTITION)/_SUCCESS" ]; then \
	  echo "particao ja completa e imutavel: $(PARTITION)"; \
	  echo "extract pulado (nao e falha)."; \
	else \
	  PYTHONPATH=$(SOURCE_SRC) $(PYTHON) -m mercadona_catalog_source extract \
	    --out $(DATA_ROOT) --wh $(WH) --date $(DATE); \
	fi

validate:
	PYTHONPATH=$(SOURCE_SRC) $(PYTHON) -m mercadona_catalog_source validate $(PARTITION) --strict

land:
	$(PLATFORM_PY) -m retail_platform land $(PARTITION)

verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(PARTITION)

# O DuckDB e single-writer. Um cliente com o arquivo aberto em leitura-escrita (DBeaver
# faz isso por padrao) faz o dbt abortar com 20 linhas de traceback para um problema que
# se resolve fechando uma conexao. A guarda troca isso por uma linha acionavel.
DUCKDB_PATH   ?= platform/dbt/retail.duckdb
silver:
	@$(PLATFORM_PY) -c "import duckdb; duckdb.connect('$(DUCKDB_PATH)').close()" 2>/dev/null || { \
	  echo "ERRO: outro processo tem $(DUCKDB_PATH) aberto em escrita."; \
	  echo "      O DuckDB e single-writer. Feche a conexao (DBeaver, notebook, CLI) e"; \
	  echo "      tente de novo, ou use: make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb"; \
	  echo "      O arquivo so guarda views: nada se perde ao recria-lo."; \
	  exit 2; }
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt

daily: extract validate land verify-landing silver
	@echo ""
	@echo "daily OK para ingestion_date=$(DATE) wh=$(WH)"

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

# Roda no Python do sistema, sem venv: se este alvo passar, a Source continua sem
# nenhuma dependencia de terceiros. E a fronteira sendo verificada, nao afirmada.
source-test:
	@echo "--- Source (sem dependencias) ---"
	$(MAKE) -C $(SOURCE_DIR) test

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
