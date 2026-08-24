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
COMPOSE       ?= docker compose -f infra/docker-compose.yml

DATA_ROOT     ?= data/source
WH            ?= mad1
DATE          ?= $(shell date -u +%F)
PARTITION      = $(DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)

.PHONY: help up down logs status venv test source-test platform-test \
        extract validate land verify-landing silver daily clean-duckdb

help:
	@echo "infra"
	@echo "  up              sobe o MinIO e cria os buckets"
	@echo "  down            derruba o stack (mantem o volume)"
	@echo "  status          estado dos containers e conteudo dos buckets"
	@echo ""
	@echo "pipeline (DATE=$(DATE) WH=$(WH))"
	@echo "  extract         extrai um snapshot para $(PARTITION)"
	@echo "  validate        valida a particao em modo --strict"
	@echo "  land            sobe a particao para o object storage, com sha256 conferido"
	@echo "  verify-landing  rele do object storage e reconfere"
	@echo "  silver          dbt build (modelos + testes)"
	@echo "  daily           os cinco acima, em ordem"
	@echo ""
	@echo "testes"
	@echo "  test            source-test + platform-test"
	@echo "  source-test     145 testes da Source, sem rede e sem dependencias"
	@echo "  platform-test   testes da plataforma, sem rede"
	@echo ""
	@echo "  venv            cria platform/.venv e instala a plataforma"

# ---- infra -----------------------------------------------------------------
up:
	$(COMPOSE) up -d
	@echo "MinIO: http://localhost:$(or $(MINIO_CONSOLE_PORT),9001) (console)"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=50

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

silver:
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt

daily: extract validate land verify-landing silver
	@echo ""
	@echo "daily OK para ingestion_date=$(DATE) wh=$(WH)"

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

clean-duckdb:
	rm -f platform/dbt/retail.duckdb
	rm -rf platform/dbt/target platform/dbt/logs
