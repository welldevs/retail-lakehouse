# Retail Platform: Mercadona + INE (population, Callejero) + simulated customers and orders.
#
# This Makefile is the entry point of the PLATFORM. Each of the five Sources has its own
# Makefile (sources/*/Makefile) and remains usable in isolation — and that is the
# boundary: nothing here changes their files.
#
#   make up             starts the object storage
#   make daily          Mercadona: extract -> validate -> land -> verify -> silver
#   make ine-refresh    INE population: same chain, on demand (no cron)
#   make callejero-refresh  INE Callejero: no API, ingests files already downloaded
#   make test           each Source's suite (offline) + the platform suite (offline)

-include .env
# Snowflake identity lives in its OWN file, and it is optional: the whole Lakehouse half (up,
# daily, silver, test) runs without it. Kept apart from .env because it has a different
# lifecycle — switching Snowflake accounts should not require touching the MinIO
# configuration. Copy from .env.snowflake.example. It holds no secret: the private key lives
# outside the repo and what travels here is its PATH.
-include .env.snowflake
export

PYTHON        ?= python3
MERCADONA_SOURCE_DIR ?= sources/mercadona-catalog-source
MERCADONA_SOURCE_SRC  = $(MERCADONA_SOURCE_DIR)/src
PLATFORM_PY   ?= platform/.venv/bin/python
DBT           ?= platform/.venv/bin/dbt
# --env-file is MANDATORY: the compose project dir is infra/, so without it the root .env
# is not read and ${AIRFLOW_UID} falls back to the default 50000 — the container runs as a
# different user and cannot read the partition files, which the Source writes with mode 600.
COMPOSE       ?= docker compose --env-file .env -f infra/docker-compose.yml

MERCADONA_DATA_ROOT ?= data/mercadona
WH            ?= mad1
DATE          ?= $(shell date -u +%F)
MERCADONA_PARTITION = $(MERCADONA_DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)

# Second source, independent of Mercadona: no warehouse axis, partitioned only by
# ingestion_date. See sources/ine-population-source/.
# 31304 = population by province+age+sex (initial phase); 29005 = population by
# municipality+sex, no age (Phase A of the municipal-granularity plan) — same generic
# fetch mechanism, only the table_id changes. See the source's CONTRACT.md, section 2.
INE_SOURCE_DIR ?= sources/ine-population-source
INE_SOURCE_SRC  = $(INE_SOURCE_DIR)/src
INE_DATA_ROOT  ?= data/ine
INE_TABLES     ?= 31304,29005
INE_PARTITION   = $(INE_DATA_ROOT)/ingestion_date=$(DATE)
# The Source default is 30s per request, sized for an ordinary API call.
# These two tables are not ordinary: MEASURED on disk, 31304 returns 264 MB and 29005 another
# 125 MB in a single GET. With 30s the extraction stalls and never completes; with 240s it
# passes, and even so a response has been seen truncating midway (JSON corrupted around
# byte 154,000,000), which only the retry fixed. The Source keeps the generic default —
# whoever asks for the table knows its size, and this is where that gets written down.
INE_TIMEOUT     ?= 240
INE_MAX_RETRIES ?= 3

# Third source: INE Callejero. No API — "extract" ingests files already downloaded
# by hand from the INE website into CALLEJERO_IN. See sources/ine-callejero-source/.
CALLEJERO_SOURCE_DIR ?= sources/ine-callejero-source
CALLEJERO_SOURCE_SRC  = $(CALLEJERO_SOURCE_DIR)/src
CALLEJERO_IN         ?= temp
CALLEJERO_DATA_ROOT  ?= data/callejero
CALLEJERO_PROVINCES  ?= 08,28,41,46
CALLEJERO_PARTITION   = $(CALLEJERO_DATA_ROOT)/ingestion_date=$(DATE)

# Fourth source: simulated OLTP. The FIRST derived source — the other three are upstream
# external data, this one consumes the Silver they produced. Since every Source is FROZEN
# (dependencies = []) and cannot talk to the Lakehouse, the platform first materializes
# what it needs into three flat JSON files (`oltp-export-reference`), and the Source reads
# them with the stdlib only. See sources/simulated-oltp-source/CONTRACT.md section 2.
OLTP_SOURCE_DIR      ?= sources/simulated-oltp-source
OLTP_SOURCE_SRC       = $(OLTP_SOURCE_DIR)/src
OLTP_DATA_ROOT       ?= data/oltp
OLTP_REFERENCE_ROOT  ?= data/oltp-reference
OLTP_REFERENCE        = $(OLTP_REFERENCE_ROOT)/ingestion_date=$(DATE)
# EMPTY BY DEFAULT, and that is deliberate. Until Phase 5 this number was 5,000 for all four
# warehouses — the same for AUFs whose populations differ by 4.6x. When empty, `extract` uses
# the target that `oltp-export-reference` derived from each warehouse's ADULT population times
# the penetration rate (customer_premises_seed). Setting the variable still works and becomes
# an explicit override, recorded as `count_source: cli` in the manifest.
OLTP_CUSTOMERS_PER_WH ?=
# Fixed seed by default, not random: the same date with the same reference must reproduce
# the same partition. Pass SEED=... to generate another synthetic population.
OLTP_SEED            ?= 20260827
OLTP_PARTITION        = $(OLTP_DATA_ROOT)/ingestion_date=$(DATE)/wh=$(WH)
# Regenerate the customer base deliberately. Without this, a complete partition is immutable
# as in the other three sources. Growing the base is ADDITIVE: the generator consumes a single
# random.Random(seed) in fixed order and nothing before the loop depends on count, so the
# first N customers of a larger generation are byte-for-byte the same as before (proven in
# tests/test_customers_generator.py::test_aumentar_count_e_aditivo_nao_reembaralha). The
# manifest keeps the seed and count of every previous run in `history`.
OLTP_OVERWRITE       ?=

# Fifth source: simulated orders as an EVENT LOG. Second derived source, like the customer
# one — and through the same mechanism: the platform materializes customer, catalog, price
# calendar and premises into four flat JSON files (`orders-export-reference`), and the Source
# reads them with the stdlib only. See sources/simulated-orders-source/CONTRACT.md.
ORDERS_SOURCE_DIR      ?= sources/simulated-orders-source
ORDERS_SOURCE_SRC       = $(ORDERS_SOURCE_DIR)/src
ORDERS_DATA_ROOT       ?= data/orders
ORDERS_REFERENCE_ROOT  ?= data/orders-reference
ORDERS_REFERENCE        = $(ORDERS_REFERENCE_ROOT)/ingestion_date=$(ORDERS_TO)
# THE WINDOW IS DECLARED, not derived from "today". Whoever operates knows from when to when
# the orders exist; guessing would produce a different fact base on every run.
#
# THE END OF THE WINDOW IS AN EXTERNAL CEILING: 2026-09-01 is as far as the observed Mercadona
# catalog reaches across the four warehouses. It is not a chosen number.
#
# THE START COMES FROM A MEASUREMENT REQUIREMENT, not from taste. The window must be LONGER
# than the stock seed's `opening_days_of_demand` (7) — otherwise the opening stock covers the
# whole period and stockouts are zero BY CONSTRUCTION, which is exactly what the first ledger
# run flagged with 4 days. From 08-24 to 09-01 there are 9 order days, and the ledger sees 10
# (the picking of an order placed late falls on the next day). 10 > 7.
#
# ONE REPLENISHMENT CYCLE CLOSES IN THIS WINDOW, TWO DO NOT. With 7 days of cover, reorder
# point at 3 and lead time of 2, the first order goes out around day 5 and arrives on day 7;
# the second would fall outside. This is declared instead of corrected: the alternative would
# be lowering the cover until two cycles fit, and choosing a premise for what it does to the
# chart is what this project refuses. `assert_stock_window_can_exercise_replenishment` guards
# the limit that matters.
#
# Measured on 2026-09-01: the 4 warehouses have catalog from 08-24 to 09-01, with day 30
# MISSING — that day produces a `carried_forward` price, a declared path that had never run.
ORDERS_FROM            ?= 2026-08-24
ORDERS_TO              ?= 2026-09-01
ORDERS_SEED            ?= 20260828
# HERE ingestion_date IS THE ORDER DATE, not the extraction day.
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
        orders-replay orders-topic orders-prove-stream \
        check-spark-iceberg spark-build stock-consumption stock-ledger \
        spark-evidence freeze freeze-check \
        iceberg-init iceberg-metadata orders-project-iceberg \
        orders-rebuild-projection orders-reconcile orders-prove-projection \
        stream-evidence \
        warehouse-export warehouse-postgres-up warehouse-bootstrap warehouse-load \
        warehouse warehouse-refresh warehouse-trigger \
        warehouse-snowflake-bootstrap warehouse-snowflake-ddl warehouse-snowflake-load \
        warehouse-snowflake warehouse-snowflake-refresh warehouse-snowflake-evidence \
        warehouse-snowflake-prove-tests \
        observability-prove-signals \
        dashboard dashboard-venv indicators-contract dashboard-check price-dashboard \
        demand-reality-check demand-check-mapping \
        seed-province-map seed-service-area seed-municipality-codes \
        seed-ambiguous-series

help:
	@echo "infra"
	@echo "  up              starts MinIO and creates the buckets (data plane only)"
	@echo "  down            stops everything (keeps the volumes)"
	@echo "  status          container state and bucket contents"
	@echo "  logs            last 50 lines from every container"
	@echo ""
	@echo "orchestration"
	@echo "  airflow         builds and starts Postgres + scheduler + webserver (:8080)"
	@echo "  airflow-trigger triggers today's DAG run and follows it"
	@echo "  airflow-logs    scheduler logs"
	@echo "  airflow-down    stops only Airflow, keeping MinIO up"
	@echo ""
	@echo "pipeline (DATE=$(DATE) WH=$(WH))"
	@echo "  extract         extracts a snapshot into $(MERCADONA_PARTITION)"
	@echo "  validate        validates the partition in --strict mode"
	@echo "  land            uploads the partition to object storage, sha256-checked"
	@echo "  verify-landing  re-reads from object storage and re-checks"
	@echo "  silver          dbt build (models + tests)"
	@echo "  daily           the five above, in order"
	@echo ""
	@echo "INE population (DATE=$(DATE) TABLES=$(INE_TABLES)) — no fixed cadence,"
	@echo "runs when someone decides to run it, not on a cron (INE publishing is irregular)"
	@echo "  ine-extract         extracts a snapshot into $(INE_PARTITION)"
	@echo "  ine-validate        validates the partition (no --strict — see the target's comment)"
	@echo "  ine-land            uploads the partition to object storage"
	@echo "  ine-verify-landing  re-reads from object storage and re-checks"
	@echo "  ine-refresh         the four above + silver, in order"
	@echo "  ine-trigger         triggers the on-demand DAG in Airflow and follows it"
	@echo ""
	@echo "INE callejero (CALLEJERO_IN=$(CALLEJERO_IN) PROVINCES=$(CALLEJERO_PROVINCES)) —"
	@echo "no API: extract ingests files already downloaded by hand, no fixed cadence"
	@echo "  callejero-extract         ingests CALLEJERO_IN into $(CALLEJERO_PARTITION)"
	@echo "  callejero-validate        validates the partition in --strict mode"
	@echo "  callejero-land            uploads the partition to object storage"
	@echo "  callejero-verify-landing  re-reads from object storage and re-checks"
	@echo "  callejero-refresh         the four above + silver, in order"
	@echo "  callejero-trigger         triggers the on-demand DAG in Airflow and follows it"
	@echo ""
	@echo "simulated OLTP (WH=$(WH) COUNT=$(or $(OLTP_CUSTOMERS_PER_WH),from reference) SEED=$(OLTP_SEED)) —"
	@echo "synthetic customers with real addresses; offline, derived from Silver:"
	@echo "  oltp-export-reference     materializes Silver into $(OLTP_REFERENCE) (run once, covers all 4 wh)"
	@echo "  oltp-extract              generates the customers of $(WH) into $(OLTP_PARTITION)"
	@echo "  oltp-validate             validates the partition and its geographic coherence"
	@echo "  oltp-land                 uploads the partition to object storage"
	@echo "  oltp-verify-landing       re-reads from object storage and re-checks"
	@echo "  oltp-refresh              the four above + silver, in order"
	@echo "  oltp-refresh-all          all four wh, and silver once at the end"
	@echo "  oltp-trigger              triggers the on-demand DAG in Airflow and follows it"
	@echo "  ...base size:             comes from the reference (adult population x rate), not from here."
	@echo "                            OLTP_CUSTOMERS_PER_WH=N only for an explicit override."
	@echo "  ...growing the base:      OLTP_CUSTOMERS_PER_WH=N OLTP_OVERWRITE=1 (additive:"
	@echo "                            existing customers are preserved)"
	@echo ""
	@echo "simulated orders (ORDERS_FROM=$(ORDERS_FROM) ORDERS_TO=$(ORDERS_TO) SEED=$(ORDERS_SEED)) —"
	@echo "event log with real customer, product and price; offline, derived from Silver:"
	@echo "  orders-export-reference   customer + catalog + price calendar + premises (once)"
	@echo "  orders-extract            generates the log of $(WH) on $(ORDERS_DATE)"
	@echo "  orders-validate           validates the partition and its coherence with catalog and customer"
	@echo "  orders-land               uploads the partition to object storage"
	@echo "  orders-verify-landing     re-reads from object storage and re-checks"
	@echo "  orders-refresh            the four above + silver, for one (warehouse, day)"
	@echo "  orders-refresh-all        the whole window: 4 warehouses x N days, silver at the end"
	@echo "  orders-trigger            triggers the simulated_orders_events DAG"
	@echo "  ...the window is bounded by the catalog: a day with no snapshot on or before it"
	@echo "     for that warehouse is REFUSED, instead of inventing a price."
	@echo ""
	@echo "stream plane (not started by \`make up\`) — orders OLTP and transactional outbox:"
	@echo "  stream-up                 starts oltp-postgres and creates orders/order_line/outbox"
	@echo "  stream-down               stops only the stream plane (keeps the volume)"
	@echo "  stream-logs               oltp-postgres and kafka logs"
	@echo "  orders-oltp-ddl           prints the OLTP DDL (connects to nothing)"
	@echo "  orders-oltp-init          creates the three tables; OLTP_RESET=--reset drops them first"
	@echo "  orders-apply              replays the log of $(WH)/$(ORDERS_DATE) into the OLTP"
	@echo "  orders-apply-all          the whole window: 4 warehouses x N days"
	@echo "  orders-outbox             outbox state; PARTITION=... rebuilds the log"
	@echo "  orders-prove-atomicity    injects a failure and proves state and event fall together"
	@echo ""
	@echo "transport (Kafka) — at-least-once at the producer, idempotent at the consumer:"
	@echo "  orders-publish            drains the outbox into $(KAFKA_TOPIC)"
	@echo "  orders-project            consumes and maintains live_order_state"
	@echo "  orders-lag                group lag, partition by partition"
	@echo "  orders-replay             rewinds the group (does NOT delete the projection)"
	@echo "  orders-topic              describes the topic on the broker"
	@echo "  orders-prove-stream       proves transport, duplicate, replay and gap"
	@echo "  check-spark-iceberg       GATE: can Spark read/write the pyiceberg catalog?"
	@echo "  spark-build               builds the Spark image (spark profile)"
	@echo "  stock-consumption         exports observed consumption from Silver to the catalog"
	@echo "  stock-ledger              the Spark job: balance, stockout and replenishment (spark profile)"
	@echo "  spark-evidence            runs the ledger on BOTH engines and publishes both timings"
	@echo "  freeze                    seals the RAW capture (docs/FREEZE.md + seed)"
	@echo "  freeze-check              re-reads RAW and fails if the sealed capture changed"
	@echo ""
	@echo "live projection (Iceberg) — two writers on the same table, one reader:"
	@echo "  iceberg-init              SQL catalog + live_order_state table"
	@echo "  iceberg-metadata          prints the CURRENT metadata, read from the catalog"
	@echo "  orders-projection-init    creates the read model in the projection Postgres"
	@echo "  orders-project-iceberg    the consumer writing to Iceberg (writer 1)"
	@echo "  orders-rebuild-projection rebuilds from RAW into the SAME table (writer 2)"
	@echo "  orders-reconcile          Iceberg x Silver x OLTP; exits 1 if they diverge"
	@echo "  orders-prove-projection   concurrency, monotonic merge and snapshot isolation"
	@echo "  stream-evidence           records OLTP, broker, projection and the three folds, dated"
	@echo ""
	@echo "demand calibration (MAPA 2025):"
	@echo "  demand-reality-check      BEFORE | MAPA | AFTER across the three dimensions"
	@echo "  demand-reality-check SNAPSHOT=before_mapa_2025_v1   freezes the BEFORE"
	@echo "  demand-check-mapping      checks the catalog's 444 triples against the mapping"
	@echo ""
	@echo "analytical warehouse (local Postgres, DB=$(WAREHOUSE_PG_DB)) — DEFAULT TARGET —"
	@echo "receives a CUT of Silver by geographic scope, never the whole Silver. Snowflake"
	@echo "was the default target until the trial expired; see the -snowflake pair below."
	@echo "  warehouse-postgres-up     starts the container (compose warehouse-postgres profile)"
	@echo "  warehouse-bootstrap       schemas, roles and grants on the local Postgres (idempotent)"
	@echo "  warehouse-export          cuts Silver into parquet under $(WAREHOUSE_CUT_DIR)"
	@echo "  warehouse-load            drop+create of the STAGE tables + COPY FROM STDIN"
	@echo "  warehouse                 dbt build --target postgres (STAGE -> GOLD -> MART)"
	@echo "  warehouse-refresh         the four above, in order"
	@echo "  warehouse-trigger         triggers the warehouse DAG in Airflow and follows it"
	@echo ""
	@echo "analytical warehouse (Snowflake, DB=$(SNOWFLAKE_DATABASE)) — OPTIONAL, account in .env.snowflake —"
	@echo "same cut as above, against the account configured in ~/.snowflake/config.toml:"
	@echo "  warehouse-snowflake-bootstrap    database, schemas, roles and grants (once, ACCOUNTADMIN)"
	@echo "  warehouse-snowflake-ddl          prints the STAGE DDL (connects to nothing)"
	@echo "  warehouse-snowflake-load         PUT to an internal stage + COPY INTO + re-check"
	@echo "  warehouse-snowflake             dbt build --target snowflake (STAGE -> GOLD -> MART)"
	@echo "  warehouse-snowflake-refresh      the three above, in order"
	@echo "  warehouse-snowflake-prove-tests  injects the defect each test claims to catch, demands red"
	@echo "  warehouse-snowflake-evidence     records ownership, volume and isolation of the target, dated"
	@echo "  ...switching accounts:    edit .env.snowflake (see .env.snowflake.example) and"
	@echo "                            ~/.snowflake/config.toml; no model or test changes"
	@echo ""
	@echo "indicator catalog and panels (Streamlit over the MART, retail_reader role) —"
	@echo "  indicators-contract       regenerates docs/indicators/ from the catalog (no connection)"
	@echo "  dashboard-venv            installs streamlit/pandas/altair (extra, outside the image)"
	@echo "  dashboard                 operations panel at http://localhost:$(DASHBOARD_PORT)"
	@echo "  price-dashboard           prices panel at http://localhost:$(PRICE_DASHBOARD_PORT)"
	@echo "  dashboard-check           renders both panels and demands zero exceptions (live warehouse)"
	@echo ""
	@echo "derived seeds (on demand; the versioned CSV is the truth, the script is the provenance) —"
	@echo "  seed-province-map         wh -> province/municipality, re-checked against the Callejero"
	@echo "  seed-service-area         wh -> AUF municipalities (AUF_XLSX=$(AUF_XLSX))"
	@echo "  seed-municipality-codes   name (Tempus3) -> official code   [network: INE API]"
	@echo "  seed-ambiguous-series     series -> code, for homonyms      [network: INE API]"
	@echo ""
	@echo "disk space —"
	@echo "  data-usage                how much the local scratch (data/) is taking"
	@echo "  prune-local PARTITION=…   deletes the local copy, only if it is intact at the target"
	@echo ""
	@echo "query"
	@echo "  query           queries Silver.  make query SQL=\"select ...\""
	@echo "  duckdb-secret   writes the secret to open the .duckdb in any client"
	@echo "  clean-duckdb    deletes the .duckdb and the dbt artifacts (local state only; the data lives in S3)"
	@echo ""
	@echo "tests"
	@echo "  test            source-test + platform-test"
	@echo "  source-test     suite of the five Sources (offline, no dependencies)"
	@echo "  platform-test   platform tests, offline"
	@echo ""
	@echo "observability — the proof required (BACKLOG.md) before instrumenting anything:"
	@echo "do the signals already exist, or are they really missing?"
	@echo "  observability-prove-signals   reads the real system (data/, SQL, DAGs) and returns"
	@echo "                                YES/PARTIAL/NO per signal, with optional --warehouse"
	@echo ""
	@echo "  venv            creates platform/.venv and installs the platform"
	@echo "  secrets         generates AIRFLOW_SECRET_KEY and AIRFLOW_FERNET_KEY in .env"

# ---- infra -----------------------------------------------------------------
# Data plane only. Airflow is ~2 GB of RAM and is not needed to iterate on a
# dbt model or to run `make daily` by hand.
up:
	$(COMPOSE) up -d minio mc
	@echo "MinIO: http://localhost:$(or $(MINIO_CONSOLE_PORT),9001) (console)"

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs --tail=50

# ---- orchestration ---------------------------------------------------------
# airflow-init runs first and creates the mercadona_api pool with 1 slot. Without it the
# LocalExecutor would run several extracts in parallel and double the rate against the source.
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
	@echo "triggered. follow with: make airflow-logs"

# No schedule: the DAG only runs when triggered, never on its own (see the DAG docstring).
# unpause is still needed for the trigger to actually execute, not just get queued.
ine-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause ine_population_on_demand
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ine_population_on_demand
	@echo "triggered. follow with: make airflow-logs"

callejero-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause ine_callejero_on_demand
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ine_callejero_on_demand
	@echo "triggered. follow with: make airflow-logs"

# The customer base changes when someone decides to change it, never on its own. To
# REGENERATE (more customers, or another seed), pass the parameters on the trigger:
#   make oltp-trigger OLTP_DAG_CONF='{"customers_per_wh":20000,"overwrite":true}'
OLTP_DAG_CONF ?=
oltp-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause simulated_oltp_customers
	$(COMPOSE) exec airflow-scheduler airflow dags trigger simulated_oltp_customers \
	  $(if $(OLTP_DAG_CONF),--conf '$(OLTP_DAG_CONF)',)
	@echo "triggered. follow with: make airflow-logs"

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
	   echo "objects in retail-raw:       $$(mc ls -r local/$$RAW_BUCKET | wc -l)" && \
	   echo "objects in retail-lakehouse: $$(mc ls -r local/$$LAKEHOUSE_BUCKET | wc -l)"'

# ---- pipeline ---------------------------------------------------------------
# A complete partition is IMMUTABLE: re-running extract on it returns exit 2
# (partition.py -> PartitionError). A `make daily` that did not check _SUCCESS first
# would abort on a day that went fine. This guard is the same logic as the DAG short-circuit.
extract:
	@if [ -f "$(MERCADONA_PARTITION)/_SUCCESS" ]; then \
	  echo "partition already complete and immutable: $(MERCADONA_PARTITION)"; \
	  echo "extract skipped (not a failure)."; \
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

# DuckDB is single-writer. A client holding the file open read-write (DBeaver does this
# by default) makes dbt abort with 20 lines of traceback for a problem that is solved by
# closing a connection. The guard replaces that with one actionable line.
DUCKDB_PATH   ?= platform/dbt/retail.duckdb
# `dbt build` compiles the WHOLE project at once: if a newly added source has not landed
# anything yet (fresh clone, or INE before the first `make ine-refresh`), its read_json
# fails with "no files found" and brings down the build of ALL sources, not just the
# empty one. The guard excludes from the build the model of a source with no data yet,
# instead of teaching the model SQL to pretend an empty partition exists (measured: the
# dbt-duckdb `external` materialization itself, when handling an empty relation, writes a
# sentinel row that reappears as a ghost on the first run with real data).
silver:
	@$(PLATFORM_PY) -c "import duckdb; duckdb.connect('$(DUCKDB_PATH)').close()" 2>/dev/null || { \
	  echo "ERROR: another process has $(DUCKDB_PATH) open for writing."; \
	  echo "      DuckDB is single-writer. Close the connection (DBeaver, notebook, CLI) and"; \
	  echo "      try again, or use: make silver DUCKDB_PATH=/tmp/retail-scratch.duckdb"; \
	  echo "      The file only holds views: nothing is lost by recreating it."; \
	  exit 2; }
# THE GATE LIVES IN ONE PLACE ONLY: retail_platform/silver_gate.py. It used to decide what
# to exclude here AND, as a partial copy, inside each of the five DAGs — six places, no two
# alike. The Mercadona DAG had no gate at all and failed every day since
# `silver_live_order_state` was born. One more `--exclude` in this file does not reach Airflow.
	RETAIL_DBT=$(DBT) $(PLATFORM_PY) -m retail_platform silver-build \
	  --project-dir platform/dbt --profiles-dir platform/dbt

daily: extract validate land verify-landing silver
	@echo ""
	@echo "daily OK for ingestion_date=$(DATE) wh=$(WH)"

# ---- INE population ----------------------------------------------------------
# Same partition immutability as Mercadona (extract.py -> PartitionError), same
# guard. No warehouse axis: one partition level less than $(MERCADONA_PARTITION) above.
ine-extract:
	@if [ -f "$(INE_PARTITION)/_SUCCESS" ]; then \
	  echo "partition already complete and immutable: $(INE_PARTITION)"; \
	  echo "extract skipped (not a failure)."; \
	else \
	  PYTHONPATH=$(INE_SOURCE_SRC) $(PYTHON) -m ine_population_source extract \
	    --out $(INE_DATA_ROOT) --tables $(INE_TABLES) --date $(DATE) \
	    --timeout $(INE_TIMEOUT) --max-retries $(INE_MAX_RETRIES); \
	fi

# No --strict: measured that table_id=29005 has 6 series (out of ~8,200 municipalities in
# all of Spain) with NO data point at all — "Gatova" (Castellon, 12) and "Palmerola"
# (Girona, 17), neither within this platform's scope (08/28/41/46; they do not appear
# in ine_municipality_codes_seed). --strict is optional by contract (the source's
# CONTRACT.md, "Quality"); failing the partition over 2 out-of-scope municipalities every
# time the extraction runs would protect nothing real. The count is still REPORTED in
# the validate output even without --strict (it does not go silent).
ine-validate:
	PYTHONPATH=$(INE_SOURCE_SRC) $(PYTHON) -m ine_population_source validate $(INE_PARTITION)

ine-land:
	$(PLATFORM_PY) -m retail_platform land $(INE_PARTITION)

ine-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(INE_PARTITION)

# Name deliberately without "daily": this runs when someone decides to run it (make
# ine-refresh, or the on-demand DAG), never on a fixed cadence — see Makefile help and the
# docstring of orchestration/airflow/dags/ine_population_on_demand.py.
ine-refresh: ine-extract ine-validate ine-land ine-verify-landing silver
	@echo ""
	@echo "ine-refresh OK for ingestion_date=$(DATE)"

# ---- INE callejero ------------------------------------------------------------
# No API: "extract" does not hit the network, it ingests files from CALLEJERO_IN (downloaded
# by hand from the INE website). Same partition immutability as the other two
# sources, same guard. Partitioned only by ingestion_date (plan decision: the artifact
# is an intake of the whole official dataset, not a cut by warehouse/province).
callejero-extract:
	@if [ -f "$(CALLEJERO_PARTITION)/_SUCCESS" ]; then \
	  echo "partition already complete and immutable: $(CALLEJERO_PARTITION)"; \
	  echo "extract skipped (not a failure)."; \
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

# Name deliberately without "daily": runs when someone decides to run it (the Callejero is
# published twice a year, with no API to check whether there is anything new) — see Makefile
# help and the docstring of orchestration/airflow/dags/ine_callejero_on_demand.py.
callejero-refresh: callejero-extract callejero-validate callejero-land callejero-verify-landing silver
	@echo ""
	@echo "callejero-refresh OK for ingestion_date=$(DATE)"

# ---- simulated OLTP ---------------------------------------------------------
# No network in any step. `oltp-export-reference` runs ONCE and covers all four
# warehouses with a single query against Silver: that is why it is NOT part of `oltp-refresh`,
# which is per warehouse — chaining it would run the same heavy query four times.
#
#   make oltp-export-reference     (once, after callejero-refresh and ine-refresh)
#   make oltp-refresh WH=mad1      (repeat for bcn1, svq1, vlc1 — or oltp-refresh-all)
oltp-export-reference:
	$(PLATFORM_PY) -m retail_platform export-oltp-reference \
	  --out $(OLTP_REFERENCE_ROOT) --date $(DATE)

oltp-extract:
	@if [ -f "$(OLTP_PARTITION)/_SUCCESS" ] && [ -z "$(OLTP_OVERWRITE)" ]; then \
	  echo "partition already complete and immutable: $(OLTP_PARTITION)"; \
	  echo "extract skipped (not a failure). Use OLTP_OVERWRITE=1 to regenerate the base."; \
	else \
	  PYTHONPATH=$(OLTP_SOURCE_SRC) $(PYTHON) -m simulated_oltp_source extract \
	    --reference $(OLTP_REFERENCE) --out $(OLTP_DATA_ROOT) --wh $(WH) --date $(DATE) \
	    $(if $(OLTP_CUSTOMERS_PER_WH),--count $(OLTP_CUSTOMERS_PER_WH),) --seed $(OLTP_SEED) \
	    $(if $(OLTP_OVERWRITE),--overwrite,); \
	fi

# --reference is not optional here, unlike the other three sources: this Source's central
# guarantee — every customer lives at a real address within its warehouse's AUF — can only
# be re-checked by re-reading the same reference that generated the partition.
oltp-validate:
	PYTHONPATH=$(OLTP_SOURCE_SRC) $(PYTHON) -m simulated_oltp_source validate \
	  $(OLTP_PARTITION) --reference $(OLTP_REFERENCE) --strict

oltp-land:
	$(PLATFORM_PY) -m retail_platform land $(OLTP_PARTITION)

oltp-verify-landing:
	$(PLATFORM_PY) -m retail_platform verify-landing $(OLTP_PARTITION)

oltp-refresh: oltp-extract oltp-validate oltp-land oltp-verify-landing silver
	@echo ""
	@echo "oltp-refresh OK for ingestion_date=$(DATE) wh=$(WH)"

# `silver` ONCE at the end, not inside the loop: `dbt build` covers the whole project, so
# chaining it per warehouse would repeat the same 215 models and tests four times to reach
# the same result. That is why the loop calls the landing chain, not `oltp-refresh`.
oltp-refresh-all:
	@for wh in mad1 bcn1 svq1 vlc1; do \
	  $(MAKE) --no-print-directory oltp-extract oltp-validate oltp-land oltp-verify-landing \
	    WH=$$wh || exit 1; \
	done
	@$(MAKE) --no-print-directory silver

# ---- fifth source: simulated orders (event log) -----------------------------
# Runs ONCE and covers all four warehouses and the whole window. Chaining it into
# orders-refresh (which is per warehouse and per day) would repeat the same heavy query 16
# times. Same reason oltp-export-reference is not part of oltp-refresh.
# FREEZES the BEFORE. A separate step on purpose: after `orders-refresh-all --overwrite`
# the previous state no longer exists anywhere, and a reality check without a BEFORE can
# only say "this is how it is today", which is half the question.
demand-reality-check:
	$(PLATFORM_PY) -m retail_platform demand-reality-check \
	  $(if $(SNAPSHOT),--snapshot $(SNAPSHOT),)

# Checks the mapping against the category TREE, not against the cut: the catalog dedup
# hides triples that exist in the source, and checking against it would measure the tie-break.
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
	  echo "partition already complete and immutable: $(ORDERS_PARTITION)"; \
	  echo "extract skipped (not a failure). Use ORDERS_OVERWRITE=1 to regenerate."; \
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
	@echo "orders-refresh OK for ingestion_date=$(ORDERS_DATE) wh=$(WH)"

# The whole window: 4 warehouses x N days, with silver ONCE at the end. The date loop uses
# `date -d`, which handles day arithmetic without needing Python here.
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

# ---- stream plane: orders OLTP and outbox -------------------------------------
# NOT STARTED BY `make up`, and that is a README promise: the data plane remains just
# MinIO. The compose `--profile stream` is what keeps that separation in the file, instead
# of in the operator's memory.
#
# WHAT THIS BLOCK PROVES, and why it comes before any line of Kafka: that the event is born
# INSIDE the transaction that changes the order. Without that, a topic would be the batch
# output with extra steps — which is exactly what ARCHITECTURE.md already refused in writing.
OLTP_DSN  ?= postgresql://oltp:oltp@localhost:$(or $(OLTP_PORT),5433)/oltp
# OLTP_RESET=--reset drops the three tables before creating them. Unlike `prune-local`,
# which has no `--force`: there the target is a landed partition, which may be the only copy;
# here it is a REPLICA of the log, which `orders-apply` rebuilds in minutes.
OLTP_RESET ?=

stream-up:
	$(COMPOSE) --profile stream up -d oltp-postgres kafka
	@echo "waiting for the OLTP and the broker to become healthy..."
	@for i in $$(seq 1 60); do \
	  pg=$$($(COMPOSE) ps --format '{{.Health}}' oltp-postgres 2>/dev/null); \
	  kf=$$($(COMPOSE) ps --format '{{.Health}}' kafka 2>/dev/null); \
	  [ "$$pg" = "healthy" ] && [ "$$kf" = "healthy" ] && break; sleep 2; \
	done
	$(COMPOSE) --profile stream up kafka-init
	@$(MAKE) --no-print-directory orders-oltp-init orders-projection-init iceberg-init
	@echo ""
	@echo "OLTP ......... $(OLTP_DSN)"
	@echo "projection ... $(PROJECTION_DSN)   (separate database: it is a read model)"
	@echo "broker ....... $(KAFKA_BOOTSTRAP)  topic $(KAFKA_TOPIC)"
	@echo "iceberg ...... $(ICEBERG_WAREHOUSE)  SQL catalog in the OLTP itself"

stream-down:
	$(COMPOSE) --profile stream stop oltp-postgres kafka
	$(COMPOSE) --profile stream rm -f oltp-postgres kafka kafka-init

stream-logs:
	$(COMPOSE) --profile stream logs --tail=60 oltp-postgres kafka

orders-oltp-ddl:
	@$(PLATFORM_PY) -m retail_platform orders-oltp-ddl

orders-oltp-init:
	$(PLATFORM_PY) -m retail_platform orders-oltp-init --dsn "$(OLTP_DSN)" $(OLTP_RESET)

orders-apply:
	$(PLATFORM_PY) -m retail_platform orders-apply "$(ORDERS_PARTITION)" \
	  --dsn "$(OLTP_DSN)"

orders-apply-all:
	@dia="$(ORDERS_FROM)"; \
	while [ "$$dia" != "$$(date -u -d "$(ORDERS_TO) +1 day" +%F)" ]; do \
	  for wh in mad1 bcn1 svq1 vlc1; do \
	    $(MAKE) --no-print-directory orders-apply WH=$$wh ORDERS_DATE=$$dia || exit 1; \
	  done; \
	  dia="$$(date -u -d "$$dia +1 day" +%F)"; \
	done

# With PARTITION=..., rebuilds the log from the outbox rows and compares the sha256
# with the manifest. Counting rows would not prove fidelity; reproducing the bytes does.
orders-outbox:
	$(PLATFORM_PY) -m retail_platform orders-outbox --dsn "$(OLTP_DSN)" \
	  $(if $(PARTITION),--verify "$(PARTITION)",)

# THE MILESTONE 4 PROOF. Injects a trigger that makes the insert blow up — in the outbox and
# then in orders — and checks that the other side did not survive either. The injection is
# in the DATABASE, not in the code: it is a failure the applier can neither foresee nor
# handle, which is the kind of failure transactions exist for. See the script header.
orders-prove-atomicity:
	@$(PLATFORM_PY) scripts/prove_oltp_atomicity.py "$(ORDERS_PARTITION)"

# ---- transport: Kafka, replay and idempotent consumption ----------------------
# THIS IS NOT "INSTALL KAFKA AND PUBLISH A MESSAGE". The four properties this block
# exists to make demonstrable, and where each one is proven:
#
#   faithful transport     the re-read topic reproduces the sha256 of the 16 manifests
#   delivery semantics     at-least-once from outbox to broker, DEMONSTRATED (not claimed):
#                          killing the publisher mid-drain produces a real duplicate
#   idempotent consumption dedup by (order_id, sequence_no) against the state — no set
#                          of event_ids growing without bound; a gap becomes a REFUSAL, not a no-op
#   replay                 rewinding the group and reprocessing leaves the digest identical
KAFKA_BOOTSTRAP ?= localhost:$(or $(KAFKA_PORT),9092)
KAFKA_TOPIC     ?= retail.orders.events.v1
KAFKA_GROUP     ?= orders-projector
PROJECTION_DSN  ?= postgresql://oltp:oltp@localhost:$(or $(OLTP_PORT),5433)/projection

orders-projection-init:
	$(PLATFORM_PY) -m retail_platform orders-projection-init \
	  --projection-dsn "$(PROJECTION_DSN)"

orders-publish:
	$(PLATFORM_PY) -m retail_platform orders-publish --dsn "$(OLTP_DSN)" \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" $(PUBLISH_ARGS)

orders-project:
	$(PLATFORM_PY) -m retail_platform orders-project \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)" \
	  --projection-dsn "$(PROJECTION_DSN)" --from-beginning $(PROJECT_ARGS)

orders-lag:
	$(PLATFORM_PY) -m retail_platform orders-lag \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)"

orders-replay:
	$(PLATFORM_PY) -m retail_platform orders-replay \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" --group "$(KAFKA_GROUP)"

orders-topic:
	$(COMPOSE) --profile stream exec kafka /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka:19092 --describe --topic "$(KAFKA_TOPIC)"

# ---- Spark: the live interop check against the catalog ---------------------------
#
# THIS GATE IS ABOUT ICEBERG, not about Spark. ARCHITECTURE justifies Iceberg with two
# properties: atomic commit with optimistic concurrency, which `orders-prove-iceberg`
# demonstrates; and INTEROP ACROSS ENGINES, which is claimed and was never demonstrated,
# because both writers are Python and use the same library.
#
# Spark is the third writer, and the first outside Python. If it cannot read the catalog
# pyiceberg wrote, Spark does not get in AND the interop clause leaves ARCHITECTURE.
#
# The service's `depends_on` cannot require `oltp-postgres` because the profiles are
# independent — compose would refuse the reference. So the prerequisite is enforced here,
# with a message that says what to do, like the DuckDB lock check in `make silver`.
check-spark-iceberg:
	@docker ps --format '{{.Names}}' | grep -qx retail-oltp-postgres || { \
	  echo "ERROR: the Iceberg catalog lives in oltp-postgres, and it is not running."; \
	  echo "      The stream plane is not started by \`make up\`. Run: make stream-up"; \
	  exit 2; }
	@$(COMPOSE) --profile spark build spark
	@$(PLATFORM_PY) scripts/check_spark_iceberg.py

spark-build:
	@$(COMPOSE) --profile spark build spark

# ---- stock: observed consumption becomes a balance, and Spark computes it --------
#
# TWO STEPS, AND THE FIRST IS A PREREQUISITE OF THE SECOND ON PURPOSE. Running the job on
# stale consumption would produce a plausible and wrong ledger — the class of defect no
# test catches, because the balance would still close. The export is cheap; making it
# optional would save seconds and cost confidence.
stock-consumption:
	@$(PLATFORM_PY) -m retail_platform stock-consumption

# ---- freeze: discipline becomes verification -----------------------------------
#
# "Do not touch RAW after closing" was honored by ATTENTION until Phase 7, and attention
# fails no build — the last three documentation reviews existed because a regeneration
# changed numbers already written and nothing warned.
#
# `freeze` seals; `freeze-check` verifies. The seal covers the DATA (path, sha256, bytes,
# records of each declared file) and NOT the run: including run_id and timestamps would make
# a re-land of identical data break the seal, and an alarm that fires without cause is worse
# than no alarm.
freeze:
	@$(PLATFORM_PY) -m retail_platform freeze

freeze-check:
	@$(PLATFORM_PY) -m retail_platform freeze-check

SPARK_EVIDENCE ?= docs/spark-evidence/README.md
# THE EVIDENCE RUNS BOTH ENGINES and publishes both timings, whoever wins. It exists to
# support a NEGATIVE claim — "Spark was not adopted for performance" — and a negative claim
# without a benchmark is the same disease as a hand-copied number, with the sign flipped.
spark-evidence:
	@$(PLATFORM_PY) -m retail_platform spark-evidence --out $(SPARK_EVIDENCE)

stock-ledger: stock-consumption
	@docker ps --format '{{.Names}}' | grep -qx retail-oltp-postgres || { \
	  echo "ERROR: the Iceberg catalog lives in oltp-postgres, and it is not running."; \
	  echo "      The stream plane is not started by \`make up\`. Run: make stream-up"; \
	  exit 2; }
	@$(COMPOSE) --profile spark run --rm --no-deps spark \
	  python3 jobs/spark/stock_ledger.py

orders-prove-stream:
	@$(PLATFORM_PY) scripts/prove_stream_semantics.py

# ---- live projection in Iceberg: two writers, one reader ----------------------
# THE ICEBERG TRIGGER FIRED ON CONCURRENCY, NOT ON VOLUME. At this volume a parquet file
# rewritten with an atomic `os.replace` would work; what Iceberg buys is snapshot isolation
# between TWO WRITERS (the streaming consumer and the batch rebuild) and a concurrent
# reader (DuckDB), plus time travel on the projection.
#
# A closed experiment measured this before this block existed (removed in CR-012; see the
# git history). Today the proof is `make orders-prove-projection`.
ICEBERG_WAREHOUSE ?= s3://retail-lakehouse/iceberg

iceberg-init:
	$(PLATFORM_PY) -m retail_platform iceberg-init --warehouse "$(ICEBERG_WAREHOUSE)"

# The path of the CURRENT metadata, asked of the catalog. `silver` calls this on its own.
iceberg-metadata:
	@$(PLATFORM_PY) -m retail_platform iceberg-metadata

# The consumer writing to Iceberg instead of Postgres: the Milestone 5 seam, exercised.
#
# ITS OWN GROUP, not the same as the Postgres sink. This is not a configuration detail: two
# groups reading the SAME topic, each at its own offset and its own pace, is exactly the
# decoupling point Kafka bought. Sharing the group would make both compete for the
# partitions and each event would reach only one — the opposite of what is wanted here.
KAFKA_GROUP_ICEBERG ?= orders-projector-iceberg

orders-project-iceberg:
	$(PLATFORM_PY) -m retail_platform orders-project --sink iceberg \
	  --bootstrap "$(KAFKA_BOOTSTRAP)" --topic "$(KAFKA_TOPIC)" \
	  --group "$(KAFKA_GROUP_ICEBERG)" --from-beginning $(PROJECT_ARGS)

# The SECOND writer of the same table.
# PROJECTION_RESET=1 drops and recreates the table before rebuilding. Needed when the Source
# was regenerated with another seed, reference, premises or demand_model_version: the
# projection merge is monotonic and assumes an order_id is always the same order — and after
# a regeneration it is not. Without the reset, the rebuild discards the new rows as "old" and
# the projection ends up with two universes mixed together (measured: 4,028 rows discarded
# on 2026-08-31).
orders-rebuild-projection:
	$(PLATFORM_PY) -m retail_platform orders-rebuild-projection --root "$(ORDERS_DATA_ROOT)" \
	  $(if $(PROJECTION_RESET),--reset,)

# Three folds, one comparison. Exits 1 on any divergence.
orders-reconcile:
	$(PLATFORM_PY) -m retail_platform orders-reconcile --dsn "$(OLTP_DSN)"

# THE MILESTONE 6 PROOF: two concurrent writers with an active reader, monotonic merge,
# snapshot isolation and time travel — against real Iceberg.
orders-prove-projection:
	@$(PLATFORM_PY) scripts/prove_iceberg_projection.py

# THE STREAMING HALF IS NOT COVERED OFFLINE, and that is declared debt, not carelessness:
# `make test` runs offline — a repo invariant — so broker, OLTP and Iceberg only exist
# while `make stream-up` is running. The in-memory doubles cover the SHAPE of the code
# (transaction order, dedup protocol, SQL construction); the SEMANTICS of the real engines
# can only be exercised against them. This target records that it was.
#
# It does not fail with the plane half up: each missing section shows up as a DECLARED
# absence, never as an invented number. It fails only when none of the three responds.
STREAM_EVIDENCE ?= docs/stream-evidence/README.md
stream-evidence:
	@$(PLATFORM_PY) -m retail_platform stream-evidence \
	  --dsn "$(OLTP_DSN)" --out $(STREAM_EVIDENCE)

# ---- analytical warehouse: Silver cut (shared by both engines) ------------------
# The cut by GEOGRAPHIC SCOPE has nothing specific to the target engine — only the
# directory name is a historical leftover from the day when only the Snowflake target
# existed (see DECISIONS.md CR-006). Both loaders below read the SAME parquet.
# See platform/src/retail_platform/warehouse_cut.py.
WAREHOUSE_CUT_DIR ?= data/warehouse-cut

warehouse-export:
	$(PLATFORM_PY) -m retail_platform export-cut --out $(WAREHOUSE_CUT_DIR)

# ---- analytical warehouse (local Postgres) — DEFAULT TARGET since 2026-09 -------
# Snowflake was a TRIAL, with a deadline, and the deadline passed: the `warehouse_load` DAG
# started failing authentication ("Your free trial has ended..."). The targets WITHOUT an
# engine suffix now serve the local Postgres — exactly the switch this second dbt target
# existed to prove cheap (see DECISIONS.md CR-006 and CR-007). The Snowflake pair remains
# right below, with the -snowflake suffix, ready for the day another account replaces the
# trial — no dbt model changes on that day, only the --target and the loader.
#
#   docker compose -f infra/docker-compose.yml --profile warehouse-postgres up -d warehouse-postgres
#   make warehouse-bootstrap
#   make warehouse-refresh
WAREHOUSE_PG_HOST ?= localhost
WAREHOUSE_PG_PORT ?= 5434
WAREHOUSE_PG_DB   ?= retail

warehouse-postgres-up:
	$(COMPOSE) --profile warehouse-postgres up -d warehouse-postgres

warehouse-bootstrap:
	$(PLATFORM_PY) -m retail_platform postgres-bootstrap \
	  --host $(WAREHOUSE_PG_HOST) --port $(WAREHOUSE_PG_PORT) --dbname $(WAREHOUSE_PG_DB)

warehouse-load:
	$(PLATFORM_PY) -m retail_platform load-postgres \
	  --stage-dir $(WAREHOUSE_CUT_DIR) \
	  --host $(WAREHOUSE_PG_HOST) --port $(WAREHOUSE_PG_PORT) --dbname $(WAREHOUSE_PG_DB)

# `--target postgres` uses the SAME `+enabled` guard from dbt_project.yml that `--target
# snowflake` already used — no model, test or business rule changes between the two.
warehouse:
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt --target postgres

warehouse-refresh: warehouse-export warehouse-bootstrap warehouse-load warehouse
	@echo ""
	@echo "warehouse-refresh OK: STAGE loaded and GOLD/MART rebuilt on the local Postgres"

# ---- analytical warehouse (Snowflake) — OPTIONAL, one --target away ----------------
# Snowflake is the FOURTH consumer that cannot reach the Lakehouse — the other three are the
# FROZEN Sources. It does not receive the whole Silver: it receives a CUT by GEOGRAPHIC SCOPE
# (the same `warehouse-export` as above). The original trial expired in 2026-09 (CR-007); on
# 2026-10-29 a new account took over without any dbt model changing (CR-008).
SNOWFLAKE_CONNECTION ?= spark_retail
# Deliberately with the SAME name profiles.yml reads: because of the bare `export` at the top
# of the file, changing it here changes the COPY INTO destination and the dbt target at once.
# It is the way to isolate a run (CI, a second person) — by DATABASE, not by schema prefix.
SNOWFLAKE_DATABASE  ?= RETAIL

# Once per account, requires ACCOUNTADMIN. Creates the database, the three schemas and the
# three roles, applies the grants and PROVES isolation role by role — with
# `use secondary roles none`, without which the check would pass by mistake for a user that
# also has ACCOUNTADMIN.
#
# CAREFUL WHEN NAMING A VARIABLE HERE. The top of this file has a bare `export`: EVERY
# make variable becomes an environment variable of the subprocesses. A `SNOWFLAKE_USER ?=`
# (empty) reached dbt as a variable that was PRESENT AND EMPTY, and `env_var('SNOWFLAKE_USER',
# 'default')` returns the empty value in that case, not the default — dbt died with "'user' is
# a required property" while `dbt debug` outside make passed. Hence the dedicated prefix:
# no name here may collide with the ones profiles.yml reads.
SNOWFLAKE_GRANT_USER ?=
warehouse-snowflake-bootstrap:
	$(PLATFORM_PY) -m retail_platform snowflake-bootstrap \
	  --database $(SNOWFLAKE_DATABASE) --connection $(SNOWFLAKE_CONNECTION) \
	  $(if $(SNOWFLAKE_GRANT_USER),--grant-to-user $(SNOWFLAKE_GRANT_USER),)

# No connection at all: the DDL is derived from the cut itself, so it can be reviewed before
# anything touches Snowflake.
warehouse-snowflake-ddl:
	@$(PLATFORM_PY) -m retail_platform snowflake-ddl --database $(SNOWFLAKE_DATABASE)

warehouse-snowflake-load:
	$(PLATFORM_PY) -m retail_platform load-snowflake \
	  --stage-dir $(WAREHOUSE_CUT_DIR) --database $(SNOWFLAKE_DATABASE) \
	  --connection $(SNOWFLAKE_CONNECTION)

warehouse-snowflake:
	$(DBT) build --project-dir platform/dbt --profiles-dir platform/dbt --target snowflake

# The Snowflake half is not reproducible offline like the Lakehouse half: it depends on a
# live account — and the one used here was a trial, which expired. This target records the
# result of the REAL run — ownership, volume, isolation and sample — with the date and account
# identity, so that the warehouse models still have proof after the account expires.
# Regenerable: linking another account and running this produces that account's evidence.
SNOWFLAKE_EVIDENCE ?= docs/warehouse-evidence/README.md
warehouse-snowflake-evidence:
	@$(PLATFORM_PY) -m retail_platform snowflake-evidence \
	  --database $(SNOWFLAKE_DATABASE) --connection $(SNOWFLAKE_CONNECTION) \
	  --out $(SNOWFLAKE_EVIDENCE)

# "A check that never failed is not a check" — and Milestone 7 showed why the rule exists:
# the first STAGE load put EVERY timestamp in the year 56,648,666 and the 166 dbt nodes
# built GREEN (right count, right type, unique grain, and a funnel made of
# `count_if(marco is not null)` stays exact with the instant shifted 56 million years).
# This target injects, into the REAL warehouse data, the defect each test claims to catch,
# demands red, undoes it and demands green again. It only touches GOLD and MART, which are
# fully rebuildable by `dbt build`.
#
# SNOWFLAKE ONLY (the injection uses `dateadd`/`number`, Snowflake-only): the Postgres
# counterpart is recorded as backlog in DECISIONS.md CR-007, and does not block switching the
# default target — the 178 dbt data tests already run, green, against both engines.
warehouse-snowflake-prove-tests:
	@$(PLATFORM_PY) scripts/prove_warehouse_orders_tests.py

warehouse-snowflake-refresh: warehouse-export warehouse-snowflake-load warehouse-snowflake
	@echo ""
	@echo "warehouse-snowflake-refresh OK: STAGE loaded and GOLD/MART rebuilt"

# ---- strategic panel (Streamlit over the MART) -------------------------------
# A CHECKING bench for the indicators before rebuilding them in Power BI. It reads only the
# MART, and wears `retail_reader` — the BI role, which this panel is the first consumer to
# actually wear (the load already wore `retail_loader` and dbt `retail_transformer`).
#
# The dependencies live in [project.optional-dependencies] of platform/pyproject.toml, OUTSIDE
# `dependencies`: Dockerfile.airflow installs exactly that list, and ~150 MB of UI has no
# business in an image that renders no dashboard.
DASHBOARD_PORT ?= 8501
dashboard-venv:
	$(PLATFORM_PY) -m pip install --quiet -e "platform[dashboard]"
	@$(PLATFORM_PY) -c "import streamlit, pandas, altair; \
	  print(f'dashboard venv OK: streamlit {streamlit.__version__}, pandas {pandas.__version__}, altair {altair.__version__}')"

dashboard:
	$(PLATFORM_PY) -m streamlit run streamlit/operations_app.py \
	  --server.port $(DASHBOARD_PORT) --server.headless true

# No connection at all: docs/indicators/ is derived from the catalog, so it can be reviewed
# before anything touches the warehouse — same property as `make warehouse-snowflake-ddl`.
indicators-contract:
	@$(PLATFORM_PY) -m retail_platform.indicators.contract

# REQUIRES A LIVE ACCOUNT, and that is why it stays out of `make test`. It runs the actual
# Streamlit script and demands zero exceptions: a `curl` on /health does not do, because
# Streamlit returns HTTP 200 with the page skeleton even when the script dies on the first
# `select` — rendering happens on the client. Every indicator's queries are only exercised
# this way.
dashboard-check:
	@$(PLATFORM_PY) streamlit/smoke.py

# ---- prices panel — DEDICATED, not one more tab -----------------------------------
# Price movement in the Mercadona catalog, and nothing else. Same session (`connection.py`,
# `retail_reader`) as the operations panel; its own catalog in indicators/prices.py.
PRICE_DASHBOARD_PORT ?= 8502
price-dashboard:
	$(PLATFORM_PY) -m streamlit run streamlit/prices_app.py \
	  --server.port $(PRICE_DASHBOARD_PORT) --server.headless true

warehouse-trigger:
	$(COMPOSE) exec airflow-scheduler airflow dags unpause warehouse_load
	$(COMPOSE) exec airflow-scheduler airflow dags trigger warehouse_load
	@echo "triggered. follow with: make airflow-logs"

# ---- seed derivation ---------------------------------------------------------
# The four seeds below are DERIVED from a source and then VERSIONED: the CSV is the
# repository's truth, and the script is how it was obtained. They run on demand, not in the
# pipeline — the source of each changes once a semester or never.
#
# EXISTING AS TARGETS IS THE POINT. Without this, "where did this CSV come from" can only be
# answered by reading the docstring of a script nobody knows exists, and a seed without
# executable provenance is indistinguishable from a typed-in number.
CALLEJERO_ROOT ?= $(CALLEJERO_IN)
AUF_XLSX       ?= temp/AUF_mun.xlsx

seed-province-map:
	$(PYTHON) scripts/derive_warehouse_province_map.py "$(CALLEJERO_ROOT)"

seed-service-area:
	$(PYTHON) scripts/derive_warehouse_service_area.py "$(AUF_XLSX)" "$(CALLEJERO_ROOT)"

# These two query the INE Tempus3 API live — the only derivation targets that use the
# network. `derive_ambiguous_series` only queries the series whose NAME is ambiguous across
# all of Spain AND exists in the 4 provinces (today 3 names, 18 series), not the ~8,200.
seed-municipality-codes:
	$(PYTHON) scripts/derive_municipality_codes.py

seed-ambiguous-series:
	$(PYTHON) scripts/derive_ambiguous_series.py

# ---- query ------------------------------------------------------------------
# retail.duckdb holds only VIEWs over the object-storage parquet — it contains no
# data. That is why opening it with an arbitrary client fails with NoSuchBucket: the new
# session does not know the endpoint. This target opens it with everything configured;
# `duckdb-secret` fixes it for good, for any client.
SQL ?=
query:
	@$(PLATFORM_PY) -m retail_platform query $(if $(SQL),"$(SQL)",)

duckdb-secret:
	@$(PLATFORM_PY) -m retail_platform duckdb-secret

# ---- disk space --------------------------------------------------------------
# `data/` is extraction scratch: after land + verify-landing, the object storage is the
# truth and the local copy is redundant. It is not small — an INE partition takes
# between 264 and 384 MB — and nothing was ever deleted.
#
# Deliberately NOT chained into any *-refresh: deleting data is the operator's decision,
# not a side effect of a pipeline. The verification runs BEFORE the removal and, if it
# fails, nothing is deleted — there is no --force.
#
#   make prune-local PARTITION=data/ine/ingestion_date=2026-08-25
PARTITION ?=
prune-local:
	@if [ -z "$(PARTITION)" ]; then \
	  echo "usage: make prune-local PARTITION=<partition path>"; \
	  echo "e.g.:  make prune-local PARTITION=$(INE_DATA_ROOT)/ingestion_date=$(DATE)"; \
	  exit 2; \
	fi
	$(PLATFORM_PY) -m retail_platform prune-local $(PARTITION)

data-usage:
	@echo "space taken by the local scratch (data/):"
	@du -sh data/* 2>/dev/null | sort -h || echo "  (data/ empty)"
	@echo ""
	@echo "free an already-landed partition with: make prune-local PARTITION=<path>"

# ---- tests -----------------------------------------------------------------
test: source-test platform-test

# Runs on the system Python, no venv: if this target passes, every Source still has no
# third-party dependency at all. It is the boundary being verified, not claimed. Two
# explicit calls, not a loop over sources/*: the same preference for clear repetition
# over premature abstraction that already existed in this Makefile before INE.
source-test:
	@echo "--- Mercadona Catalog Source (no dependencies) ---"
	$(MAKE) -C $(MERCADONA_SOURCE_DIR) test
	@echo "--- INE Population Source (no dependencies) ---"
	$(MAKE) -C $(INE_SOURCE_DIR) test
	@echo "--- INE Callejero Source (no dependencies) ---"
	$(MAKE) -C $(CALLEJERO_SOURCE_DIR) test
	@echo "--- Simulated OLTP Source (no dependencies) ---"
	$(MAKE) -C $(OLTP_SOURCE_DIR) test
	@echo "--- Simulated Orders Source (no dependencies) ---"
	$(MAKE) -C $(ORDERS_SOURCE_DIR) test

platform-test:
	@echo "--- Platform ---"
	cd platform && ../$(PLATFORM_PY) -m unittest discover -s tests -t .

# Read-only: reads data/, the platform SQL and the DAGs, and returns YES/PARTIAL/NO per signal.
# It is the trigger BACKLOG.md requires before building any collector —
# "prove useful signals exist" before "application -> OTel -> Collector -> backend".
observability-prove-signals:
	@$(PLATFORM_PY) scripts/prove_observability_signals.py

# ---- setup -----------------------------------------------------------------
venv:
	$(PYTHON) -m venv platform/.venv
	$(PLATFORM_PY) -m pip install --quiet --upgrade pip
	$(PLATFORM_PY) -m pip install --quiet -e platform/
	@$(PLATFORM_PY) -c "import boto3, duckdb; print('platform ready')"

# Random keys in .env (which is in .gitignore). Compose refuses to start without them,
# so there is no path where an example value becomes the real key by oversight.
secrets:
	@$(PYTHON) scripts/gen_secrets.py

clean-duckdb:
	rm -f platform/dbt/retail.duckdb
	rm -rf platform/dbt/target platform/dbt/logs
