#!/usr/bin/env python3
"""CLOSED EXPERIMENT: does Spark read and write the Iceberg catalog that pyiceberg created?

    make check-spark-iceberg

WHY THIS EXISTS BEFORE PHASE 7, AND NOT INSIDE IT — AND WHY IT IS A GATE ABOUT
ICEBERG, NOT ABOUT SPARK.

ARCHITECTURE justifies Iceberg with two properties. The first — atomic commit with
optimistic concurrency between two writers — is PROVEN by
`scripts/prove_iceberg_projection.py`. The second — *interop across engines* — is CLAIMED and
was never demonstrated, because both writers are Python and use the same library. A
claimed and untested property is exactly what this project spent six phases
refusing elsewhere.

The concrete risk: the catalog is a pyiceberg `SqlCatalog`, and Spark has to read it as
`org.apache.iceberg.jdbc.JdbcCatalog`. They are two independent implementations of the same
table convention. They may simply not talk to each other.

BOTH OUTCOMES ARE ACCEPTABLE, which is why this is an experiment and not a task:

  PASSED    interop stops being a claim. Spark comes in as the third writer.
  FAILED    Spark does NOT come in, AND the interop clause is DELETED from the Iceberg
            justification in ARCHITECTURE. What remains is still proven and still enough.

HOW IT RUNS. One file, two sides, because the question is about the boundary between them:

    host       platform venv, speaks pyiceberg      ->  orchestrates and checks
    container  Spark image, speaks JdbcCatalog      ->  `--side spark`

The Spark side prints `RESULT<TAB>ok<TAB>question<TAB>detail` lines, which the host
collects and merges with its own. Without that the verdict would live in two places.

WHAT IT DOES NOT TOUCH. Its own namespace (`spike_spark`), its own warehouse prefix. The
live table `projection.live_order_state` is only READ — and the last question checks that
it is still intact and that Spark did not change the catalog schema.

The `--fase` flag and its values (`leitura`, `escrita`, ...) and the columns of the spike
table (`chave`, `escritor`, `sequencia`) keep their original names: they are the script's
command-line interface and the experiment's data, not prose.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import traceback

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

NAMESPACE = "spike_spark"
TABLE = f"{NAMESPACE}.engine_interop"
CATALOG = os.environ.get("ICEBERG_CATALOG_NAME", "retail")
LIVE_TABLE = os.environ.get("ICEBERG_NAMESPACE", "projection") + ".live_order_state"

results: list[tuple[str, bool, str]] = []


def answer(question: str, ok: bool, detail: str = "") -> None:
    results.append((question, ok, detail))
    print(f"  [{'YES  ' if ok else 'NO   '}] {question}", flush=True)
    for line in (detail or "").split("\n"):
        if line:
            print(f"          {line}", flush=True)


def failed(question: str, exc: Exception) -> None:
    answer(question, False, f"{type(exc).__name__}: {str(exc)[:400]}")


# ======================================================================================
# SPARK SIDE — runs INSIDE the container, with the `retail-lakehouse/spark` image.
# ======================================================================================

def spark_session():
    """The session comes from `jobs/spark/session.py`, and NOT from a local copy.

    The temptation was to freeze the configuration here, so the recorded evidence would
    describe exactly what was measured. It would be worse: the day the production
    configuration changed, this experiment would keep APPROVING a configuration nobody
    uses. By importing from there, `make check-spark-iceberg` becomes a live check of what
    the stock job actually uses.
    """
    from jobs.spark.session import sessao

    return sessao("check-spark-iceberg")


def emit(ok: bool, question: str, detail: str = "") -> None:
    """A line the host knows how to read. The verdict lives in one place, and it belongs to the host."""
    clean = (detail or "").replace("\n", " | ").replace("\t", " ")
    print(f"RESULT\t{'1' if ok else '0'}\t{question}\t{clean}", flush=True)


def spark_side(phase: str) -> int:
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        return SPARK_PHASES[phase](spark)
    finally:
        spark.stop()


def spark_phase_read(spark) -> int:
    """S1-S3: Spark SEES and READS what pyiceberg wrote."""
    import pyspark

    print(f"pyspark {pyspark.__version__} · catalog `{CATALOG}` via JdbcCatalog",
          flush=True)

    # S1. the catalog responds, and the namespaces are the ones pyiceberg created.
    try:
        names = [r[0] for r in spark.sql(f"show namespaces in {CATALOG}").collect()]
        emit(len(names) > 0, "S1. Spark opens the pyiceberg SQL catalog as a JdbcCatalog",
             f"namespaces: {sorted(names)}")
    except Exception as exc:
        emit(False, "S1. Spark opens the pyiceberg SQL catalog as a JdbcCatalog",
             f"{type(exc).__name__}: {exc}")
        return 1

    # S2. the table written by Python shows up in the listing.
    ns, table = LIVE_TABLE.split(".")
    try:
        names = [r[1] for r in spark.sql(f"show tables in {CATALOG}.{ns}").collect()]
        emit(table in names, f"S2. Spark SEES `{LIVE_TABLE}`, written by pyiceberg",
             f"tables in {ns}: {sorted(names)}")
    except Exception as exc:
        emit(False, f"S2. Spark SEES `{LIVE_TABLE}`, written by pyiceberg",
             f"{type(exc).__name__}: {exc}")
        return 1

    # S3. and reads the CONTENT — not just the metadata. The host checks the count against pyiceberg.
    try:
        rows = spark.table(f"{CATALOG}.{LIVE_TABLE}").count()
        distinct = spark.sql(
            f"select count(distinct written_by) n from {CATALOG}.{LIVE_TABLE}"
        ).collect()[0]["n"]
        emit(rows > 0, f"S3. Spark READS the content of `{LIVE_TABLE}`",
             f"COUNT={rows} distinct_writers={distinct}")
    except Exception as exc:
        emit(False, f"S3. Spark READS the content of `{LIVE_TABLE}`",
             f"{type(exc).__name__}: {exc}")
        return 1
    return 0


def spark_phase_write(spark) -> int:
    """S4-S5: Spark CREATES and writes a new table in the same catalog."""
    spark.sql(f"create namespace if not exists {CATALOG}.{NAMESPACE}")
    target = f"{CATALOG}.{TABLE}"
    spark.sql(f"drop table if exists {target}")

    try:
        # `written_by` from the first CREATE: provenance is not a column you add
        # later, because the rows already written never get the missing value.
        spark.sql(f"""
            create table {target} (
                chave        string  not null,
                escritor     string  not null,
                sequencia    int     not null,
                written_by   string  not null
            ) using iceberg
        """)
        spark.sql(f"""
            insert into {target} values
                ('k_spark_01', 'spark', 1, 'spark'),
                ('k_spark_02', 'spark', 1, 'spark'),
                ('k_spark_03', 'spark', 1, 'spark')
        """)
        n = spark.table(target).count()
        emit(n == 3, f"S4. Spark CREATES `{TABLE}` in the catalog and writes to it", f"{n} rows")
    except Exception as exc:
        emit(False, f"S4. Spark CREATES `{TABLE}` in the catalog and writes to it",
             f"{type(exc).__name__}: {exc}")
        return 1

    # S5. MERGE INTO: the keyed upsert the ledger will need, on the Spark side.
    try:
        spark.sql(f"""
            merge into {target} t
            using (select 'k_spark_01' chave, 'spark' escritor, 9 sequencia,
                          'spark' written_by
                   union all
                   select 'k_spark_99', 'spark', 1, 'spark') s
            on t.chave = s.chave
            when matched then update set t.sequencia = s.sequencia
            when not matched then insert *
        """)
        state = {r["chave"]: r["sequencia"] for r in spark.table(target).collect()}
        ok = state.get("k_spark_01") == 9 and state.get("k_spark_99") == 1
        emit(ok, "S5. MERGE INTO by key works (updates 1, inserts 1)",
             f"rows={len(state)} k_spark_01.sequencia={state.get('k_spark_01')}")
    except Exception as exc:
        emit(False, "S5. MERGE INTO by key works (updates 1, inserts 1)",
             f"{type(exc).__name__}: {exc}")
    return 0


def spark_phase_concurrent(spark) -> int:
    """S6: writes slowly, announcing itself, so the host writes OVER it at the same time."""
    target = f"{CATALOG}.{TABLE}"
    print("READY", flush=True)  # the host waits for this line before it starts writing
    errors = 0
    for k in range(5):
        for attempt in range(20):
            try:
                spark.sql(
                    f"insert into {target} values "
                    f"('k_conc_spark_{k:02d}', 'spark', 1, 'spark')"
                )
                break
            except Exception:
                errors += 1
                time.sleep(0.3)
        else:
            emit(False, "S6. the 5 Spark writes under concurrency arrived",
                 f"gave up at k={k}")
            return 1
        time.sleep(0.6)
    emit(True, "S6. the 5 Spark writes under concurrency arrived",
         f"{errors} conflict(s) resolved by retry on the Spark side")
    return 0


def spark_phase_shape(spark) -> int:
    """S7: `applyInPandas` — the SHAPE that justifies Spark, and the proof that SQL lacks it.

    A running sum is trivial SQL. This is not a running sum: it is a running sum whose
    INPUTS are generated by decisions taken from its own state. Tomorrow's balance
    depends on an order issued because of today's balance, which arrives `lead time` days later.

    S7b does not CLAIM that the window function cannot express this — it MEASURES it. The same
    input, with the running sum SQL knows how to do, and the divergence that remains.

    The case is tiny on purpose: what is measured here is whether the engine supports the shape,
    not how long it takes. Timing, with its counter-argument, goes to `make spark-evidence`.
    """
    import pandas as pd
    from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField,
                                   StructType)

    DAYS, OPENING_BALANCE, LEAD_TIME, REORDER_POINT, TARGET = 10, 100.0, 2, 40.0, 120.0

    schema = StructType([
        StructField("serie", StringType()),
        StructField("dia", IntegerType()),
        StructField("consumo", DoubleType()),
        StructField("saldo", DoubleType()),
        StructField("recebido", DoubleType()),
        StructField("pedido", DoubleType()),
        StructField("chegada_prevista", IntegerType()),
    ])

    def ledger(key, pdf: pd.DataFrame) -> pd.DataFrame:
        """The loop: balance -> decision -> future arrival -> balance. One group at a time."""
        pdf = pdf.sort_values("dia")
        balance = OPENING_BALANCE
        arrivals: dict[int, float] = {}
        rows = []
        for _, row in pdf.iterrows():
            day = int(row["dia"])
            received = arrivals.pop(day, 0.0)
            balance += received
            consumed = min(float(row["consumo"]), balance)   # you cannot sell what is not there
            balance -= consumed
            order, arrival = 0.0, -1
            if balance < REORDER_POINT and not arrivals:      # one open order at a time
                order = TARGET - balance
                arrival = day + LEAD_TIME
                arrivals[arrival] = order                     # <- the decision changes the FUTURE
            rows.append((key[0], day, consumed, balance, received, order, arrival))
        return pd.DataFrame(rows, columns=[c.name for c in schema.fields])

    try:
        source = spark.createDataFrame(
            [(f"s{s}", d, 15.0 + s) for s in range(3) for d in range(DAYS)],
            "serie string, dia int, consumo double",
        )
        rows = source.groupBy("serie").applyInPandas(ledger, schema=schema).collect()

        orders = [r for r in rows if r["pedido"] > 0]
        arrivals = [r for r in rows if r["recebido"] > 0]
        in_transit = [r for r in orders if r["chegada_prevista"] > DAYS - 1]
        negatives = [r for r in rows if r["saldo"] < 0]

        # THE CONSERVATION LAW, and the reason the first version of this question failed
        # through MY fault. I had demanded `arrivals == orders`, and measured 5 orders for 3
        # arrivals. It was not a defect: an order issued on day 8 with lead time 2 arrives on
        # day 10, which is OUTSIDE the window. An order in transit at the end of the period is
        # a real property of any ledger — demanding it disappear would be demanding the model lie.
        orders_conserved = len(arrivals) + len(in_transit) == len(orders)

        # The second invariant, this one about QUANTITY: each series' final balance must be
        # exactly the opening balance plus what came in minus what went out. It catches errors
        # the event count does not — an arrival applied twice passes the first one.
        balance_conserved = True
        for series in {r["serie"] for r in rows}:
            of_series = sorted([r for r in rows if r["serie"] == series],
                               key=lambda r: r["dia"])
            expected = (OPENING_BALANCE
                        + sum(r["recebido"] for r in of_series)
                        - sum(r["consumo"] for r in of_series))
            if abs(of_series[-1]["saldo"] - expected) > 1e-9:
                balance_conserved = False

        ok = (len(rows) == 3 * DAYS and orders and orders_conserved and balance_conserved
              and not negatives)
        emit(ok, "S7a. `applyInPandas` expresses the feedback loop (balance -> order -> arrival)",
             f"{len(rows)} rows · {len(orders)} order(s) · {len(arrivals)} arrival(s) "
             f"· {len(in_transit)} in transit at the end of the window · "
             f"{len(negatives)} negative balance(s) · "
             f"orders_conserved={orders_conserved} balance_conserved={balance_conserved}")
    except Exception as exc:
        emit(False, "S7a. `applyInPandas` expresses the feedback loop (balance -> order -> arrival)",
             f"{type(exc).__name__}: {exc}")
        return 1

    # ---- S7b: the same input through the running sum SQL knows how to do -------------
    try:
        source.createOrReplaceTempView("consumo_diario")
        sql = spark.sql(f"""
            select serie, dia,
                   {OPENING_BALANCE} - sum(consumo) over (
                       partition by serie order by dia
                       rows between unbounded preceding and current row) as saldo_sql
            from consumo_diario
        """).collect()

        ledger_balance = {(r["serie"], r["dia"]): r["saldo"] for r in rows}
        divergent = [r for r in sql
                     if abs(r["saldo_sql"] - ledger_balance[(r["serie"], r["dia"])]) > 1e-9]
        sql_negatives = [r for r in sql if r["saldo_sql"] < 0]

        # The window function MUST diverge. If it agreed, the job would not need to
        # exist — and this question is what keeps the Spark justification from being a
        # paragraph nobody checked.
        emit(bool(divergent) and bool(sql_negatives),
             "S7b. the SQL running sum does NOT reproduce the ledger (which is why the job exists)",
             f"{len(divergent)}/{len(sql)} days diverge · SQL reaches a balance of "
             f"{min(r['saldo_sql'] for r in sql):.1f} on {len(sql_negatives)} days, "
             f"because there is no way for a window function to inject the arrival that its "
             f"own decision generated")
    except Exception as exc:
        emit(False, "S7b. the SQL running sum does NOT reproduce the ledger (which is why the job exists)",
             f"{type(exc).__name__}: {exc}")
    return 0


def spark_phase_cleanup(spark) -> int:
    target = f"{CATALOG}.{TABLE}"
    try:
        spark.sql(f"drop table if exists {target} purge")
        spark.sql(f"drop namespace if exists {CATALOG}.{NAMESPACE}")
        emit(True, "S8. Spark removes what it created", "spike table and namespace")
    except Exception as exc:
        emit(False, "S8. Spark removes what it created", f"{type(exc).__name__}: {exc}")
    return 0


# Keys are the `--fase` values (CLI interface): kept as they were.
SPARK_PHASES = {
    "leitura": spark_phase_read,
    "escrita": spark_phase_write,
    "concorrente": spark_phase_concurrent,
    "forma": spark_phase_shape,
    "limpeza": spark_phase_cleanup,
}


# ======================================================================================
# HOST SIDE — orchestrates the container and checks through pyiceberg.
# ======================================================================================

def compose(*args: str) -> list[str]:
    return ["docker", "compose", "--env-file", ".env", "-f", "infra/docker-compose.yml",
            "--profile", "spark", *args]


def run_on_spark(phase: str, *, marker: str = None):
    """Runs a phase in the container. Returns (returncode, RESULT lines, process|None).

    With `marker`, returns as soon as the container prints that line, with the process
    STILL ALIVE — which is what allows writing concurrently with it through pyiceberg.
    """
    cmd = compose("run", "--rm", "--no-deps", "spark",
                  "python3", "scripts/check_spark_iceberg.py", "--side", "spark",
                  "--fase", phase)
    proc = subprocess.Popen(cmd, cwd=REPO, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    collected = []
    for line in proc.stdout:
        if line.startswith("RESULT\t"):
            collected.append(line.rstrip("\n").split("\t"))
        else:
            text = line.rstrip()
            if text and not text.startswith(("WARNING", "NOTE:", "Using ")):
                print(f"      spark| {text}", flush=True)
        if marker and line.strip() == marker:
            return None, collected, proc
    proc.wait()
    return proc.returncode, collected, None


def record(collected) -> None:
    for _, ok, question, detail in collected:
        answer(question, ok == "1", detail)


def detail_of(collected, prefix: str) -> str:
    for _, _, question, detail in collected:
        if question.startswith(prefix):
            return detail
    return ""


def catalog_ddl() -> str:
    """The columns of `iceberg_tables`, to prove that Spark did not change them.

    The Java JdbcCatalog knows how to migrate the catalog schema (version V1 adds a column
    to support views). If it does that on the LIVE catalog during the spike, an
    experiment that should be isolated would have silently changed production — which is
    exactly the class of thing this project treats as a defect.
    """
    output = subprocess.run(
        ["docker", "exec", "retail-oltp-postgres", "psql", "-U", "oltp",
         "-d", os.environ.get("ICEBERG_CATALOG_DB", "iceberg_catalog"), "-t", "-A", "-c",
         "select table_name || '.' || column_name || ':' || data_type "
         "from information_schema.columns "
         "where table_name in ('iceberg_tables','iceberg_namespace_properties') "
         "order by 1"],
        capture_output=True, text=True, check=True)
    return output.stdout.strip()


def main_host() -> int:
    sys.path.insert(0, os.path.join(REPO, "platform", "src"))
    from retail_platform import orders_projection as proj

    print("EXPERIMENT: does Spark read and write the pyiceberg Iceberg catalog?")
    print(f"catalog .......... {CATALOG}")
    print(f"live table ....... {LIVE_TABLE}  (read-only)")
    print(f"spike table ...... {TABLE}")
    print()

    # ---- H0: prerequisite, with a message that says what to do -----------------------
    try:
        cat = proj.catalog()
        live = cat.load_table(LIVE_TABLE)
        rows_before = live.scan().to_arrow().num_rows
        ddl_before = catalog_ddl()
    except Exception as exc:
        print(f"ERROR: the Iceberg catalog did not respond ({type(exc).__name__}: {exc})")
        print("      The stream plane is not started by `make up`. Run `make stream-up` and,")
        print("      if the projection was never built, `make orders-projection-init`.")
        return 2
    answer("H0. pyiceberg reads the live table BEFORE the experiment", rows_before > 0,
           f"{rows_before} rows · metadata {os.path.basename(live.metadata_location)}")

    # ---- S1-S3: Spark sees and reads -------------------------------------------------
    print()
    print("S1-S3. Spark opens the catalog, sees the Python table and reads its content")
    rc, collected, _ = run_on_spark("leitura")
    record(collected)
    if rc != 0:
        return verdict(rows_before, ddl_before)

    # H1. Spark's count matches pyiceberg's. Reading "something" is not interop.
    read_detail = detail_of(collected, "S3.")
    spark_count = None
    for piece in read_detail.split():
        if piece.startswith("COUNT="):
            spark_count = int(piece.split("=", 1)[1])
    answer("H1. Spark's count MATCHES pyiceberg's on the same table",
           spark_count == rows_before,
           f"spark={spark_count} pyiceberg={rows_before}")

    # ---- S4-S5: Spark writes ---------------------------------------------------------
    print()
    print("S4-S5. Spark creates a table in the same catalog and writes to it")
    rc, collected, _ = run_on_spark("escrita")
    record(collected)
    if rc != 0:
        return verdict(rows_before, ddl_before)

    # ---- H2: the way back. pyiceberg reads what Spark wrote --------------------------
    print()
    print("H2. pyiceberg SEES and READS the table Spark created (the return trip)")
    try:
        cat = proj.catalog()  # reloaded: the namespace was born after the first open
        spark_tbl = cat.load_table(TABLE)
        rows = spark_tbl.scan().to_arrow()
        keys = {r["chave"] for r in rows.to_pylist()}
        answer("H2. pyiceberg reads the table created by Spark",
               {"k_spark_01", "k_spark_99"} <= keys,
               f"{rows.num_rows} rows · columns {rows.column_names}")
    except Exception as exc:
        failed("H2. pyiceberg reads the table created by Spark", exc)
        return verdict(rows_before, ddl_before)

    # ---- H3: CROSS-ENGINE optimistic conflict, with full recovery --------------------
    #
    # Observing that one side loses is not enough. Losing is not the property; the property is
    # detect -> reload -> reapply -> commit, with the stale commit NEVER
    # overwriting the new one. It is the cycle any concurrent writer will need.
    print()
    print("H3. CROSS-ENGINE conflict: pyiceberg with a stale snapshot after a Spark commit")
    try:
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        schema = spark_tbl.schema().as_arrow()

        def batch(key):
            return pa.Table.from_pylist(
                [{"chave": key, "escritor": "pyiceberg", "sequencia": 1,
                  "written_by": "pyiceberg"}], schema=schema)

        stale = cat.load_table(TABLE)          # reference loaded BEFORE Spark commits
        rc, extra, _ = run_on_spark("escrita")  # Spark recreates and commits: new snapshot
        del extra

        refused, reason = False, ""
        try:
            stale.append(batch("k_cruzado"))
        except CommitFailedException as conflict:
            refused, reason = True, str(conflict)[:150]
        except Exception as conflict:
            refused, reason = True, f"{type(conflict).__name__}: {str(conflict)[:150]}"
        answer("H3a. pyiceberg with a stale snapshot is REFUSED, not lost",
               refused, reason or "committed silently — this would be a lost update")

        fresh = cat.load_table(TABLE)          # reload
        fresh.append(batch("k_cruzado"))       # reapply
        final = cat.load_table(TABLE).scan().to_arrow()
        keys = {r["chave"] for r in final.to_pylist()}
        answer("H3b. after reloading, pyiceberg's retry commits",
               "k_cruzado" in keys)
        answer("H3c. Spark's write was NOT overwritten by the older commit",
               {"k_spark_01", "k_spark_02", "k_spark_03", "k_cruzado"} <= keys,
               f"{final.num_rows} rows: {sorted(keys)}")
    except Exception as exc:
        failed("H3a. pyiceberg with a stale snapshot is REFUSED, not lost", exc)

    # ---- S6 + H4: TRULY simultaneous writes, one engine on each side -----------------
    print()
    print("S6/H4. Spark and pyiceberg writing to the SAME table at the same time")
    try:
        import pyarrow as pa
        from pyiceberg.exceptions import CommitFailedException

        _, _, proc = run_on_spark("concorrente", marker="READY")
        conflicts = 0
        for k in range(5):
            for attempt in range(30):
                try:
                    t = cat.load_table(TABLE)
                    t.append(pa.Table.from_pylist(
                        [{"chave": f"k_conc_py_{k:02d}", "escritor": "pyiceberg",
                          "sequencia": 1, "written_by": "pyiceberg"}], schema=schema))
                    break
                except CommitFailedException:
                    conflicts += 1
                    time.sleep(0.2)
            else:
                raise RuntimeError(f"pyiceberg gave up at k={k}")
            time.sleep(0.5)

        for line in proc.stdout:
            if line.startswith("RESULT\t"):
                record([line.rstrip("\n").split("\t")])
        proc.wait()

        final = cat.load_table(TABLE).scan().to_arrow()
        keys = {r["chave"] for r in final.to_pylist()}
        expected = ({f"k_conc_spark_{k:02d}" for k in range(5)}
                    | {f"k_conc_py_{k:02d}" for k in range(5)})
        missing = expected - keys
        # HONESTY ABOUT WHAT THIS QUESTION PROVES. It proves the ABSENCE OF LOST UPDATES
        # under simultaneous writes. It does NOT prove there was a collision: if the commits do
        # not cross on the clock, the retry loop does not even fire — and a `0 conflict(s)` would
        # be read as "concurrency works" when in fact it was not exercised. What exercises
        # the conflict path DETERMINISTICALLY is H3a, with the stale reference
        # forced. Both questions exist because one does not replace the other.
        exercised = (" · the retry WAS exercised" if conflicts
                     else " · no clock collision in this run; what proves the "
                          "conflict path is H3a, which is deterministic")
        answer("H4. the 10 simultaneous writes from BOTH engines are all present",
               not missing,
               f"{final.num_rows} rows · {conflicts} conflict(s) on the pyiceberg side"
               + exercised
               + (f" · MISSING {sorted(missing)}" if missing else ""))
    except Exception as exc:
        failed("H4. the 10 simultaneous writes from BOTH engines are all present", exc)

    # ---- S7: the shape ---------------------------------------------------------------
    print()
    print("S7. the shape of the job: per-series feedback")
    _, collected, _ = run_on_spark("forma")
    record(collected)

    # ---- cleanup ---------------------------------------------------------------------
    print()
    _, collected, _ = run_on_spark("limpeza")
    record(collected)

    return verdict(rows_before, ddl_before)


def verdict(rows_before: int, ddl_before: str) -> int:
    # ---- H5/H6: the experiment must not have touched what was live -------------------
    print()
    print("H5/H6. did the experiment leave production as it found it?")
    try:
        sys.path.insert(0, os.path.join(REPO, "platform", "src"))
        from retail_platform import orders_projection as proj

        after = proj.catalog().load_table(LIVE_TABLE).scan().to_arrow().num_rows
        answer("H5. the live table still has the same rows", after == rows_before,
               f"before={rows_before} after={after}")
    except Exception as exc:
        failed("H5. the live table still has the same rows", exc)
    try:
        same = catalog_ddl() == ddl_before
        answer("H6. Spark did NOT change the schema of the catalog tables", same,
               "iceberg_tables and iceberg_namespace_properties intact" if same
               else "The JdbcCatalog migrated the schema of the LIVE catalog — investigate before\n"
                    "anything else. See `jdbc.schema-version`.")
    except Exception as exc:
        failed("H6. Spark did NOT change the schema of the catalog tables", exc)
    # H7 informs, it does not rule: `drop table ... purge` on the JdbcCatalog leaves the
    # data and metadata files behind, and 60 orphan objects piled up under the spike prefix
    # before anyone noticed (CR-016). The check removes what it wrote.
    try:
        from retail_platform.config import from_env

        bucket, _, root = proj.WAREHOUSE.removeprefix("s3://").partition("/")
        prefix = f"{root}/{NAMESPACE}/"
        client = from_env().client()
        keys = [o["Key"] for page in client.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=prefix) for o in page.get("Contents") or []]
        for start in range(0, len(keys), 1000):
            client.delete_objects(Bucket=bucket, Delete={
                "Objects": [{"Key": k} for k in keys[start:start + 1000]]})
        answer("H7. no files left under the spike prefix", True,
               f"s3://{bucket}/{prefix}: {len(keys)} orphan objects removed")
    except Exception as exc:
        failed("H7. no files left under the spike prefix", exc)

    print()
    print("=" * 80)
    for question, ok, _ in results:
        print(f"  {'YES' if ok else 'NO '}  {question}")
    print()

    # The questions that decide. The others inform; these rule.
    essential = ("S1.", "S2.", "S3.", "H1.", "S4.", "H2.", "H3a", "H3b", "H3c", "H4.",
                 "S7a", "S7b", "H5.", "H6.")
    missing = [p for p in essential
               if not any(q.startswith(p) and ok for q, ok, _ in results)]
    if missing:
        print("VERDICT: the experiment FAILED.")
        for p in missing:
            print(f"  missing: {p}")
        print()
        print("WHAT THIS DECIDES, and it is not only about Spark: the *interop across")
        print("engines* clause of the Iceberg justification was TESTED and does not hold. It leaves")
        print("ARCHITECTURE. What is still proven — atomic commit and optimistic concurrency")
        print("between the two Python writers — stays there, and is still enough.")
        return 1

    print("VERDICT: PASSED. Spark reads the catalog written by pyiceberg, writes to it,")
    print("and the two engines resolve optimistic conflicts without losing writes. The interop")
    print("that justified Iceberg stops being a claim. `applyInPandas` expresses the")
    print("feedback loop SQL cannot express — which is the shape of the stock job.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--side", choices=["host", "spark"], default="host",
                        help="`spark` runs INSIDE the container; the default orchestrates.")
    parser.add_argument("--fase", choices=sorted(SPARK_PHASES), help="only with --side spark")
    args = parser.parse_args()

    if args.side == "spark":
        if not args.fase:
            parser.error("--side spark requires --fase")
        try:
            return spark_side(args.fase)
        except Exception:
            traceback.print_exc()
            emit(False, f"S?. phase `{args.fase}` raised an exception", "see traceback above")
            return 1
    return main_host()


if __name__ == "__main__":
    raise SystemExit(main())
