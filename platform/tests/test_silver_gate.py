"""The Silver build gate — decided in one place, and verified without network.

WHY THIS SUITE EXISTS: because of a defect that spent three days in production.

The decision "what to exclude from `dbt build`" lived in SIX files: the Makefile `silver`
target, with all five gates, and each of the five DAGs, with its own source's gate and
nothing else. No DAG had the Iceberg gate, and the Mercadona one had no gate at all.

The symptom: `mercadona_catalog_daily` failed EVERY DAY on its last task. The four
warehouses extracted, validated, landed and verified successfully; `silver` fell over with
"no version-hint could be found". The data was right the whole time — the gate just lived
in the wrong file.

Nothing caught it because there was nothing to catch it: `make silver` passed (it has the
full gate), `make test` passed (it doesn't look at DAGs), and the dbt suite never got to run.
Only the real run failed, and a day later.

These tests aim at both sides of the defect:

  * `plan()` is PURE, so the whole decision can be exercised without MinIO or catalog;
  * and there is a SOURCE check requiring every DAG that builds Silver to go through the
    verb, instead of assembling its own dbt command. A correct gate in one place is only
    worth something while it is the only place.
"""

from __future__ import annotations

import ast
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.silver_gate import (  # noqa: E402
    ICEBERG_NODES,
    ICEBERG_TABLES,
    ICEBERG_VAR,
    SOURCE_MODELS,
    plan,
)

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DAGS = os.path.join(REPO, "orchestration", "airflow", "dags")
MODELS = os.path.join(REPO, "platform", "dbt", "models", "silver")
TESTS = os.path.join(REPO, "platform", "dbt", "tests")

ALL_LANDED = {prefix: True for prefix, _ in SOURCE_MODELS}
METADATA = "s3://retail-lakehouse/iceberg/projection/live_order_state/metadata/00052-x.json"


class PlanTest(unittest.TestCase):
    def test_with_everything_in_place_nothing_is_excluded(self):
        complete = {key: METADATA for key, _, _ in ICEBERG_TABLES}
        self.assertEqual(plan(ALL_LANDED, complete)[:1], ["--vars"])
        self.assertNotIn("--exclude", plan(ALL_LANDED, complete))

    def test_old_single_string_form_still_applies_to_the_live_projection(self):
        """Until Phase 7 `plan` received a single path, because there was only one Iceberg
        table. The string is still accepted and means exactly what it meant — and nothing
        more: the stock ledger stays out, which is right for whoever hasn't run Spark."""
        arguments = plan(ALL_LANDED, METADATA)
        self.assertIn(f'{ICEBERG_VAR}: "{METADATA}"',
                      arguments[arguments.index("--vars") + 1])
        self.assertIn("silver_stock_ledger", arguments)

    def test_source_without_data_takes_all_its_models_with_it(self):
        """Excluding the model and leaving the sibling that `ref()`s it would trade one
        error for another: `silver_price_change` reads `silver_product_price`, and
        `silver_order`/`silver_order_line` read `silver_order_event`."""
        landed = dict(ALL_LANDED, mercadona_catalog_api=False)
        arguments = plan(landed, METADATA)
        for model in ("silver_product_price", "silver_price_change", "silver_category",
                      "raw_manifest"):
            self.assertIn(model, arguments)

    def test_unavailable_catalog_excludes_the_model_AND_the_test_comparing_it(self):
        """Excluding only the model would leave the test trying to `ref()` something that
        was not built — a compilation error instead of a run error."""
        arguments = plan(ALL_LANDED, None)
        for node in ICEBERG_NODES:
            self.assertIn(node, arguments)
        self.assertNotIn("--vars", arguments)

    def test_SPARK_IS_OPTIONAL_and_this_is_what_proves_it(self):
        """The property that keeps Spark out of the project's default path.

        With the live projection up and Spark NEVER having run, the build leaves the stock
        ledger out and everything else in. On a machine where nobody ran `make
        stock-ledger` — which is the case for any fresh clone — `make silver` builds the
        whole Silver without Spark, without a JVM and without a downloaded image.

        Without this test the promise would be a README paragraph, and paragraphs don't fail.
        """
        arguments = plan(ALL_LANDED, {"live_order_state": METADATA, "stock_ledger": None})
        for node in ICEBERG_TABLES[1][2]:
            self.assertIn(node, arguments)
        for node in ICEBERG_TABLES[0][2]:
            self.assertNotIn(node, arguments)
        # and the live projection still receives its path
        self.assertIn(f'{ICEBERG_VAR}: "{METADATA}"',
                      arguments[arguments.index("--vars") + 1])

    def test_the_reverse_also_holds_one_table_does_not_drag_the_other(self):
        """Spark ran, the stream plane didn't come up. The two tables are independent: a
        gate that treated them together would exclude the ledger because of Kafka."""
        arguments = plan(ALL_LANDED, {"live_order_state": None, "stock_ledger": METADATA})
        for node in ICEBERG_TABLES[0][2]:
            self.assertIn(node, arguments)
        for node in ICEBERG_TABLES[1][2]:
            self.assertNotIn(node, arguments)

    def test_with_both_tables_both_vars_travel_together(self):
        arguments = plan(ALL_LANDED, {key: METADATA for key, _, _ in ICEBERG_TABLES})
        self.assertNotIn("--exclude", arguments)
        var = arguments[arguments.index("--vars") + 1]
        for _, name, _ in ICEBERG_TABLES:
            self.assertIn(f'{name}: "{METADATA}"', var)

    def test_this_is_the_2026_08_31_defect(self):
        """The exact combination that failed: ALL sources with data, catalog down.

        It was the normal state of any machine without `make stream-up`, and of the Airflow
        container, which until then had no way to reach the catalog. The build has to stay
        GREEN with one model fewer, not red with an impossible model.
        """
        arguments = plan(ALL_LANDED, None)
        self.assertEqual(arguments[0], "--exclude")
        expected = {node for _, _, nodes in ICEBERG_TABLES for node in nodes}
        self.assertEqual(set(arguments[1:]), expected)

    def test_prefix_missing_from_the_dict_counts_as_not_landed(self):
        """The safe side: excluding a model that could be built costs one run; trying to
        build one that can't costs the whole build."""
        arguments = plan({}, None)
        for _, models in SOURCE_MODELS:
            for model in models:
                self.assertIn(model, arguments)

    def test_metadata_path_is_quoted_in_the_yaml(self):
        """`s3://...` unquoted makes YAML read `s3:` as a key and the var arrive EMPTY — and
        an empty var doesn't fail the parse, it fails the model, three steps later."""
        arguments = plan(ALL_LANDED, METADATA)
        var = arguments[arguments.index("--vars") + 1]
        self.assertEqual(var, f'{{{ICEBERG_VAR}: "{METADATA}"}}')


class DeclaredModelsExistTest(unittest.TestCase):
    def test_every_NODE_cited_by_the_gate_exists_on_disk(self):
        """A wrong name here fails nothing: dbt ignores `--exclude` of a nonexistent node,
        and the node one meant to exclude gets built anyway.

        THE SCAN INCLUDES `tests/`, AND THAT WAS A GAP UNTIL PHASE 7. The gate always
        excluded TESTS along with models — `assert_live_projection_matches_batch_fold` has
        been there since Milestone 6 — but this check only looked at `models/`, and only at
        the FIRST node of each Iceberg table. A typo in a test name would pass: dbt would
        ignore the `--exclude`, the test would run against a model that wasn't built, and
        the error would show up as a compilation failure three steps away from the cause.
        """
        on_disk = {
            os.path.splitext(filename)[0]
            for root in (MODELS, TESTS)
            for _, _, filenames in os.walk(root)
            for filename in filenames if filename.endswith(".sql")
        }
        self.assertTrue(on_disk, f"no .sql file in {MODELS} or in {TESTS}")
        for _, models in SOURCE_MODELS:
            for model in models:
                self.assertIn(model, on_disk)
        for _, _, nodes in ICEBERG_TABLES:
            for node in nodes:
                self.assertIn(node, on_disk, f"the gate cites `{node}`, which does not exist")

    def test_every_source_model_is_assigned_to_some_source(self):
        """The inverse, and it is what catches the NEW model: whoever creates a source model
        and forgets to register it here produces exactly the 2026-08-31 defect — a model the
        gate doesn't know how to exclude, in a repository where that source hasn't landed
        yet."""
        declared = ({m for _, models in SOURCE_MODELS for m in models}
                    | {n for _, _, nodes in ICEBERG_TABLES for n in nodes})
        # Models directly under silver/ are seed passthroughs: they depend on no RAW.
        for root, _, filenames in os.walk(MODELS):
            if os.path.abspath(root) == os.path.abspath(MODELS):
                continue
            for filename in filenames:
                if not filename.endswith(".sql"):
                    continue
                name = os.path.splitext(filename)[0]
                self.assertIn(name, declared,
                              f"{name} is in neither SOURCE_MODELS nor ICEBERG_NODES")


class EveryDagGoesThroughTheGateTest(unittest.TestCase):
    """The check that would have prevented the defect: a single gate is only worth something
    if it is the only one.

    The gate being right is not enough — nothing stops the next DAG from assembling its own
    `dbt build`, which is exactly how the six copies were born.
    """

    def _dags(self):
        for filename in sorted(os.listdir(DAGS)):
            if not filename.endswith(".py"):
                continue
            with open(os.path.join(DAGS, filename), encoding="utf-8") as handle:
                yield filename, handle.read()

    def test_no_silver_dag_assembles_its_own_dbt_command(self):
        for filename, source in self._dags():
            if filename == "warehouse_load.py":
                # The ONLY legitimate exception: it runs the warehouse target (postgres since
                # CR-007), where the tree reads only `source()` over STAGE and no gate applies.
                continue
            self.assertNotIn('"build"', source,
                             f"{filename} assembles its own `dbt build` instead of using the verb")

    def test_every_dag_with_a_silver_task_calls_the_verb(self):
        seen = 0
        for filename, source in self._dags():
            tree = ast.parse(source)
            has_silver = any(
                isinstance(node, ast.FunctionDef) and node.name == "silver"
                for node in ast.walk(tree)
            )
            if not has_silver:
                continue
            seen += 1
            self.assertIn("silver-build", source,
                          f"{filename} has a `silver` task that bypasses the gate")
        self.assertEqual(seen, 5, "expected the five source DAGs with a `silver` task")

    def test_the_makefile_also_goes_through_the_verb(self):
        """The Makefile was the RIGHT place for the longest time, and it still needs the
        check: the defect was not that it was wrong, it was that it was alone."""
        with open(os.path.join(REPO, "Makefile"), encoding="utf-8") as handle:
            makefile = handle.read()
        target = makefile.split("\nsilver:", 1)[1].split("\n\n", 1)[0]
        # Comments out: the target DESCRIBES the gate it lost, and the word `--exclude`
        # appears in that explanation. What must not come back is the target BUILDING the list.
        commands = "\n".join(
            line for line in target.split("\n") if not line.lstrip().startswith("#")
        )
        self.assertIn("silver-build", commands)
        self.assertNotIn("--exclude", commands)
        self.assertNotIn("--vars", commands)
        self.assertNotIn("has-data", commands)
        self.assertNotIn("iceberg-metadata", commands)


def _calls(source: str, name: str):
    """Every `name(...)` call in the DAG, with its literal keywords."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == name:
            yield {k.arg: k.value for k in node.keywords if k.arg}


def _task_id(kwargs) -> str | None:
    value = kwargs.get("task_id")
    return value.value if isinstance(value, ast.Constant) else None


class DagTriggerRulesTest(unittest.TestCase):
    """The two-warehouse defect (DECISIONS.md § "Two orchestration defects") only showed up
    in a real, mixed run: `mad1`'s short-circuit dragged `silver` down even with `bcn1`
    freshly landed. It took TWO fixes, and no offline test guarded either of them. Undoing
    either one brings the symptom back with no error at all.
    """

    def _dags(self):
        return EveryDagGoesThroughTheGateTest._dags(self)

    def test_every_gate_honors_the_downstream_trigger_rule(self):
        seen = 0
        for filename, source in self._dags():
            for kwargs in _calls(source, "ShortCircuitOperator"):
                seen += 1
                value = kwargs.get("ignore_downstream_trigger_rules")
                self.assertTrue(
                    isinstance(value, ast.Constant) and value.value is False,
                    f"{filename}: ShortCircuitOperator without ignore_downstream_trigger_rules=False"
                    " — the default skips ALL downstream, including `silver`",
                )
        self.assertEqual(seen, 6, "expected one gate in each of the six DAGs")

    def test_silver_runs_if_at_least_one_warehouse_landed(self):
        seen = 0
        for filename, source in self._dags():
            for kwargs in _calls(source, "PythonOperator"):
                if _task_id(kwargs) != "silver":
                    continue
                seen += 1
                rule = kwargs.get("trigger_rule")
                self.assertEqual(
                    getattr(rule, "attr", None), "NONE_FAILED_MIN_ONE_SUCCESS",
                    f"{filename}: `silver` without NONE_FAILED_MIN_ONE_SUCCESS — with all_success,"
                    " a warehouse that short-circuited blocks the other one's Silver",
                )
        self.assertEqual(seen, 5, "expected the `silver` task in the five source DAGs")

    def test_warehouse_trigger_stays_on_all_success(self):
        """The inverse: here the default is right. Suspicious Silver doesn't go to the warehouse."""
        seen = 0
        for filename, source in self._dags():
            for kwargs in _calls(source, "TriggerDagRunOperator"):
                if _task_id(kwargs) != "trigger_warehouse_load":
                    continue
                seen += 1
                self.assertNotIn("trigger_rule", kwargs, filename)
        self.assertGreaterEqual(seen, 1)


class PartitionedSilverOverwritesTest(unittest.TestCase):
    """`overwrite_or_ignore` on a PARTITIONED write only replaces files with the same name:
    when one run writes `data_0` + `data_1` into a partition and the next writes only
    `data_0`, the stale `data_1` stays and every reader counts its rows twice. Seen on
    2026-10-30: 8,648 duplicated grain rows in `silver_product_price` (CR-016). `overwrite`
    clears the target first — measured to work on MinIO with DuckDB 1.5.
    """

    def test_no_partitioned_model_keeps_stale_files(self):
        models = os.path.join(REPO, "platform", "dbt", "models")
        seen = 0
        for root, _, files in os.walk(models):
            for name in files:
                if not name.endswith(".sql"):
                    continue
                with open(os.path.join(root, name), encoding="utf-8") as fh:
                    source = fh.read()
                if "'partition_by'" not in source:
                    continue
                seen += 1
                self.assertIn("'overwrite': 1", source, name)
                self.assertNotIn("overwrite_or_ignore", source, name)
        self.assertEqual(seen, 3, "expected the three partitioned Mercadona models")


def _load_function(path: str, name: str):
    """Executes ONE top-level function of a DAG file without importing Airflow."""
    with open(path, encoding="utf-8") as fh:
        tree = ast.parse(fh.read())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    namespace: dict = {}
    exec(compile(ast.Module(body=[node], type_ignores=[]), path, "exec"),
         {"datetime": __import__("datetime").datetime}, namespace)
    return namespace[name]


class DailyCollectionDateTest(unittest.TestCase):
    """The daily DAG's scheduled runs were ALL refused by `guard_date` (48 of 48): a cron run
    is dated with the START of its interval, so the 06:00 run executing on 10-30 claimed
    10-29. Every collection that landed came from a manual trigger. CR-016.
    """

    def setUp(self):
        from datetime import datetime, timezone
        self.at = lambda *a: datetime(*a, tzinfo=timezone.utc)
        self.collection_date = _load_function(
            os.path.join(DAGS, "mercadona_catalog_daily.py"), "collection_date")

    def test_a_scheduled_run_collects_the_day_it_executes(self):
        # logical date = interval start (yesterday 06:00); interval end = today 06:00
        self.assertEqual(
            self.collection_date(self.at(2026, 10, 29, 6), self.at(2026, 10, 30, 6)),
            "2026-10-30")

    def test_a_manual_run_collects_the_trigger_day(self):
        # interval of a manual run ends at the last 06:00 BEFORE the trigger
        self.assertEqual(
            self.collection_date(self.at(2026, 10, 30, 3, 15), self.at(2026, 10, 29, 6)),
            "2026-10-30")

    def test_a_manual_run_dated_in_the_past_stays_in_the_past(self):
        """So `guard_date` still refuses it: no backfill for this source."""
        self.assertEqual(
            self.collection_date(self.at(2026, 10, 1, 12), self.at(2026, 10, 1, 6)),
            "2026-10-01")

    def test_no_task_of_the_daily_dag_reads_ds(self):
        with open(os.path.join(DAGS, "mercadona_catalog_daily.py"), encoding="utf-8") as fh:
            tree = ast.parse(fh.read())
        for node in tree.body:
            if isinstance(node, ast.FunctionDef):
                self.assertNotIn("ds", [a.arg for a in node.args.args],
                                 f"{node.name} takes `ds` — a scheduled run's ds is yesterday")


if __name__ == "__main__":
    unittest.main()
