"""The export's three queries, against real DuckDB fixtures — no network, no MinIO.

Each fixture case reproduces a problem MEASURED in the real Lakehouse, and exists so it is
caught here and not there:

  * the municipality whose tramos hang off a nucleus, never off the aggregate row: the naive
    join by unit_code returned 0 of Valencia's 7,194 tramos;
  * the tramo that matches neither a street nor a pseudo-address (3 in the real scope): it
    must be excluded AND counted;
  * the municipality with a homonymous series coming from another province
    (Torrent/Girona=182 against Torrent/Valencia=90,928): resolved in the Silver model by
    INE's official code, and here only RE-CHECKED — if it shows up again, the export refuses
    instead of choosing;
  * the aggregate age labels 'Total' and '85 y mas anos', which added to the 101 single ages
    inflate the pyramid 2.03x;
  * the series table duplicated across two ingestion_date values, which would double each
    province.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.customers_reference import (  # noqa: E402
    ReferenceExportError,
    build,
    write,
)

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


SERVICE_AREA_CSV = """wh,province_code,municipality_code,municipality_name,is_home_municipality
wh1,46,244,Torrent,false
wh1,46,250,València,true
wh2,08,019,Barcelona,true
"""

PROVINCE_MAP_CSV = """wh,province_code,province_name,municipio_code,municipio_name
wh1,46,Valencia/València,250,València
wh2,08,Barcelona,019,Barcelona
"""

# The four customer-base premises. `min_customer_age` cuts the pyramid and
# `customer_penetration_source` POINTS to the other seed — the 2.2 does not appear here, and a
# test below proves it does not.
CUSTOMER_PREMISES_CSV = """premise_key,value,unit,label,rationale
min_customer_age,18,years,synthetic,"Capacidade legal para contratar."
customer_penetration_source,demand_profile.channel_reference_pct,reference,synthetic,"Ponteiro, nao copia."
customer_population_basis,adult_resident_population,rule,synthetic,"O denominador e adulto."
customer_allocation,per_warehouse_population,rule,synthetic,"O total e consequencia."
"""

# Only the row this export reads. The real seed has 17 parameters; repeating them all here
# would make the fixture age along with the demand model for no gain.
DEMAND_PROFILE_CSV = """param_key,value,unit,label,rationale
channel_reference_pct,2.2,percent,observed,"E-commerce no volume de alimentacao, MAPA sec.3."
"""

SCHEMA = """
create table silver_callejero_tramos (
    ingestion_date varchar, is_latest_ingestion boolean, section_code varchar, province_code varchar,
    municipality_code varchar, entity_suffix varchar, unit_code varchar,
    street_id varchar, street_code varchar, pseudo_address_id varchar,
    pseudo_address_code varchar, postal_code varchar, numbering_type varchar,
    number_from varchar, number_from_qualifier varchar,
    number_to varchar, number_to_qualifier varchar
);
create table silver_callejero_population_units (
    ingestion_date varchar, is_latest_ingestion boolean, unit_code varchar, province_code varchar,
    municipality_code varchar, entity_suffix varchar,
    is_municipality_aggregate boolean, municipality_name varchar,
    population_unit_name varchar
);
create table silver_callejero_streets (
    ingestion_date varchar, is_latest_ingestion boolean, street_code varchar, street_name_full varchar
);
create table silver_callejero_pseudo_addresses (
    ingestion_date varchar, is_latest_ingestion boolean, pseudo_address_code varchar, name_full varchar
);
create table silver_ine_population_by_municipality (
    ingestion_date varchar, is_latest_ingestion boolean, series_code varchar, province_code varchar,
    municipality_code varchar, municipality_name varchar, sex_label varchar,
    year bigint, reference_date varchar, population_value double
);
create table silver_ine_population_series (
    ingestion_date varchar, is_latest_ingestion boolean, table_id varchar, province_name varchar,
    age_label varchar, sex_label varchar, year bigint, fk_periodo bigint,
    reference_date varchar, fk_tipo_dato bigint, population_value double
);
"""

TODAY = "2026-08-26"
YESTERDAY = "2026-08-25"


def tramo(municipality, street_id, code, postal, numbering="1", low="0001", high="0009",
          unit="4625000101", section="4625001001", pseudo_id="00000", pseudo_code="00000",
          latest=True):
    province = "08" if municipality == "019" else "46"
    return (
        (TODAY if latest else YESTERDAY), latest, section, province, municipality, "01", unit,
        street_id, code, pseudo_id, pseudo_code, postal, numbering,
        low, "", high, "",
    )


PROPENSITY_SEED_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dbt", "seeds", "ine_online_propensity_seed.csv")


@unittest.skipIf(duckdb is None, "duckdb not installed")
class OltpReferenceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.seeds = os.path.join(self.tmp.name, "seeds")
        os.makedirs(self.seeds)
        self._seed("warehouse_service_area_seed.csv", SERVICE_AREA_CSV)
        self._seed("warehouse_province_map_seed.csv", PROVINCE_MAP_CSV)
        self._seed("customer_premises_seed.csv", CUSTOMER_PREMISES_CSV)
        self._seed("demand_profile_seed.csv", DEMAND_PROFILE_CSV)
        # The REAL INE seed, not a fixture: its bands are what the propensity is about.
        with open(PROPENSITY_SEED_PATH, encoding="utf-8") as handle:
            self._seed("ine_online_propensity_seed.csv", handle.read())

        self.connection = duckdb.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.execute(SCHEMA)
        self._populate()

    def _seed(self, name, content):
        with open(os.path.join(self.seeds, name), "w", encoding="utf-8") as handle:
            handle.write(content)

    def _insert(self, table, rows):
        if not rows:
            return
        marks = ", ".join(["?"] * len(rows[0]))
        self.connection.executemany(f"insert into {table} values ({marks})", rows)

    def _populate(self):
        self._insert("silver_callejero_tramos", [
            # Torrent (46/244): two tramos on named streets.
            tramo("244", "00001", "4624400001", "46900", unit="4624400101"),
            tramo("244", "00002", "4624400002", "46901", numbering="2",
                  low="0002", high="0010", unit="4624400101"),
            # València (46/250): the case that broke. The tramos hang off a NUCLEUS
            # unit_code; the municipality's aggregate row has another unit_code.
            tramo("250", "00010", "4625000010", "46001", unit="4625000101"),
            tramo("250", "00011", "4625000011", "46002", unit="4625000102"),
            # València: a pseudo-address (sentinel street_id "00000").
            tramo("250", "00000", "4625000000", "46003", numbering="0",
                  low="0000", high="0000", unit="4625000101",
                  pseudo_id="00007", pseudo_code="4625000007"),
            # València: ORPHAN — matches neither a street nor a pseudo-address.
            tramo("250", "09999", "4625009999", "46004", unit="4625000101"),
            # Barcelona (08/019), another warehouse.
            tramo("019", "00050", "0801900050", "08001", unit="0801900101"),
            # EARLIER extraction of the same publication: Silver stacks both dates. If the
            # export did not filter by is_latest_ingestion, this tramo would come in again and
            # each Torrent address would appear twice among the candidates.
            tramo("244", "00001", "4624400001", "46900", unit="4624400101", latest=False),
        ])
        self._insert("silver_callejero_population_units", [
            # The municipality's aggregate rows: unit_code ENDING IN 0000, different from the
            # tramos' unit_code. That is exactly why the join has to be by
            # (province_code, municipality_code) and not by unit_code.
            (TODAY, True, "4624400000", "46", "244", "00", True, "TORRENT", ""),
            (TODAY, True, "4625000000", "46", "250", "00", True, "VALÈNCIA", ""),
            (TODAY, True, "0801900000", "08", "019", "00", True, "BARCELONA", ""),
            # Nuclei/hamlets where the tramos actually hang.
            (TODAY, True, "4624400101", "46", "244", "01", False, "TORRENT", "TORRENT"),
            (TODAY, True, "4625000101", "46", "250", "01", False, "VALÈNCIA", "VALENCIA"),
            (TODAY, True, "4625000102", "46", "250", "01", False, "VALÈNCIA", "EL SALER"),
            (TODAY, True, "0801900101", "08", "019", "01", False, "BARCELONA", "BARCELONA"),
        ])
        self._insert("silver_callejero_streets", [
            (TODAY, True, "4624400001", "CARRER U"),
            (TODAY, True, "4624400002", "CARRER DOS"),
            (TODAY, True, "4625000010", "AVINGUDA DEL PORT"),
            (TODAY, True, "4625000011", "CAMI DEL SALER"),
            (TODAY, True, "0801900050", "CARRER DE MALLORCA"),
        ])
        self._insert("silver_callejero_pseudo_addresses", [
            (TODAY, True, "4625000007", "PARC DE CAPCALERA"),
        ])

        population = []
        for municipality, name, total, male in (
            ("244", "Torrent", 90928, 44000),
            ("250", "València", 800215, 380000),
            ("019", "Barcelona", 1660122, 790000),
        ):
            province = "08" if municipality == "019" else "46"
            for sex, value in (
                ("Total", total), ("Hombres", male), ("Mujeres", total - male)
            ):
                population.append(
                    (TODAY, True, f"DP{municipality}{sex[:1]}", province, municipality,
                     name, sex, 2025, "2024-12-31", float(value))
                )
        self._insert("silver_ine_population_by_municipality", population)

        series = []
        # Two ingestion_date values with identical rows: only the latest may be used.
        for ingestion in (YESTERDAY, TODAY):
            latest = ingestion == TODAY
            for province_name in ("Valencia/València", "Barcelona"):
                single_ages = [(age, 100.0) for age in range(0, 101)]
                for age, value in single_ages:
                    label = "100 y más años" if age == 100 else f"{age} años"
                    series.append((ingestion, latest, "31304", province_name, label,
                                   "Ambos sexos", 2022, 27, "2022-06-30", 2, value))
                # Overlapping aggregates: added to the single ages, they would inflate the pyramid.
                series.append((ingestion, latest, "31304", province_name, "Total",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 10100.0))
                series.append((ingestion, latest, "31304", province_name, "85 y más años",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 1600.0))
                # A label that is not one of our provinces.
                series.append((ingestion, latest, "31304", "Total Nacional", "30 años",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 999.0))
        self._insert("silver_ine_population_series", series)

    def _inject_homonym(self):
        """Fanout regression: a second 'Torrent' series, the Girona one, with 182.

        The Silver model now resolves this by INE's official code
        (ine_ambiguous_series_seed), so this row should no longer exist. If it exists again,
        the export must REFUSE — not pick a value on its own.
        """
        self._insert("silver_ine_population_by_municipality", [
            (TODAY, True, f"DPGIRONA{sex[:1]}", "46", "244", "Torrent", sex, 2025,
             "2024-12-31", float(value))
            for sex, value in (("Total", 182), ("Hombres", 90), ("Mujeres", 92))
        ])

    def build(self):
        return build(self.connection, seeds_dir=self.seeds)


class CustomerAllocationTest(OltpReferenceTestCase):
    """How many customers each warehouse has, and where that number comes from.

    Until Phase 5 it was 5,000 per warehouse, typed on the command line — the same number
    for AUFs that differ 4.6x in population. Nothing failed: the totals added up, the
    addresses were real, the manifest matched. The only thing wrong was that density did not
    exist, and density does not show up in any total.
    """

    # Fixture: wh1 = Torrent 90,928 + Valencia 800,215 = 891,143 inhabitants; wh2 = Barcelona
    # 1,660,122. The pyramid has 101 ages of equal value, so the adult share is 83/101.
    ADULT_SHARE = 83 / 101

    def test_allocation_derives_from_the_adult_population_per_warehouse(self):
        allocation = self.build()["weights"]["customer_allocation"]
        by_wh = {row["wh"]: row for row in allocation["by_warehouse"]}

        self.assertEqual(by_wh["wh1"]["population_total"], 891143)
        self.assertEqual(by_wh["wh2"]["population_total"], 1660122)
        # 891,143 x 83/101 x 2.2% = 16,111.16 -> 16,111
        self.assertEqual(by_wh["wh1"]["customers"], 16111)
        # 1,660,122 x 83/101 x 2.2% = 30,013.69 -> 30,014
        self.assertEqual(by_wh["wh2"]["customers"], 30014)
        self.assertEqual(allocation["total_customers"], 16111 + 30014)

    def test_allocating_by_total_population_would_give_another_number(self):
        """The ADULT denominator is not decorative: with the total, wh1 would have 1,494 more.

        Without this case, swapping `adult_population` for `population_total` in the export
        would pass every other test — the proportions between warehouses barely change, and
        the total would remain plausible.
        """
        allocation = self.build()["weights"]["customer_allocation"]
        by_wh = {row["wh"]: row for row in allocation["by_warehouse"]}
        by_total_population = round(891143 * 0.022)
        self.assertEqual(by_total_population, 19605)
        self.assertNotEqual(by_wh["wh1"]["customers"], by_total_population)

    def test_the_total_is_a_consequence_not_a_split_quota(self):
        """Summing the parts must give the total, and no part may be a remainder.

        With a split quota, the last warehouse would get `total - sum(the others)` and absorb
        all the rounding. Here each warehouse comes from its own population and the total is
        the sum.
        """
        allocation = self.build()["weights"]["customer_allocation"]
        self.assertEqual(
            allocation["total_customers"],
            sum(row["customers"] for row in allocation["by_warehouse"]),
        )
        self.assertEqual(
            allocation["served_population"],
            sum(row["population_total"] for row in allocation["by_warehouse"]),
        )

    def test_the_allocation_is_a_sorted_and_stable_list(self):
        """A LIST, not a dict — and `sorted()` is the second lock, not the first.

        Honesty about what this case proves and what it does not: `_population_sql` already
        returns the rows with `order by a.wh`, so removing `sorted()` from the module would
        NOT make this case fail. What it really protects is the shape — a list, which
        survives JSON with its order intact — against a dict, whose iteration on the other
        side of the boundary would depend on insertion order. `sorted()` stays in the module
        because the guarantee should not depend on an `order by` in a query that may be
        rewritten.
        """
        first = self.build()["weights"]["customer_allocation"]
        second = self.build()["weights"]["customer_allocation"]
        self.assertIsInstance(first["by_warehouse"], list)
        names = [row["wh"] for row in first["by_warehouse"]]
        self.assertEqual(names, sorted(names))
        self.assertEqual(
            json.dumps(first, sort_keys=True), json.dumps(second, sort_keys=True)
        )

    def test_the_allocation_does_not_sit_on_the_rounding_edge(self):
        """The margin that keeps rounding from being a coin toss.

        MEASURED in the real Lakehouse: two runs of `export-oltp-reference` produce
        `adult_share` values that differ in the LAST BIT of the double — 0.8224668719886548
        against 0.8224668719886545 — because DuckDB's parallel aggregation does not fix the
        order of the floating-point sum. That is not a defect of this module and does not
        propagate: the four targets come out identical, and the base generated from the two
        exports has the same sha256.

        But it only does not propagate because none of the four products falls near a `.5`.
        If one ever does, the target starts alternating between two integers from export to
        export, the base changes size without anything having been decided, and the dbt test
        `assert_customer_base_follows_the_declared_population_allocation` — which tolerates 1
        customer exactly because of this — starts passing by luck. This case is the warning.

        The tightest margin today is mad1's: 128,770.524404, 0.024 away from a tie. In
        population that is about 135 residents — if Madrid grows or shrinks that much in the
        next 29005, the target starts alternating between 128,770 and 128,771.
        """
        allocation = self.build()["weights"]["customer_allocation"]
        rate = allocation["penetration_pct"]
        for row in allocation["by_warehouse"]:
            exact = row["adult_population"] * rate / 100.0
            distance = abs((exact % 1.0) - 0.5)
            self.assertGreater(
                distance, 1e-6,
                f"{row['wh']}: {exact} is {distance} away from a rounding tie; "
                f"the target would start alternating between exports",
            )

    def test_the_rate_comes_from_the_demand_seed_and_is_not_repeated(self):
        """Changing `channel_reference_pct` must change the base — and the 2.2 cannot be in
        customer_premises_seed, or there would be two places to change it."""
        content = open(
            os.path.join(self.seeds, "customer_premises_seed.csv"), encoding="utf-8"
        ).read()
        self.assertNotIn("2.2", content)

        before = self.build()["weights"]["customer_allocation"]["total_customers"]
        self._seed(
            "demand_profile_seed.csv",
            DEMAND_PROFILE_CSV.replace("channel_reference_pct,2.2", "channel_reference_pct,4.4"),
        )
        after = self.build()["weights"]["customer_allocation"]["total_customers"]
        self.assertAlmostEqual(after / before, 2.0, places=3)

    def test_pointer_to_somewhere_else_fails(self):
        self._seed(
            "customer_premises_seed.csv",
            CUSTOMER_PREMISES_CSV.replace(
                "demand_profile.channel_reference_pct", "algum_outro_seed.qualquer_coisa"
            ),
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("points to", str(caught.exception))

    def test_missing_premise_fails(self):
        self._seed(
            "customer_premises_seed.csv",
            "\n".join(
                line for line in CUSTOMER_PREMISES_CSV.splitlines()
                if not line.startswith("min_customer_age")
            ) + "\n",
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("min_customer_age", str(caught.exception))

    def test_missing_premises_seed_fails_with_an_actionable_message(self):
        os.remove(os.path.join(self.seeds, "customer_premises_seed.csv"))
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("customer_premises_seed.csv", str(caught.exception))


class AdultAgeTest(OltpReferenceTestCase):
    """The delivered distribution is the CUSTOMER BASE's, not the population's."""

    def test_the_distribution_starts_at_the_declared_minimum(self):
        ages = self.build()["ages"]
        self.assertEqual(ages["min_customer_age"], 18)
        self.assertEqual(min(int(row["age"]) for row in ages["rows"]), 18)
        self.assertEqual(max(int(row["age"]) for row in ages["rows"]), 100)

    def test_the_truncated_distribution_is_renormalized_to_sum_to_one(self):
        """Truncating without renormalizing would deliver a vector summing to ~0.82.

        That is the silent failure mode of this change: `rng.choices` with `cum_weights` does
        not complain about a vector that does not sum to 1 — it simply never draws the
        missing tail.
        """
        rows = self.build()["ages"]["rows"]
        by_province = {}
        for row in rows:
            by_province.setdefault(row["province_code"], 0.0)
            by_province[row["province_code"]] += float(row["proportion"])
        self.assertEqual(sorted(by_province), ["08", "46"])
        for province, total in by_province.items():
            self.assertAlmostEqual(total, 1.0, places=9, msg=f"province {province}")

    def test_the_adult_share_is_measured_before_truncation(self):
        """Measuring after the cut would return 100% in every province.

        It is a bug that would fail nothing on its own: 100% is a plausible number, and the
        effect would be to size the whole base by the TOTAL population as if it were adult.
        """
        shares = self.build()["ages"]["adult_share_by_province"]
        self.assertEqual([s["province_code"] for s in shares], ["08", "46"])
        for share in shares:
            self.assertAlmostEqual(share["adult_share"], 83 / 101, places=9)
            self.assertLess(share["adult_share"], 1.0)
            self.assertEqual(share["population_value"], 101 * 100.0)
            self.assertEqual(share["adult_population_value"], 83 * 100.0)

    def test_pyramid_already_truncated_at_the_source_fails(self):
        """If Silver only had adults, the share would come out 1.0 and the base would double
        in size without any total looking wrong."""
        self.connection.execute(
            "delete from silver_ine_population_series where "
            "cast(regexp_extract(age_label, '^(\\d+)', 1) as integer) < 18 "
            "and age_label not in ('Total', '85 y más años')"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("adult share outside", str(caught.exception))


class AddressTest(OltpReferenceTestCase):
    def test_the_municipality_name_resolves_even_when_the_tramo_hangs_off_a_nucleus(self):
        """The Valencia bug: the join by unit_code would return 0 rows for the municipality."""
        rows = self.build()["candidates"]["rows"]
        valencia = [r for r in rows if r["municipality_code"] == "250"]
        self.assertEqual(len(valencia), 3, "Valencia's tramos vanished in the join")
        for row in valencia:
            self.assertEqual(row["municipality_name_callejero"], "VALÈNCIA")

    def test_the_earlier_ingestion_does_not_enter_the_candidates(self):
        """Silver stacks every extraction; the export reads only the latest.

        The fixture has the SAME Torrent tramo in two ingestion_date values. Without the
        is_latest_ingestion filter it would appear twice, and every address in that
        municipality would have twice the chance of being drawn — silent bias, not a visible
        error.
        """
        rows = self.build()["candidates"]["rows"]
        repeated = [r for r in rows if r["postal_code"] == "46900"]
        self.assertEqual(len(repeated), 1, "the earlier extraction's tramo came in too")

    def test_the_orphan_tramo_is_excluded_and_counted(self):
        candidates = self.build()["candidates"]
        self.assertEqual(candidates["excluded_rows"]["no_street_or_pseudo_match"], 1)
        self.assertNotIn("46004", [r["postal_code"] for r in candidates["rows"]])

    def test_pseudo_address_enters_with_the_pseudo_address_name(self):
        rows = self.build()["candidates"]["rows"]
        pseudo = [r for r in rows if r["is_pseudo_address"]]
        self.assertEqual(len(pseudo), 1)
        self.assertEqual(pseudo[0]["street_name"], "PARC DE CAPCALERA")
        self.assertEqual(pseudo[0]["numbering_type"], "0")

    def test_only_service_area_municipalities_enter(self):
        rows = self.build()["candidates"]["rows"]
        self.assertEqual(
            {(r["wh"], r["municipality_code"]) for r in rows},
            {("wh1", "244"), ("wh1", "250"), ("wh2", "019")},
        )

    def test_numbers_come_out_as_integers(self):
        row = self.build()["candidates"]["rows"][0]
        self.assertIsInstance(row["number_from"], int)
        self.assertIsInstance(row["number_to"], int)

    def test_the_row_order_is_stable_across_runs(self):
        """The row position is the customer's candidate_index: if the order varies, the
        lineage points to another tramo."""
        first = [r["postal_code"] for r in self.build()["candidates"]["rows"]]
        second = [r["postal_code"] for r in self.build()["candidates"]["rows"]]
        self.assertEqual(first, second)

    def test_municipality_without_aggregate_row_fails(self):
        self.connection.execute(
            "delete from silver_callejero_population_units where is_municipality_aggregate"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("without an aggregate row", str(caught.exception))


class PopulationTest(OltpReferenceTestCase):
    def rows(self):
        return self.build()["weights"]["rows"]

    def test_one_row_per_municipality(self):
        torrent = [r for r in self.rows() if r["municipality_code"] == "244"]
        self.assertEqual(len(torrent), 1)
        self.assertEqual(torrent[0]["population_total"], 90928)

    def test_duplicated_series_fails_instead_of_choosing_a_value(self):
        """One series per municipality is a guarantee of the Silver model; here it is only
        re-checked.

        If it breaks, choosing a value on our own would silently bias the warehouse weight —
        this export prefers to refuse and point at the model.
        """
        self._inject_homonym()
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        message = str(caught.exception)
        self.assertIn("more than one population series", message)
        self.assertIn("Torrent", message)

    def test_the_proportion_uses_the_right_municipality_population(self):
        wh1 = [r for r in self.rows() if r["wh"] == "wh1"]
        self.assertEqual(len(wh1), 2)
        total = 90928 + 800215
        torrent = next(r for r in wh1 if r["municipality_code"] == "244")
        self.assertAlmostEqual(torrent["census_proportion_within_wh"], 90928 / total, places=12)
        # Torrent (90,928) falls in INE's 50k-100k band (35.2%), València in >100k (39.5%).
        online = 90928 * 35.2 / (90928 * 35.2 + 800215 * 39.5)
        self.assertAlmostEqual(torrent["proportion_within_wh"], online, places=12)

    def test_proportions_sum_to_one_per_warehouse(self):
        for wh in ("wh1", "wh2"):
            total = sum(r["proportion_within_wh"] for r in self.rows() if r["wh"] == wh)
            self.assertAlmostEqual(total, 1.0, places=12)

    def test_sex_proportion_sums_to_one(self):
        for row in self.rows():
            self.assertAlmostEqual(
                row["sex_hombres_proportion"] + row["sex_mujeres_proportion"], 1.0, places=12
            )

    def test_province_name_comes_from_the_seed_with_the_exact_spelling(self):
        wh1 = next(r for r in self.rows() if r["wh"] == "wh1")
        self.assertEqual(wh1["province_name"], "Valencia/València")


class AgeTest(OltpReferenceTestCase):
    def payload(self):
        return self.build()["ages"]

    def test_the_overlapping_aggregates_are_excluded(self):
        """Without this the pyramid comes out 2.03x inflated, and no Source test would catch it.

        83 and not 101 since the file started delivering the CUSTOMER BASE distribution: ages
        18 to 100 are what is left of the 101 single ages after the min_customer_age cut. The
        two aggregate labels stay out for the usual reason, which is a different one — they
        OVERLAP the single ages, they are not a slice of them.
        """
        rows = self.payload()["rows"]
        by_province = [r for r in rows if r["province_code"] == "46"]
        self.assertEqual(len(by_province), 83, "labels other than single ages came in")
        self.assertEqual(max(r["age"] for r in by_province), 100)

    def test_the_terminal_100_year_bucket_remains(self):
        """'100 y mas anos' is a legitimate single age; only 'Total' and '85 y mas anos' go.

        The minimum-age cut cannot become an excuse to lose the tail: a range bug that also
        cut the top would go unnoticed in a test that only looks at the floor, and the
        customer base would be left without centenarians with nobody noticing.
        """
        ages = {r["age"] for r in self.payload()["rows"] if r["province_code"] == "46"}
        self.assertIn(100, ages)
        self.assertEqual(ages, set(range(18, 101)))

    def test_proportions_sum_to_one_per_province(self):
        rows = self.payload()["rows"]
        for province in ("46", "08"):
            total = sum(r["proportion"] for r in rows if r["province_code"] == province)
            self.assertAlmostEqual(total, 1.0, places=12)

    def test_duplicated_ingestion_date_does_not_duplicate_the_rows(self):
        rows = self.payload()["rows"]
        keys = [(r["province_code"], r["age"]) for r in rows]
        self.assertEqual(len(keys), len(set(keys)))

    def test_declares_the_pinned_period_and_the_nature_of_the_data(self):
        payload = self.payload()
        self.assertEqual(payload["year"], 2022)
        self.assertEqual(payload["fk_periodo"], 27)
        self.assertEqual(payload["reference_date"], "2022-06-30")
        self.assertEqual(payload["fk_tipo_dato"], 2)
        self.assertEqual(payload["excluded_age_labels"], ["Total", "85 y más años"])

    def test_national_total_does_not_enter(self):
        provinces = {r["province_code"] for r in self.payload()["rows"]}
        self.assertEqual(provinces, {"46", "08"})


class CoverageTest(OltpReferenceTestCase):
    def test_municipality_with_weight_but_no_address_fails(self):
        """Coverage is checked at run time, never presumed permanent."""
        self.connection.execute(
            "delete from silver_callejero_tramos where municipality_code = '244'"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("no address candidate", str(caught.exception))

    def test_province_without_age_distribution_fails(self):
        self.connection.execute(
            "delete from silver_ine_population_series where province_name = 'Barcelona'"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("without an age distribution", str(caught.exception))

    def test_missing_seed_fails_with_an_actionable_message(self):
        os.unlink(os.path.join(self.seeds, "warehouse_service_area_seed.csv"))
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("--seeds-dir", str(caught.exception))


class WriteTest(OltpReferenceTestCase):
    def test_writes_the_three_files_and_rereads_them_as_json(self):
        out = os.path.join(self.tmp.name, "out")
        sizes = write(self.build(), out)
        self.assertEqual(len(sizes), 3)
        for name in sizes:
            with open(os.path.join(out, name), encoding="utf-8") as handle:
                self.assertIn("rows", json.load(handle))

    def test_rewrite_produces_a_byte_identical_file(self):
        """Same data, same output: that is what the queries' `ORDER BY` guarantees."""
        out = os.path.join(self.tmp.name, "out")
        write(self.build(), out)
        path = os.path.join(out, "address_candidates.json")
        with open(path, "rb") as handle:
            first = handle.read()
        payloads = self.build()
        # generated_at_utc changes; we compare the content that describes the Lakehouse.
        payloads["candidates"]["generated_at_utc"] = json.loads(
            first.decode("utf-8")
        )["generated_at_utc"]
        write(payloads, out)
        with open(path, "rb") as handle:
            self.assertEqual(first, handle.read())




class OnlinePropensityTest(OltpReferenceTestCase):
    """CR-017: the INE TIC-H rate reshapes WHO the customers are, never HOW MANY."""

    def test_an_age_weighs_by_its_census_share_times_its_ine_rate(self):
        ages = [r for r in self.build()["ages"]["rows"] if r["province_code"] == "46"]
        by_age = {int(r["age"]): r for r in ages}
        # The fixture pyramid is flat, so the ratio is the ratio of the INE rates.
        self.assertAlmostEqual(by_age[30]["proportion"] / by_age[70]["proportion"],
                               52.4 / 12.3, places=9)
        self.assertAlmostEqual(sum(r["proportion"] for r in ages), 1.0, places=12)

    def test_75_and_over_take_the_oldest_band_the_ine_measures(self):
        ages = [r for r in self.build()["ages"]["rows"] if r["province_code"] == "46"]
        by_age = {int(r["age"]): r for r in ages}
        self.assertEqual(by_age[90]["online_pct"], 12.3)

    def test_the_warehouse_totals_do_not_move(self):
        """The MAPA channel closure reads these totals; the propensity must not reach them."""
        payloads = self.build()
        allocation = payloads["weights"]["customer_allocation"]["by_warehouse"]
        census = {r["wh"]: 0.0 for r in payloads["weights"]["rows"]}
        for row in payloads["weights"]["rows"]:
            census[row["wh"]] += row["population_total"]
        for entry in allocation:
            self.assertEqual(entry["population_total"], census[entry["wh"]])

    def test_the_header_names_the_source(self):
        payloads = self.build()
        for name in ("weights", "ages"):
            self.assertIn("50116", payloads[name]["online_propensity"]["source"])


if __name__ == "__main__":
    unittest.main()
