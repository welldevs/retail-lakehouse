"""The Orders export's four queries, against real DuckDB fixtures — no network, no MinIO.

Each case reproduces a MEASURED constraint, and exists so it is caught here and not later:

  * the day without a catalog snapshot for that warehouse — 27 (warehouse, day) pairs in the
    real scope when the window opens on 2026-08-15. It must REFUSE, never invent a price;
  * the day with an earlier snapshot but none on the day itself — becomes an EXPLICIT
    `carried_forward`, which is rule 4 of the five the FAQ had written before Orders existed;
  * the product that appears in more than one category (4,581 rows for 4,311 products in
    mad1/2026-08-24): the grain dedup is mandatory, otherwise the basket would draw the same
    product twice with different weights;
  * the customer that appears with two warehouses: it would break "a customer of W only
    orders from W" without any downstream test noticing, because the order would be
    consistent with one of the two rows;
  * a premise labelled with anything other than `synthetic`: no source in this repo measures
    basket, cadence or availability, so `observed` or `proxy` there would be a false promise;
  * a catalog smaller than the largest possible basket: the generator draws WITHOUT
    REPLACEMENT, and the defect would show up as a smaller order, never as an error.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.orders_reference import (  # noqa: E402
    OrdersReferenceError,
    _date_range,
    build,
    write,
)

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


PROVINCE_MAP_CSV = """wh,province_code,province_name,municipio_code,municipio_name
mad1,28,Madrid,079,Madrid
bcn1,08,Barcelona,019,Barcelona
"""

PREMISES_CSV_HEADER = "premise_key,value,unit,label,rationale\n"

# Only what the export REQUIRES. The real list has 30 keys; the mandatory set is enough here.
from retail_platform.orders_reference import REQUIRED_PREMISES  # noqa: E402

PREMISES_ROWS = {
    "basket_lines_max": "5",
    "basket_lines_min": "2",
    "basket_lines_mode": "3",
    "min_buyer_age": "18",
}


def premises_csv(extra: dict | None = None, label: str = "synthetic") -> str:
    values = {key: "1" for key in REQUIRED_PREMISES}
    values.update(PREMISES_ROWS)
    values.update(extra or {})
    lines = [PREMISES_CSV_HEADER]
    for key, value in values.items():
        lines.append(f"{key},{value},unidade,{label},motivo declarado\n")
    return "".join(lines)


def demand_seeds() -> dict:
    """The eight calibration seeds, minimal, covering the fixture's two level-1 categories."""
    return {
        "mapa_2025_benchmark_seed.csv": (
            "mapa_key,mapa_label,scope,use_as_weight,volume_share_pct,value_share_pct,"
            "avg_price_eur_kg,volume_yoy_pct,value_yoy_pct,ecommerce_volume_pct,"
            "channel_basis,informe_section,provenance,note\n"
            "FRUTAS_FRESCAS,Frutas frescas,fresh,true,14.13,9.94,2.28,2.7,9.8,,coarse,4.9,informe_table,\n"
            "SIN_BENCHMARK,Sem benchmark,rest,false,,,,,,,coarse,,none,destino declarado\n"
            "NO_FOOD,Nao alimentar,none,false,,,,,,,coarse,,none,fora do universo\n"
        ),
        "demand_category_mapping_seed.csv": (
            "l1,l2,l3,mapa_key,rationale\n"
            f"{L1_FOOD},Fruta,*,FRUTAS_FRESCAS,fixture\n"
            f"{L1_NON_FOOD},*,*,NO_FOOD,fixture\n"
        ),
        "demand_profile_seed.csv": (
            "param_key,value,unit,label,rationale\n"
            "demand_model_version,fixture_v1,version,synthetic,x\n"
            "food_line_share,0.85,proportion,synthetic,x\n"
            "benchmark_volume_coverage_pct,100.00,percent,derived,x\n"
            "unbenchmarked_allocation,assortment,rule,synthetic,x\n"
            "within_group_selection,uniform,rule,synthetic,x\n"
            "volume_coverage_min,0.5,proportion,synthetic,x\n"
            "channel_reference_pct,2.2,percent,observed,x\n"
            "channel_fresh_pct,1.1,percent,observed,x\n"
            "channel_rest_pct,2.8,percent,observed,x\n"
            "cohort_dimensions,\"age,region\",rule,synthetic,x\n"
            "cohort_independence,multiplicative,rule,synthetic,x\n"
            "cohort_calibration,ipf,rule,synthetic,x\n"
            "ipf_tolerance,0.000000001,proportion,synthetic,x\n"
            "ipf_max_iterations,200,iterations,synthetic,x\n"
            "region_frequency_basis,per_capita_volume,rule,synthetic,x\n"
            "region_frequency_normalization,served_regions,rule,synthetic,x\n"
        ),
        "demand_seasonality_seed.csv": (
            "month,factor,label,rationale\n"
            + "".join(f"{m},1.0,synthetic,x\n" for m in range(1, 13))
        ),
        # Unequal shares across the bands: with a flat index the per-cohort profile would be
        # indistinguishable from the aggregate and the fixture would not exercise IPF.
        "demand_cohort_age_seed.csv": (
            "mapa_key,age_band,population_share_pct,volume_share_pct,informe_page,"
            "provenance,note\n"
            "FRUTAS_FRESCAS,LT35,8.89,3.59,222,informe_chart,\n"
            "FRUTAS_FRESCAS,35_49,30.33,20.91,222,informe_chart,\n"
            "FRUTAS_FRESCAS,50_64,31.34,33.23,222,informe_chart,\n"
            "FRUTAS_FRESCAS,GE65,29.44,42.27,222,informe_chart,\n"
        ),
        "demand_cohort_region_seed.csv": (
            "mapa_key,ccaa_code,ccaa_label,population_share_pct,volume_share_pct,"
            "informe_page,provenance,note\n"
            "FRUTAS_FRESCAS,13,Comunidad de Madrid,13.86,13.36,224,informe_chart,\n"
            "FRUTAS_FRESCAS,09,Cataluna,16.26,16.05,224,informe_chart,\n"
        ),
        "mapa_2025_region_seed.csv": (
            "ccaa_code,ccaa_label,per_capita_kg_l,per_capita_eur,informe_section,"
            "provenance,note\n"
            "00,Total Espana,577.32,1874.75,3,informe_prose,\n"
            "13,Comunidad de Madrid,505.86,1754.95,3,informe_prose,\n"
            "09,Cataluna,620.82,2130.97,3,informe_prose,\n"
        ),
        "ine_ccaa_map_seed.csv": (
            "province_code,province_name,ccaa_code,ccaa_label\n"
            "28,Madrid,13,Comunidad de Madrid\n"
            "08,Barcelona,09,Cataluna\n"
        ),
    }


SCHEMA = """
create table silver_product_price (
    ingestion_date date, warehouse varchar, source_product_id varchar, display_name varchar,
    category_id bigint, category_name varchar, subgroup_id bigint, subgroup_name varchar,
    product_level1_category_name varchar,
    unit_price decimal(10,2), purchasable_unit_price decimal(10,2), price_basis varchar,
    net_content_kg_l decimal(12,4), tax_percentage decimal(6,3)
);
create table silver_customer (
    ingestion_date date, customer_id varchar, wh varchar, province_code varchar,
    municipality_code varchar, postal_code varchar, birth_year integer
);
"""


# Two level-1 categories on purpose: one food and one non-food. With only one, the demand
# profile would have an empty block and the fixture would exercise the redistribution path
# instead of the normal path.
L1_FOOD = "Fruta y verdura"
L1_NON_FOOD = "Limpieza y hogar"


def catalog(wh: str, day: str, total: int = 8, first: int = 0) -> list[tuple]:
    rows = []
    for i in range(total):
        food = i % 2 == 0
        l1 = L1_FOOD if food else L1_NON_FOOD
        l2 = "Fruta" if food else "Limpieza cocina"
        price = 1.00 + i
        rows.append(
            (day, wh, f"p{first + i:04d}", f"Produto {i}", 10 + i % 2, l2, 100 + i % 3,
             "Sub", l1, price, price, "unit", 0.5 + i * 0.1, 21.0)
        )
    return rows


class Fixture:
    """In-memory DuckDB base with seeds on disk, following the test_customers_reference pattern."""

    def __init__(self, tmpdir: str):
        self.tmpdir = tmpdir
        self.seeds = os.path.join(tmpdir, "seeds")
        os.makedirs(self.seeds, exist_ok=True)
        self.write_seed("warehouse_province_map_seed.csv", PROVINCE_MAP_CSV)
        self.write_seed("order_premises_seed.csv", premises_csv())
        for name, content in demand_seeds().items():
            self.write_seed(name, content)
        self.con = duckdb.connect()
        self.con.execute(SCHEMA)

    def write_seed(self, name: str, content: str) -> None:
        with open(os.path.join(self.seeds, name), "w", encoding="utf-8") as handle:
            handle.write(content)

    def add_catalog(self, rows) -> None:
        self.con.executemany(
            "insert into silver_product_price values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows
        )

    # One age per band: even with 4 customers per warehouse, none of the four cohorts of
    # that community is empty, and IPF has mass in all of them.
    AGES = (25, 42, 57, 71)

    def add_customers(self, wh: str, day: str, total: int = 4, ages=None) -> None:
        ages = tuple(ages or self.AGES)
        province = "28" if wh == "mad1" else "08"
        year = int(day[:4])
        self.con.executemany(
            "insert into silver_customer values (?,?,?,?,?,?,?)",
            [
                (day, f"cust_{wh}_{i:06d}", wh, province, "079", "28001",
                 year - ages[i % len(ages)])
                for i in range(total)
            ],
        )

    def populated(self, days=("2026-08-24",)) -> "Fixture":
        for wh in ("mad1", "bcn1"):
            for day in days:
                self.add_catalog(catalog(wh, day))
            self.add_customers(wh, days[0])
        return self


@unittest.skipIf(duckdb is None, "duckdb not installed")
class WindowTest(unittest.TestCase):
    def test_range_is_inclusive_at_both_ends(self):
        self.assertEqual(
            _date_range("2026-08-24", "2026-08-27"),
            ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27"],
        )

    def test_single_day(self):
        self.assertEqual(_date_range("2026-08-24", "2026-08-24"), ["2026-08-24"])

    def test_inverted_window_is_refused(self):
        with self.assertRaises(OrdersReferenceError) as caught:
            _date_range("2026-08-27", "2026-08-24")
        self.assertIn("inverted", str(caught.exception))

    def test_malformed_date_is_refused(self):
        with self.assertRaises(OrdersReferenceError):
            _date_range("24/08/2026", "2026-08-27")


@unittest.skipIf(duckdb is None, "duckdb not installed")
class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fx = Fixture(self.tmp.name)
        self.addCleanup(self.fx.con.close)


class PriceCalendarTest(Base):
    def test_day_with_its_own_snapshot_is_observed(self):
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        rows = payloads["calendar"]["rows"]
        self.assertEqual(len(rows), 2)  # 2 warehouses x 1 day
        for row in rows:
            self.assertEqual(row["price_source"], "observed")
            self.assertEqual(row["price_as_of"], "2026-08-24")

    def test_day_without_snapshot_carries_the_last_known_price(self):
        # Rule 4 of the FAQ's five: a retailer sells every day, the source was observed only
        # on some. EXPLICIT carry-forward, never dissolved into the number.
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-26", seeds_dir=self.fx.seeds)
        by_day = {(r["wh"], r["order_date"]): r for r in payloads["calendar"]["rows"]}
        self.assertEqual(by_day[("mad1", "2026-08-24")]["price_source"], "observed")
        for day in ("2026-08-25", "2026-08-26"):
            row = by_day[("mad1", day)]
            self.assertEqual(row["price_source"], "carried_forward")
            self.assertEqual(row["price_as_of"], "2026-08-24")
        self.assertEqual(payloads["calendar"]["carried_forward_rows"], 4)

    def test_day_before_every_snapshot_is_refused(self):
        # The measured case: 27 (warehouse, day) pairs when the real window opens on 08-15.
        # An order on that day would have to invent a price, so the export refuses.
        self.fx.populated()
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-20", "2026-08-24", seeds_dir=self.fx.seeds)
        message = str(caught.exception)
        self.assertIn("without ANY catalog snapshot", message)
        self.assertIn("invent a price", message)

    def test_one_warehouse_with_catalog_and_another_without_is_refused(self):
        # Real asymmetry: mad1 has a catalog from 08-15, the other three only from 08-24.
        self.fx.add_catalog(catalog("mad1", "2026-08-24"))
        self.fx.add_customers("mad1", "2026-08-24")
        self.fx.add_customers("bcn1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)


class CatalogTest(Base):
    def test_grain_dedup_one_row_per_product(self):
        # The same product in two categories is source semantics. Without dedup the basket
        # would draw the same product twice, with different weights.
        self.fx.populated()
        self.fx.add_catalog([
            ("2026-08-24", "mad1", "p0000", "Produto 0", 99, "Fruta", 999, "Sub",
             L1_FOOD, 1.00, 1.00, "unit", 0.5, 21.0)
        ])
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        mad1_rows = [r for r in payloads["catalog"]["rows"] if r["wh"] == "mad1"]
        ids = [r["source_product_id"] for r in mad1_rows]
        self.assertEqual(len(ids), len(set(ids)))
        chosen = next(r for r in mad1_rows if r["source_product_id"] == "p0000")
        self.assertEqual(chosen["category_id"], 10, "the tie-break is the lowest category")

    def test_price_travels_as_a_string(self):
        # Obligation 4.4 of the Mercadona contract: float loses decimal precision in currency.
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        for row in payloads["catalog"]["rows"][:5]:
            self.assertIsInstance(row["unit_price"], str)

    def test_catalog_smaller_than_the_largest_basket_is_refused(self):
        # The generator draws WITHOUT REPLACEMENT: the defect would show up as a smaller
        # order, not an error.
        self.fx.add_catalog(catalog("mad1", "2026-08-24", total=3))
        self.fx.add_catalog(catalog("bcn1", "2026-08-24", total=3))
        self.fx.add_customers("mad1", "2026-08-24")
        self.fx.add_customers("bcn1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("basket_lines_max", str(caught.exception))


class CustomersTest(Base):
    def test_one_row_per_customer_with_the_first_generation(self):
        self.fx.populated()
        self.fx.add_customers("mad1", "2026-08-27")  # second generation of the same ids
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        customers = payloads["customers"]
        ids = [r["customer_id"] for r in customers["rows"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(customers["customer_ingestion_dates"], ["2026-08-24", "2026-08-27"])
        mad1_row = next(r for r in customers["rows"] if r["wh"] == "mad1")
        self.assertEqual(mad1_row["first_ingestion_date"], "2026-08-24")

    def test_customer_in_two_warehouses_is_refused(self):
        # It would break "a customer of W only orders from W" without anything downstream
        # noticing: the order would be consistent with one of the two rows.
        self.fx.populated()
        self.fx.con.execute(
            "insert into silver_customer values "
            "(date '2026-08-24','cust_mad1_000000','bcn1','08','019','08001',1990)"
        )
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("more than one warehouse", str(caught.exception))

    def test_warehouse_without_customers_is_refused(self):
        self.fx.add_catalog(catalog("mad1", "2026-08-24"))
        self.fx.add_catalog(catalog("bcn1", "2026-08-24"))
        self.fx.add_customers("mad1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("without any customer", str(caught.exception))

    def test_empty_silver_customer_is_refused(self):
        self.fx.add_catalog(catalog("mad1", "2026-08-24"))
        self.fx.add_catalog(catalog("bcn1", "2026-08-24"))
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)


class PremisesTest(Base):
    def test_digest_follows_the_table(self):
        self.fx.populated()
        first = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.fx.write_seed("order_premises_seed.csv", premises_csv({"quantity_max": "9"}))
        second = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertNotEqual(
            first["premises"]["seed_sha256"], second["premises"]["seed_sha256"],
            "changing a rate must change the digest, otherwise the change leaves no trace",
        )

    def test_label_other_than_synthetic_is_refused(self):
        # No source in this repo measures basket, cadence or availability. Calling any of
        # these `observed` or `proxy` would promise data that does not exist.
        self.fx.populated()
        for label in ("observed", "proxy", ""):
            self.fx.write_seed("order_premises_seed.csv", premises_csv(label=label))
            with self.assertRaises(OrdersReferenceError) as caught:
                build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
            self.assertIn("synthetic", str(caught.exception))

    def test_missing_mandatory_premise_is_refused(self):
        self.fx.populated()
        lines = premises_csv().splitlines(keepends=True)
        without_rate = [l for l in lines if not l.startswith("substitution_rate_fresh,")]
        self.fx.write_seed("order_premises_seed.csv", "".join(without_rate))
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("substitution_rate_fresh", str(caught.exception))

    def test_non_numeric_value_is_refused(self):
        self.fx.populated()
        self.fx.write_seed("order_premises_seed.csv", premises_csv({"quantity_max": "muitos"}))
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)

    def test_duplicated_key_is_refused(self):
        self.fx.populated()
        self.fx.write_seed(
            "order_premises_seed.csv",
            premises_csv() + "quantity_max,7,unidade,synthetic,duplicada\n",
        )
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("duplicated", str(caught.exception))

    def test_missing_seed_is_refused_with_an_actionable_message(self):
        self.fx.populated()
        os.unlink(os.path.join(self.fx.seeds, "order_premises_seed.csv"))
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("--seeds-dir", str(caught.exception))


class WriteTest(Base):
    def test_writes_the_five_files_and_returns_their_size(self):
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        target = os.path.join(self.tmp.name, "ref")
        sizes = write(payloads, target)
        self.assertEqual(
            sorted(sizes),
            ["calendar.json", "catalog.json", "customers.json", "demand_profile.json",
             "premises.json"],
        )
        for name, size in sizes.items():
            path = os.path.join(target, name)
            self.assertTrue(os.path.exists(path), name)
            self.assertEqual(os.path.getsize(path), size)
            with open(path, encoding="utf-8") as handle:
                json.load(handle)

    def test_leaves_no_temporary_file_behind(self):
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        target = os.path.join(self.tmp.name, "ref")
        write(payloads, target)
        self.assertEqual([n for n in os.listdir(target) if n.startswith(".tmp-")], [])

    def test_rewriting_is_idempotent(self):
        self.fx.populated()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        target = os.path.join(self.tmp.name, "ref")
        first = write(payloads, target)
        second = write(payloads, target)
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
