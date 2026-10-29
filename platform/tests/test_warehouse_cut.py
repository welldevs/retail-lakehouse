"""The cut for the warehouse, against real DuckDB fixtures — no network, no credential.

The TRANSPORT is not what goes wrong silently. What goes wrong silently is the cut: a source
aggregate summed together with the detail, a forgotten scope filter, a deduplication that
picks arbitrarily between values that diverge. None of those fails — they all produce a
plausible number. That is why the suite lives here, and not in the PUT.

Each fixture reproduces a case MEASURED in the real Lakehouse:

  * `sex_label = 'Total'` living alongside Hombres/Mujeres in the same column — summing the
    three gives twice the population (the same trap that in Phase 1 inflated the age
    pyramid 2.03x with `age_label`);
  * the same product repeated because it appears in two categories, with the SAME price
    across appearances — the reduction is safe, but the category count must not vanish;
  * the same product with a DIFFERENT price across warehouses (187 real cases) — the `wh`
    axis cannot leave the grain;
  * a postal code that crosses a municipality border (29 real cases) — (municipality,
    postal code) is the key, not the postal code;
  * a municipality OUTSIDE the AUF, which must not cross the cut;
  * an old ingestion_date stacked under the current one, which would double the counts.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.warehouse_cut import (  # noqa: E402
    SPECS,
    WarehouseCutError,
    build,
    ddl_for,
    dump_ddl,
    _snowflake_type,
)

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


SCHEMA = """
create table silver_product_price (
    ingestion_date date, warehouse varchar, category_id bigint, category_name varchar,
    subgroup_id varchar, subgroup_name varchar, source_product_id varchar,
    display_name varchar, product_level1_category_id varchar,
    product_level1_category_name varchar,
    unit_price decimal(10,2), purchasable_unit_price decimal(10,2), price_basis varchar,
    net_content_kg_l decimal(12,4), min_bunch_amount double,
    bulk_price decimal(10,2), reference_price decimal(12,3),
    reference_format varchar, previous_unit_price decimal(10,2), price_decreased boolean,
    tax_percentage decimal(6,3), is_pack boolean, pack_size bigint, unit_size double,
    size_format varchar, unit_name varchar, total_units bigint, selling_method bigint,
    approx_size boolean, packaging varchar, thumbnail varchar, share_url varchar,
    slug varchar, published boolean, is_new_arrival boolean
);
create table silver_price_change (
    warehouse varchar, ingestion_date date, previous_ingestion_date date,
    source_product_id varchar, display_name varchar,
    previous_unit_price decimal(10,2), unit_price decimal(10,2),
    price_delta decimal(10,2), catalog_appearances bigint,
    change_type varchar, name_seen_before boolean
);
create table silver_category (
    ingestion_date date, warehouse varchar, category_id bigint, category_name varchar,
    category_order bigint, parent_category_id bigint, parent_category_name varchar,
    parent_category_order bigint
);
create table silver_ine_population_by_municipality (
    ingestion_date date, is_latest_ingestion boolean, series_code varchar,
    series_name varchar, province_code varchar, municipality_code varchar,
    municipality_name varchar, sex_label varchar, year bigint, fk_periodo bigint,
    reference_date date, is_secret boolean, population_value double
);
create table silver_callejero_tramos (
    ingestion_date date, is_latest_ingestion boolean, section_code varchar,
    province_code varchar, municipality_code varchar, street_id varchar,
    street_code varchar, postal_code varchar
);
create table silver_callejero_population_units (
    ingestion_date date, is_latest_ingestion boolean, province_code varchar,
    municipality_code varchar, is_municipality_aggregate boolean,
    municipality_name varchar
);
create table warehouse_service_area (
    wh varchar, province_code varchar, municipality_code varchar,
    municipality_name varchar, is_home_municipality boolean
);
create table warehouse_province_map (
    wh varchar, province_code varchar, province_name varchar,
    municipio_code varchar, municipio_name varchar
);
create table silver_customer (
    ingestion_date date, is_latest_ingestion boolean, wh varchar, partition_wh varchar,
    customer_id varchar, province_code varchar, province_name varchar,
    municipality_code varchar, municipality_name varchar, candidate_index bigint,
    street_name varchar, postal_code varchar, numbering_type varchar,
    house_number bigint, first_name varchar, last_name varchar, sex_label varchar,
    birth_year bigint, age_at_ingestion bigint
);
create table raw_manifest (
    ingestion_date date, warehouse varchar, run_id varchar, complete boolean,
    source_name varchar, declared_product_rows bigint,
    failure_count bigint, anomaly_count bigint,
    started_at_utc timestamp, finished_at_utc timestamp, duration_seconds double
);
create table silver_oltp_manifest (
    ingestion_date date, wh varchar, run_id varchar, complete boolean,
    source_name varchar, declared_customer_rows bigint,
    failure_count bigint, anomaly_count bigint,
    started_at_utc timestamp, finished_at_utc timestamp, duration_seconds double
);
create table silver_orders_manifest (
    ingestion_date date, wh varchar, run_id varchar, complete boolean,
    source_name varchar, declared_event_rows bigint, declared_order_rows bigint,
    failure_count bigint, anomaly_count bigint,
    started_at_utc timestamp, finished_at_utc timestamp, duration_seconds double
);
create table silver_order (
    ingestion_date date, wh varchar, order_id varchar, customer_id varchar,
    customer_ingestion_date date, province_code varchar, municipality_code varchar,
    postal_code varchar, price_as_of date, price_source varchar,
    order_status varchar, last_event_type varchar, is_terminal boolean,
    placed_at timestamp, picked_at timestamp, delivered_at timestamp,
    line_count_placed integer, substituted_lines integer, removed_lines integer,
    gross_amount_placed decimal(12,2), net_amount decimal(12,2)
);
create table silver_order_line (
    ingestion_date date, wh varchar, order_id varchar, line_no integer,
    price_as_of date, source_product_id varchar, category_id integer,
    quantity integer, unit_price decimal(10,2), line_status varchar,
    fulfilled_source_product_id varchar, fulfilled_unit_price decimal(10,2),
    line_amount decimal(12,2), line_amount_placed decimal(12,2)
);
create table silver_order_event (
    ingestion_date date, wh varchar, partition_wh varchar, event_id varchar,
    event_type varchar, event_version integer, order_id varchar, sequence_no integer,
    producer varchar, occurred_at timestamp, event_date date,
    crosses_order_date boolean, payload json
);
create table order_premises (
    premise_key varchar, value decimal(18,6), unit varchar, label varchar,
    rationale varchar
);
create table stock_premises (
    premise_key varchar, value decimal(18,6), unit varchar, label varchar,
    rationale varchar
);
create table silver_stock_ledger (
    wh varchar, source_product_id varchar, stock_date date,
    opening_balance bigint, units_received bigint, units_demanded bigint,
    units_fulfilled bigint, units_short bigint, closing_balance bigint,
    reorder_units bigint, reorder_eta date,
    mean_daily_demand decimal(12,4), days_of_cover decimal(12,4),
    written_by varchar
);
"""


def populate(con):
    """Minimal fixture, but with a real trap case in every table."""
    con.execute("""
        insert into warehouse_service_area values
            ('wh1','46','244','Torrent',false),
            ('wh1','46','250','Valencia',true),
            ('wh2','08','019','Barcelona',true)
    """)
    con.execute("""
        insert into warehouse_province_map values
            ('wh1','46','Valencia/Valencia','250','Valencia'),
            ('wh2','08','Barcelona','019','Barcelona')
    """)

    # PRODUCT: p1 appears in TWO categories (10 and 20) with the SAME price — the reduction
    # is safe, but category_appearances must keep saying "2". And p1 costs differently in
    # wh1 and wh2, the case of the 187 real products.
    for wh, price in (("wh1", "1.50"), ("wh2", "1.75")):
        for cat in (10, 20):
            con.execute(f"""
                insert into silver_product_price (ingestion_date, warehouse, category_id,
                    subgroup_id, source_product_id, display_name, product_level1_category_id,
                    unit_price, purchasable_unit_price, price_basis, net_content_kg_l,
                    min_bunch_amount, bulk_price, reference_price, previous_unit_price,
                    tax_percentage, is_pack, pack_size, unit_size, total_units,
                    selling_method, published, is_new_arrival)
                values ('2026-08-26','{wh}',{cat},'s1','p1','Leche','1',
                        {price}, {price}, 'unit', 1.0, 1.0,
                        {price}, 1.500, null, 4.000, false, 1, 1.0, 1, 0, true, false)
            """)
    con.execute("""
        insert into silver_product_price (ingestion_date, warehouse, category_id,
            subgroup_id, source_product_id, display_name, product_level1_category_id,
            unit_price, purchasable_unit_price, price_basis, tax_percentage, published)
        values ('2026-08-26','wh1',10,'s1','p2','Pan','1', 0.90, 0.90, 'unit', 4.000, true)
    """)

    con.execute("""
        insert into silver_price_change values
            ('wh1','2026-08-26','2026-08-25','p1','Leche',1.40,1.50,0.10,2,'preco_alterado',false)
    """)

    # CATEGORY: the same tree repeated in both warehouses and on two dates. The cut must
    # return ONE row per category_id.
    for day in ("2026-08-25", "2026-08-26"):
        for wh in ("wh1", "wh2"):
            con.execute(f"""
                insert into silver_category values
                    ('{day}','{wh}',10,'Lacteos',1,100,'Frescos',1),
                    ('{day}','{wh}',20,'Panaderia',2,100,'Frescos',1)
            """)

    # POPULATION: 'Total' lives alongside Hombres/Mujeres — if it crosses, every sum doubles.
    # '999' is a municipality OUTSIDE the AUF. The old stacked date must be filtered out.
    for sex, value in (("Hombres", 100.0), ("Mujeres", 120.0), ("Total", 220.0)):
        con.execute(f"""
            insert into silver_ine_population_by_municipality values
                ('2026-08-26',true,'S1','n','46','244','Torrent','{sex}',2025,28,
                 '2024-12-31',false,{value}),
                ('2026-08-25',false,'S1','n','46','244','Torrent','{sex}',2025,28,
                 '2024-12-31',false,{value}),
                ('2026-08-26',true,'S9','n','46','999','OutsideAUF','{sex}',2025,28,
                 '2024-12-31',false,{value})
        """)

    # TRAMOS: postal code 46900 crosses municipalities 244 and 250 — the case of the 29 real
    # ones. Municipality 999 is outside the AUF. The old date must not enter the count.
    con.execute("""
        insert into silver_callejero_tramos values
            ('2026-08-25',true,'4624401001','46','244','00001','4624400001','46900'),
            ('2026-08-25',true,'4624401002','46','244','00002','4624400002','46900'),
            ('2026-08-25',true,'4625001001','46','250','00003','4625000003','46900'),
            ('2026-08-25',true,'4625001002','46','250','00000','4625000000','46001'),
            ('2026-08-25',true,'4699901001','46','999','00004','4699900004','46999'),
            ('2026-08-24',false,'4624401001','46','244','00001','4624400001','46900')
    """)
    con.execute("""
        insert into silver_callejero_population_units values
            ('2026-08-25',true,'46','244',true,'TORRENT'),
            ('2026-08-25',true,'46','250',true,'VALENCIA')
    """)

    con.execute("""
        insert into silver_customer values
            ('2026-08-27',true,'wh1','wh1','cust_wh1_000000','46','Valencia/Valencia',
             '244','Torrent',1,'RUA A','46900','1',3,'Eva','Gil','Mujeres',1987,39),
            ('2026-08-26',false,'wh1','wh1','cust_wh1_000000','46','Valencia/Valencia',
             '250','Valencia',2,'RUA B','46001','0',null,'Ana','Gil','Mujeres',1986,40)
    """)

    con.execute("""
        insert into raw_manifest values
            ('2026-08-26','wh1','r1',true,'mercadona_catalog',3,0,0,
             '2026-08-26 06:00:00','2026-08-26 06:00:05',5.0)
    """)
    con.execute("""
        insert into silver_oltp_manifest values
            ('2026-08-27','wh1','r2',true,'simulated_oltp',1,0,0,
             '2026-08-27 06:00:00','2026-08-27 06:00:01',0.8)
    """)
    # ORDERS declares EVENTS (7) and ORDERS (2). Mapping the wrong column would make the
    # reconciliation compare two different quantities and pass by mistake.
    con.execute("""
        insert into silver_orders_manifest values
            ('2026-08-27','wh1','r3',true,'simulated_orders',7,2,0,0,
             '2026-08-27 06:05:00','2026-08-27 06:05:02',1.9)
    """)

    # ORDERS on TWO ingestion_dates. Each one is an OPERATING DAY, not a re-ingestion: a
    # "latest ingestion" filter would leave half of the orders out of the warehouse.
    con.execute("""
        insert into silver_order values
            ('2026-08-26','wh1','ord_wh1_20260826_000001','cust_wh1_000000','2026-08-26',
             '46','244','46900','2026-08-26','observed','DELIVERED','order_delivered',
             false,'2026-08-26 08:00:00','2026-08-26 09:00:00','2026-08-26 11:00:00',
             2, 1, 0, 10.00, 11.50),
            ('2026-08-27','wh1','ord_wh1_20260827_000001','cust_wh1_000000','2026-08-27',
             '46','244','46900','2026-08-27','observed','CANCELLED','order_cancelled',
             true,'2026-08-27 08:00:00',null,null,
             1, 0, 0, 4.00, null)
    """)
    con.execute("""
        insert into silver_order_line values
            ('2026-08-26','wh1','ord_wh1_20260826_000001',1,'2026-08-26','p1',10,2,1.50,
             'substituted','p2',0.90,1.80,3.00),
            ('2026-08-26','wh1','ord_wh1_20260826_000001',2,'2026-08-26','p2',20,1,0.90,
             'fulfilled','p2',0.90,0.90,0.90),
            ('2026-08-27','wh1','ord_wh1_20260827_000001',1,'2026-08-27','p1',10,1,1.50,
             'not_picked',null,null,0.00,1.50)
    """)
    # `partition_wh` is a Silver AUDIT column — it must not cross the boundary.
    con.execute("""
        insert into silver_order_event values
            ('2026-08-26','wh1','wh1','ev1','order_placed',1,
             'ord_wh1_20260826_000001',1,'simulated_orders','2026-08-26 08:00:00',
             '2026-08-26',false,'{"line_count": 2}'),
            ('2026-08-26','wh1','wh1','ev2','order_delivered',1,
             'ord_wh1_20260826_000001',2,'simulated_orders','2026-08-27 11:00:00',
             '2026-08-27',true,'{"delivered_within_slot": true}')
    """)
    con.execute("""
        insert into order_premises values
            ('sla_minutes_picking',77.0,'minutes','synthetic','Alert threshold.'),
            ('basket_lines_max',40.0,'lines','synthetic','Largest possible basket.')
    """)
    con.execute("""
        insert into stock_premises values
            ('opening_days_of_demand',7.0,'days','synthetic','Initial cover.'),
            ('supplier_lead_days',2.0,'days','synthetic','Supplier lead time.')
    """)
    # THIS TABLE'S TRAP CASE is the STOCKOUT DAY: `units_short > 0` with
    # `closing_balance = 0`, which is the only legitimate combination. If the cut lost either
    # column, the parquet would still be valid and the mart would count a stockout without a
    # zeroed balance.
    con.execute("""
        insert into silver_stock_ledger values
            ('wh1','p1','2026-08-26',100,0,30,30,0,70,0,null,10.0,7.0,'spark'),
            ('wh1','p1','2026-08-27',70,0,80,70,10,0,120,'2026-08-29',10.0,0.0,'spark')
    """)


@unittest.skipIf(duckdb is None, "duckdb not installed")
class CutCase(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute(SCHEMA)
        populate(self.con)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.con.close)

    def rows(self, name):
        spec = next(s for s in SPECS if s["name"] == name)
        sql = spec["sql"].strip()
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


class PopulationTest(CutCase):
    def test_the_total_aggregate_never_crosses_the_cut(self):
        """The most expensive trap in the repo, now in another column: summing Hombres +
        Mujeres + Total gives TWICE the population. That is exactly how Phase 1's age
        pyramid came out 2.03x inflated, with age_label."""
        labels = {r["sex_label"] for r in self.rows("STG_POPULATION_MUNICIPALITY")}
        self.assertNotIn("Total", labels)
        self.assertEqual(labels, {"Hombres", "Mujeres"})

    def test_the_sum_of_the_detail_matches_the_source_total(self):
        source_total = self.con.execute(
            "select population_value from silver_ine_population_by_municipality "
            "where sex_label='Total' and municipality_code='244' and is_latest_ingestion"
        ).fetchone()[0]
        summed = sum(r["population_value"] for r in self.rows("STG_POPULATION_MUNICIPALITY")
                     if r["municipality_code"] == "244")
        self.assertEqual(summed, source_total)

    def test_municipality_outside_the_auf_does_not_cross(self):
        codes = {r["municipality_code"] for r in self.rows("STG_POPULATION_MUNICIPALITY")}
        self.assertNotIn("999", codes)

    def test_old_ingestion_does_not_double_the_rows(self):
        rows = self.rows("STG_POPULATION_MUNICIPALITY")
        self.assertEqual(len(rows), 2, "the old ingestion_date crossed the filter")


class ProductTest(CutCase):
    def test_the_warehouse_axis_stays_in_the_grain(self):
        """Measured on real data: 187 products have a different price across warehouses on
        the same day. A fact without `wh` would measure an average that exists nowhere."""
        prices = {r["warehouse"]: r["unit_price"] for r in self.rows("STG_PRODUCT_PRICE")
                  if r["source_product_id"] == "p1"}
        self.assertEqual({k: str(v) for k, v in prices.items()},
                         {"wh1": "1.50", "wh2": "1.75"})

    def test_the_per_category_repetition_is_reduced_to_one_row(self):
        p1 = [r for r in self.rows("STG_PRODUCT_PRICE")
              if r["source_product_id"] == "p1" and r["warehouse"] == "wh1"]
        self.assertEqual(len(p1), 1)

    def test_but_the_category_count_does_not_disappear(self):
        """Reducing must not ERASE: the multi-category product keeps declaring 2."""
        by_product = {r["source_product_id"]: r for r in self.rows("STG_PRODUCT_PRICE")
                      if r["warehouse"] == "wh1"}
        self.assertEqual(by_product["p1"]["category_appearances"], 2)
        self.assertEqual(by_product["p2"]["category_appearances"], 1)

    def test_the_primary_category_is_deterministic(self):
        p1 = next(r for r in self.rows("STG_PRODUCT_PRICE")
                  if r["source_product_id"] == "p1" and r["warehouse"] == "wh1")
        self.assertEqual(p1["primary_category_id"], 10)

    def test_the_declared_grain_is_actually_unique(self):
        rows = self.rows("STG_PRODUCT_PRICE")
        keys = {(r["ingestion_date"], r["warehouse"], r["source_product_id"]) for r in rows}
        self.assertEqual(len(keys), len(rows))


class CategoryTest(CutCase):
    def test_one_row_per_category_despite_4_partitions(self):
        """The tree is identical in the 4 warehouses (measured: 151 in each). Loading 2,114
        rows to serve a 151-row dimension would be pure duplication."""
        rows = self.rows("STG_CATEGORY")
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["category_id"] for r in rows}, {10, 20})


class GeographyTest(CutCase):
    def test_the_grain_is_municipality_plus_postal_code_not_just_postal_code(self):
        """Measured: 29 postal codes cross a municipality border. Keying only by postal code
        would merge two municipalities into one row."""
        rows = self.rows("STG_GEOGRAPHY")
        postal_codes = [r["postal_code"] for r in rows]
        self.assertEqual(postal_codes.count("46900"), 2, "the shared postal code collapsed")
        keys = {(r["province_code"], r["municipality_code"], r["postal_code"]) for r in rows}
        self.assertEqual(len(keys), len(rows))

    def test_municipality_outside_the_auf_does_not_cross(self):
        self.assertNotIn("999", {r["municipality_code"] for r in self.rows("STG_GEOGRAPHY")})

    def test_both_municipality_names_travel_with_distinct_labels(self):
        """Callejero and INE 29005 spell the same municipality differently in 370 of 370
        cases. A single label would hide the divergence; the same label would be a trap."""
        r = next(r for r in self.rows("STG_GEOGRAPHY") if r["municipality_code"] == "244")
        self.assertEqual(r["municipality_name_callejero"], "TORRENT")
        self.assertEqual(r["municipality_name_ine"], "Torrent")

    def test_the_attribute_population_excludes_the_aggregate(self):
        r = next(r for r in self.rows("STG_GEOGRAPHY") if r["municipality_code"] == "244")
        self.assertEqual(r["municipality_population"], 220.0)

    def test_old_ingestion_does_not_inflate_the_tramo_count(self):
        r = next(r for r in self.rows("STG_GEOGRAPHY")
                 if r["municipality_code"] == "244" and r["postal_code"] == "46900")
        self.assertEqual(r["tramo_count"], 2)


class CustomerTest(CutCase):
    def test_loads_the_whole_history_not_only_the_current_base(self):
        """DIM_CUSTOMER is SCD2: without the previous versions there is nothing to version."""
        rows = self.rows("STG_CUSTOMER")
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(1 for r in rows if r["is_latest_ingestion"]), 1)


class RunTest(CutCase):
    def test_the_three_sources_enter_in_the_same_shape(self):
        """Without this table, "there was no price" is indistinguishable from "there was no
        observation"."""
        rows = self.rows("STG_INGESTION_RUN")
        self.assertEqual({r["source_name"] for r in rows},
                         {"mercadona_catalog", "simulated_oltp", "simulated_orders"})
        self.assertTrue(all(r["wh"] == "wh1" for r in rows))

    def test_orders_declares_events_not_orders(self):
        """The `union all by name` trap: the column name decides what crosses.

        The orders manifest declares `declared_event_rows` (7) AND `declared_order_rows`
        (2). Mapping the second to `declared_rows` would make the "file rows x declared rows"
        reconciliation compare 2 against 7 — or, worse, pass by mistake if someone also
        swapped the file side. The partition file is NDJSON with one row per EVENT, so the
        right number is 7.
        """
        rows = {r["source_name"]: r for r in self.rows("STG_INGESTION_RUN")}
        self.assertEqual(rows["simulated_orders"]["declared_rows"], 7)


class OrderTest(CutCase):
    def test_loads_every_day_not_only_the_last(self):
        """Each ingestion_date is an OPERATING DAY, not a re-ingestion of the same day.

        The obvious variant — copying STG_CUSTOMER's `where is_latest_ingestion` — would
        leave 4,800 of the 6,400 real orders out of the warehouse, and the funnel mart would
        keep producing plausible rates over a quarter of the data.
        """
        rows = self.rows("STG_ORDER")
        self.assertEqual(len(rows), 2)
        self.assertEqual({str(r["ingestion_date"]) for r in rows},
                         {"2026-08-26", "2026-08-27"})

    def test_null_net_amount_crosses_as_null(self):
        """Null here means "nobody settled it". Filling it with the placed amount was a real
        Milestone 4 defect, found by comparing two independent folds."""
        rows = {r["order_id"]: r for r in self.rows("STG_ORDER")}
        self.assertIsNone(rows["ord_wh1_20260827_000001"]["net_amount"])
        self.assertIsNotNone(rows["ord_wh1_20260826_000001"]["net_amount"])

    def test_the_ordered_and_the_fulfilled_line_both_cross(self):
        """Loading only the fulfilled product would erase the substitution — the one fact
        the event model exists to record."""
        rows = {(r["order_id"], r["line_no"]): r for r in self.rows("STG_ORDER_LINE")}
        substituted = rows[("ord_wh1_20260826_000001", 1)]
        self.assertEqual(substituted["source_product_id"], "p1")
        self.assertEqual(substituted["fulfilled_source_product_id"], "p2")
        self.assertNotEqual(substituted["unit_price"],
                            substituted["fulfilled_unit_price"])


class EventTest(CutCase):
    def test_the_payload_crosses_as_text_not_as_json(self):
        """STAGE transports; GOLD does the typing, where there is an engine that understands
        VARIANT.

        The cast is EXPLICIT in the query on purpose: _TYPE_MAP refuses what it does not know
        precisely so it does not silently guess VARCHAR, so the choice needs to be visible
        instead of happening in the type map.
        """
        rows = self.rows("STG_ORDER_EVENT")
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertIsInstance(r["payload_json"], str)
            self.assertIn("{", r["payload_json"])

    def test_partition_wh_does_not_cross_the_boundary(self):
        """It is a Silver audit column: it exists so a dbt test can compare the file path
        with the record's content. Loading it into the warehouse would be transporting a
        PROOF, and the proof has already been executed on the other side."""
        self.assertNotIn("partition_wh", self.rows("STG_ORDER_EVENT")[0])

    def test_both_dates_travel_together(self):
        """An event can occur after midnight of the order day — 4,567 of the 44,456 in the
        real data. A fact with a single date would have to choose between "where the order
        lives" and "when the thing happened", and both choices lie to half of the
        questions."""
        rows = {r["event_id"]: r for r in self.rows("STG_ORDER_EVENT")}
        late = rows["ev2"]
        self.assertNotEqual(str(late["ingestion_date"]), str(late["event_date"]))
        self.assertTrue(late["crosses_order_date"])


class PremiseTest(CutCase):
    def test_the_synthetic_label_crosses_together_with_the_number(self):
        """It is the only way for whoever queries the warehouse to know, without opening the
        CONTRACT, that these numbers are a declared premise and not a measurement."""
        rows = self.rows("STG_ORDER_PREMISE")
        self.assertEqual({r["label"] for r in rows}, {"synthetic"})

    def test_the_sla_threshold_arrives_as_decimal_not_float(self):
        """`date_diff(...) > value` on a threshold that exists to be compared must not depend
        on binary rounding.

        The fixture's 77 is NOT the seed's value, and that is deliberate: a fixture that
        copies the real premise becomes accidental documentation of it, and then rots along
        with it — which is what happened to the 90 that lived here, kept after the seed
        dropped to 60. What this test measures is the TYPE in transit, never the value.
        """
        rows = {r["premise_key"]: r for r in self.rows("STG_ORDER_PREMISE")}
        value = rows["sla_minutes_picking"]["value"]
        self.assertEqual(str(value), "77.000000")


class CountIsNeverFloatTest(CutCase):
    """The trap the derived DDL caught, reproduced so it stays on record.

    `sum()` over INTEGER returns HUGEINT in DuckDB. Parquet has no INT128, so the write
    silently demotes the column to DOUBLE — and an EVENT COUNT reaches the warehouse declared
    as FLOAT, claiming it could have a fractional part. It corrupted no value (none goes
    above 40), but it is the same class of defect the repo refuses in money.

    THIS TEST DOES NOT GUARD `silver_order.sql` — it guards the MECHANISM. What guards the
    model is the DDL derived from the cut itself, reviewable by `make warehouse-ddl` with no
    connection at all; a hand-written DDL would have said NUMBER(38,0) and the divergence
    between what the file has and what the table declares would only show up in COPY INTO,
    or never.
    """

    def _type_in_parquet(self, expression):
        target = os.path.join(self.tmp.name, "count.parquet")
        self.con.execute(
            f"copy (select {expression} as n from silver_order_event group by order_id) "
            f"to '{target}' (format parquet)"
        )
        return self.con.execute(
            f"describe select * from read_parquet('{target}')"
        ).fetchall()[0][1]

    def test_sum_without_cast_becomes_double_in_parquet(self):
        self.assertEqual(self._type_in_parquet("sum(case when event_version = 1 then 1 else 0 end)"),
                         "DOUBLE")

    def test_the_explicit_cast_keeps_the_count_as_integer(self):
        kind = self._type_in_parquet(
            "cast(sum(case when event_version = 1 then 1 else 0 end) as integer)")
        self.assertEqual(kind, "INTEGER")
        self.assertEqual(_snowflake_type(kind), "NUMBER(38,0)")


class DdlTest(CutCase):
    def test_decimal_becomes_number_not_float(self):
        """Decimal precision in currency must not become floating point along the way."""
        self.assertEqual(_snowflake_type("DECIMAL(10,2)"), "NUMBER(10,2)")

    def test_unmapped_type_fails_instead_of_becoming_varchar(self):
        """Falling into a VARCHAR by default would silently lose the type — better to refuse."""
        with self.assertRaises(WarehouseCutError):
            _snowflake_type("STRUCT(a INTEGER)")

    def test_the_ddl_declares_the_same_columns_the_cut_produces(self):
        """The DDL comes from the query itself. If it is ever hand-written, this test breaks —
        which is exactly the point."""
        for spec in SPECS:
            cur = self.con.execute(spec["sql"].strip())
            columns = [d[0] for d in cur.description]
            cur.fetchall()
            ddl = ddl_for(spec, [(c, "VARCHAR") for c in columns], "RETAIL")
            for column in columns:
                self.assertIn(column, ddl, f"{spec['name']} lost {column} in the DDL")

    def test_dump_ddl_runs_without_any_snowflake_connection(self):
        ddl = dump_ddl(self.con, "RETAIL")
        self.assertIn("create schema if not exists RETAIL.STAGE", ddl)
        for spec in SPECS:
            self.assertIn(spec["name"], ddl)


class BuildTest(CutCase):
    def test_writes_one_parquet_per_spec_and_counts_right(self):
        summary = build(self.con, self.tmp.name)
        self.assertEqual(set(summary), {s["name"] for s in SPECS})
        for name, d in summary.items():
            self.assertTrue(os.path.exists(d["path"]), name)
            read = self.con.execute(
                f"select count(*) from read_parquet('{d['path']}')"
            ).fetchone()[0]
            self.assertEqual(read, d["rows"], f"{name}: parquet != declared count")

    def test_leaves_no_temporary_behind(self):
        build(self.con, self.tmp.name)
        leftovers = [f for f in os.listdir(self.tmp.name) if f.startswith(".tmp-")]
        self.assertEqual(leftovers, [])

    def test_rerunning_produces_the_same_count(self):
        first = build(self.con, self.tmp.name)
        second = build(self.con, self.tmp.name)
        self.assertEqual({k: v["rows"] for k, v in first.items()},
                         {k: v["rows"] for k, v in second.items()})


if __name__ == "__main__":
    unittest.main()
