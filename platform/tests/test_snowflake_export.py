"""O recorte para o Snowflake, contra fixtures DuckDB reais — sem rede, sem credencial.

O TRANSPORTE nao e o que da errado em silencio. O que da errado em silencio e o recorte:
um agregado da fonte somado junto do detalhe, um filtro de escopo esquecido, uma
deduplicacao que escolhe arbitrariamente entre valores que divergem. Nenhum desses falha —
todos produzem um numero plausivel. Por isso a suite mora aqui, e nao no PUT.

Cada fixture reproduz um caso MEDIDO no Lakehouse real:

  * `sex_label = 'Total'` convivendo com Hombres/Mujeres na mesma coluna — somar os tres
    da o dobro da populacao (a mesma armadilha que na Fase 1 inflou a piramide etaria em
    2,03x com `age_label`);
  * o mesmo produto repetido por aparecer em duas categorias, com o preco IGUAL entre as
    aparicoes — a reducao e segura, mas a contagem de categorias nao pode sumir;
  * o mesmo produto com preco DIFERENTE entre armazens (187 casos reais) — o eixo `wh`
    nao pode sair do grao;
  * um CEP que cruza fronteira de municipio (29 casos reais) — (municipio, CEP) e a chave,
    nao o CEP;
  * um municipio FORA da AUF, que nao pode atravessar o recorte;
  * uma ingestion_date antiga empilhada sob a corrente, que dobraria as contagens.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.snowflake_export import (  # noqa: E402
    SPECS,
    SnowflakeExportError,
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


def popular(con):
    """Fixture minima, mas com um caso-armadilha real em cada tabela."""
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

    # PRODUTO: p1 aparece em DUAS categorias (10 e 20) com o MESMO preco — a reducao e
    # segura, mas category_appearances tem de continuar dizendo "2". E p1 custa
    # diferente em wh1 e wh2, o caso dos 187 produtos reais.
    for wh, preco in (("wh1", "1.50"), ("wh2", "1.75")):
        for cat in (10, 20):
            con.execute(f"""
                insert into silver_product_price (ingestion_date, warehouse, category_id,
                    subgroup_id, source_product_id, display_name, product_level1_category_id,
                    unit_price, purchasable_unit_price, price_basis, net_content_kg_l,
                    min_bunch_amount, bulk_price, reference_price, previous_unit_price,
                    tax_percentage, is_pack, pack_size, unit_size, total_units,
                    selling_method, published, is_new_arrival)
                values ('2026-08-26','{wh}',{cat},'s1','p1','Leite','1',
                        {preco}, {preco}, 'unit', 1.0, 1.0,
                        {preco}, 1.500, null, 4.000, false, 1, 1.0, 1, 0, true, false)
            """)
    con.execute("""
        insert into silver_product_price (ingestion_date, warehouse, category_id,
            subgroup_id, source_product_id, display_name, product_level1_category_id,
            unit_price, purchasable_unit_price, price_basis, tax_percentage, published)
        values ('2026-08-26','wh1',10,'s1','p2','Pao','1', 0.90, 0.90, 'unit', 4.000, true)
    """)

    con.execute("""
        insert into silver_price_change values
            ('wh1','2026-08-26','2026-08-25','p1','Leite',1.40,1.50,0.10,2,'preco_alterado',false)
    """)

    # CATEGORIA: a mesma arvore repetida nos dois armazens e em duas datas. O recorte tem
    # de devolver UMA linha por category_id.
    for data in ("2026-08-25", "2026-08-26"):
        for wh in ("wh1", "wh2"):
            con.execute(f"""
                insert into silver_category values
                    ('{data}','{wh}',10,'Lacteos',1,100,'Frescos',1),
                    ('{data}','{wh}',20,'Padaria',2,100,'Frescos',1)
            """)

    # POPULACAO: 'Total' convive com Hombres/Mujeres — se atravessar, toda soma dobra.
    # '999' e um municipio FORA da AUF. A data antiga empilhada tem de ser filtrada.
    for sexo, valor in (("Hombres", 100.0), ("Mujeres", 120.0), ("Total", 220.0)):
        con.execute(f"""
            insert into silver_ine_population_by_municipality values
                ('2026-08-26',true,'S1','n','46','244','Torrent','{sexo}',2025,28,
                 '2024-12-31',false,{valor}),
                ('2026-08-25',false,'S1','n','46','244','Torrent','{sexo}',2025,28,
                 '2024-12-31',false,{valor}),
                ('2026-08-26',true,'S9','n','46','999','ForaDaAUF','{sexo}',2025,28,
                 '2024-12-31',false,{valor})
        """)

    # TRAMOS: o CEP 46900 cruza os municipios 244 e 250 — o caso dos 29 reais. O
    # municipio 999 esta fora da AUF. A data antiga nao pode entrar na contagem.
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
    # ORDERS declara EVENTOS (7) e PEDIDOS (2). Mapear a coluna errada faria a
    # reconciliacao comparar duas grandezas diferentes e passar por engano.
    con.execute("""
        insert into silver_orders_manifest values
            ('2026-08-27','wh1','r3',true,'simulated_orders',7,2,0,0,
             '2026-08-27 06:05:00','2026-08-27 06:05:02',1.9)
    """)

    # PEDIDOS em DUAS ingestion_date. Cada uma e um DIA DE OPERACAO, nao uma reingestao:
    # um filtro de "ultima ingestao" deixaria metade dos pedidos fora do warehouse.
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
    # `partition_wh` e coluna de AUDITORIA do Silver — nao pode atravessar a fronteira.
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
            ('sla_minutes_picking',77.0,'minutes','synthetic','Limiar de alerta.'),
            ('basket_lines_max',40.0,'lines','synthetic','Maior cesta possivel.')
    """)
    con.execute("""
        insert into stock_premises values
            ('opening_days_of_demand',7.0,'days','synthetic','Cobertura inicial.'),
            ('supplier_lead_days',2.0,'days','synthetic','Prazo do fornecedor.')
    """)
    # O CASO-ARMADILHA DESTA TABELA e o DIA DE RUPTURA: `units_short > 0` com
    # `closing_balance = 0`, que e a unica combinacao legitima. Se o recorte perdesse uma das
    # duas colunas, o parquet continuaria valido e o mart contaria ruptura sem saldo zerado.
    con.execute("""
        insert into silver_stock_ledger values
            ('wh1','p1','2026-08-26',100,0,30,30,0,70,0,null,10.0,7.0,'spark'),
            ('wh1','p1','2026-08-27',70,0,80,70,10,0,120,'2026-08-29',10.0,0.0,'spark')
    """)


@unittest.skipIf(duckdb is None, "duckdb nao instalado")
class RecorteCase(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect(":memory:")
        self.con.execute(SCHEMA)
        popular(self.con)
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(self.con.close)

    def rows(self, nome):
        spec = next(s for s in SPECS if s["name"] == nome)
        sql = spec["sql"].strip()
        cur = self.con.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


class PopulacaoTest(RecorteCase):
    def test_o_agregado_total_nunca_atravessa_o_recorte(self):
        """A armadilha mais cara do repo, agora em outra coluna: somar Hombres + Mujeres +
        Total da o DOBRO da populacao. Foi exatamente assim que a piramide etaria da Fase 1
        saiu 2,03x inflada, com age_label."""
        rotulos = {r["sex_label"] for r in self.rows("STG_POPULATION_MUNICIPALITY")}
        self.assertNotIn("Total", rotulos)
        self.assertEqual(rotulos, {"Hombres", "Mujeres"})

    def test_a_soma_do_detalhe_bate_com_o_total_da_fonte(self):
        total_fonte = self.con.execute(
            "select population_value from silver_ine_population_by_municipality "
            "where sex_label='Total' and municipality_code='244' and is_latest_ingestion"
        ).fetchone()[0]
        somado = sum(r["population_value"] for r in self.rows("STG_POPULATION_MUNICIPALITY")
                     if r["municipality_code"] == "244")
        self.assertEqual(somado, total_fonte)

    def test_municipio_fora_da_auf_nao_atravessa(self):
        codigos = {r["municipality_code"] for r in self.rows("STG_POPULATION_MUNICIPALITY")}
        self.assertNotIn("999", codigos)

    def test_ingestao_antiga_nao_dobra_as_linhas(self):
        rows = self.rows("STG_POPULATION_MUNICIPALITY")
        self.assertEqual(len(rows), 2, "a ingestion_date antiga atravessou o filtro")


class ProdutoTest(RecorteCase):
    def test_o_eixo_de_armazem_fica_no_grao(self):
        """Medido no dado real: 187 produtos tem preco diferente entre armazens no mesmo
        dia. Um fato sem `wh` mediria uma media que nao existe em lugar nenhum."""
        precos = {r["warehouse"]: r["unit_price"] for r in self.rows("STG_PRODUCT_PRICE")
                  if r["source_product_id"] == "p1"}
        self.assertEqual({k: str(v) for k, v in precos.items()},
                         {"wh1": "1.50", "wh2": "1.75"})

    def test_a_repeticao_por_categoria_e_reduzida_a_uma_linha(self):
        p1 = [r for r in self.rows("STG_PRODUCT_PRICE")
              if r["source_product_id"] == "p1" and r["warehouse"] == "wh1"]
        self.assertEqual(len(p1), 1)

    def test_mas_a_contagem_de_categorias_nao_desaparece(self):
        """Reduzir nao pode APAGAR: o produto multi-categoria continua declarando 2."""
        por_produto = {r["source_product_id"]: r for r in self.rows("STG_PRODUCT_PRICE")
                       if r["warehouse"] == "wh1"}
        self.assertEqual(por_produto["p1"]["category_appearances"], 2)
        self.assertEqual(por_produto["p2"]["category_appearances"], 1)

    def test_a_categoria_primaria_e_determinista(self):
        p1 = next(r for r in self.rows("STG_PRODUCT_PRICE")
                  if r["source_product_id"] == "p1" and r["warehouse"] == "wh1")
        self.assertEqual(p1["primary_category_id"], 10)

    def test_o_grao_declarado_e_de_fato_unico(self):
        rows = self.rows("STG_PRODUCT_PRICE")
        chaves = {(r["ingestion_date"], r["warehouse"], r["source_product_id"]) for r in rows}
        self.assertEqual(len(chaves), len(rows))


class CategoriaTest(RecorteCase):
    def test_uma_linha_por_categoria_apesar_de_4_particoes(self):
        """A arvore e identica nos 4 armazens (medido: 151 em cada). Carregar 2.114 linhas
        para servir uma dimensao de 151 seria duplicacao pura."""
        rows = self.rows("STG_CATEGORY")
        self.assertEqual(len(rows), 2)
        self.assertEqual({r["category_id"] for r in rows}, {10, 20})


class GeografiaTest(RecorteCase):
    def test_o_grao_e_municipio_mais_cep_nao_so_cep(self):
        """Medido: 29 CEPs cruzam fronteira de municipio. Chavear so pelo CEP fundiria
        dois municipios numa linha."""
        rows = self.rows("STG_GEOGRAPHY")
        ceps = [r["postal_code"] for r in rows]
        self.assertEqual(ceps.count("46900"), 2, "o CEP compartilhado colapsou")
        chaves = {(r["province_code"], r["municipality_code"], r["postal_code"]) for r in rows}
        self.assertEqual(len(chaves), len(rows))

    def test_municipio_fora_da_auf_nao_atravessa(self):
        self.assertNotIn("999", {r["municipality_code"] for r in self.rows("STG_GEOGRAPHY")})

    def test_os_dois_nomes_de_municipio_viajam_com_rotulos_distintos(self):
        """Callejero e INE 29005 grafam o mesmo municipio diferente em 370 de 370 casos.
        Um rotulo so esconderia a divergencia; o mesmo rotulo seria armadilha."""
        r = next(r for r in self.rows("STG_GEOGRAPHY") if r["municipality_code"] == "244")
        self.assertEqual(r["municipality_name_callejero"], "TORRENT")
        self.assertEqual(r["municipality_name_ine"], "Torrent")

    def test_a_populacao_do_atributo_exclui_o_agregado(self):
        r = next(r for r in self.rows("STG_GEOGRAPHY") if r["municipality_code"] == "244")
        self.assertEqual(r["municipality_population"], 220.0)

    def test_ingestao_antiga_nao_infla_a_contagem_de_tramos(self):
        r = next(r for r in self.rows("STG_GEOGRAPHY")
                 if r["municipality_code"] == "244" and r["postal_code"] == "46900")
        self.assertEqual(r["tramo_count"], 2)


class ClienteTest(RecorteCase):
    def test_carrega_o_historico_inteiro_nao_so_a_base_vigente(self):
        """DIM_CUSTOMER e SCD2: sem as versoes anteriores nao ha o que versionar."""
        rows = self.rows("STG_CUSTOMER")
        self.assertEqual(len(rows), 2)
        self.assertEqual(sum(1 for r in rows if r["is_latest_ingestion"]), 1)


class ExecucaoTest(RecorteCase):
    def test_as_tres_sources_entram_na_mesma_forma(self):
        """Sem esta tabela, "nao houve preco" e indistinguivel de "nao houve observacao"."""
        rows = self.rows("STG_INGESTION_RUN")
        self.assertEqual({r["source_name"] for r in rows},
                         {"mercadona_catalog", "simulated_oltp", "simulated_orders"})
        self.assertTrue(all(r["wh"] == "wh1" for r in rows))

    def test_orders_declara_eventos_e_nao_pedidos(self):
        """A armadilha do `union all by name`: o nome da coluna decide o que atravessa.

        O manifesto de orders declara `declared_event_rows` (7) E `declared_order_rows`
        (2). Mapear o segundo para `declared_rows` faria a reconciliacao "linhas do
        arquivo x linhas declaradas" comparar 2 contra 7 — ou, pior, passar por engano se
        alguem tambem trocasse o lado do arquivo. O arquivo da particao e NDJSON com uma
        linha por EVENTO, entao o numero certo e 7.
        """
        rows = {r["source_name"]: r for r in self.rows("STG_INGESTION_RUN")}
        self.assertEqual(rows["simulated_orders"]["declared_rows"], 7)


class PedidoTest(RecorteCase):
    def test_carrega_todos_os_dias_e_nao_so_o_ultimo(self):
        """Cada ingestion_date e um DIA DE OPERACAO, nao uma reingestao do mesmo dia.

        A variante obvia — copiar o `where is_latest_ingestion` de STG_CUSTOMER — deixaria
        4.800 dos 6.400 pedidos reais fora do warehouse, e o mart de funil continuaria
        produzindo taxas plausiveis sobre um quarto do dado.
        """
        rows = self.rows("STG_ORDER")
        self.assertEqual(len(rows), 2)
        self.assertEqual({str(r["ingestion_date"]) for r in rows},
                         {"2026-08-26", "2026-08-27"})

    def test_net_amount_nulo_atravessa_como_nulo(self):
        """Nulo aqui significa "ninguem apurou". Preenche-lo com o valor colocado foi um
        defeito real do Marco 4, achado comparando dois folds independentes."""
        rows = {r["order_id"]: r for r in self.rows("STG_ORDER")}
        self.assertIsNone(rows["ord_wh1_20260827_000001"]["net_amount"])
        self.assertIsNotNone(rows["ord_wh1_20260826_000001"]["net_amount"])

    def test_a_linha_pedida_e_a_cumprida_atravessam_as_duas(self):
        """Carregar so o produto cumprido apagaria a substituicao — o unico fato que o
        modelo de eventos existe para registrar."""
        rows = {(r["order_id"], r["line_no"]): r for r in self.rows("STG_ORDER_LINE")}
        substituida = rows[("ord_wh1_20260826_000001", 1)]
        self.assertEqual(substituida["source_product_id"], "p1")
        self.assertEqual(substituida["fulfilled_source_product_id"], "p2")
        self.assertNotEqual(substituida["unit_price"],
                            substituida["fulfilled_unit_price"])


class EventoTest(RecorteCase):
    def test_o_payload_atravessa_como_texto_e_nao_como_json(self):
        """O STAGE transporta; quem tipa e o GOLD, onde ha motor que entende VARIANT.

        O cast e EXPLICITO na consulta de proposito: _TYPE_MAP recusa o que nao conhece
        justamente para nao adivinhar VARCHAR em silencio, entao a escolha precisa estar
        visivel em vez de acontecer no mapa de tipos.
        """
        rows = self.rows("STG_ORDER_EVENT")
        self.assertEqual(len(rows), 2)
        for r in rows:
            self.assertIsInstance(r["payload_json"], str)
            self.assertIn("{", r["payload_json"])

    def test_partition_wh_nao_atravessa_a_fronteira(self):
        """E coluna de auditoria do Silver: existe para que um teste dbt compare o caminho
        do arquivo com o conteudo do registro. Carrega-la para o warehouse seria transportar
        uma PROVA, e a prova ja foi executada do outro lado."""
        self.assertNotIn("partition_wh", self.rows("STG_ORDER_EVENT")[0])

    def test_as_duas_datas_viajam_juntas(self):
        """Um evento pode ocorrer depois da meia-noite do dia do pedido — 4.567 dos 44.456
        no dado real. Um fato com uma unica data teria de escolher entre "onde o pedido
        mora" e "quando a coisa aconteceu", e as duas escolhas mentem para metade das
        perguntas."""
        rows = {r["event_id"]: r for r in self.rows("STG_ORDER_EVENT")}
        atrasado = rows["ev2"]
        self.assertNotEqual(str(atrasado["ingestion_date"]), str(atrasado["event_date"]))
        self.assertTrue(atrasado["crosses_order_date"])


class PremissaTest(RecorteCase):
    def test_o_rotulo_synthetic_atravessa_junto_do_numero(self):
        """E o unico jeito de quem consulta o warehouse saber, sem abrir o CONTRACT, que
        estes numeros sao premissa declarada e nao medicao."""
        rows = self.rows("STG_ORDER_PREMISE")
        self.assertEqual({r["label"] for r in rows}, {"synthetic"})

    def test_o_limiar_de_sla_chega_como_decimal_e_nao_como_float(self):
        """`date_diff(...) > value` num limiar que existe para ser comparado nao pode
        depender de arredondamento binario.

        O 77 do fixture NAO e o valor do seed, e isso e deliberado: um fixture que copia a
        premissa real vira documentacao acidental dela, e depois apodrece junto — foi o que
        aconteceu com o 90 que morava aqui, mantido depois de o seed cair para 60. O que
        este teste afere e o TIPO na travessia, nunca o valor.
        """
        rows = {r["premise_key"]: r for r in self.rows("STG_ORDER_PREMISE")}
        valor = rows["sla_minutes_picking"]["value"]
        self.assertEqual(str(valor), "77.000000")


class ContagemNuncaEFloatTest(RecorteCase):
    """A armadilha que o DDL derivado pegou, reproduzida para que fique registrada.

    `sum()` sobre INTEGER devolve HUGEINT no DuckDB. O parquet nao tem INT128, entao a
    escrita rebaixa a coluna para DOUBLE em silencio — e uma CONTAGEM DE EVENTOS chega ao
    warehouse declarada como FLOAT, afirmando poder ter parte fracionaria. Nao corrompeu
    nenhum valor (nenhum passa de 40), mas e a mesma classe de defeito que o repo recusa em
    dinheiro.

    ESTE TESTE NAO GUARDA `silver_order.sql` — guarda o MECANISMO. Quem guarda o modelo e o
    DDL derivado do proprio recorte, revisavel por `make warehouse-ddl` sem conexao nenhuma;
    um DDL escrito a mao teria dito NUMBER(38,0) e a divergencia entre o que o arquivo tem e
    o que a tabela declara so apareceria no COPY INTO, ou nunca.
    """

    def _tipo_no_parquet(self, expressao):
        destino = os.path.join(self.tmp.name, "contagem.parquet")
        self.con.execute(
            f"copy (select {expressao} as n from silver_order_event group by order_id) "
            f"to '{destino}' (format parquet)"
        )
        return self.con.execute(
            f"describe select * from read_parquet('{destino}')"
        ).fetchall()[0][1]

    def test_sum_sem_cast_vira_double_no_parquet(self):
        self.assertEqual(self._tipo_no_parquet("sum(case when event_version = 1 then 1 else 0 end)"),
                         "DOUBLE")

    def test_o_cast_explicito_preserva_a_contagem_como_inteiro(self):
        tipo = self._tipo_no_parquet(
            "cast(sum(case when event_version = 1 then 1 else 0 end) as integer)")
        self.assertEqual(tipo, "INTEGER")
        self.assertEqual(_snowflake_type(tipo), "NUMBER(38,0)")


class DdlTest(RecorteCase):
    def test_decimal_vira_number_e_nao_float(self):
        """Precisao decimal em moeda nao pode virar ponto flutuante no caminho."""
        self.assertEqual(_snowflake_type("DECIMAL(10,2)"), "NUMBER(10,2)")

    def test_tipo_sem_mapeamento_falha_em_vez_de_virar_varchar(self):
        """Cair num VARCHAR por omissao perderia tipo em silencio — melhor recusar."""
        with self.assertRaises(SnowflakeExportError):
            _snowflake_type("STRUCT(a INTEGER)")

    def test_o_ddl_declara_as_mesmas_colunas_que_o_recorte_produz(self):
        """O DDL vem da propria query. Se algum dia for escrito a mao, este teste quebra —
        que e exatamente o ponto."""
        for spec in SPECS:
            cur = self.con.execute(spec["sql"].strip())
            colunas = [d[0] for d in cur.description]
            cur.fetchall()
            ddl = ddl_for(spec, [(c, "VARCHAR") for c in colunas], "RETAIL")
            for coluna in colunas:
                self.assertIn(coluna, ddl, f"{spec['name']} perdeu {coluna} no DDL")

    def test_dump_ddl_roda_sem_nenhuma_conexao_com_o_snowflake(self):
        ddl = dump_ddl(self.con, "RETAIL")
        self.assertIn("create schema if not exists RETAIL.STAGE", ddl)
        for spec in SPECS:
            self.assertIn(spec["name"], ddl)


class BuildTest(RecorteCase):
    def test_escreve_um_parquet_por_spec_e_conta_certo(self):
        resumo = build(self.con, self.tmp.name)
        self.assertEqual(set(resumo), {s["name"] for s in SPECS})
        for nome, d in resumo.items():
            self.assertTrue(os.path.exists(d["path"]), nome)
            lidas = self.con.execute(
                f"select count(*) from read_parquet('{d['path']}')"
            ).fetchone()[0]
            self.assertEqual(lidas, d["rows"], f"{nome}: parquet != contagem declarada")

    def test_nao_deixa_temporario_para_tras(self):
        build(self.con, self.tmp.name)
        sobras = [f for f in os.listdir(self.tmp.name) if f.startswith(".tmp-")]
        self.assertEqual(sobras, [])

    def test_reexecutar_produz_a_mesma_contagem(self):
        primeiro = build(self.con, self.tmp.name)
        segundo = build(self.con, self.tmp.name)
        self.assertEqual({k: v["rows"] for k, v in primeiro.items()},
                         {k: v["rows"] for k, v in segundo.items()})


if __name__ == "__main__":
    unittest.main()
