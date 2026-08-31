"""As quatro consultas do export de Orders, contra fixtures DuckDB reais — sem rede, sem MinIO.

Cada caso reproduz uma restricao MEDIDA, e existe para que ela seja pega aqui e nao depois:

  * o dia sem snapshot de catalogo para aquele armazem — 27 pares (armazem, dia) no escopo
    real quando a janela abre para 2026-08-15. Tem de RECUSAR, nunca inventar preco;
  * o dia com snapshot anterior mas nao no proprio dia — vira `carried_forward` EXPLICITO,
    que e a regra 4 das cinco que o FAQ ja tinha escrito antes de Orders existir;
  * o produto que aparece em mais de uma categoria (4.581 linhas para 4.311 produtos em
    mad1/2026-08-24): o dedup de grao e obrigatorio, senao a cesta sortearia o mesmo produto
    duas vezes com pesos diferentes;
  * o cliente que aparece com dois armazens: quebraria "um cliente de W so pede de W" sem que
    nenhum teste a jusante percebesse, porque o pedido seria coerente com uma das duas linhas;
  * a premissa rotulada com qualquer coisa que nao seja `synthetic`: nenhuma fonte deste repo
    mede cesta, cadencia ou disponibilidade, entao `observed` ou `proxy` ali seria promessa
    falsa;
  * o catalogo menor que a maior cesta possivel: o gerador sorteia SEM REPOSICAO, e o defeito
    apareceria como pedido menor, nunca como erro.
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

# So o que o export EXIGE. A lista real tem 30 chaves; aqui basta o conjunto obrigatorio.
from retail_platform.orders_reference import REQUIRED_PREMISES  # noqa: E402

PREMISES_ROWS = {
    "basket_lines_max": "5",
    "basket_lines_min": "2",
    "basket_lines_mode": "3",
}


def premises_csv(extra: dict | None = None, label: str = "synthetic") -> str:
    valores = {chave: "1" for chave in REQUIRED_PREMISES}
    valores.update(PREMISES_ROWS)
    valores.update(extra or {})
    linhas = [PREMISES_CSV_HEADER]
    for chave, valor in valores.items():
        linhas.append(f"{chave},{valor},unidade,{label},motivo declarado\n")
    return "".join(linhas)


def demand_seeds() -> dict:
    """Os quatro seeds da calibracao, minimos, cobrindo os dois niveis 1 da fixture."""
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
            f"{L1_ALIMENTAR},Fruta,*,FRUTAS_FRESCAS,fixture\n"
            f"{L1_NAO_ALIMENTAR},*,*,NO_FOOD,fixture\n"
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
        ),
        "demand_seasonality_seed.csv": (
            "month,factor,label,rationale\n"
            + "".join(f"{m},1.0,synthetic,x\n" for m in range(1, 13))
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
    municipality_code varchar, postal_code varchar
);
"""


# Dois niveis 1 de proposito: um alimentar e um nao alimentar. Com um so, o perfil de
# demanda teria um bloco vazio e a fixture exercitaria o caminho de redistribuicao em vez do
# caminho normal.
L1_ALIMENTAR = "Fruta y verdura"
L1_NAO_ALIMENTAR = "Limpieza y hogar"


def catalogo(wh: str, dia: str, total: int = 8, primeiro: int = 0) -> list[tuple]:
    linhas = []
    for i in range(total):
        alimentar = i % 2 == 0
        l1 = L1_ALIMENTAR if alimentar else L1_NAO_ALIMENTAR
        l2 = "Fruta" if alimentar else "Limpieza cocina"
        preco = 1.00 + i
        linhas.append(
            (dia, wh, f"p{primeiro + i:04d}", f"Produto {i}", 10 + i % 2, l2, 100 + i % 3,
             "Sub", l1, preco, preco, "unit", 0.5 + i * 0.1, 21.0)
        )
    return linhas


class Fixture:
    """Base DuckDB em memoria com seeds em disco, no padrao de test_oltp_reference."""

    def __init__(self, tmpdir: str):
        self.tmpdir = tmpdir
        self.seeds = os.path.join(tmpdir, "seeds")
        os.makedirs(self.seeds, exist_ok=True)
        self.write_seed("warehouse_province_map_seed.csv", PROVINCE_MAP_CSV)
        self.write_seed("order_premises_seed.csv", premises_csv())
        for nome, conteudo in demand_seeds().items():
            self.write_seed(nome, conteudo)
        self.con = duckdb.connect()
        self.con.execute(SCHEMA)

    def write_seed(self, nome: str, conteudo: str) -> None:
        with open(os.path.join(self.seeds, nome), "w", encoding="utf-8") as handle:
            handle.write(conteudo)

    def add_catalog(self, linhas) -> None:
        self.con.executemany(
            "insert into silver_product_price values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", linhas
        )

    def add_customers(self, wh: str, dia: str, total: int = 4) -> None:
        self.con.executemany(
            "insert into silver_customer values (?,?,?,?,?,?)",
            [(dia, f"cust_{wh}_{i:06d}", wh, "28", "079", "28001") for i in range(total)],
        )

    def povoado(self, dias=("2026-08-24",)) -> "Fixture":
        for wh in ("mad1", "bcn1"):
            for dia in dias:
                self.add_catalog(catalogo(wh, dia))
            self.add_customers(wh, dias[0])
        return self


@unittest.skipIf(duckdb is None, "duckdb nao instalado")
class JanelaTest(unittest.TestCase):
    def test_intervalo_inclusivo_nas_duas_pontas(self):
        self.assertEqual(
            _date_range("2026-08-24", "2026-08-27"),
            ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27"],
        )

    def test_um_dia_so(self):
        self.assertEqual(_date_range("2026-08-24", "2026-08-24"), ["2026-08-24"])

    def test_janela_invertida_recusa(self):
        with self.assertRaises(OrdersReferenceError) as caught:
            _date_range("2026-08-27", "2026-08-24")
        self.assertIn("invertida", str(caught.exception))

    def test_data_malformada_recusa(self):
        with self.assertRaises(OrdersReferenceError):
            _date_range("24/08/2026", "2026-08-27")


@unittest.skipIf(duckdb is None, "duckdb nao instalado")
class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fx = Fixture(self.tmp.name)
        self.addCleanup(self.fx.con.close)


class CalendarioDePrecoTest(Base):
    def test_dia_com_snapshot_proprio_e_observed(self):
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        linhas = payloads["calendar"]["rows"]
        self.assertEqual(len(linhas), 2)  # 2 armazens x 1 dia
        for linha in linhas:
            self.assertEqual(linha["price_source"], "observed")
            self.assertEqual(linha["price_as_of"], "2026-08-24")

    def test_dia_sem_snapshot_carrega_o_ultimo_preco_conhecido(self):
        # Regra 4 das cinco do FAQ: um varejista vende todo dia, a fonte so foi observada em
        # alguns. Carry-forward EXPLICITO, nunca dissolvido no numero.
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-26", seeds_dir=self.fx.seeds)
        por_dia = {(r["wh"], r["order_date"]): r for r in payloads["calendar"]["rows"]}
        self.assertEqual(por_dia[("mad1", "2026-08-24")]["price_source"], "observed")
        for dia in ("2026-08-25", "2026-08-26"):
            linha = por_dia[("mad1", dia)]
            self.assertEqual(linha["price_source"], "carried_forward")
            self.assertEqual(linha["price_as_of"], "2026-08-24")
        self.assertEqual(payloads["calendar"]["carried_forward_rows"], 4)

    def test_dia_anterior_a_todo_snapshot_recusa(self):
        # O caso medido: 27 pares (armazem, dia) quando a janela real abre para 08-15. Um
        # pedido nesse dia teria de inventar preco, entao o export recusa.
        self.fx.povoado()
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-20", "2026-08-24", seeds_dir=self.fx.seeds)
        mensagem = str(caught.exception)
        self.assertIn("sem NENHUM snapshot", mensagem)
        self.assertIn("inventar preco", mensagem)

    def test_armazem_com_catalogo_e_outro_sem_recusa(self):
        # Assimetria real: mad1 tem catalogo de 08-15, os outros tres so de 08-24.
        self.fx.add_catalog(catalogo("mad1", "2026-08-24"))
        self.fx.add_customers("mad1", "2026-08-24")
        self.fx.add_customers("bcn1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)


class CatalogoTest(Base):
    def test_dedup_de_grao_uma_linha_por_produto(self):
        # O mesmo produto em duas categorias e semantica da fonte. Sem dedup a cesta
        # sortearia o mesmo produto duas vezes, com pesos diferentes.
        self.fx.povoado()
        self.fx.add_catalog([
            ("2026-08-24", "mad1", "p0000", "Produto 0", 99, "Fruta", 999, "Sub",
             L1_ALIMENTAR, 1.00, 1.00, "unit", 0.5, 21.0)
        ])
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        do_mad1 = [r for r in payloads["catalog"]["rows"] if r["wh"] == "mad1"]
        ids = [r["source_product_id"] for r in do_mad1]
        self.assertEqual(len(ids), len(set(ids)))
        escolhido = next(r for r in do_mad1 if r["source_product_id"] == "p0000")
        self.assertEqual(escolhido["category_id"], 10, "o desempate e a menor categoria")

    def test_preco_viaja_como_string(self):
        # Obrigacao 4.4 do contrato da Mercadona: float perde precisao decimal em moeda.
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        for linha in payloads["catalog"]["rows"][:5]:
            self.assertIsInstance(linha["unit_price"], str)

    def test_catalogo_menor_que_a_maior_cesta_recusa(self):
        # O gerador sorteia SEM REPOSICAO: o defeito apareceria como pedido menor, nao erro.
        self.fx.add_catalog(catalogo("mad1", "2026-08-24", total=3))
        self.fx.add_catalog(catalogo("bcn1", "2026-08-24", total=3))
        self.fx.add_customers("mad1", "2026-08-24")
        self.fx.add_customers("bcn1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("basket_lines_max", str(caught.exception))


class ClientesTest(Base):
    def test_uma_linha_por_cliente_com_a_primeira_geracao(self):
        self.fx.povoado()
        self.fx.add_customers("mad1", "2026-08-27")  # segunda geracao dos mesmos ids
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        clientes = payloads["customers"]
        ids = [r["customer_id"] for r in clientes["rows"]]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(clientes["customer_ingestion_dates"], ["2026-08-24", "2026-08-27"])
        do_mad1 = next(r for r in clientes["rows"] if r["wh"] == "mad1")
        self.assertEqual(do_mad1["first_ingestion_date"], "2026-08-24")

    def test_cliente_em_dois_armazens_recusa(self):
        # Quebraria "um cliente de W so pede de W" sem que nada a jusante percebesse: o
        # pedido seria coerente com uma das duas linhas.
        self.fx.povoado()
        self.fx.con.execute(
            "insert into silver_customer values "
            "(date '2026-08-24','cust_mad1_000000','bcn1','08','019','08001')"
        )
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("mais de um armazem", str(caught.exception))

    def test_armazem_sem_cliente_recusa(self):
        self.fx.add_catalog(catalogo("mad1", "2026-08-24"))
        self.fx.add_catalog(catalogo("bcn1", "2026-08-24"))
        self.fx.add_customers("mad1", "2026-08-24")
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("sem nenhum cliente", str(caught.exception))

    def test_silver_customer_vazio_recusa(self):
        self.fx.add_catalog(catalogo("mad1", "2026-08-24"))
        self.fx.add_catalog(catalogo("bcn1", "2026-08-24"))
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)


class PremissasTest(Base):
    def test_digest_acompanha_a_tabela(self):
        self.fx.povoado()
        primeiro = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.fx.write_seed("order_premises_seed.csv", premises_csv({"quantity_max": "9"}))
        segundo = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertNotEqual(
            primeiro["premises"]["seed_sha256"], segundo["premises"]["seed_sha256"],
            "trocar uma taxa tem de trocar o digest, senao a mudanca nao deixa rastro",
        )

    def test_rotulo_diferente_de_synthetic_recusa(self):
        # Nenhuma fonte deste repo mede cesta, cadencia ou disponibilidade. Chamar qualquer
        # uma destas de `observed` ou `proxy` prometeria um dado que nao existe.
        self.fx.povoado()
        for rotulo in ("observed", "proxy", ""):
            self.fx.write_seed("order_premises_seed.csv", premises_csv(label=rotulo))
            with self.assertRaises(OrdersReferenceError) as caught:
                build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
            self.assertIn("synthetic", str(caught.exception))

    def test_premissa_obrigatoria_ausente_recusa(self):
        self.fx.povoado()
        linhas = premises_csv().splitlines(keepends=True)
        sem_taxa = [l for l in linhas if not l.startswith("substitution_rate,")]
        self.fx.write_seed("order_premises_seed.csv", "".join(sem_taxa))
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("substitution_rate", str(caught.exception))

    def test_valor_nao_numerico_recusa(self):
        self.fx.povoado()
        self.fx.write_seed("order_premises_seed.csv", premises_csv({"quantity_max": "muitos"}))
        with self.assertRaises(OrdersReferenceError):
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)

    def test_chave_duplicada_recusa(self):
        self.fx.povoado()
        self.fx.write_seed(
            "order_premises_seed.csv",
            premises_csv() + "quantity_max,7,unidade,synthetic,duplicada\n",
        )
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("duplicada", str(caught.exception))

    def test_seed_ausente_recusa_com_mensagem_acionavel(self):
        self.fx.povoado()
        os.unlink(os.path.join(self.fx.seeds, "order_premises_seed.csv"))
        with self.assertRaises(OrdersReferenceError) as caught:
            build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        self.assertIn("--seeds-dir", str(caught.exception))


class EscritaTest(Base):
    def test_grava_os_cinco_arquivos_e_devolve_o_tamanho(self):
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        destino = os.path.join(self.tmp.name, "ref")
        tamanhos = write(payloads, destino)
        self.assertEqual(
            sorted(tamanhos),
            ["calendar.json", "catalog.json", "customers.json", "demand_profile.json",
             "premises.json"],
        )
        for nome, tamanho in tamanhos.items():
            caminho = os.path.join(destino, nome)
            self.assertTrue(os.path.exists(caminho), nome)
            self.assertEqual(os.path.getsize(caminho), tamanho)
            with open(caminho, encoding="utf-8") as handle:
                json.load(handle)

    def test_nao_deixa_temporario_para_tras(self):
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        destino = os.path.join(self.tmp.name, "ref")
        write(payloads, destino)
        self.assertEqual([n for n in os.listdir(destino) if n.startswith(".tmp-")], [])

    def test_reescrever_e_idempotente(self):
        self.fx.povoado()
        payloads = build(self.fx.con, "2026-08-24", "2026-08-24", seeds_dir=self.fx.seeds)
        destino = os.path.join(self.tmp.name, "ref")
        primeiro = write(payloads, destino)
        segundo = write(payloads, destino)
        self.assertEqual(primeiro, segundo)


if __name__ == "__main__":
    unittest.main()
