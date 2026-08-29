"""A referencia e desconfiada: truncada, vazia ou de outro schema tem de reprovar."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from decimal import Decimal

from simulated_orders_source.reference_data import ReferenceError, load

from .support import (
    FIRST_INGESTION,
    ORDER_DATE,
    PRICE_AS_OF,
    WH,
    catalog_rows,
    customer_rows,
    write_reference,
)


class CargaTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = write_reference(os.path.join(self.tmp.name, "ref"))

    def test_carrega_e_indexa(self):
        reference = load(self.dir)
        self.assertEqual(reference.warehouses(), [WH])
        self.assertEqual(reference.order_dates(WH), [ORDER_DATE])
        self.assertEqual(len(reference.catalog_of(WH, PRICE_AS_OF)), 30)
        self.assertEqual(len(reference.customers_of(WH)), 8)

    def test_preco_vira_decimal_e_nao_float(self):
        # Obrigacao 4.4 do contrato da Mercadona: float perde precisao decimal em moeda.
        preco = load(self.dir).catalog_of(WH, PRICE_AS_OF)[0]["unit_price"]
        self.assertIsInstance(preco, Decimal)

    def test_diretorio_ausente_reprova(self):
        with self.assertRaises(ReferenceError):
            load(os.path.join(self.tmp.name, "nao-existe"))

    def test_arquivo_ausente_reprova(self):
        os.unlink(os.path.join(self.dir, "catalog.json"))
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("ausente", str(caught.exception))

    def test_json_invalido_reprova(self):
        with open(os.path.join(self.dir, "calendar.json"), "w", encoding="utf-8") as handle:
            handle.write("{nao e json")
        with self.assertRaises(ReferenceError):
            load(self.dir)

    def test_rows_vazio_reprova(self):
        # Um export truncado nao pode virar "zero pedidos" em silencio.
        write_reference(self.dir, catalog=[])
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("vazia", str(caught.exception))

    def test_campo_obrigatorio_ausente_reprova(self):
        magro = [{k: v for k, v in row.items() if k != "unit_price"} for row in catalog_rows()]
        write_reference(self.dir, catalog=magro)
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("unit_price", str(caught.exception))

    def test_preco_nao_positivo_reprova(self):
        ruim = catalog_rows()
        ruim[0]["unit_price"] = "0.00"
        write_reference(self.dir, catalog=ruim)
        with self.assertRaises(ReferenceError):
            load(self.dir)

    def test_price_source_fora_do_vocabulario_reprova(self):
        write_reference(
            self.dir,
            calendar=[
                {
                    "wh": WH,
                    "order_date": ORDER_DATE,
                    "price_as_of": PRICE_AS_OF,
                    "price_source": "chutado",
                }
            ],
        )
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir)
        self.assertIn("price_source", str(caught.exception))

    def test_armazem_sem_cliente_reprova_na_consulta(self):
        with self.assertRaises(ReferenceError):
            load(self.dir).customers_of("svq1")

    def test_dia_fora_da_janela_reprova_na_consulta(self):
        with self.assertRaises(ReferenceError) as caught:
            load(self.dir).calendar_of(WH, "2026-01-01")
        self.assertIn("janela", str(caught.exception))


class VersaoDeClienteTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _reference(self, dates, first):
        return load(
            write_reference(
                os.path.join(self.tmp.name, "ref"),
                customers=customer_rows(first=first),
                customer_ingestion_dates=dates,
            )
        )

    def test_escolhe_a_maior_geracao_ate_a_data_do_pedido(self):
        reference = self._reference(["2026-08-24", "2026-08-27"], "2026-08-24")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-08-25"), "2026-08-24")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-08-27"), "2026-08-27")
        self.assertEqual(reference.customer_version_at("2026-08-24", "2026-09-01"), "2026-08-27")

    def test_pedido_antes_da_primeira_aparicao_nao_tem_versao(self):
        reference = self._reference(["2026-08-27"], "2026-08-27")
        self.assertIsNone(reference.customer_version_at("2026-08-27", "2026-08-24"))


class SubstituicaoTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = load(write_reference(os.path.join(self.tmp.name, "ref")))

    def test_indice_de_subgrupo_aponta_para_o_mesmo_subgrupo(self):
        catalogo = self.reference.catalog_of(WH, PRICE_AS_OF)
        alvo = catalogo[0]["subgroup_id"]
        for posicao in self.reference.substitutes_in_subgroup(WH, PRICE_AS_OF, alvo):
            self.assertEqual(catalogo[posicao]["subgroup_id"], alvo)

    def test_subgrupo_inexistente_devolve_lista_vazia(self):
        self.assertEqual(self.reference.substitutes_in_subgroup(WH, PRICE_AS_OF, 99999), [])


if __name__ == "__main__":
    unittest.main()
