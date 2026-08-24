"""Navegacao defensiva do payload e impressao digital de schema."""

from __future__ import annotations

import unittest

from mercadona_catalog_source import schema
from tests.support import catalog, tree


class FlattenTreeTest(unittest.TestCase):
    def test_achata_para_nivel_2(self):
        nodes = schema.flatten_tree(tree((1, "Pai", [(10, "A"), (11, "B")])))
        self.assertEqual([n["id"] for n in nodes], [10, 11])
        self.assertEqual(nodes[0]["parent_name"], "Pai")

    def test_deduplica_ids_repetidos_sob_pais_diferentes(self):
        nodes = schema.flatten_tree(
            tree((1, "P1", [(10, "A")]), (2, "P2", [(10, "A"), (11, "B")]))
        )
        self.assertEqual([n["id"] for n in nodes], [10, 11])

    def test_descarta_filho_sem_id(self):
        payload = {"results": [{"id": 1, "name": "P", "categories": [{"name": "sem id"}]}]}
        self.assertEqual(schema.flatten_tree(payload), [])

    def test_tolera_filho_sem_nome(self):
        payload = {"results": [{"id": 1, "name": "P", "categories": [{"id": 10}]}]}
        self.assertEqual(schema.flatten_tree(payload)[0]["name"], "")

    def test_formato_inesperado_levanta_schema_error(self):
        for payload in ([], "texto", {"resultados": []}, {"results": "nao-lista"}, None):
            with self.subTest(payload=payload):
                with self.assertRaises(schema.SchemaError):
                    schema.flatten_tree(payload)


class DefensiveCountingTest(unittest.TestCase):
    def test_conta_produtos_de_multiplos_subgrupos(self):
        payload = catalog(1, "C", ("g1", [(1, "a", "1.00"), (2, "b", "2.00")]), ("g2", [(3, "c", "3.00")]))
        self.assertEqual(schema.count_products(payload), 3)
        self.assertEqual(schema.collect_product_ids(payload), {"1", "2", "3"})

    def test_nao_levanta_em_payload_degenerado(self):
        for payload in (None, [], "x", {}, {"categories": "x"}, {"categories": [None, 3]}):
            with self.subTest(payload=payload):
                self.assertEqual(schema.count_products(payload), 0)
                self.assertEqual(schema.collect_product_ids(payload), set())

    def test_produto_sem_id_conta_como_linha_mas_nao_como_unico(self):
        payload = {"categories": [{"name": "g", "products": [{"display_name": "x"}]}]}
        self.assertEqual(schema.count_products(payload), 1)
        self.assertEqual(schema.collect_product_ids(payload), set())

    def test_conta_categorias_de_nivel_1(self):
        self.assertEqual(schema.count_level1(tree((1, "A", []), (2, "B", []))), 2)
        self.assertEqual(schema.count_level1("lixo"), 0)


class FingerprintTest(unittest.TestCase):
    def test_mesma_forma_produz_mesmo_hash(self):
        a = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        b = catalog(2, "D", ("g", [(2, "b", "9.99")]))
        self.assertEqual(schema.fingerprint([a])["sha256"], schema.fingerprint([b])["sha256"])

    def test_campo_renomeado_muda_o_hash(self):
        base = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        mutado = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        produto = mutado["categories"][0]["products"][0]
        produto["nome_de_exibicao"] = produto.pop("display_name")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutado])["sha256"]
        )

    def test_campo_de_preco_renomeado_muda_o_hash(self):
        base = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        mutado = catalog(1, "C", ("g", [(1, "a", "1.00")]))
        precos = mutado["categories"][0]["products"][0]["price_instructions"]
        precos["preco_unitario"] = precos.pop("unit_price")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutado])["sha256"]
        )

    def test_registra_as_chaves_observadas(self):
        fp = schema.fingerprint([catalog(1, "C", ("g", [(1, "a", "1.00")]))])
        self.assertIn("display_name", fp["product_keys"])
        self.assertIn("unit_price", fp["price_instruction_keys"])


if __name__ == "__main__":
    unittest.main()
