"""Navegacao defensiva do payload e impressao digital de schema."""

from __future__ import annotations

import unittest

from ine_population_source import schema
from tests.support import series, table_payload


class SeriesOfTest(unittest.TestCase):
    def test_lista_series_validas(self):
        payload = table_payload(series("A", "Total. Madrid.", (2025, 1)), series("B", "Total. Sevilla.", (2025, 2)))
        self.assertEqual([s["COD"] for s in schema.series_of(payload)], ["A", "B"])

    def test_descarta_item_que_nao_e_mapeamento(self):
        payload = [series("A", "x", (2025, 1)), "lixo", None, 42]
        self.assertEqual(len(schema.series_of(payload)), 1)

    def test_formato_inesperado_devolve_lista_vazia(self):
        for payload in (None, "texto", {"nao": "lista"}, 42):
            with self.subTest(payload=payload):
                self.assertEqual(schema.series_of(payload), [])


class DataPointsOfTest(unittest.TestCase):
    def test_lista_pontos_validos(self):
        s = series("A", "x", (2024, 1), (2025, 2))
        self.assertEqual(len(schema.data_points_of(s)), 2)

    def test_descarta_ponto_que_nao_e_mapeamento(self):
        s = series("A", "x", (2025, 1))
        s["Data"].append("lixo")
        self.assertEqual(len(schema.data_points_of(s)), 1)

    def test_formato_inesperado_devolve_lista_vazia(self):
        for value in (None, "texto", {"Data": "nao-lista"}, {"sem_data": True}):
            with self.subTest(value=value):
                self.assertEqual(schema.data_points_of(value), [])


class DefensiveCountingTest(unittest.TestCase):
    def test_conta_series_e_pontos(self):
        payload = table_payload(series("A", "x", (2024, 1), (2025, 2)), series("B", "y", (2025, 3)))
        self.assertEqual(schema.count_series(payload), 2)
        self.assertEqual(schema.count_data_points(payload), 3)

    def test_nao_levanta_em_payload_degenerado(self):
        for payload in (None, [], "x", {}, [None, 3]):
            with self.subTest(payload=payload):
                self.assertEqual(schema.count_series(payload), 0)
                self.assertEqual(schema.count_data_points(payload), 0)


class ValidatePayloadTest(unittest.TestCase):
    def test_aceita_lista_de_series(self):
        payload = table_payload(series("A", "x", (2025, 1)))
        self.assertEqual(len(schema.validate_payload(payload)), 1)

    def test_raiz_que_nao_e_lista_levanta_schema_error(self):
        for payload in ({"results": []}, "texto", None, 42):
            with self.subTest(payload=payload):
                with self.assertRaises(schema.SchemaError):
                    schema.validate_payload(payload)

    def test_lista_sem_serie_reconhecivel_levanta_schema_error(self):
        with self.assertRaises(schema.SchemaError):
            schema.validate_payload(["lixo", None])


class FingerprintTest(unittest.TestCase):
    def test_mesma_forma_produz_mesmo_hash(self):
        a = table_payload(series("A", "x", (2025, 1)))
        b = table_payload(series("B", "y", (2024, 9)))
        self.assertEqual(schema.fingerprint([a])["sha256"], schema.fingerprint([b])["sha256"])

    def test_campo_renomeado_na_serie_muda_o_hash(self):
        base = table_payload(series("A", "x", (2025, 1)))
        mutado = table_payload(series("A", "x", (2025, 1)))
        mutado[0]["Codigo"] = mutado[0].pop("COD")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutado])["sha256"]
        )

    def test_campo_renomeado_no_ponto_muda_o_hash(self):
        base = table_payload(series("A", "x", (2025, 1)))
        mutado = table_payload(series("A", "x", (2025, 1)))
        mutado[0]["Data"][0]["Ano"] = mutado[0]["Data"][0].pop("Anyo")
        self.assertNotEqual(
            schema.fingerprint([base])["sha256"], schema.fingerprint([mutado])["sha256"]
        )

    def test_registra_as_chaves_observadas(self):
        fp = schema.fingerprint([table_payload(series("A", "x", (2025, 1)))])
        self.assertIn("COD", fp["series_keys"])
        self.assertIn("Valor", fp["data_point_keys"])
        self.assertNotIn("Data", fp["series_keys"])


if __name__ == "__main__":
    unittest.main()
