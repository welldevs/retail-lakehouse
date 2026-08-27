"""Impressao digital de schema e campos obrigatorios."""

from __future__ import annotations

import unittest

from simulated_oltp_source.schema import (
    CUSTOMER_FIELDS,
    count_customers,
    customers_of,
    fingerprint,
    missing_fields,
)


def customer(**overrides) -> dict:
    base = {field: "x" for field in CUSTOMER_FIELDS}
    base["house_number"] = 12
    base["birth_year"] = 1990
    base["candidate_index"] = 0
    base.update(overrides)
    return base


class FormaTest(unittest.TestCase):
    def test_forma_inesperada_nao_levanta_excecao(self):
        for payload in (None, {}, 3, "texto", [1, 2], [{"a": 1}, 5]):
            with self.subTest(payload=payload):
                self.assertIsInstance(customers_of(payload), list)

    def test_conta_apenas_objetos(self):
        self.assertEqual(count_customers([customer(), 7, customer()]), 2)


class FingerprintTest(unittest.TestCase):
    def test_mesmas_chaves_dao_o_mesmo_hash(self):
        a = fingerprint([[customer(customer_id="a")]])
        b = fingerprint([[customer(customer_id="b")]])
        self.assertEqual(a["sha256"], b["sha256"])

    def test_chave_a_mais_muda_o_hash(self):
        a = fingerprint([[customer()]])
        b = fingerprint([[customer(extra=1)]])
        self.assertNotEqual(a["sha256"], b["sha256"])

    def test_a_impressao_digital_e_a_uniao_das_chaves(self):
        """E por isso que house_number tem de existir sempre, mesmo nulo: se a chave
        aparecesse so em alguns registros, o hash da particao dependeria da seed."""
        sem = customer()
        sem.pop("house_number")
        misto = fingerprint([[sem, customer()]])
        self.assertIn("house_number", misto["customer_keys"])
        self.assertEqual(misto["sha256"], fingerprint([[customer()]])["sha256"])

    def test_chaves_saem_ordenadas(self):
        keys = fingerprint([[customer()]])["customer_keys"]
        self.assertEqual(keys, sorted(keys))


class CamposObrigatoriosTest(unittest.TestCase):
    def test_registro_completo_nao_acusa_nada(self):
        self.assertEqual(missing_fields([customer()]), [])

    def test_campo_faltando_e_reportado_uma_vez(self):
        incompleto = customer()
        incompleto.pop("postal_code")
        self.assertEqual(missing_fields([incompleto, customer()]), ["postal_code"])

    def test_house_number_nulo_conta_como_presente(self):
        self.assertEqual(missing_fields([customer(house_number=None)]), [])


if __name__ == "__main__":
    unittest.main()
