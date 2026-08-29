"""Premissas: sem default, e faixa invertida reprova."""

from __future__ import annotations

import unittest

from simulated_orders_source.premises import PremiseError, Premises

from .support import PREMISES


class PremissasTest(unittest.TestCase):
    def test_tabela_valida_passa(self):
        p = Premises(PREMISES)
        self.assertEqual(p.integer("basket_lines_mode"), 3)
        self.assertAlmostEqual(p.number("substitution_rate"), 0.2)

    def test_chave_ausente_levanta_em_vez_de_devolver_default(self):
        # E o ponto inteiro do modulo: um default escondido no gerador seria uma premissa
        # nao declarada, e a tabela existe justamente para impedir isso.
        p = Premises(PREMISES)
        with self.assertRaises(PremiseError) as caught:
            p.number("taxa_que_ninguem_declarou")
        self.assertIn("default", str(caught.exception))

    def test_faixa_invertida_reprova_na_construcao(self):
        ruim = dict(PREMISES, minutes_to_picking_min="240", minutes_to_picking_max="10")
        with self.assertRaises(PremiseError) as caught:
            Premises(ruim)
        self.assertIn("invertida", str(caught.exception))

    def test_proporcao_fora_de_zero_um_reprova(self):
        for valor in ("1.5", "-0.1"):
            with self.assertRaises(PremiseError):
                Premises(dict(PREMISES, substitution_rate=valor))

    def test_moda_fora_da_faixa_reprova(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, basket_lines_mode="99"))

    def test_valor_nao_numerico_reprova(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES, quantity_max="tres"))

    def test_inteiro_exige_valor_inteiro(self):
        with self.assertRaises(PremiseError):
            Premises(dict(PREMISES)).integer("substitution_rate")


if __name__ == "__main__":
    unittest.main()
