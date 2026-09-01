"""A secao de coorte do reality check.

Por que esta secao merece teste proprio: o efeito da camada de coorte e INTEIRAMENTE
condicional — o agregado nao se move, de proposito. Um relatorio que publicasse so totais
concluiria que a fase nao fez nada, e essa conclusao seria indistinguivel da verdade se a
fase realmente nao tivesse feito nada. E o mesmo motivo pelo qual a Fase 4 recusou julgar
realismo por receita.
"""

from __future__ import annotations

import unittest

from retail_platform import demand_check as dc


def _grupos(**pesos) -> dict:
    return {
        nome: {
            "linhas": qtd,
            "unidades": str(qtd),
            "receita": str(qtd * 2),
            "kg_l": str(qtd),
            "linhas_sem_kg": 0,
        }
        for nome, qtd in pesos.items()
    }


def _snapshot(com_coorte: bool = True) -> dict:
    base = {
        "measured_at_utc": "2026-09-01T00:00:00Z",
        "groups": _grupos(VINO=100, ARROZ=100, NO_FOOD=100),
    }
    if not com_coorte:
        return {**base, "cohorts": None, "warehouses": None}
    return {
        **base,
        "cohorts": {
            "LT35": {
                g: {"linhas": n, "unidades": str(n), "receita": str(n)}
                for g, n in (("VINO", 10), ("ARROZ", 60), ("NO_FOOD", 30))
            },
            "GE65": {
                g: {"linhas": n, "unidades": str(n), "receita": str(n)}
                for g, n in (("VINO", 50), ("ARROZ", 20), ("NO_FOOD", 30))
            },
        },
        "warehouses": {
            "bcn1": {"pedidos": 1436, "clientes": 1251},
            "mad1": {"pedidos": 1176, "clientes": 1049},
        },
    }


class SecaoDeCoorteTest(unittest.TestCase):
    def test_a_pagina_traz_a_dimensao_de_coorte(self):
        # O erro que isto pega e o mesmo que o §7 da fase anterior existia para impedir, um
        # eixo adiante: um relatorio que publica so a dimensao em que nada mudou.
        pagina = "\n".join(dc._cohort_section(_snapshot(), None))
        self.assertIn("Propensao por coorte", pagina)
        self.assertIn("LT35", pagina)
        self.assertIn("GE65", pagina)
        self.assertIn("Frequencia por comunidade", pagina)

    def test_a_razao_entre_faixas_e_calculada_e_ordena_a_tabela(self):
        # A coluna que resume a fase inteira numa linha. Com 10% contra 50% de vinho, a
        # razao e 5,00 — e vinho tem de aparecer ANTES de arroz, cuja razao e 0,33.
        pagina = "\n".join(dc._cohort_section(_snapshot(), None))
        self.assertIn("| VINO | 10,00 | 50,00 | 5,00 |", pagina)
        self.assertLess(pagina.index("| VINO |"), pagina.index("| ARROZ |"))

    def test_ausencia_de_coorte_e_declarada_e_nao_omitida(self):
        # Uma janela gerada por um modelo anterior nao tem o carimbo. Omitir a secao faria
        # a pagina parecer completa; declarar a ausencia diz por que ela nao esta la.
        pagina = "\n".join(dc._cohort_section(_snapshot(com_coorte=False), None))
        self.assertIn("Ausente nesta janela", pagina)
        self.assertNotIn("| VINO |", pagina)

    def test_pedidos_por_armazem_aparecem_com_o_denominador_certo(self):
        pagina = "\n".join(dc._cohort_section(_snapshot(), None))
        self.assertIn("| bcn1 | 1436 | 1251 |", pagina)
        self.assertIn("| mad1 | 1176 | 1049 |", pagina)


class MedicaoToleranteTest(unittest.TestCase):
    def test_janela_sem_a_coluna_de_faixa_ainda_pode_ser_congelada(self):
        # Sem isto, o ANTES desta fase seria incongelavel — e sem ANTES nao ha como PROVAR
        # que o agregado nao se moveu, que e o criterio de aceitacao da fase inteira.
        class ConexaoSemColuna:
            def execute(self, sql):
                raise RuntimeError("Binder Error: buyer_age_band not found")

        self.assertEqual(
            dc._measure_cohorts(ConexaoSemColuna()),
            {"cohorts": None, "warehouses": None},
        )


if __name__ == "__main__":
    unittest.main()
