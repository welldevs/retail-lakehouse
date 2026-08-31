"""O sorteio ponderado do grupo de demanda.

Cada teste aqui nomeia o modo de falha que pega E por que a variante obvia nao pegaria. A
classe de defeito que ronda este modulo e sempre a mesma: uma CDF errada, um peso trocado ou
uma ordem instavel produzem um mix PLAUSIVEL. Nada estoura, nenhum total fecha errado, e o
erro so aparece meses depois num grafico que ninguem sabe explicar.
"""

from __future__ import annotations

import os
import random
import subprocess
import sys
import unittest

from simulated_orders_source.demand import DemandError, DemandModel

from .support import demand_payload

PACKAGE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


class CargaTest(unittest.TestCase):
    def test_perfil_valido_carrega(self):
        modelo = DemandModel(demand_payload())
        self.assertEqual(modelo.version, "fixture_v1")
        self.assertEqual(modelo.groups, ("GRUPO_A", "GRUPO_B", "GRUPO_C"))
        self.assertAlmostEqual(sum(modelo.weights.values()), 1.0)

    def test_pesos_que_nao_somam_um_reprovam(self):
        # PERFIL TRUNCADO. Este e o defeito mais provavel de todos: alguem edita o seed,
        # remove uma linha e o resto continua parecendo certo. Sem esta checagem o sorteio
        # simplesmente ignoraria o grupo que sumiu, e o mix seria plausivel.
        with self.assertRaises(DemandError) as caught:
            DemandModel(demand_payload(weights={"GRUPO_A": "0.6", "GRUPO_B": "0.3"}))
        self.assertIn("somam", str(caught.exception))

    def test_peso_negativo_reprova(self):
        with self.assertRaises(DemandError):
            DemandModel(demand_payload(weights={"GRUPO_A": "1.3", "GRUPO_B": "-0.3"}))

    def test_perfil_sem_versao_reprova(self):
        payload = demand_payload()
        del payload["demand_model_version"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("demand_model_version", str(caught.exception))

    def test_grupo_repetido_reprova(self):
        payload = demand_payload()
        payload["groups"].append({"demand_group": "GRUPO_A", "line_weight": "0.0"})
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("repetido", str(caught.exception))

    def test_grupo_sem_peso_reprova_na_consulta(self):
        # NAO EXISTE PESO PADRAO. Um `.get(grupo, 0)` aqui seria uma premissa de demanda nao
        # declarada — o grupo desapareceria da cesta sem que nada avisasse.
        modelo = DemandModel(demand_payload())
        with self.assertRaises(DemandError) as caught:
            modelo.weight_of("GRUPO_INEXISTENTE")
        self.assertIn("nao tem peso", str(caught.exception))


class SorteioTest(unittest.TestCase):
    def setUp(self):
        self.modelo = DemandModel(demand_payload())
        self.disponiveis = self.modelo.groups
        self.cdf = self.modelo.cumulative(self.disponiveis)

    def test_cdf_termina_em_um_e_e_crescente(self):
        self.assertAlmostEqual(self.cdf[-1], 1.0)
        self.assertEqual(list(self.cdf), sorted(self.cdf))

    def test_converge_para_os_pesos_declarados(self):
        # O TESTE CENTRAL. Uma CDF invertida, um indice deslocado em um ou uma normalizacao
        # pelo total errado produzem todos um sorteio que FUNCIONA — devolve grupos validos,
        # nunca estoura — com as proporcoes erradas. Comparar contra os pesos declarados e a
        # unica forma de distinguir.
        #
        # A variante obvia ("sorteia sem erro") passaria em todas as tres implementacoes
        # erradas. 60.000 extracoes deixam o erro amostral em torno de 0,2 ponto para o
        # grupo de 0,6; a tolerancia de 1 ponto e folgada o suficiente para nao piscar e
        # estreita o suficiente para pegar uma troca de pesos entre dois grupos.
        rng = random.Random(20260831)
        contagem = {g: 0 for g in self.disponiveis}
        extracoes = 60000
        for _ in range(extracoes):
            contagem[self.modelo.pick(rng, self.disponiveis, self.cdf)] += 1
        for grupo, esperado in (("GRUPO_A", 0.6), ("GRUPO_B", 0.3), ("GRUPO_C", 0.1)):
            observado = contagem[grupo] / extracoes
            self.assertAlmostEqual(
                observado, esperado, delta=0.01,
                msg=f"{grupo}: esperado ~{esperado}, observado {observado:.4f}",
            )

    def test_pesos_trocados_entre_si_sao_detectados(self):
        # Prova que o teste acima REPROVA de fato, e nao passa por ser frouxo: com A e C
        # trocados, a proporcao de A cai de 0,6 para 0,1.
        modelo = DemandModel(
            demand_payload(weights={"GRUPO_A": "0.1", "GRUPO_B": "0.3", "GRUPO_C": "0.6"})
        )
        cdf = modelo.cumulative(modelo.groups)
        rng = random.Random(20260831)
        contagem = {g: 0 for g in modelo.groups}
        for _ in range(20000):
            contagem[modelo.pick(rng, modelo.groups, cdf)] += 1
        self.assertLess(contagem["GRUPO_A"] / 20000, 0.2)

    def test_subconjunto_e_renormalizado(self):
        # Um grupo pode nao ter produto num (armazem, dia). O peso dele tem de ser
        # redistribuido na PROPORCAO dos presentes, nao dividido igualmente: dividir igual
        # apagaria a calibracao justamente no armazem com sortimento menor.
        parcial = ("GRUPO_A", "GRUPO_B")
        cdf = self.modelo.cumulative(parcial)
        self.assertAlmostEqual(cdf[0], 0.6 / 0.9, places=6)
        self.assertAlmostEqual(cdf[-1], 1.0)

    def test_subconjunto_de_peso_zero_reprova(self):
        modelo = DemandModel(
            demand_payload(weights={"GRUPO_A": "1.0", "GRUPO_B": "0.0", "GRUPO_C": "0.0"})
        )
        with self.assertRaises(DemandError) as caught:
            modelo.cumulative(("GRUPO_B", "GRUPO_C"))
        self.assertIn("peso zero", str(caught.exception))

    def test_lista_vazia_reprova(self):
        with self.assertRaises(DemandError):
            self.modelo.cumulative(())

    def test_uma_extracao_de_rng_por_sorteio(self):
        # A reprodutibilidade por seed depende de QUANTAS vezes o rng e chamado, nao apenas
        # de com que seed ele comecou. Se `pick` passar a consumir duas extracoes, todo
        # pedido gerado depois dele muda — e o sha256 da particao muda sem que nada no
        # modelo tenha mudado.
        class Contador:
            def __init__(self):
                self.chamadas = 0
                self._rng = random.Random(1)

            def random(self):
                self.chamadas += 1
                return self._rng.random()

        contador = Contador()
        for _ in range(50):
            self.modelo.pick(contador, self.disponiveis, self.cdf)
        self.assertEqual(contador.chamadas, 50)


class DeterminismoTest(unittest.TestCase):
    def test_nao_depende_de_pythonhashseed(self):
        # A CDF e construida a partir de uma TUPLA ORDENADA, nunca da iteracao de um dict.
        # O hash de str em CPython e aleatorizado por processo: uma CDF montada na ordem de
        # iteracao de um dicionario faria a saida depender da variavel de ambiente, e o
        # defeito apareceria como divergencia ENTRE MAQUINAS — a mais caro de diagnosticar.
        code = (
            "import sys, random, json; sys.path.insert(0, 'src'); sys.path.insert(0, '.');"
            "from simulated_orders_source.demand import DemandModel;"
            "from tests.support import demand_payload;"
            "m = DemandModel(demand_payload());"
            "cdf = m.cumulative(m.groups);"
            "r = random.Random(99);"
            "print(''.join(m.pick(r, m.groups, cdf)[-1] for _ in range(200)))"
        )
        saidas = {
            subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                cwd=PACKAGE_DIR,
                env={"PYTHONHASHSEED": str(seed), "PATH": "/usr/bin:/bin"},
            ).stdout.strip()
            for seed in (0, 13, 5150)
        }
        self.assertEqual(len(saidas), 1, f"sorteio variou entre processos: {saidas}")
        self.assertTrue(saidas.pop(), "o subprocesso nao produziu saida")


class SazonalidadeTest(unittest.TestCase):
    def test_perfil_neutro_e_no_op(self):
        modelo = DemandModel(demand_payload())
        self.assertEqual(
            [modelo.seasonal_factor(m) for m in range(1, 13)], [1.0] * 12
        )

    def test_perfil_nao_neutro_muda_a_saida(self):
        # O PAR COM O TESTE ACIMA E O QUE VALE. Sozinho, "neutro e no-op" passaria tambem
        # numa implementacao que IGNORA o perfil sazonal — um slot inerte, entregue como se
        # funcionasse. Este teste prova que o mecanismo existe; o outro prova que o perfil
        # entregue esta neutro de proposito.
        fatores = {m: (1.4 if m == 12 else 1.0) for m in range(1, 13)}
        modelo = DemandModel(demand_payload(seasonality=fatores))
        self.assertEqual(modelo.seasonal_factor(12), 1.4)
        self.assertEqual(modelo.seasonal_factor(8), 1.0)

    def test_perfil_sazonal_incompleto_reprova(self):
        payload = demand_payload()
        del payload["seasonality"]["7"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("12 meses", str(caught.exception))

    def test_mes_ausente_na_consulta_reprova(self):
        modelo = DemandModel(demand_payload())
        with self.assertRaises(DemandError):
            modelo.seasonal_factor(13)


if __name__ == "__main__":
    unittest.main()
