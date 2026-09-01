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

from .support import WH, demand_payload

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


class CoorteTest(unittest.TestCase):
    """A propensao por coorte: (faixa etaria x comunidade autonoma) -> pesos.

    O modo de falha que ronda esta secao e o mais barato de cometer e o mais caro de
    encontrar: o codigo LE a coorte, ignora, e continua sorteando pelo vetor agregado. Tudo
    soma 1, nenhum total quebra, e o painel mostra um mix perfeitamente plausivel em que
    idoso e jovem compram exatamente a mesma coisa.
    """

    def test_pesos_de_toda_coorte_somam_um(self):
        modelo = DemandModel(demand_payload())
        for coorte in modelo.cohorts:
            total = sum(modelo.cohort_weights[coorte].values())
            self.assertAlmostEqual(total, 1.0, places=9, msg=coorte)

    def test_coorte_com_vetor_truncado_reprova(self):
        # Um vetor que perde um grupo NAO soma 1, e sortear com ele daria mais peso a todos
        # os outros — plausivel, e invisivel num total por armazem.
        payload = demand_payload()
        payload["cohorts"]["weights"][0]["groups"].pop()
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("somam", str(caught.exception))

    def test_perfil_sem_secao_de_coorte_reprova(self):
        # Sem isto, um perfil da versao anterior carregaria sob a versao nova e o sorteio
        # cairia no vetor agregado. Seria um no-op silencioso — o pior resultado possivel
        # para uma camada cujo criterio de sucesso e nao mover o agregado.
        payload = demand_payload()
        del payload["cohorts"]
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("cohorts", str(caught.exception))

    def test_faixa_etaria_pela_idade(self):
        modelo = DemandModel(demand_payload())
        self.assertEqual(modelo.band_of(18), "LT35")
        self.assertEqual(modelo.band_of(34), "LT35")
        self.assertEqual(modelo.band_of(35), "35_49")
        self.assertEqual(modelo.band_of(49), "35_49")
        self.assertEqual(modelo.band_of(50), "50_64")
        self.assertEqual(modelo.band_of(64), "50_64")
        self.assertEqual(modelo.band_of(65), "GE65")
        self.assertEqual(modelo.band_of(101), "GE65")

    def test_faixas_sem_topo_aberto_reprovam(self):
        # Sem exatamente uma faixa de teto nulo, `band_of` nao teria onde por quem passa do
        # ultimo limite — e devolver a ultima faixa por acaso da ordem do JSON seria pior.
        payload = demand_payload()
        for banda in payload["cohorts"]["age_bands"]:
            if banda["max_age"] is None:
                banda["max_age"] = 99
        with self.assertRaises(DemandError) as caught:
            DemandModel(payload)
        self.assertIn("aberta", str(caught.exception))

    def test_coorte_desconhecida_reprova(self):
        modelo = DemandModel(demand_payload())
        with self.assertRaises(DemandError):
            modelo.cumulative(modelo.groups, "GE65|99")
        with self.assertRaises(DemandError) as caught:
            modelo.region_of("wh_inexistente")
        self.assertIn("comunidade", str(caught.exception))

    def test_matriz_plana_reproduz_o_sorteio_agregado(self):
        # Prova que a mudanca esta no PERFIL, e nao escondida no codigo: com todas as
        # coortes carregando o vetor agregado, a sequencia sorteada tem de ser identica a
        # que o sorteio sem coorte produziria com a mesma seed.
        modelo = DemandModel(demand_payload())
        disponiveis = modelo.groups

        rng_a = random.Random(20260901)
        agregado = modelo.cumulative(disponiveis)
        saida_a = [modelo.pick(rng_a, disponiveis, agregado) for _ in range(500)]

        rng_b = random.Random(20260901)
        por_coorte = modelo.cumulative(disponiveis, f"GE65|{modelo.region_of(WH)}")
        saida_b = [modelo.pick(rng_b, disponiveis, por_coorte) for _ in range(500)]

        self.assertEqual(saida_a, saida_b)

    def test_duas_coortes_com_pesos_diferentes_sorteiam_diferente(self):
        # O PAR COM O TESTE ACIMA E O QUE VALE, exatamente como no perfil sazonal. Sozinho,
        # "matriz plana reproduz o agregado" passaria tambem numa implementacao que IGNORA a
        # coorte. Este prova que o mecanismo existe.
        jovem = {"GRUPO_A": "0.1", "GRUPO_B": "0.2", "GRUPO_C": "0.7"}
        idoso = {"GRUPO_A": "0.8", "GRUPO_B": "0.15", "GRUPO_C": "0.05"}
        vetores = {}
        for ccaa in ("13", "09"):
            for banda in ("LT35", "35_49", "50_64", "GE65"):
                vetores[f"{banda}|{ccaa}"] = idoso if banda == "GE65" else jovem
        modelo = DemandModel(demand_payload(cohort_weights=vetores))

        def mix(coorte):
            rng = random.Random(7)
            cdf = modelo.cumulative(modelo.groups, coorte)
            saida = [modelo.pick(rng, modelo.groups, cdf) for _ in range(4000)]
            return saida.count("GRUPO_A") / len(saida)

        self.assertGreater(mix("GE65|13"), 0.7)
        self.assertLess(mix("LT35|13"), 0.2)

    def test_cdf_por_coorte_nao_depende_de_pythonhashseed(self):
        # Mesma armadilha do sorteio agregado, um nivel acima. O alvo e a iteracao de um
        # `set`: o hash de `str` em CPython e aleatorizado por processo, entao uma tupla de
        # coortes ou de faixas derivada de um set sai em ordem diferente a cada execucao. A
        # saida so muda entre PROCESSOS, nunca dentro de um — por isso o teste roda tres
        # subprocessos com PYTHONHASHSEED distinto em vez de repetir a chamada aqui.
        script = (
            "import json,random;"
            "from simulated_orders_source.demand import DemandModel;"
            "import sys;"
            "m=DemandModel(json.load(sys.stdin));"
            "r=random.Random(11);"
            "c=m.cumulative(m.groups,'GE65|13');"
            "print('|'.join(m.cohorts));"
            "print('|'.join(b for b,_ in m.age_bands));"
            "print(','.join(m.pick(r,m.groups,c) for _ in range(60)))"
        )
        import json as _json
        payload = _json.dumps(demand_payload())
        saidas = set()
        for semente in ("0", "1", "424242"):
            ambiente = dict(os.environ, PYTHONHASHSEED=semente, PYTHONPATH="src")
            resultado = subprocess.run(
                [sys.executable, "-c", script],
                cwd=PACKAGE_DIR, env=ambiente, input=payload,
                capture_output=True, text=True, check=True,
            )
            saidas.add(resultado.stdout.strip())
        self.assertEqual(len(saidas), 1, f"saidas divergentes: {saidas}")

    def test_indice_de_frequencia_e_por_armazem(self):
        modelo = DemandModel(demand_payload(frequency={"13": "0.9", "09": "1.1"}))
        self.assertAlmostEqual(modelo.frequency_index("mad1"), 0.9)
        self.assertAlmostEqual(modelo.frequency_index("bcn1"), 1.1)


if __name__ == "__main__":
    unittest.main()
