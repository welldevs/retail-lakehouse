"""A geracao e deterministica, geograficamente coerente e nunca fabrica endereco."""

from __future__ import annotations

import collections
import os
import random
import tempfile
import unittest

from simulated_oltp_source import customers_generator, reference_data
from simulated_oltp_source.customers_generator import GenerationError, generate, pick_house_number

from tests import support


class GeneratorTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.reference = reference_data.load(
            support.write_reference(os.path.join(self.tmp.name, "ref"))
        )

    def gen(self, wh=support.WH_A, count=50, seed=7, date="2026-08-27"):
        return generate(self.reference, wh, count, seed, date)


class ReprodutibilidadeTest(GeneratorTestCase):
    def test_mesma_seed_produz_exatamente_a_mesma_saida(self):
        self.assertEqual(self.gen(seed=99), self.gen(seed=99))

    def test_seeds_diferentes_produzem_saidas_diferentes(self):
        # Sem isto, um --seed ignorado passaria despercebido: a saida continuaria
        # "deterministica", so que insensivel ao parametro.
        self.assertNotEqual(self.gen(seed=1), self.gen(seed=2))

    def test_nao_depende_do_relogio(self):
        """birth_year vem da data da particao, nunca de datetime.now()."""
        de_2026 = self.gen(count=20, date="2026-08-27")
        de_2030 = self.gen(count=20, date="2030-01-01")
        for antes, depois in zip(de_2026, de_2030):
            self.assertEqual(depois["birth_year"] - antes["birth_year"], 4)

    def test_a_ordem_dos_candidatos_define_o_candidate_index(self):
        """O indice tem de apontar para a linha certa da referencia, nao para outra."""
        for customer in self.gen():
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(customer["postal_code"], candidate["postal_code"])
            self.assertEqual(customer["street_name"], candidate["street_name"])


class CoerenciaGeograficaTest(GeneratorTestCase):
    def test_todo_cliente_pertence_ao_warehouse_pedido(self):
        for customer in self.gen(wh=support.WH_B, count=30):
            self.assertEqual(customer["wh"], support.WH_B)

    def test_nenhum_cliente_cruza_a_area_urbana_funcional(self):
        """O municipio do cliente tem de estar na AUF do proprio warehouse."""
        for wh in (support.WH_A, support.WH_B):
            permitidos = {
                (m["province_code"], m["municipality_code"])
                for m in self.reference.municipalities(wh)
            }
            for customer in self.gen(wh=wh, count=60):
                chave = (customer["province_code"], customer["municipality_code"])
                self.assertIn(chave, permitidos, f"{customer['customer_id']} fora da AUF")

    def test_endereco_e_sempre_uma_linha_real_da_referencia(self):
        for customer in self.gen(count=100):
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(customer["wh"], candidate["wh"])
            self.assertEqual(customer["province_code"], candidate["province_code"])
            self.assertEqual(customer["municipality_code"], candidate["municipality_code"])
            self.assertEqual(customer["numbering_type"], candidate["numbering_type"])

    def test_cep_sempre_existe_na_referencia_para_aquele_municipio(self):
        validos = collections.defaultdict(set)
        for row in support.CANDIDATES:
            validos[(row["wh"], row["province_code"], row["municipality_code"])].add(
                row["postal_code"]
            )
        for customer in self.gen(count=100):
            chave = (customer["wh"], customer["province_code"], customer["municipality_code"])
            self.assertIn(customer["postal_code"], validos[chave])

    def test_o_candidato_sorteado_pertence_ao_municipio_sorteado(self):
        for customer in self.gen(count=100):
            candidate = self.reference.candidate(customer["candidate_index"])
            self.assertEqual(
                (candidate["province_code"], candidate["municipality_code"]),
                (customer["province_code"], customer["municipality_code"]),
            )


class NumeracaoTest(GeneratorTestCase):
    def test_numbering_type_zero_nunca_gera_numero(self):
        gerados = 0
        for customer in self.gen(count=300):
            if customer["numbering_type"] == "0":
                gerados += 1
                self.assertIsNone(
                    customer["house_number"],
                    f"{customer['customer_id']} recebeu numero em via sem numeracao",
                )
        self.assertGreater(gerados, 0, "a fixture nao exercitou nenhum tramo sem numeracao")

    def test_house_number_nunca_e_zero(self):
        """Faixa 0000..0000 com numeracao declarada existe no dado real (14 tramos)."""
        for customer in self.gen(count=300):
            self.assertNotEqual(customer["house_number"], 0)

    def test_faixa_degenerada_zero_a_zero_nao_produz_numero(self):
        rng = random.Random(0)
        self.assertIsNone(pick_house_number(rng, "2", 0, 0))

    def test_faixa_sem_numero_da_paridade_declarada_nao_produz_numero(self):
        rng = random.Random(0)
        self.assertIsNone(pick_house_number(rng, "2", 3, 3))
        self.assertEqual(pick_house_number(rng, "1", 3, 3), 3)

    def test_paridade_e_faixa_sempre_respeitadas(self):
        for customer in self.gen(count=300):
            numero = customer["house_number"]
            if numero is None:
                continue
            candidate = self.reference.candidate(customer["candidate_index"])
            esperado = 1 if candidate["numbering_type"] == "1" else 0
            self.assertEqual(numero % 2, esperado)
            self.assertGreaterEqual(numero, min(candidate["number_from"], candidate["number_to"]))
            self.assertLessEqual(numero, max(candidate["number_from"], candidate["number_to"]))

    def test_a_chave_house_number_existe_em_todo_registro(self):
        """Chave opcional faria o schema_fingerprint da particao depender da seed."""
        for customer in self.gen(count=100):
            self.assertIn("house_number", customer)

    def test_cada_tramo_produz_o_numero_que_a_faixa_real_permite(self):
        """Tramo a tramo, sem media: faixa degenerada e faixa sem paridade dao None; a
        faixa com um unico impar da exatamente aquele impar."""
        esperado = {3: None, 4: None, 5: 3}  # ver tests/support.py
        vistos = set()
        for customer in self.gen(count=400):
            indice = customer["candidate_index"]
            if indice in esperado:
                vistos.add(indice)
                self.assertEqual(
                    customer["house_number"],
                    esperado[indice],
                    f"tramo {indice} produziu {customer['house_number']!r}",
                )
        self.assertEqual(vistos, set(esperado), "a fixture nao exercitou os tres tramos")


class DistribuicaoTest(GeneratorTestCase):
    def test_o_peso_populacional_do_municipio_e_respeitado(self):
        clientes = self.gen(count=3000)
        contagem = collections.Counter(c["municipality_code"] for c in clientes)
        fracao = contagem["001"] / len(clientes)
        # Peso real na fixture: 0,9. Tolerancia larga o bastante para nao ser flaky e
        # estreita o bastante para reprovar peso ignorado (que daria 0,5).
        self.assertGreater(fracao, 0.86)
        self.assertLess(fracao, 0.94)

    def test_a_escolha_do_tramo_e_uniforme_e_nao_ponderada(self):
        """Populacao NAO e densidade de rua: dentro do municipio todo tramo tem a mesma
        chance. Ponderar aqui inventaria uma distribuicao que nenhuma fonte mediu."""
        clientes = [c for c in self.gen(count=3000) if c["municipality_code"] == "001"]
        contagem = collections.Counter(c["candidate_index"] for c in clientes)
        self.assertEqual(set(contagem), {0, 1, 2}, "algum tramo do municipio nunca foi sorteado")
        for indice in (0, 1, 2):
            fracao = contagem[indice] / len(clientes)
            self.assertGreater(fracao, 0.28, f"tramo {indice} sub-representado: {fracao:.3f}")
            self.assertLess(fracao, 0.39, f"tramo {indice} super-representado: {fracao:.3f}")

    def test_a_proporcao_de_sexo_do_municipio_e_respeitada(self):
        # whb tem sex_hombres_proportion = 1.0 na fixture: nenhuma mulher pode sair.
        clientes = self.gen(wh=support.WH_B, count=200)
        self.assertEqual(
            set(c["sex_label"] for c in clientes), {customers_generator.SEX_MALE}
        )

    def test_a_idade_vem_da_distribuicao_da_propria_provincia(self):
        clientes = self.gen(wh=support.WH_B, count=100, date="2026-08-27")
        # Provincia 02 so tem idades 40 e 41 na fixture.
        self.assertEqual(
            set(c["birth_year"] for c in clientes), {2026 - 40, 2026 - 41}
        )


class IdentidadeTest(GeneratorTestCase):
    def test_customer_id_e_sequencial_unico_e_traz_o_warehouse(self):
        clientes = self.gen(count=50)
        ids = [c["customer_id"] for c in clientes]
        self.assertEqual(len(set(ids)), len(ids))
        self.assertEqual(ids[0], f"cust_{support.WH_A}_000000")
        self.assertEqual(ids[-1], f"cust_{support.WH_A}_000049")

    def test_customer_id_nao_deriva_de_nenhum_campo_geografico(self):
        """A identidade e do cliente; o endereco e atribuicao. Trocar a seed muda o
        endereco de cust_..._000000 e mantem o id — prova que um nao deriva do outro."""
        a = self.gen(count=20, seed=1)[0]
        b = self.gen(count=20, seed=2)[0]
        self.assertEqual(a["customer_id"], b["customer_id"])
        self.assertNotEqual(
            (a["candidate_index"], a["postal_code"]),
            (b["candidate_index"], b["postal_code"]),
        )

    def test_ids_nao_colidem_entre_warehouses_da_mesma_data(self):
        a = {c["customer_id"] for c in self.gen(wh=support.WH_A, count=30)}
        b = {c["customer_id"] for c in self.gen(wh=support.WH_B, count=30)}
        self.assertEqual(a & b, set())

    def test_dois_clientes_podem_compartilhar_o_mesmo_endereco(self):
        """Endereco e atribuicao, nao identidade: repeticao e esperada, nao e colisao."""
        clientes = self.gen(count=300)
        contagem = collections.Counter(c["candidate_index"] for c in clientes)
        self.assertTrue(any(n > 1 for n in contagem.values()))


class ErroTest(GeneratorTestCase):
    def test_count_invalido_e_recusado(self):
        with self.assertRaises(GenerationError):
            self.gen(count=0)

    def test_warehouse_desconhecido_e_recusado(self):
        with self.assertRaises(reference_data.ReferenceError):
            self.gen(wh="inexistente")


if __name__ == "__main__":
    unittest.main()
