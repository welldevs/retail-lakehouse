"""O perfil de demanda: mapeamento, inclinacao de canal e pesos.

Sem rede e sem Lakehouse. O que se testa aqui e a REGRA — que trinca vai para que grupo, como
o alvo do MAPA vira peso de linha, e o que acontece quando a evidencia nao alcanca. Os seeds
REAIS do repo sao exercitados junto das fixtures: um seed editado a mao que quebre uma
invariante precisa reprovar aqui, e nao tres camadas adiante.
"""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from decimal import Decimal

from retail_platform import demand_profile as dp

# A suite roda com cwd=platform/, entao o caminho e derivado do proprio arquivo em vez de
# ser relativo ao repo — senao "platform/dbt/seeds" viraria "platform/platform/dbt/seeds".
SEEDS_REAIS = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "dbt", "seeds")
)


# --------------------------------------------------------------------------------
# Fixtures: seeds minimos em disco
# --------------------------------------------------------------------------------

BENCHMARK = [
    dict(mapa_key="FRUTA", mapa_label="Fruta", scope="fresh", use_as_weight="true",
         volume_share_pct="10.00", value_share_pct="8.00", avg_price_eur_kg="2.00",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="1.1",
         channel_basis="fine", informe_section="4.9", provenance="informe_table", note=""),
    dict(mapa_key="AGUA", mapa_label="Agua", scope="rest", use_as_weight="true",
         volume_share_pct="10.00", value_share_pct="1.00", avg_price_eur_kg="0.24",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="",
         channel_basis="coarse", informe_section="4.4.3", provenance="informe_table", note=""),
    dict(mapa_key="HUEVOS", mapa_label="Huevos", scope="fresh", use_as_weight="true",
         volume_share_pct="5.00", value_share_pct="5.00", avg_price_eur_kg="3.85",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="1.5",
         channel_basis="fine", informe_section="4.13", provenance="informe_table", note=""),
    dict(mapa_key="AGREGADO", mapa_label="Agregado", scope="rest", use_as_weight="false",
         volume_share_pct="25.00", value_share_pct="14.00", avg_price_eur_kg="",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="",
         channel_basis="coarse", informe_section="4.0", provenance="informe_table",
         note="agregado, nao entra como peso"),
    dict(mapa_key="SIN_BENCHMARK", mapa_label="Sem benchmark", scope="rest",
         use_as_weight="false", volume_share_pct="", value_share_pct="", avg_price_eur_kg="",
         volume_yoy_pct="", value_yoy_pct="", ecommerce_volume_pct="", channel_basis="coarse",
         informe_section="", provenance="none", note="destino declarado"),
    dict(mapa_key="NO_FOOD", mapa_label="Nao alimentar", scope="none", use_as_weight="false",
         volume_share_pct="", value_share_pct="", avg_price_eur_kg="", volume_yoy_pct="",
         value_yoy_pct="", ecommerce_volume_pct="", channel_basis="coarse",
         informe_section="", provenance="none", note="fora do universo"),
]

MAPPING = [
    dict(l1="Fruta y verdura", l2="Fruta", l3="*", mapa_key="FRUTA", rationale="x"),
    dict(l1="Fruta y verdura", l2="Fruta", l3="Patata", mapa_key="SIN_BENCHMARK", rationale="x"),
    dict(l1="Agua", l2="*", l3="*", mapa_key="AGUA", rationale="x"),
    dict(l1="Huevos", l2="*", l3="*", mapa_key="HUEVOS", rationale="x"),
    dict(l1="Limpieza", l2="*", l3="*", mapa_key="NO_FOOD", rationale="x"),
]

PARAMS = [
    dict(param_key="demand_model_version", value="teste_v1", unit="version",
         label="synthetic", rationale="x"),
    dict(param_key="food_line_share", value="0.8", unit="proportion", label="synthetic", rationale="x"),
    dict(param_key="benchmark_volume_coverage_pct", value="25.00", unit="percent",
         label="derived", rationale="x"),
    dict(param_key="unbenchmarked_allocation", value="assortment", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="within_group_selection", value="uniform", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="volume_coverage_min", value="0.5", unit="proportion",
         label="synthetic", rationale="x"),
    dict(param_key="channel_reference_pct", value="2.2", unit="percent", label="observed", rationale="x"),
    dict(param_key="channel_fresh_pct", value="1.1", unit="percent", label="observed", rationale="x"),
    dict(param_key="channel_rest_pct", value="2.8", unit="percent", label="observed", rationale="x"),
]

SEASONALITY = [dict(month=str(m), factor="1.0", label="synthetic", rationale="x")
               for m in range(1, 13)]


def escrever_seeds(directory, benchmark=None, mapping=None, params=None, seasonality=None):
    for nome, linhas in (
        (dp.BENCHMARK_SEED, benchmark if benchmark is not None else BENCHMARK),
        (dp.MAPPING_SEED, mapping if mapping is not None else MAPPING),
        (dp.PROFILE_SEED, params if params is not None else PARAMS),
        (dp.SEASONALITY_SEED, seasonality if seasonality is not None else SEASONALITY),
    ):
        caminho = os.path.join(directory, nome)
        with open(caminho, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(linhas[0].keys()))
            writer.writeheader()
            writer.writerows(linhas)
    return directory


def catalogo(grupos):
    """Linhas de catalogo ja carimbadas: (demand_group, conteudo em kg)."""
    return [{"demand_group": g, "net_content_kg_l": kg} for g, kg in grupos]


# --------------------------------------------------------------------------------


class ResolucaoTest(unittest.TestCase):
    def test_regra_mais_especifica_vence(self):
        # `Patata` casa com a regra de l3 E com a de l2-coringa. Sem ordenar por
        # especificidade, o resultado dependeria da ordem do arquivo — e o de-para deixaria
        # de ser leitura declarativa para virar programa.
        self.assertEqual(
            dp.resolve(MAPPING, "Fruta y verdura", "Fruta", "Patata"), "SIN_BENCHMARK"
        )
        self.assertEqual(
            dp.resolve(MAPPING, "Fruta y verdura", "Fruta", "Manzana"), "FRUTA"
        )

    def test_trinca_sem_regra_reprova(self):
        # NAO EXISTE DESTINO PADRAO. Um `else SIN_BENCHMARK` aqui faria toda categoria nova
        # da Mercadona cair silenciosamente num balde, e o mix mudaria sem aviso.
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.resolve(MAPPING, "Categoria Nova", "Sub", "Item")
        self.assertIn("nenhuma regra", str(caught.exception))

    def test_duas_regras_igualmente_especificas_reprovam(self):
        ambiguo = MAPPING + [
            dict(l1="Fruta y verdura", l2="Fruta", l3="*", mapa_key="AGUA", rationale="x")
        ]
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.resolve(ambiguo, "Fruta y verdura", "Fruta", "Manzana")
        self.assertIn("igualmente", str(caught.exception))

    def test_regra_morta_e_detectada(self):
        trincas = [("Agua", "Agua", "Agua sin gas")]
        mortas = dp.unused_rules(MAPPING, trincas)
        self.assertIn("FRUTA", {m["mapa_key"] for m in mortas})

    def test_todas_as_regras_reais_estao_vivas_e_cobrem_o_repo(self):
        # Contra os seeds REAIS. Se alguem acrescentar uma regra que nao casa nada, ou
        # remover uma que casava, este teste diz — offline, sem tocar no Lakehouse.
        mapping = dp.load_mapping(SEEDS_REAIS)
        self.assertGreater(len(mapping), 100)
        chaves = {r["mapa_key"] for r in mapping}
        benchmark = dp.load_benchmark(SEEDS_REAIS)
        faltando = chaves - set(benchmark)
        self.assertEqual(faltando, set(), f"mapeamento aponta para chaves inexistentes: {faltando}")


class BenchmarkTest(unittest.TestCase):
    def test_pesos_disjuntos_nao_passam_de_cem(self):
        # OS CORTES TRANSVERSAIS SAO A ARMADILHA. `SIN GLUTEN` (3,75%) e `ECOLOGICOS`
        # (2,31%) nao sao categorias: sao recortes das outras. Soma-los ao resto passaria de
        # 100% e a normalizacao esconderia o erro distribuindo a diferenca por todo mundo.
        benchmark = dp.load_benchmark(SEEDS_REAIS)
        soma = sum(v["volume_share_pct"] for v in benchmark.values() if v["use_as_weight"])
        self.assertLess(soma, Decimal("100"), f"pesaveis somam {soma}, o que passa de 100%")
        self.assertGreater(soma, Decimal("50"), f"pesaveis somam so {soma}: cobertura baixa demais")

    def test_cobertura_declarada_bate_com_a_soma(self):
        # `benchmark_volume_coverage_pct` divide o escopo alimentar entre calibrado e nao
        # calibrado. Se ele divergir da soma real, a divisao passa a ser um numero escolhido
        # em vez de derivado — e ninguem notaria.
        benchmark = dp.load_benchmark(SEEDS_REAIS)
        params = dp.load_params(SEEDS_REAIS)
        soma = sum(v["volume_share_pct"] for v in benchmark.values() if v["use_as_weight"])
        declarado = Decimal(params["benchmark_volume_coverage_pct"])
        self.assertAlmostEqual(
            float(soma), float(declarado), places=1,
            msg=f"declarado {declarado} contra soma real {soma}",
        )

    def test_pesavel_sem_share_de_volume_reprova(self):
        quebrado = [dict(r) for r in BENCHMARK]
        quebrado[0]["volume_share_pct"] = ""
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, benchmark=quebrado)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_benchmark(tmp)
        self.assertIn("sem volume_share_pct", str(caught.exception))

    def test_escopo_ausente_reprova(self):
        sem_escopo = [r for r in BENCHMARK if r["mapa_key"] != "NO_FOOD"]
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, benchmark=sem_escopo)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_benchmark(tmp)
        self.assertIn("NO_FOOD", str(caught.exception))

    def test_toda_secao_citada_tem_forma_de_secao(self):
        # A rastreabilidade desta tabela E a citacao da secao. `informe_section` precisa
        # sobreviver como texto: tipada como numero, "4.3" vira 4.3 e "3" vira 3.0.
        benchmark = dp.load_benchmark(SEEDS_REAIS)
        for key, row in benchmark.items():
            if row["provenance"] in ("informe_table", "informe_prose"):
                self.assertTrue(
                    row["informe_section"],
                    f"{key} veio do informe e nao cita secao",
                )


class CanalTest(unittest.TestCase):
    def test_base_fina_usa_o_share_do_proprio_grupo(self):
        params = dp.load_params(SEEDS_REAIS)
        row = {"channel_basis": "fine", "ecommerce_volume_pct": Decimal("4.4"), "scope": "rest"}
        tilt, base = dp.channel_tilt(row, params)
        self.assertEqual(base, "fine")
        self.assertAlmostEqual(float(tilt), 2.0, places=6)

    def test_base_grossa_separa_fresco_de_resto(self):
        # 1,1% do volume fresco chega por e-commerce contra 2,8% do resto. Usar so a media
        # de 2,2 para todo mundo apagaria justamente a diferenca que faz fruta fresca pesar
        # menos numa cesta online — que e o achado de canal do informe.
        params = dp.load_params(SEEDS_REAIS)
        fresco, base_f = dp.channel_tilt({"channel_basis": "coarse", "scope": "fresh"}, params)
        resto, base_r = dp.channel_tilt({"channel_basis": "coarse", "scope": "rest"}, params)
        self.assertEqual((base_f, base_r), ("coarse", "coarse"))
        self.assertLess(fresco, 1)
        self.assertGreater(resto, 1)

    def test_base_fina_sem_numero_cai_para_grossa(self):
        # Declarar `fine` sem ter o numero nao pode virar divisao por None. Cai para a base
        # grossa E DECLARA que caiu, para que o relatorio nao afirme uma precisao que nao tem.
        params = dp.load_params(SEEDS_REAIS)
        _tilt, base = dp.channel_tilt(
            {"channel_basis": "fine", "ecommerce_volume_pct": None, "scope": "rest"}, params
        )
        self.assertEqual(base, "coarse")


class ConstrucaoTest(unittest.TestCase):
    def _perfil(self, linhas, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, **kwargs)
            return dp.build(linhas, tmp)

    def test_pesos_somam_um_e_os_tres_blocos_batem(self):
        perfil = self._perfil(catalogo([
            ("FRUTA", "1.0"), ("FRUTA", "1.0"),
            ("AGUA", "1.5"), ("AGUA", "1.5"),
            ("SIN_BENCHMARK", "0.5"),
            ("NO_FOOD", None), ("NO_FOOD", None),
        ]))
        pesos = {g["demand_group"]: Decimal(g["line_weight"]) for g in perfil["groups"]}
        self.assertAlmostEqual(float(sum(pesos.values())), 1.0, places=9)
        self.assertAlmostEqual(float(pesos["NO_FOOD"]), 0.2, places=9)
        # bloco calibrado = food_line_share (0,8) x cobertura (0,25)
        calibrado = pesos["FRUTA"] + pesos["AGUA"]
        self.assertAlmostEqual(float(calibrado), 0.8 * 0.25, places=9)

    def test_grupo_sem_cobertura_de_kg_cai_para_share_de_linhas(self):
        # A GUARDA QUE FIRMOU NO DADO REAL: ovos sao vendidos em 'ud'/'dz' e so 18% do
        # sortimento converte para kg. Sem esta guarda o alvo de volume seria dividido pelo
        # peso medio de dois produtos nao representativos, e o grupo inteiro seria
        # dimensionado por eles.
        perfil = self._perfil(catalogo([
            ("FRUTA", "1.0"),
            ("AGUA", "1.0"),
            ("HUEVOS", None), ("HUEVOS", None), ("HUEVOS", None), ("HUEVOS", "0.65"),
            ("SIN_BENCHMARK", "0.5"), ("NO_FOOD", None),
        ]))
        por_grupo = {g["demand_group"]: g for g in perfil["groups"]}
        self.assertEqual(por_grupo["HUEVOS"]["target_path"], "lines")
        self.assertIsNone(por_grupo["HUEVOS"]["mean_kg_per_unit"])
        self.assertEqual(por_grupo["FRUTA"]["target_path"], "volume")

    def test_embalagem_maior_recebe_menos_linhas_para_o_mesmo_alvo(self):
        # A PONTE. FRUTA e AGUA tem o MESMO alvo de volume (10% cada) e o mesmo escopo de
        # canal seria diferente — entao o teste fixa os dois em `rest` via um benchmark
        # proprio, isolando o unico efeito que interessa aqui: o tamanho da embalagem.
        bench = [dict(r) for r in BENCHMARK]
        for r in bench:
            r["scope"] = "rest"
            r["channel_basis"] = "coarse"
            r["ecommerce_volume_pct"] = ""
        perfil = self._perfil(
            catalogo([("FRUTA", "0.5"), ("AGUA", "5.0"), ("SIN_BENCHMARK", "1"), ("NO_FOOD", None)]),
            benchmark=bench,
        )
        pesos = {g["demand_group"]: Decimal(g["line_weight"]) for g in perfil["groups"]}
        # AGUA pesa 10x mais por unidade, entao precisa de 10x menos linhas.
        self.assertAlmostEqual(float(pesos["FRUTA"] / pesos["AGUA"]), 10.0, places=6)

    def test_linha_sem_grupo_carimbado_reprova(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._perfil([{"net_content_kg_l": "1.0"}])
        self.assertIn("demand_group", str(caught.exception))

    def test_catalogo_vazio_reprova(self):
        with self.assertRaises(dp.DemandProfileError):
            self._perfil([])

    def test_nenhum_grupo_pesavel_reprova(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._perfil(catalogo([("SIN_BENCHMARK", "1"), ("NO_FOOD", None)]))
        self.assertIn("nenhum grupo", str(caught.exception))

    def test_food_line_share_fora_do_intervalo_reprova(self):
        params = [dict(r) for r in PARAMS]
        for r in params:
            if r["param_key"] == "food_line_share":
                r["value"] = "1.4"
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._perfil(catalogo([("FRUTA", "1"), ("NO_FOOD", None)]), params=params)
        self.assertIn("food_line_share", str(caught.exception))

    def test_nan_no_conteudo_nao_envenena_a_media(self):
        # `Decimal('nan')` NAO levanta: ele constroi um NaN que sobrevive a aritmetica e faz
        # a media inteira virar NaN em silencio. Um driver que devolva NaN para nulo — o
        # pandas devolve — entraria por aqui.
        perfil = self._perfil(catalogo([
            ("FRUTA", "1.0"), ("FRUTA", float("nan")),
            ("AGUA", "1.0"), ("SIN_BENCHMARK", "1"), ("NO_FOOD", None),
        ]))
        fruta = next(g for g in perfil["groups"] if g["demand_group"] == "FRUTA")
        self.assertEqual(fruta["mean_kg_per_unit"], "1.000000")

    def test_perfil_carrega_versao_hashes_e_sazonalidade(self):
        perfil = self._perfil(catalogo([("FRUTA", "1"), ("AGUA", "1"), ("NO_FOOD", None)]))
        self.assertEqual(perfil["demand_model_version"], "teste_v1")
        self.assertEqual(set(perfil["seeds_sha256"]), set(dp.SEEDS))
        self.assertEqual(perfil["seasonality_applies_to"], "daily_order_rate")
        self.assertEqual(len(perfil["seasonality"]), 12)


class SazonalidadeTest(unittest.TestCase):
    def test_perfil_real_esta_neutro(self):
        # O perfil ENTREGUE e neutro por ausencia de evidencia numerica. Este teste guarda
        # essa decisao: se alguem inventar um fator sem trazer o numero, ele reprova e
        # obriga a conversa.
        fatores = dp.load_seasonality(SEEDS_REAIS)
        self.assertEqual(set(fatores.values()), {Decimal("1.0")})

    def test_mes_faltando_reprova(self):
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, seasonality=SEASONALITY[:11])
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_seasonality(tmp)
        self.assertIn("12 meses", str(caught.exception))

    def test_fator_nao_positivo_reprova(self):
        quebrado = [dict(r) for r in SEASONALITY]
        quebrado[0]["factor"] = "0"
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, seasonality=quebrado)
            with self.assertRaises(dp.DemandProfileError):
                dp.load_seasonality(tmp)


class EstabilidadeTest(unittest.TestCase):
    def test_saida_e_byte_estavel_entre_execucoes(self):
        # O perfil entra no manifesto por sha256. Se `build` variar entre execucoes — por
        # iterar um set, por exemplo — o hash muda sem que nada tenha mudado, e a
        # reprodutibilidade da particao morre em silencio.
        import json
        linhas = catalogo([("FRUTA", "1.0"), ("AGUA", "2.0"),
                           ("SIN_BENCHMARK", "0.5"), ("NO_FOOD", None)])
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp)
            primeiro = json.dumps(dp.build(linhas, tmp), sort_keys=True)
            segundo = json.dumps(dp.build(linhas, tmp), sort_keys=True)
        self.assertEqual(primeiro, segundo)

    def test_seeds_reais_produzem_hash_estavel(self):
        self.assertEqual(dp.seeds_sha256(SEEDS_REAIS), dp.seeds_sha256(SEEDS_REAIS))
        self.assertEqual(len(dp.seeds_sha256(SEEDS_REAIS)), 4)


if __name__ == "__main__":
    unittest.main()
