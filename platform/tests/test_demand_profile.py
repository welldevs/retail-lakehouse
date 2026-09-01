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
    dict(param_key="cohort_dimensions", value="age,region", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="cohort_independence", value="multiplicative", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="cohort_calibration", value="ipf", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="ipf_tolerance", value="0.000000001", unit="proportion",
         label="synthetic", rationale="x"),
    dict(param_key="ipf_max_iterations", value="200", unit="iterations",
         label="synthetic", rationale="x"),
    dict(param_key="region_frequency_basis", value="per_capita_volume", unit="rule",
         label="synthetic", rationale="x"),
    dict(param_key="region_frequency_normalization", value="served_regions", unit="rule",
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

# Coortes da fixture. Os shares de VOLUME sao desiguais de proposito e diferentes entre os
# tres grupos: com indices iguais o perfil por coorte seria indistinguivel do agregado, e um
# teste que passa nas duas implementacoes nao testa nenhuma. As quatro faixas somam 100 nas
# duas colunas, que e o mesmo checksum que o seed real precisa fechar.
COHORT_AGE = []
for _key, _vols in (
    ("FRUTA", ("5.00", "20.00", "33.00", "42.00")),
    ("AGUA", ("15.00", "35.00", "30.00", "20.00")),
    ("HUEVOS", ("10.00", "25.00", "32.00", "33.00")),
):
    for _banda, _pop, _vol in zip(
        ("LT35", "35_49", "50_64", "GE65"),
        ("8.89", "30.33", "31.34", "29.44"),
        _vols,
    ):
        COHORT_AGE.append(dict(
            mapa_key=_key, age_band=_banda, population_share_pct=_pop,
            volume_share_pct=_vol, informe_page="1", provenance="informe_chart", note="",
        ))

COHORT_REGION = []
for _key, _vols in (
    ("FRUTA", ("18.00", "12.00")),
    ("AGUA", ("14.00", "9.00")),
    ("HUEVOS", ("16.00", "11.00")),
):
    for _ccaa, _label, _pop, _vol in zip(
        ("13", "09"), ("Madrid", "Cataluna"), ("13.86", "16.26"), _vols
    ):
        COHORT_REGION.append(dict(
            mapa_key=_key, ccaa_code=_ccaa, ccaa_label=_label,
            population_share_pct=_pop, volume_share_pct=_vol,
            informe_page="1", provenance="informe_chart", note="",
        ))

REGIONS = [
    dict(ccaa_code="00", ccaa_label="Total Espana", per_capita_kg_l="577.32",
         per_capita_eur="1874.75", informe_section="3", provenance="informe_prose", note=""),
    dict(ccaa_code="13", ccaa_label="Madrid", per_capita_kg_l="505.86",
         per_capita_eur="1754.95", informe_section="3", provenance="informe_prose", note=""),
    dict(ccaa_code="09", ccaa_label="Cataluna", per_capita_kg_l="620.82",
         per_capita_eur="2130.97", informe_section="3", provenance="informe_prose", note=""),
]

CCAA_MAP = [
    dict(province_code="28", province_name="Madrid", ccaa_code="13", ccaa_label="Madrid"),
    dict(province_code="08", province_name="Barcelona", ccaa_code="09", ccaa_label="Cataluna"),
]

# Base de clientes da fixture: as quatro faixas nas duas comunidades, com tamanhos
# desiguais — se todas as coortes tivessem a mesma massa, o IPF nunca teria o que corrigir.
COHORT_COUNTS = {
    ("LT35", "13"): 120, ("35_49", "13"): 200, ("50_64", "13"): 180, ("GE65", "13"): 150,
    ("LT35", "09"): 90, ("35_49", "09"): 160, ("50_64", "09"): 140, ("GE65", "09"): 170,
}
WAREHOUSE_REGIONS = {"mad1": "13", "bcn1": "09"}


def escrever_seeds(
    directory,
    benchmark=None,
    mapping=None,
    params=None,
    seasonality=None,
    cohort_age=None,
    cohort_region=None,
    regions=None,
    ccaa_map=None,
):
    for nome, linhas in (
        (dp.BENCHMARK_SEED, benchmark if benchmark is not None else BENCHMARK),
        (dp.MAPPING_SEED, mapping if mapping is not None else MAPPING),
        (dp.PROFILE_SEED, params if params is not None else PARAMS),
        (dp.SEASONALITY_SEED, seasonality if seasonality is not None else SEASONALITY),
        (dp.COHORT_AGE_SEED, cohort_age if cohort_age is not None else COHORT_AGE),
        (dp.COHORT_REGION_SEED, cohort_region if cohort_region is not None else COHORT_REGION),
        (dp.REGION_SEED, regions if regions is not None else REGIONS),
        (dp.CCAA_MAP_SEED, ccaa_map if ccaa_map is not None else CCAA_MAP),
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
    def _perfil(self, linhas, counts=None, regions=None, **kwargs):
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, **kwargs)
            return dp.build(
                linhas,
                tmp,
                customers_by_cohort=COHORT_COUNTS if counts is None else counts,
                warehouse_regions=WAREHOUSE_REGIONS if regions is None else regions,
            )

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
            saida = lambda: json.dumps(  # noqa: E731 - duas chamadas identicas, uma linha
                dp.build(
                    linhas,
                    tmp,
                    customers_by_cohort=COHORT_COUNTS,
                    warehouse_regions=WAREHOUSE_REGIONS,
                ),
                sort_keys=True,
            )
            primeiro, segundo = saida(), saida()
        self.assertEqual(primeiro, segundo)

    def test_seeds_reais_produzem_hash_estavel(self):
        self.assertEqual(dp.seeds_sha256(SEEDS_REAIS), dp.seeds_sha256(SEEDS_REAIS))
        self.assertEqual(len(dp.seeds_sha256(SEEDS_REAIS)), 8)

class CoorteTest(unittest.TestCase):
    """A camada de coorte, e os dois checksums que a leitura do informe precisa fechar.

    O erro realista nesta parte nao e de logica, e de LEITURA: os cortes demograficos do
    informe estao em graficos, e um digito trocado num rotulo produz um indice plausivel que
    nada reprova. As duas somas existem para que isso pare aqui.
    """

    def test_shares_de_volume_das_quatro_faixas_somam_cem(self):
        # Nos seeds REAIS do repo, e nao so nas fixtures: as 39 leituras precisam fechar.
        dados = dp.load_cohort_age(SEEDS_REAIS)
        self.assertEqual(len(dados), 39)
        for chave, linhas in sorted(dados.items()):
            soma = sum(v for _pop, v in linhas.values())
            self.assertLess(
                abs(soma - Decimal("100")), dp.COHORT_SUM_TOLERANCE,
                f"{chave}: volume soma {soma}",
            )

    def test_um_digito_trocado_no_seed_reprova(self):
        # A prova de que o checksum e util: sem ele, 42,27 -> 24,27 passaria e a faixa de
        # 65+ perderia 43% da sua propensao a fruta fresca sem que nada avisasse.
        errado = [dict(linha) for linha in COHORT_AGE]
        errado[3]["volume_share_pct"] = "24.00"
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, cohort_age=errado)
            with self.assertRaises(dp.DemandProfileError) as caught:
                dp.load_cohort_age(tmp)
        self.assertIn("soma", str(caught.exception))

    def test_share_de_populacao_e_o_mesmo_em_todo_grupo(self):
        # O `% Poblacion` e o UNIVERSO, nao uma medicao da categoria: ele tem de repetir em
        # toda secao do informe. Uma divergencia aqui e leitura errada, nao dado novo.
        #
        # Tolerancia, e nao igualdade: o proprio informe publica ora uma casa decimal (8,9)
        # ora duas (8,89), e uma secao — carne transformada, pagina 206 — traz 30,5/31,7/29,0
        # onde todas as outras trazem 30,3/31,3/29,4. A discrepancia e da fonte e esta
        # registrada na coluna `note` daquela linha.
        for carregar, esperado in (
            (dp.load_cohort_age, {"LT35": "8.89", "35_49": "30.33",
                                  "50_64": "31.34", "GE65": "29.44"}),
            (dp.load_cohort_region, {"09": "16.26", "10": "11.14",
                                     "01": "17.52", "13": "13.86"}),
        ):
            dados = carregar(SEEDS_REAIS)
            for chave, linhas in sorted(dados.items()):
                for coorte, (populacao, _vol) in sorted(linhas.items()):
                    self.assertLess(
                        abs(populacao - Decimal(esperado[coorte])), Decimal("0.5"),
                        f"{chave}/{coorte}: populacao {populacao}, universo {esperado[coorte]}",
                    )

    def test_indice_de_afinidade_e_adimensional(self):
        # 1,0 significa "compra na proporcao do proprio tamanho". Nao ha teto nem piso: o
        # informe mede AGUA em 4,3% do volume para 13,86% da populacao de Madrid, e aparar
        # esse 0,31 seria descartar a medicao mais forte que a fonte publica.
        self.assertEqual(
            dp.affinity_index(Decimal("20"), Decimal("20")), Decimal("1")
        )
        regiao = dp.load_cohort_region(SEEDS_REAIS)["AGUA"]["13"]
        self.assertLess(dp.affinity_index(*regiao), Decimal("0.4"))

    def test_indices_de_frequencia_somam_o_numero_de_regioes(self):
        # A renormalizacao e o que mantem o TOTAL de pedidos onde estava. Sem ela, os quatro
        # armazens servidos — que consomem menos que a media espanhola — produziriam alguns
        # por cento menos pedidos sem que isso significasse nada.
        referencia = dp.load_region_reference(SEEDS_REAIS)
        servidas = ["01", "09", "10", "13"]
        indices = dp.region_frequency(referencia, servidas)
        self.assertEqual(len(indices), 4)
        self.assertAlmostEqual(float(sum(indices.values())), 4.0, places=9)
        self.assertGreater(indices["09"], indices["13"])

    def test_frequencia_ponderada_pela_base_tambem_soma_um(self):
        # Com bases desiguais, a media que normaliza precisa ser PONDERADA — senao a soma
        # ponderada dos indices deixa de ser 1 e o total de pedidos anda.
        referencia = dp.load_region_reference(SEEDS_REAIS)
        pesos = {"01": 100, "09": 300, "10": 50, "13": 550}
        indices = dp.region_frequency(referencia, list(pesos), weights=pesos)
        total = sum(Decimal(pesos[c]) for c in pesos)
        ponderada = sum(Decimal(pesos[c]) * indices[c] for c in sorted(pesos)) / total
        self.assertAlmostEqual(float(ponderada), 1.0, places=9)

    def test_provincia_sem_comunidade_reprova(self):
        # Sem destino padrao, pelo mesmo motivo do de-para de categoria: uma provincia nova
        # herdaria em silencio o perfil de consumo de outra regiao.
        mapa = dp.load_province_ccaa(SEEDS_REAIS)
        self.assertEqual(dp.ccaa_of(mapa, "08"), "09")
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.ccaa_of(mapa, "33")
        self.assertIn("nao existe comunidade padrao", str(caught.exception))

    def test_ipf_com_indices_planos_e_a_identidade(self):
        # Quando nao ha nada a corrigir, o IPF nao pode corrigir nada. Sem isto, um IPF que
        # "ajusta" pesos ja corretos deslocaria a calibracao agregada em toda execucao.
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        massa = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        plano = {c: {g: Decimal("1") for g in base} for c in massa}
        fator, iteracoes, desvio = dp.ipf_calibrate(
            base, plano, massa, Decimal("1e-9"), 200
        )
        self.assertEqual(iteracoes, 1)
        self.assertEqual(set(fator.values()), {Decimal("1")})
        self.assertEqual(desvio, Decimal("0"))

    def test_ipf_converge_e_reproduz_os_pesos_agregados(self):
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        massa = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        indice = {
            "x": {"A": Decimal("1.5"), "B": Decimal("0.5"), "C": Decimal("1.0")},
            "y": {"A": Decimal("0.4"), "B": Decimal("2.0"), "C": Decimal("1.0")},
        }
        fator, _it, desvio = dp.ipf_calibrate(base, indice, massa, Decimal("1e-9"), 200)
        self.assertLessEqual(desvio, Decimal("1e-9"))

        agregado = {g: Decimal("0") for g in base}
        for coorte, peso in massa.items():
            bruto = {g: base[g] * fator[g] * indice[coorte][g] for g in base}
            total = sum(bruto.values())
            for g in base:
                agregado[g] += peso * bruto[g] / total
        for g in base:
            self.assertAlmostEqual(float(agregado[g]), float(base[g]), places=8)

    def test_ipf_que_nao_converge_reprova(self):
        # Tolerancia abaixo da precisao do proprio Decimal (28 digitos): o alvo nunca e
        # alcancado, e o modulo precisa REPROVAR em vez de devolver uma matriz que nao
        # fecha. Aceita-la em silencio desfaria a calibracao agregada da versao anterior.
        base = {"A": Decimal("0.5"), "B": Decimal("0.3"), "C": Decimal("0.2")}
        massa = {"x": Decimal("0.6"), "y": Decimal("0.4")}
        indice = {
            "x": {"A": Decimal("40"), "B": Decimal("0.05"), "C": Decimal("1")},
            "y": {"A": Decimal("0.02"), "B": Decimal("60"), "C": Decimal("1")},
        }
        with self.assertRaises(dp.DemandProfileError) as caught:
            dp.ipf_calibrate(base, indice, massa, Decimal("1e-40"), 5)
        self.assertIn("nao convergiu", str(caught.exception))


class PerfilPorCoorteTest(unittest.TestCase):
    """O perfil completo com a camada de coorte, sobre as fixtures."""

    def _perfil(self, **kwargs):
        linhas = catalogo([
            ("FRUTA", "1.0"), ("FRUTA", "1.0"),
            ("AGUA", "1.5"), ("AGUA", "1.5"),
            ("HUEVOS", None), ("HUEVOS", None),
            ("SIN_BENCHMARK", "0.5"),
            ("NO_FOOD", None), ("NO_FOOD", None),
        ])
        with tempfile.TemporaryDirectory() as tmp:
            escrever_seeds(tmp, **kwargs)
            return dp.build(
                linhas, tmp,
                customers_by_cohort=COHORT_COUNTS,
                warehouse_regions=WAREHOUSE_REGIONS,
            )

    def test_o_agregado_nao_se_move(self):
        # O CRITERIO DE ACEITACAO DA FASE INTEIRA. A media dos pesos por coorte, ponderada
        # pela massa real das coortes, tem de reproduzir os pesos agregados — senao esta
        # camada teria desfeito a calibracao da anterior de lado, sem nada falhar.
        perfil = self._perfil()
        base = {g["demand_group"]: Decimal(g["line_weight"]) for g in perfil["groups"]}
        coortes = perfil["cohorts"]
        massa = {m["cohort"]: Decimal(m["share"]) for m in coortes["mass"]}
        pesos = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in coortes["weights"]
        }
        self.assertAlmostEqual(float(sum(massa.values())), 1.0, places=9)
        for grupo, esperado in base.items():
            obtido = sum(massa[c] * pesos[c][grupo] for c in massa)
            self.assertAlmostEqual(float(obtido), float(esperado), places=8, msg=grupo)

    def test_a_fatia_de_cada_bloco_e_constante_entre_coortes(self):
        # NO_FOOD e SIN_BENCHMARK tem indice neutro por AUSENCIA DE EVIDENCIA. Sem confinar
        # o IPF a cada bloco, eles absorviam o residuo da normalizacao e o modelo passava a
        # afirmar que idoso compra menos drogaria — numero que ninguem mediu, e maior que a
        # maioria dos efeitos que sao medidos.
        perfil = self._perfil()
        blocos = {g["demand_group"]: g["block"] for g in perfil["groups"]}
        pesos = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in perfil["cohorts"]["weights"]
        }
        for bloco in sorted(set(blocos.values())):
            fatias = [
                sum(p[g] for g, b in blocos.items() if b == bloco) for p in pesos.values()
            ]
            # Tolerancia no ultimo digito do Decimal, e nao igualdade exata: a divisao que
            # normaliza cada bloco tem 28 digitos significativos, e o residuo de
            # arredondamento nao e o defeito que este teste procura — o defeito e a fatia
            # ANDAR, na terceira casa ou antes.
            self.assertLess(
                max(fatias) - min(fatias), Decimal("1e-20"),
                f"{bloco} varia entre coortes: min {min(fatias)} max {max(fatias)}",
            )

    def test_coortes_diferentes_produzem_pesos_diferentes(self):
        # O par do teste acima: sem ele, "o agregado nao se move" e "o bloco e constante"
        # passariam tambem numa implementacao que devolve o mesmo vetor para toda coorte.
        perfil = self._perfil()
        pesos = {
            v["cohort"]: {g["demand_group"]: Decimal(g["line_weight"]) for g in v["groups"]}
            for v in perfil["cohorts"]["weights"]
        }
        self.assertGreater(pesos["GE65|13"]["FRUTA"], pesos["LT35|13"]["FRUTA"] * 2)
        self.assertLess(pesos["GE65|13"]["AGUA"], pesos["LT35|13"]["AGUA"])

    def test_grupo_pesavel_sem_corte_demografico_reprova(self):
        # Deixar um grupo neutro em silencio esconderia uma lacuna de EXTRACAO atras de um
        # mix plausivel — e a extracao e a parte desta fase mais sujeita a erro humano.
        parcial = [linha for linha in COHORT_AGE if linha["mapa_key"] != "AGUA"]
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._perfil(cohort_age=parcial)
        self.assertIn("sem corte demografico", str(caught.exception))

    def test_armazem_em_comunidade_sem_consumo_publicado_reprova(self):
        with self.assertRaises(dp.DemandProfileError) as caught:
            self._perfil(regions=[r for r in REGIONS if r["ccaa_code"] != "09"])
        self.assertIn("consumo per capita", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
