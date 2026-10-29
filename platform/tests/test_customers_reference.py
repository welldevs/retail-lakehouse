"""As tres queries do export, contra fixtures DuckDB reais — sem rede, sem MinIO.

Cada caso da fixture reproduz um problema MEDIDO no Lakehouse de verdade, e existe para
que ele seja pego aqui e nao la:

  * o municipio cujos tramos penduram em nucleo, nunca na linha agregada: o join ingenuo
    por unit_code devolvia 0 de 7.194 tramos de Valencia;
  * o tramo que nao casa nem com rua nem com pseudovia (3 no escopo real): tem de ser
    excluido E contado;
  * o municipio com serie homonima vinda de outra provincia (Torrent/Girona=182 contra
    Torrent/Valencia=90.928): resolvido no modelo Silver pelo codigo oficial do INE, e
    aqui apenas RECONFERIDO — se voltar a aparecer, o export recusa em vez de escolher;
  * os rotulos de idade agregados 'Total' e '85 y mas anos', que somados as 101 idades
    simples inflam a piramide em 2,03x;
  * a tabela de series duplicada em duas ingestion_date, que dobraria cada provincia.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.oltp_reference import (  # noqa: E402
    ReferenceExportError,
    build,
    write,
)

try:
    import duckdb
except ImportError:  # pragma: no cover
    duckdb = None


SERVICE_AREA_CSV = """wh,province_code,municipality_code,municipality_name,is_home_municipality
wh1,46,244,Torrent,false
wh1,46,250,València,true
wh2,08,019,Barcelona,true
"""

PROVINCE_MAP_CSV = """wh,province_code,province_name,municipio_code,municipio_name
wh1,46,Valencia/València,250,València
wh2,08,Barcelona,019,Barcelona
"""

# As quatro premissas do cadastro. `min_customer_age` corta a piramide e
# `customer_penetration_source` APONTA para o outro seed — o 2,2 nao aparece aqui, e um teste
# abaixo prova que ele nao aparece.
CUSTOMER_PREMISES_CSV = """premise_key,value,unit,label,rationale
min_customer_age,18,years,synthetic,"Capacidade legal para contratar."
customer_penetration_source,demand_profile.channel_reference_pct,reference,synthetic,"Ponteiro, nao copia."
customer_population_basis,adult_resident_population,rule,synthetic,"O denominador e adulto."
customer_allocation,per_warehouse_population,rule,synthetic,"O total e consequencia."
"""

# So a linha que este export le. O seed real tem 17 parametros; repetir todos aqui faria a
# fixture envelhecer junto do modelo de demanda sem nenhum ganho.
DEMAND_PROFILE_CSV = """param_key,value,unit,label,rationale
channel_reference_pct,2.2,percent,observed,"E-commerce no volume de alimentacao, MAPA sec.3."
"""

SCHEMA = """
create table silver_callejero_tramos (
    ingestion_date varchar, is_latest_ingestion boolean, section_code varchar, province_code varchar,
    municipality_code varchar, entity_suffix varchar, unit_code varchar,
    street_id varchar, street_code varchar, pseudo_address_id varchar,
    pseudo_address_code varchar, postal_code varchar, numbering_type varchar,
    number_from varchar, number_from_qualifier varchar,
    number_to varchar, number_to_qualifier varchar
);
create table silver_callejero_population_units (
    ingestion_date varchar, is_latest_ingestion boolean, unit_code varchar, province_code varchar,
    municipality_code varchar, entity_suffix varchar,
    is_municipality_aggregate boolean, municipality_name varchar,
    population_unit_name varchar
);
create table silver_callejero_streets (
    ingestion_date varchar, is_latest_ingestion boolean, street_code varchar, street_name_full varchar
);
create table silver_callejero_pseudo_addresses (
    ingestion_date varchar, is_latest_ingestion boolean, pseudo_address_code varchar, name_full varchar
);
create table silver_ine_population_by_municipality (
    ingestion_date varchar, is_latest_ingestion boolean, series_code varchar, province_code varchar,
    municipality_code varchar, municipality_name varchar, sex_label varchar,
    year bigint, reference_date varchar, population_value double
);
create table silver_ine_population_series (
    ingestion_date varchar, is_latest_ingestion boolean, table_id varchar, province_name varchar,
    age_label varchar, sex_label varchar, year bigint, fk_periodo bigint,
    reference_date varchar, fk_tipo_dato bigint, population_value double
);
"""

TODAY = "2026-08-26"
YESTERDAY = "2026-08-25"


def tramo(municipality, street_id, code, postal, numbering="1", low="0001", high="0009",
          unit="4625000101", section="4625001001", pseudo_id="00000", pseudo_code="00000",
          latest=True):
    province = "08" if municipality == "019" else "46"
    return (
        (TODAY if latest else YESTERDAY), latest, section, province, municipality, "01", unit,
        street_id, code, pseudo_id, pseudo_code, postal, numbering,
        low, "", high, "",
    )


@unittest.skipIf(duckdb is None, "duckdb nao instalado")
class OltpReferenceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.seeds = os.path.join(self.tmp.name, "seeds")
        os.makedirs(self.seeds)
        self._seed("warehouse_service_area_seed.csv", SERVICE_AREA_CSV)
        self._seed("warehouse_province_map_seed.csv", PROVINCE_MAP_CSV)
        self._seed("customer_premises_seed.csv", CUSTOMER_PREMISES_CSV)
        self._seed("demand_profile_seed.csv", DEMAND_PROFILE_CSV)

        self.connection = duckdb.connect(":memory:")
        self.addCleanup(self.connection.close)
        self.connection.execute(SCHEMA)
        self._popular()

    def _seed(self, name, content):
        with open(os.path.join(self.seeds, name), "w", encoding="utf-8") as handle:
            handle.write(content)

    def _insert(self, table, rows):
        if not rows:
            return
        marks = ", ".join(["?"] * len(rows[0]))
        self.connection.executemany(f"insert into {table} values ({marks})", rows)

    def _popular(self):
        self._insert("silver_callejero_tramos", [
            # Torrent (46/244): dois tramos em ruas nomeadas.
            tramo("244", "00001", "4624400001", "46900", unit="4624400101"),
            tramo("244", "00002", "4624400002", "46901", numbering="2",
                  low="0002", high="0010", unit="4624400101"),
            # València (46/250): o caso que quebrava. Os tramos penduram em unit_code de
            # NUCLEO; a linha agregada do municipio tem outro unit_code.
            tramo("250", "00010", "4625000010", "46001", unit="4625000101"),
            tramo("250", "00011", "4625000011", "46002", unit="4625000102"),
            # València: uma pseudovia (street_id sentinela "00000").
            tramo("250", "00000", "4625000000", "46003", numbering="0",
                  low="0000", high="0000", unit="4625000101",
                  pseudo_id="00007", pseudo_code="4625000007"),
            # València: ORFAO — nao casa com rua nem com pseudovia.
            tramo("250", "09999", "4625009999", "46004", unit="4625000101"),
            # Barcelona (08/019), outro warehouse.
            tramo("019", "00050", "0801900050", "08001", unit="0801900101"),
            # Extracao ANTERIOR da mesma publicacao: o Silver empilha as duas datas. Se o
            # export nao filtrasse por is_latest_ingestion, este tramo entraria de novo e
            # cada endereco de Torrent apareceria duas vezes entre os candidatos.
            tramo("244", "00001", "4624400001", "46900", unit="4624400101", latest=False),
        ])
        self._insert("silver_callejero_population_units", [
            # Linhas agregadas do municipio: unit_code TERMINADO EM 0000, diferente do
            # unit_code dos tramos. E exatamente por isso que o join tem de ser por
            # (province_code, municipality_code) e nao por unit_code.
            (TODAY, True, "4624400000", "46", "244", "00", True, "TORRENT", ""),
            (TODAY, True, "4625000000", "46", "250", "00", True, "VALÈNCIA", ""),
            (TODAY, True, "0801900000", "08", "019", "00", True, "BARCELONA", ""),
            # Nucleos/pedanias onde os tramos realmente penduram.
            (TODAY, True, "4624400101", "46", "244", "01", False, "TORRENT", "TORRENT"),
            (TODAY, True, "4625000101", "46", "250", "01", False, "VALÈNCIA", "VALENCIA"),
            (TODAY, True, "4625000102", "46", "250", "01", False, "VALÈNCIA", "EL SALER"),
            (TODAY, True, "0801900101", "08", "019", "01", False, "BARCELONA", "BARCELONA"),
        ])
        self._insert("silver_callejero_streets", [
            (TODAY, True, "4624400001", "CARRER U"),
            (TODAY, True, "4624400002", "CARRER DOS"),
            (TODAY, True, "4625000010", "AVINGUDA DEL PORT"),
            (TODAY, True, "4625000011", "CAMI DEL SALER"),
            (TODAY, True, "0801900050", "CARRER DE MALLORCA"),
        ])
        self._insert("silver_callejero_pseudo_addresses", [
            (TODAY, True, "4625000007", "PARC DE CAPCALERA"),
        ])

        populacao = []
        for municipality, name, total, male in (
            ("244", "Torrent", 90928, 44000),
            ("250", "València", 800215, 380000),
            ("019", "Barcelona", 1660122, 790000),
        ):
            province = "08" if municipality == "019" else "46"
            for sex, value in (
                ("Total", total), ("Hombres", male), ("Mujeres", total - male)
            ):
                populacao.append(
                    (TODAY, True, f"DP{municipality}{sex[:1]}", province, municipality,
                     name, sex, 2025, "2024-12-31", float(value))
                )
        self._insert("silver_ine_population_by_municipality", populacao)

        series = []
        # Duas ingestion_date com linhas identicas: so a mais recente pode ser usada.
        for ingestion in (YESTERDAY, TODAY):
            latest = ingestion == TODAY
            for province_name in ("Valencia/València", "Barcelona"):
                simples = [(age, 100.0) for age in range(0, 101)]
                for age, value in simples:
                    rotulo = "100 y más años" if age == 100 else f"{age} años"
                    series.append((ingestion, latest, "31304", province_name, rotulo,
                                   "Ambos sexos", 2022, 27, "2022-06-30", 2, value))
                # Agregados sobrepostos: somados aos simples, inflariam a piramide.
                series.append((ingestion, latest, "31304", province_name, "Total",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 10100.0))
                series.append((ingestion, latest, "31304", province_name, "85 y más años",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 1600.0))
                # Rotulo que nao e provincia nossa.
                series.append((ingestion, latest, "31304", "Total Nacional", "30 años",
                               "Ambos sexos", 2022, 27, "2022-06-30", 2, 999.0))
        self._insert("silver_ine_population_series", series)

    def _injetar_homonimo(self):
        """Regressao do fanout: segunda serie de 'Torrent', a de Girona, com 182.

        O modelo Silver passou a resolver isso pelo codigo oficial do INE
        (ine_ambiguous_series_seed), entao esta linha nao deveria mais existir. Se voltar
        a existir, o export tem de RECUSAR — nao escolher um valor por conta propria.
        """
        self._insert("silver_ine_population_by_municipality", [
            (TODAY, True, f"DPGIRONA{sex[:1]}", "46", "244", "Torrent", sex, 2025,
             "2024-12-31", float(value))
            for sex, value in (("Total", 182), ("Hombres", 90), ("Mujeres", 92))
        ])

    def build(self):
        return build(self.connection, seeds_dir=self.seeds)


class AlocacaoDeClientesTest(OltpReferenceTestCase):
    """Quantos clientes cada armazem tem, e de onde esse numero vem.

    Ate a Fase 5 eram 5.000 por armazem, digitados na linha de comando — o mesmo numero para
    AUFs que diferem por 4,6x em populacao. Nada reprovava: os totais fechavam, os enderecos
    eram reais, o manifesto batia. A unica coisa errada era que a densidade nao existia, e
    densidade nao aparece em nenhum total.
    """

    # Fixture: wh1 = Torrent 90.928 + Valencia 800.215 = 891.143 habitantes; wh2 = Barcelona
    # 1.660.122. A piramide tem 101 idades de valor igual, entao o share adulto e 83/101.
    ADULT_SHARE = 83 / 101

    def test_alocacao_deriva_da_populacao_adulta_por_armazem(self):
        alocacao = self.build()["weights"]["customer_allocation"]
        por_wh = {linha["wh"]: linha for linha in alocacao["by_warehouse"]}

        self.assertEqual(por_wh["wh1"]["population_total"], 891143)
        self.assertEqual(por_wh["wh2"]["population_total"], 1660122)
        # 891.143 x 83/101 x 2,2% = 16.111,16 -> 16.111
        self.assertEqual(por_wh["wh1"]["customers"], 16111)
        # 1.660.122 x 83/101 x 2,2% = 30.013,69 -> 30.014
        self.assertEqual(por_wh["wh2"]["customers"], 30014)
        self.assertEqual(alocacao["total_customers"], 16111 + 30014)

    def test_alocar_por_populacao_total_daria_outro_numero(self):
        """O denominador ADULTO nao e decorativo: com o total, wh1 teria 1.494 a mais.

        Sem este caso, trocar `adult_population` por `population_total` no export passaria em
        todos os outros testes — as proporcoes entre armazens quase nao mudam, e o total
        continuaria plausivel.
        """
        alocacao = self.build()["weights"]["customer_allocation"]
        por_wh = {linha["wh"]: linha for linha in alocacao["by_warehouse"]}
        pela_populacao_total = round(891143 * 0.022)
        self.assertEqual(pela_populacao_total, 19605)
        self.assertNotEqual(por_wh["wh1"]["customers"], pela_populacao_total)

    def test_o_total_e_consequencia_e_nao_cota_repartida(self):
        """Somar as partes tem de dar o total, e nenhuma parte pode ser um resto.

        Com cota repartida, o ultimo armazem receberia `total - soma(os outros)` e absorveria
        todo o arredondamento. Aqui cada armazem sai da propria populacao e o total e a soma.
        """
        alocacao = self.build()["weights"]["customer_allocation"]
        self.assertEqual(
            alocacao["total_customers"],
            sum(linha["customers"] for linha in alocacao["by_warehouse"]),
        )
        self.assertEqual(
            alocacao["served_population"],
            sum(linha["population_total"] for linha in alocacao["by_warehouse"]),
        )

    def test_a_alocacao_e_uma_lista_ordenada_e_estavel(self):
        """LISTA, e nao dict — e o `sorted()` e a segunda tranca, nao a primeira.

        Honestidade sobre o que este caso prova e o que ele nao prova: `_population_sql` ja
        devolve as linhas com `order by a.wh`, entao remover o `sorted()` do modulo NAO faria
        este caso reprovar. O que ele protege de verdade e a forma — uma lista, que sobrevive
        ao JSON com a ordem intacta — contra um dict, cuja iteracao do outro lado da fronteira
        dependeria de ordem de insercao. O `sorted()` continua no modulo porque a garantia nao
        deve depender de um `order by` numa consulta que pode ser reescrita.
        """
        primeira = self.build()["weights"]["customer_allocation"]
        segunda = self.build()["weights"]["customer_allocation"]
        self.assertIsInstance(primeira["by_warehouse"], list)
        nomes = [linha["wh"] for linha in primeira["by_warehouse"]]
        self.assertEqual(nomes, sorted(nomes))
        self.assertEqual(
            json.dumps(primeira, sort_keys=True), json.dumps(segunda, sort_keys=True)
        )

    def test_a_alocacao_nao_fica_na_beira_do_arredondamento(self):
        """A margem que faz o arredondamento nao ser um cara-ou-coroa.

        MEDIDO no Lakehouse real: duas execucoes de `export-oltp-reference` produzem
        `adult_share` diferentes no ULTIMO BIT do double — 0,8224668719886548 contra
        0,8224668719886545 — porque a agregacao paralela do DuckDB nao fixa a ordem da soma
        de ponto flutuante. Isso nao e defeito deste modulo e nao propaga: os quatro alvos
        saem identicos, e a base gerada a partir dos dois exports tem o mesmo sha256.

        Mas so nao propaga porque nenhum dos quatro produtos cai perto de um `.5`. Se um dia
        cair, o alvo passa a alternar entre dois inteiros de export para export, a base muda
        de tamanho sem que nada tenha sido decidido, e o teste dbt
        `assert_customer_base_follows_the_declared_population_allocation` — que tolera 1
        cliente exatamente por causa disto — comeca a passar por sorte. Este caso e o aviso.

        A margem mais apertada hoje e a de mad1: 128.770,524404, a 0,024 de um empate. Em
        populacao isso e cerca de 135 residentes — se Madrid crescer ou encolher esse tanto na
        proxima 29005, o alvo passa a alternar entre 128.770 e 128.771.
        """
        alocacao = self.build()["weights"]["customer_allocation"]
        taxa = alocacao["penetration_pct"]
        for linha in alocacao["by_warehouse"]:
            exato = linha["adult_population"] * taxa / 100.0
            distancia = abs((exato % 1.0) - 0.5)
            self.assertGreater(
                distancia, 1e-6,
                f"{linha['wh']}: {exato} fica a {distancia} de um empate de arredondamento; "
                f"o alvo passaria a alternar entre exports",
            )

    def test_a_taxa_vem_do_seed_de_demanda_e_nao_esta_repetida(self):
        """Mudar `channel_reference_pct` tem de mudar a base — e o 2,2 nao pode estar em
        customer_premises_seed, ou existiriam dois lugares para muda-lo."""
        conteudo = open(
            os.path.join(self.seeds, "customer_premises_seed.csv"), encoding="utf-8"
        ).read()
        self.assertNotIn("2.2", conteudo)

        antes = self.build()["weights"]["customer_allocation"]["total_customers"]
        self._seed(
            "demand_profile_seed.csv",
            DEMAND_PROFILE_CSV.replace("channel_reference_pct,2.2", "channel_reference_pct,4.4"),
        )
        depois = self.build()["weights"]["customer_allocation"]["total_customers"]
        self.assertAlmostEqual(depois / antes, 2.0, places=3)

    def test_ponteiro_para_outro_lugar_reprova(self):
        self._seed(
            "customer_premises_seed.csv",
            CUSTOMER_PREMISES_CSV.replace(
                "demand_profile.channel_reference_pct", "algum_outro_seed.qualquer_coisa"
            ),
        )
        with self.assertRaises(ReferenceExportError) as erro:
            self.build()
        self.assertIn("aponta para", str(erro.exception))

    def test_premissa_ausente_reprova(self):
        self._seed(
            "customer_premises_seed.csv",
            "\n".join(
                linha for linha in CUSTOMER_PREMISES_CSV.splitlines()
                if not linha.startswith("min_customer_age")
            ) + "\n",
        )
        with self.assertRaises(ReferenceExportError) as erro:
            self.build()
        self.assertIn("min_customer_age", str(erro.exception))

    def test_seed_de_premissas_ausente_reprova_com_mensagem_acionavel(self):
        os.remove(os.path.join(self.seeds, "customer_premises_seed.csv"))
        with self.assertRaises(ReferenceExportError) as erro:
            self.build()
        self.assertIn("customer_premises_seed.csv", str(erro.exception))


class IdadeAdultaTest(OltpReferenceTestCase):
    """A distribuicao entregue e a do CADASTRO, nao a da populacao."""

    def test_a_distribuicao_comeca_no_minimo_declarado(self):
        ages = self.build()["ages"]
        self.assertEqual(ages["min_customer_age"], 18)
        self.assertEqual(min(int(row["age"]) for row in ages["rows"]), 18)
        self.assertEqual(max(int(row["age"]) for row in ages["rows"]), 100)

    def test_a_distribuicao_truncada_e_renormalizada_para_somar_um(self):
        """Truncar sem renormalizar entregaria um vetor somando ~0,82.

        E o modo de falha silencioso desta mudanca: `rng.choices` com `cum_weights` nao
        reclama de um vetor que nao soma 1 — ele simplesmente nunca sorteia a cauda que falta.
        """
        rows = self.build()["ages"]["rows"]
        por_provincia = {}
        for row in rows:
            por_provincia.setdefault(row["province_code"], 0.0)
            por_provincia[row["province_code"]] += float(row["proportion"])
        self.assertEqual(sorted(por_provincia), ["08", "46"])
        for provincia, soma in por_provincia.items():
            self.assertAlmostEqual(soma, 1.0, places=9, msg=f"provincia {provincia}")

    def test_o_share_adulto_e_medido_antes_da_truncagem(self):
        """Medir depois do corte devolveria 100% em toda provincia.

        E um bug que nao reprovaria nada sozinho: 100% e um numero plausivel, e o efeito seria
        dimensionar a base inteira pela populacao TOTAL como se ela fosse adulta.
        """
        shares = self.build()["ages"]["adult_share_by_province"]
        self.assertEqual([s["province_code"] for s in shares], ["08", "46"])
        for share in shares:
            self.assertAlmostEqual(share["adult_share"], 83 / 101, places=9)
            self.assertLess(share["adult_share"], 1.0)
            self.assertEqual(share["population_value"], 101 * 100.0)
            self.assertEqual(share["adult_population_value"], 83 * 100.0)

    def test_piramide_ja_truncada_na_fonte_reprova(self):
        """Se o Silver so tivesse adultos, o share sairia 1,0 e a base dobraria de tamanho
        sem que nenhum total parecesse errado."""
        self.connection.execute(
            "delete from silver_ine_population_series where "
            "cast(regexp_extract(age_label, '^(\\d+)', 1) as integer) < 18 "
            "and age_label not in ('Total', '85 y más años')"
        )
        with self.assertRaises(ReferenceExportError) as erro:
            self.build()
        self.assertIn("share adulto fora de", str(erro.exception))


class EnderecoTest(OltpReferenceTestCase):
    def test_o_nome_do_municipio_resolve_mesmo_quando_o_tramo_pendura_em_nucleo(self):
        """O bug de Valencia: o join por unit_code devolveria 0 linhas para o municipio."""
        rows = self.build()["candidates"]["rows"]
        valencia = [r for r in rows if r["municipality_code"] == "250"]
        self.assertEqual(len(valencia), 3, "tramos de Valencia sumiram no join")
        for row in valencia:
            self.assertEqual(row["municipality_name_callejero"], "VALÈNCIA")

    def test_a_ingestion_anterior_nao_entra_nos_candidatos(self):
        """O Silver empilha todas as extracoes; o export le so a mais recente.

        A fixture tem o MESMO tramo de Torrent em duas ingestion_date. Sem o filtro por
        is_latest_ingestion ele apareceria duas vezes, e todo endereco daquele municipio
        teria o dobro da chance de ser sorteado — vies silencioso, nao erro visivel.
        """
        rows = self.build()["candidates"]["rows"]
        repetidos = [r for r in rows if r["postal_code"] == "46900"]
        self.assertEqual(len(repetidos), 1, "o tramo da extracao anterior entrou junto")

    def test_o_tramo_orfao_e_excluido_e_contado(self):
        candidates = self.build()["candidates"]
        self.assertEqual(candidates["excluded_rows"]["no_street_or_pseudo_match"], 1)
        self.assertNotIn("46004", [r["postal_code"] for r in candidates["rows"]])

    def test_pseudovia_entra_com_o_nome_da_pseudovia(self):
        rows = self.build()["candidates"]["rows"]
        pseudo = [r for r in rows if r["is_pseudo_address"]]
        self.assertEqual(len(pseudo), 1)
        self.assertEqual(pseudo[0]["street_name"], "PARC DE CAPCALERA")
        self.assertEqual(pseudo[0]["numbering_type"], "0")

    def test_so_entram_municipios_da_service_area(self):
        rows = self.build()["candidates"]["rows"]
        self.assertEqual(
            {(r["wh"], r["municipality_code"]) for r in rows},
            {("wh1", "244"), ("wh1", "250"), ("wh2", "019")},
        )

    def test_numeros_saem_como_inteiro(self):
        row = self.build()["candidates"]["rows"][0]
        self.assertIsInstance(row["number_from"], int)
        self.assertIsInstance(row["number_to"], int)

    def test_a_ordem_das_linhas_e_estavel_entre_execucoes(self):
        """A posicao da linha e o candidate_index do cliente: se a ordem variar, a
        linhagem aponta para outro tramo."""
        primeira = [r["postal_code"] for r in self.build()["candidates"]["rows"]]
        segunda = [r["postal_code"] for r in self.build()["candidates"]["rows"]]
        self.assertEqual(primeira, segunda)

    def test_municipio_sem_linha_agregada_reprova(self):
        self.connection.execute(
            "delete from silver_callejero_population_units where is_municipality_aggregate"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("sem linha agregada", str(caught.exception))


class PopulacaoTest(OltpReferenceTestCase):
    def rows(self):
        return self.build()["weights"]["rows"]

    def test_uma_linha_por_municipio(self):
        torrent = [r for r in self.rows() if r["municipality_code"] == "244"]
        self.assertEqual(len(torrent), 1)
        self.assertEqual(torrent[0]["population_total"], 90928)

    def test_serie_duplicada_reprova_em_vez_de_escolher_um_valor(self):
        """Uma serie por municipio e garantia do modelo Silver; aqui e so reconferida.

        Se cair, escolher um valor por conta propria enviesaria o peso do warehouse em
        silencio — este export prefere recusar e apontar o modelo.
        """
        self._injetar_homonimo()
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        mensagem = str(caught.exception)
        self.assertIn("mais de uma serie", mensagem)
        self.assertIn("Torrent", mensagem)

    def test_a_proporcao_usa_a_populacao_do_municipio_certo(self):
        wh1 = [r for r in self.rows() if r["wh"] == "wh1"]
        self.assertEqual(len(wh1), 2)
        total = 90928 + 800215
        torrent = next(r for r in wh1 if r["municipality_code"] == "244")
        self.assertAlmostEqual(torrent["proportion_within_wh"], 90928 / total, places=12)

    def test_proporcoes_somam_um_por_warehouse(self):
        for wh in ("wh1", "wh2"):
            soma = sum(r["proportion_within_wh"] for r in self.rows() if r["wh"] == wh)
            self.assertAlmostEqual(soma, 1.0, places=12)

    def test_proporcao_de_sexo_soma_um(self):
        for row in self.rows():
            self.assertAlmostEqual(
                row["sex_hombres_proportion"] + row["sex_mujeres_proportion"], 1.0, places=12
            )

    def test_province_name_vem_do_seed_com_a_grafia_exata(self):
        wh1 = next(r for r in self.rows() if r["wh"] == "wh1")
        self.assertEqual(wh1["province_name"], "Valencia/València")


class IdadeTest(OltpReferenceTestCase):
    def payload(self):
        return self.build()["ages"]

    def test_os_agregados_sobrepostos_sao_excluidos(self):
        """Sem isto a piramide sai 2,03x inflada, e nenhum teste da Source pegaria.

        83 e nao 101 desde que o arquivo passou a entregar a distribuicao do CADASTRO: as
        idades de 18 a 100 sao o que sobra das 101 simples depois do corte de
        min_customer_age. Os dois rotulos agregados continuam fora pelo motivo de sempre, que
        e outro — eles se SOBREPOEM as idades simples, nao sao um recorte delas.
        """
        rows = self.payload()["rows"]
        por_provincia = [r for r in rows if r["province_code"] == "46"]
        self.assertEqual(len(por_provincia), 83, "entraram rotulos alem das idades simples")
        self.assertEqual(max(r["age"] for r in por_provincia), 100)

    def test_o_balde_terminal_de_100_anos_permanece(self):
        """'100 y mas anos' e idade simples legitima; so 'Total' e '85 y mas anos' saem.

        O corte de idade minima nao pode virar desculpa para perder a cauda: um bug de
        intervalo que cortasse tambem o topo passaria despercebido num teste que so olha o
        piso, e o cadastro ficaria sem centenarios sem ninguem notar.
        """
        idades = {r["age"] for r in self.payload()["rows"] if r["province_code"] == "46"}
        self.assertIn(100, idades)
        self.assertEqual(idades, set(range(18, 101)))

    def test_proporcoes_somam_um_por_provincia(self):
        rows = self.payload()["rows"]
        for province in ("46", "08"):
            soma = sum(r["proportion"] for r in rows if r["province_code"] == province)
            self.assertAlmostEqual(soma, 1.0, places=12)

    def test_ingestion_date_duplicada_nao_duplica_as_linhas(self):
        rows = self.payload()["rows"]
        chaves = [(r["province_code"], r["age"]) for r in rows]
        self.assertEqual(len(chaves), len(set(chaves)))

    def test_declara_o_periodo_pinado_e_a_natureza_do_dado(self):
        payload = self.payload()
        self.assertEqual(payload["year"], 2022)
        self.assertEqual(payload["fk_periodo"], 27)
        self.assertEqual(payload["reference_date"], "2022-06-30")
        self.assertEqual(payload["fk_tipo_dato"], 2)
        self.assertEqual(payload["excluded_age_labels"], ["Total", "85 y más años"])

    def test_total_nacional_nao_entra(self):
        provincias = {r["province_code"] for r in self.payload()["rows"]}
        self.assertEqual(provincias, {"46", "08"})


class CoberturaTest(OltpReferenceTestCase):
    def test_municipio_com_peso_mas_sem_endereco_reprova(self):
        """Cobertura e verificada em execucao, nunca presumida como permanente."""
        self.connection.execute(
            "delete from silver_callejero_tramos where municipality_code = '244'"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("sem nenhum candidato de endereco", str(caught.exception))

    def test_provincia_sem_distribuicao_etaria_reprova(self):
        self.connection.execute(
            "delete from silver_ine_population_series where province_name = 'Barcelona'"
        )
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("sem distribuicao etaria", str(caught.exception))

    def test_seed_ausente_reprova_com_mensagem_acionavel(self):
        os.unlink(os.path.join(self.seeds, "warehouse_service_area_seed.csv"))
        with self.assertRaises(ReferenceExportError) as caught:
            self.build()
        self.assertIn("--seeds-dir", str(caught.exception))


class EscritaTest(OltpReferenceTestCase):
    def test_grava_os_tres_arquivos_e_relele_como_json(self):
        out = os.path.join(self.tmp.name, "out")
        tamanhos = write(self.build(), out)
        self.assertEqual(len(tamanhos), 3)
        for name in tamanhos:
            with open(os.path.join(out, name), encoding="utf-8") as handle:
                self.assertIn("rows", json.load(handle))

    def test_reescrita_produz_arquivo_byte_identico(self):
        """Mesmo dado, mesma saida: e o que o `ORDER BY` das queries garante."""
        out = os.path.join(self.tmp.name, "out")
        write(self.build(), out)
        path = os.path.join(out, "address_candidates.json")
        with open(path, "rb") as handle:
            primeiro = handle.read()
        payloads = self.build()
        # generated_at_utc muda; comparamos o conteudo que descreve o Lakehouse.
        payloads["candidates"]["generated_at_utc"] = json.loads(
            primeiro.decode("utf-8")
        )["generated_at_utc"]
        write(payloads, out)
        with open(path, "rb") as handle:
            self.assertEqual(primeiro, handle.read())


if __name__ == "__main__":
    unittest.main()
