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
        """Sem isto a piramide sai 2,03x inflada, e nenhum teste da Source pegaria."""
        rows = self.payload()["rows"]
        por_provincia = [r for r in rows if r["province_code"] == "46"]
        self.assertEqual(len(por_provincia), 101, "entraram rotulos alem das idades simples")
        self.assertEqual(max(r["age"] for r in por_provincia), 100)

    def test_o_balde_terminal_de_100_anos_permanece(self):
        """'100 y mas anos' e idade simples legitima; so 'Total' e '85 y mas anos' saem."""
        idades = {r["age"] for r in self.payload()["rows"] if r["province_code"] == "46"}
        self.assertIn(100, idades)
        self.assertEqual(idades, set(range(0, 101)))

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
