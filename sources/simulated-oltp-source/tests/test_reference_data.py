"""A referencia e desconfiada: incompleta ou incoerente tem de reprovar a geracao."""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from simulated_oltp_source import reference_data
from simulated_oltp_source.reference_data import ReferenceError, load

from tests import support


class LoadTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.directory = os.path.join(self.tmp.name, "ref")

    def write(self, **kwargs) -> str:
        return support.write_reference(self.directory, **kwargs)

    def corromper(self, name: str, mutate) -> str:
        self.write()
        path = os.path.join(self.directory, name)
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        mutate(payload)
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)
        return self.directory

    def test_le_os_tres_arquivos_e_expoe_a_proveniencia(self):
        reference = load(self.write())
        self.assertEqual(reference.callejero_ingestion_date, "2026-08-25")
        self.assertEqual(reference.population_ingestion_date, "2026-08-26")
        self.assertEqual(reference.population_series_ingestion_date, "2026-08-26")
        self.assertEqual(reference.population_year, 2025)
        self.assertEqual(reference.age_year, 2022)
        self.assertEqual(reference.age_fk_periodo, 27)
        self.assertEqual(reference.orphan_tramos_excluded, 3)

    def test_indexa_candidatos_por_municipio_preservando_a_ordem_do_arquivo(self):
        reference = load(self.write())
        self.assertEqual(reference.candidates_of(support.WH_A, "01", "001"), [0, 1, 2])
        self.assertEqual(reference.candidates_of(support.WH_A, "01", "002"), [3, 4, 5])
        self.assertEqual(reference.candidates_of(support.WH_B, "02", "010"), [6])

    def test_warehouses_e_municipios(self):
        reference = load(self.write())
        self.assertEqual(reference.warehouses(), [support.WH_A, support.WH_B])
        self.assertEqual(len(reference.municipalities(support.WH_A)), 2)

    def test_idades_por_provincia_saem_como_listas_paralelas(self):
        ages, proportions = load(self.write()).ages_of("01")
        self.assertEqual(ages, [30, 31, 32])
        self.assertEqual(len(proportions), 3)

    def test_diretorio_inexistente_reprova(self):
        with self.assertRaises(ReferenceError):
            load(os.path.join(self.tmp.name, "nao-existe"))

    def test_arquivo_ausente_reprova_com_mensagem_acionavel(self):
        self.write()
        os.unlink(os.path.join(self.directory, reference_data.AGE_DISTRIBUTION_FILE))
        with self.assertRaises(ReferenceError) as caught:
            load(self.directory)
        self.assertIn("oltp-export-reference", str(caught.exception))

    def test_json_invalido_reprova(self):
        self.write()
        with open(
            os.path.join(self.directory, reference_data.POPULATION_WEIGHTS_FILE), "w"
        ) as handle:
            handle.write("{ nao e json")
        with self.assertRaises(ReferenceError):
            load(self.directory)

    def test_lista_rows_vazia_reprova(self):
        directory = self.corromper(
            reference_data.ADDRESS_CANDIDATES_FILE, lambda p: p.update(rows=[])
        )
        with self.assertRaises(ReferenceError):
            load(directory)

    def test_campo_obrigatorio_ausente_reprova(self):
        """Um export de schema antigo nao pode passar por bom."""
        def remover_cep(payload):
            payload["rows"][0].pop("postal_code")

        with self.assertRaises(ReferenceError) as caught:
            load(self.corromper(reference_data.ADDRESS_CANDIDATES_FILE, remover_cep))
        self.assertIn("postal_code", str(caught.exception))

    def test_raiz_que_nao_e_objeto_reprova(self):
        self.write()
        with open(
            os.path.join(self.directory, reference_data.AGE_DISTRIBUTION_FILE), "w"
        ) as handle:
            json.dump([1, 2, 3], handle)
        with self.assertRaises(ReferenceError):
            load(self.directory)

    def test_municipio_sem_candidato_reprova_na_consulta(self):
        reference = load(self.write())
        with self.assertRaises(ReferenceError):
            reference.candidates_of(support.WH_A, "99", "999")

    def test_provincia_sem_idade_reprova_na_consulta(self):
        reference = load(self.write())
        with self.assertRaises(ReferenceError):
            reference.ages_of("99")


if __name__ == "__main__":
    unittest.main()
