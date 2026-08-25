"""Identidade e regras da particao: tokens, nome de arquivo do INE, imutabilidade."""

from __future__ import annotations

import os
import tempfile
import unittest

from ine_callejero_source import partition as part


class ProvinceTokenTest(unittest.TestCase):
    def test_aceita_codigos_reais(self):
        for value in ("08", "28", "41", "46"):
            self.assertEqual(part.validate_province(value), value)

    def test_rejeita_formato_errado(self):
        for value in ("8", "280", "ab", "", None, "-1"):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_province(value)

    def test_data_precisa_ser_iso(self):
        self.assertEqual(part.validate_date("2026-08-25"), "2026-08-25")
        for value in ("2026-8-25", "25/08/2026", "hoje", "", None):
            with self.subTest(value=value):
                with self.assertRaises(part.PartitionError):
                    part.validate_date(value)


class InputFileNameTest(unittest.TestCase):
    def test_reconhece_nomes_oficiais(self):
        for name, expected in [
            ("SECC.P08.D260630.G260702", {"dataset": "SECC", "province": "08"}),
            ("VIAS.P28.D260630.G260702", {"dataset": "VIAS", "province": "28"}),
            ("UP.P41.D260630.G260702", {"dataset": "UP", "province": "41"}),
            ("PSEU.P46.D260630.G260702", {"dataset": "PSEU", "province": "46"}),
        ]:
            with self.subTest(name=name):
                match = part.INPUT_FILE.match(name)
                self.assertIsNotNone(match)
                self.assertEqual(match.group("dataset"), expected["dataset"])
                self.assertEqual(match.group("province"), expected["province"])

    def test_tram_nao_bate_no_padrao_de_entrada(self):
        # TRAM esta fora de escopo (CONTRACT.md secao 2): nem candidato a match.
        self.assertIsNone(part.INPUT_FILE.match("TRAM.P28.D260630.G260702"))

    def test_rejeita_nomes_fora_do_padrao(self):
        for name in ("SECC.txt", "secc.p28.d260630.g260702", "SECC.P280.D260630.G260702", "../../etc"):
            with self.subTest(name=name):
                self.assertIsNone(part.INPUT_FILE.match(name))

    def test_dataset_path_recusa_nome_fora_do_padrao(self):
        with self.assertRaises(part.PartitionError):
            part.dataset_path("/tmp/p", "28", "../../etc/passwd")


class PartitionPathTest(unittest.TestCase):
    def test_particao_tem_um_nivel_so_sem_segundo_eixo(self):
        path = part.partition_path("/root", "2026-08-25")
        self.assertEqual(path, os.path.join("/root", "ingestion_date=2026-08-25"))

    def test_dataset_path_preserva_o_nome_original(self):
        partition = part.partition_path("/root", "2026-08-25")
        path = part.dataset_path(partition, "28", "VIAS.P28.D260630.G260702")
        self.assertEqual(
            path,
            os.path.join(partition, "provinces", "province=28", "VIAS.P28.D260630.G260702"),
        )


class PartitionRulesTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = part.partition_path(self.root, "2026-01-01")
        os.makedirs(self.partition, exist_ok=True)

    def _write(self, complete: bool):
        part.write_manifest(
            self.partition,
            {
                "run_id": "r1",
                "complete": complete,
                "source": {"name": "ine_callejero"},
                "totals": {"files_landed": 4},
                "files": [],
            },
        )

    def test_particao_inexistente_e_gravavel(self):
        self.assertIsNone(part.assert_writable(self.partition, False))

    def test_particao_incompleta_pode_ser_retomada(self):
        self._write(complete=False)
        self.assertIsNotNone(part.assert_writable(self.partition, False))

    def test_particao_completa_e_imutavel_sem_overwrite(self):
        self._write(complete=True)
        with self.assertRaises(part.PartitionError):
            part.assert_writable(self.partition, False)

    def test_particao_completa_pode_ser_reescrita_com_overwrite(self):
        self._write(complete=True)
        self.assertIsNotNone(part.assert_writable(self.partition, True))

    def test_historico_acumula_execucoes(self):
        self._write(complete=False)
        previous = part.read_manifest(self.partition)
        history = part.build_history(previous)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["run_id"], "r1")
        self.assertEqual(history[0]["files_landed"], 4)

    def test_marcador_success_segue_o_estado_da_particao(self):
        part.mark_success(self.partition, True, "r1")
        self.assertTrue(os.path.exists(part.success_path(self.partition)))
        part.mark_success(self.partition, False, "r2")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))


if __name__ == "__main__":
    unittest.main()
