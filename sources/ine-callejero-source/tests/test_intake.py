"""Intake ponta a ponta, sem rede: cada gap fechado tem um teste que falha sem a correcao."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_callejero_source import intake, partition as part
from ine_callejero_source.canonical import digest, read_bytes
from tests.support import build_input_dir, extract_args, write_callejero_file


def run_intake(out, in_dir, **overrides):
    args = extract_args(out, in_dir, **overrides)
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = intake.run(args)
    return code, buffer.getvalue()


class IntakeHappyPathTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_intake_completo(self):
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 0)

        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["manifest_version"], 1)
        self.assertEqual(manifest["source"]["name"], "ine_callejero")
        self.assertEqual(manifest["partition"], {"ingestion_date": "2026-01-01"})
        self.assertEqual(manifest["totals"]["files_landed"], 5)  # SECC+UP+VIAS+PSEU+TRAM
        self.assertEqual(manifest["failures"], [])
        self.assertEqual(manifest["history"], [])

    def test_marcador_success_criado_quando_completo(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertTrue(os.path.exists(part.success_path(self.partition)))

    def test_nome_original_do_arquivo_e_preservado(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        landed = os.path.join(
            self.partition, "provinces", "province=28", "VIAS.P28.D260630.G260702"
        )
        self.assertTrue(os.path.exists(landed))

    def test_bytes_landados_sao_identicos_ao_arquivo_de_entrada(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        original = os.path.join(
            self.in_dir, "call_p28_726", "call_p28_072026", "SECC.P28.D260630.G260702"
        )
        landed = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        self.assertEqual(read_bytes(original), read_bytes(landed))

    def test_tram_e_landado_como_os_demais(self):
        in_dir = build_input_dir(self.root, provinces=("41",))
        code, _ = run_intake(self.root, in_dir, provinces=["41"])
        self.assertEqual(code, 0)
        province_dir = os.path.join(self.partition, "provinces", "province=41")
        landed_names = os.listdir(province_dir)
        self.assertEqual(len(landed_names), 5)
        self.assertTrue(any(name.startswith("TRAM") for name in landed_names))

    def test_multiplas_provincias(self):
        in_dir = build_input_dir(self.root, provinces=("08", "28", "41", "46"))
        code, _ = run_intake(self.root, in_dir, provinces=["08", "28", "41", "46"])
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertEqual(manifest["totals"]["files_landed"], 20)  # 4 provincias x 5 datasets


class ArgumentGuardTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_lista_de_provincias_vazia_e_recusada(self):
        self.assertEqual(run_intake(self.root, self.in_dir, provinces=[])[0], 2)

    def test_provincia_malformada_e_recusada(self):
        self.assertEqual(run_intake(self.root, self.in_dir, provinces=["280"])[0], 2)

    def test_data_invalida_e_recusada(self):
        self.assertEqual(run_intake(self.root, self.in_dir, date="25-08-2026")[0], 2)

    def test_in_dir_inexistente_e_recusado(self):
        code, output = run_intake(self.root, os.path.join(self.root, "nao-existe"))
        self.assertEqual(code, 2)
        self.assertIn("--in", output)


class MissingFileTest(unittest.TestCase):
    """Cobertura parcial: uma provincia sem todos os datasets em --in e falha parcial."""

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = os.path.join(self.root, "in")
        # so 4 dos 5 datasets: falta PSEU
        for dataset in ("SECC", "UP", "VIAS", "TRAM"):
            write_callejero_file(self.in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")

    def test_falha_parcial_devolve_1_e_nao_marca_success(self):
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 1)
        manifest = part.read_manifest(self.partition)
        self.assertFalse(manifest["complete"])
        self.assertEqual(len(manifest["failures"]), 1)
        self.assertEqual(manifest["failures"][0]["dataset"], "PSEU")
        self.assertFalse(os.path.exists(part.success_path(self.partition)))

    def test_reexecucao_retoma_apos_arquivo_faltante_aparecer(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        write_callejero_file(self.in_dir, "PSEU", "28", subdir="call_p28_726/call_p28_072026")
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        self.assertTrue(manifest["complete"])
        self.assertEqual(manifest["totals"]["files_landed"], 5)

    def test_historico_preserva_a_execucao_anterior(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        write_callejero_file(self.in_dir, "PSEU", "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, self.in_dir, provinces=["28"])
        manifest = part.read_manifest(self.partition)
        self.assertEqual(len(manifest["history"]), 1)
        self.assertFalse(manifest["history"][0]["complete"])


class ResumeAndImmutabilityTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = build_input_dir(self.root, provinces=("28",))

    def test_reexecucao_de_particao_incompleta_reaproveita_o_que_ja_landou(self):
        # so 3 dos 5 datasets em --in: primeira execucao fica incompleta (resumivel).
        from tests.support import write_callejero_file

        parcial_in = os.path.join(self.root, "in-parcial")
        for dataset in ("SECC", "UP", "VIAS"):
            write_callejero_file(parcial_in, dataset, "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, parcial_in, provinces=["28"])

        code, _ = run_intake(self.root, self.in_dir, provinces=["28"])  # in_dir tem os 5
        self.assertEqual(code, 0)
        manifest = part.read_manifest(self.partition)
        por_dataset = {f["dataset"]: f["reused"] for f in manifest["files"]}
        self.assertTrue(por_dataset["SECC"])  # ja landado antes, --in nao mudou: reaproveita
        self.assertFalse(por_dataset["PSEU"])  # so apareceu em --in nesta execucao

    def test_particao_completa_e_imutavel_sem_overwrite(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        code, output = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 2)
        self.assertIn("imutavel", output)

    def test_overwrite_reescreve_particao_completa(self):
        run_intake(self.root, self.in_dir, provinces=["28"])
        code, _ = run_intake(self.root, self.in_dir, provinces=["28"], overwrite=True)
        self.assertEqual(code, 0)


class TamperedLandedFileTest(unittest.TestCase):
    """Um arquivo ja landado que diverge do --in atual exige decisao explicita.

    So testavel numa particao INCOMPLETA: uma particao completa ja e bloqueada por
    assert_writable antes de qualquer comparacao de conteudo (imutabilidade), entao a
    divergencia so importa quando ha algo a retomar.
    """

    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = os.path.join(self.root, "ingestion_date=2026-01-01")
        self.in_dir = os.path.join(self.root, "in")
        from tests.support import write_callejero_file

        # so 3 dos 5 datasets: particao fica incompleta, resumivel sem --overwrite.
        for dataset in ("SECC", "UP", "VIAS"):
            write_callejero_file(self.in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")
        run_intake(self.root, self.in_dir, provinces=["28"])
        self.landed = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )

    def test_divergencia_do_landado_com_in_atual_e_falha_sem_overwrite(self):
        with open(self.landed, "ab") as handle:
            handle.write(b"adulterado")
        code, output = run_intake(self.root, self.in_dir, provinces=["28"])
        self.assertEqual(code, 1)
        self.assertIn("diverge", output)

    def test_overwrite_resolve_a_divergencia_a_favor_de_in(self):
        with open(self.landed, "ab") as handle:
            handle.write(b"adulterado")
        run_intake(self.root, self.in_dir, provinces=["28"], overwrite=True)
        original = os.path.join(
            self.in_dir, "call_p28_726", "call_p28_072026", "SECC.P28.D260630.G260702"
        )
        self.assertEqual(read_bytes(self.landed), read_bytes(original))


if __name__ == "__main__":
    unittest.main()
