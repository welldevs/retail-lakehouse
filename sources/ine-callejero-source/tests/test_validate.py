"""Validacao: cada modo de falha do snapshot precisa ser detectado, nunca silenciado."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_callejero_source import intake, partition as part, validate
from ine_callejero_source.canonical import write_json
from tests.support import build_input_dir, extract_args, validate_args


def build_partition(root, provinces=("28",), **overrides):
    in_dir = build_input_dir(root, provinces=provinces)
    overrides.setdefault("provinces", list(provinces))
    with contextlib.redirect_stdout(io.StringIO()):
        intake.run(extract_args(root, in_dir, **overrides))
    return os.path.join(root, "ingestion_date=2026-01-01")


def run_validate(partition, strict=False):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = validate.run(validate_args(partition, strict))
    return code, buffer.getvalue()


def _entry(partition, dataset):
    manifest = part.read_manifest(partition)
    return next(f for f in manifest["files"] if f["dataset"] == dataset)


class ValidPartitionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_particao_integra_e_aprovada(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("OK:", output)
        self.assertIn("orfaos ........... 0", output)

    def test_relata_cobertura(self):
        _, output = run_validate(self.partition)
        self.assertIn("cobertura ........ 4/4", output)


class IntegrityFailureTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_manifesto_ausente(self):
        os.unlink(part.manifest_path(self.partition))
        self.assertEqual(run_validate(self.partition)[0], 1)

    def test_manifesto_corrompido_e_reportado_sem_traceback(self):
        with open(part.manifest_path(self.partition), "wb") as handle:
            handle.write(b"{nao e json")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("ilegivel", output)

    def test_arquivo_ausente(self):
        alvo = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        os.unlink(alvo)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("arquivo ausente", output)

    def test_checksum_divergente(self):
        alvo = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(alvo, "ab") as handle:
            handle.write(b"adulterado")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum divergente", output)

    def test_contagem_de_linhas_divergente(self):
        alvo = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(alvo, "a", encoding="latin-1", newline="") as handle:
            handle.write("0" * 10 + " \r\n")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("contagem de linhas divergente", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_arquivo_nao_declarado_e_detectado(self):
        destino = os.path.join(self.partition, "provinces", "province=28", "EXTRA.P28.D260630.G260702")
        with open(destino, "w", encoding="latin-1") as handle:
            handle.write("x")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("fora do manifesto", output)

    def test_arquivo_solto_na_raiz_da_particao_e_detectado(self):
        with open(os.path.join(self.partition, "anotacao.txt"), "w", encoding="utf-8") as handle:
            handle.write("nota")
        self.assertEqual(run_validate(self.partition)[0], 1)

    def test_log_e_marcador_nao_sao_orfaos(self):
        self.assertEqual(run_validate(self.partition)[0], 0)


class SuccessMarkerTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_success_removido_contradiz_o_manifesto(self):
        os.unlink(part.success_path(self.partition))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("_SUCCESS", output)

    def test_success_indevido_em_particao_incompleta(self):
        root = tempfile.mkdtemp()
        in_dir = os.path.join(root, "in")
        from tests.support import write_callejero_file

        for dataset in ("SECC", "UP", "VIAS"):  # falta PSEU: incompleta
            write_callejero_file(in_dir, dataset, "28", subdir="call_p28_726/call_p28_072026")
        with contextlib.redirect_stdout(io.StringIO()):
            intake.run(extract_args(root, in_dir, provinces=["28"]))
        partial = os.path.join(root, "ingestion_date=2026-01-01")
        with open(part.success_path(partial), "w", encoding="utf-8") as handle:
            handle.write("mentira\n")
        self.assertEqual(run_validate(partial)[0], 1)


class TotalsAndCoverageTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_manifest(self, mutate):
        manifest = part.read_manifest(self.partition)
        mutate(manifest)
        write_json(part.manifest_path(self.partition), manifest)

    def test_bytes_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_files_landed_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(files_landed=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.files_landed", output)

    def test_manifest_version_incompativel(self):
        self._tamper_manifest(lambda m: m.update(manifest_version=99))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("manifest_version", output)

    def test_combinacao_configurada_sem_arquivo_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [f for f in manifest["files"] if f["dataset"] != "PSEU"]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(
            os.path.join(self.partition, "provinces", "province=28", "PSEU.P28.D260630.G260702")
        )
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("combinacao(oes) configurada(s) sem arquivo", output)
        self.assertIn("cobertura ........ 3/4", output)

    def test_falha_registrada_no_manifesto_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [{"province": "28", "dataset": "PSEU", "error": "x"}]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("registradas como falha no manifesto", output)


class PerFileRecheckTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_entry(self, dataset, campo, valor):
        manifest = part.read_manifest(self.partition)
        entrada = next(f for f in manifest["files"] if f["dataset"] == dataset)
        entrada[campo] = valor
        write_json(part.manifest_path(self.partition), manifest)

    def test_bytes_de_cada_arquivo_e_reconferido(self):
        self._tamper_entry("SECC", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("tamanho divergente", output)

    def test_entrada_apontando_fora_da_raiz_do_snapshot_e_barrada(self):
        self._tamper_entry("SECC", "path", "../../../../etc/passwd")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("fora da raiz", output)

    def test_entrada_de_manifesto_malformada_e_reportada(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"].append({"sem": "path"})
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("malformada", output)


class LineWidthWarningTest(unittest.TestCase):
    """Mudanca de layout do INE nao e fatal por padrao, so sob --strict."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_largura_dominante_divergente_e_aviso_sem_strict(self):
        alvo = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(alvo, "a", encoding="latin-1", newline="") as handle:
            handle.write("9" * 20 + "\r\n")
        code, output = run_validate(self.partition)
        # o manifesto ainda reconfere contagem de linhas (agora 2, nao 1): isso E fatal.
        self.assertEqual(code, 1)
        self.assertIn("contagem de linhas divergente", output)

    def test_strict_reprova_aviso_de_largura(self):
        # duas linhas de larguras diferentes desde o inicio: contagem bate (2==2), so a
        # largura dominante muda de sinal quando comparada a entrada original.
        alvo = os.path.join(
            self.partition, "provinces", "province=28", "SECC.P28.D260630.G260702"
        )
        with open(alvo, encoding="latin-1", newline="") as handle:
            original = handle.read()
        manifest = part.read_manifest(self.partition)
        entrada = next(f for f in manifest["files"] if f["dataset"] == "SECC")
        entrada["line_count"] = 2
        write_json(part.manifest_path(self.partition), manifest)
        with open(alvo, "a", encoding="latin-1", newline="") as handle:
            handle.write("9" * 20 + "\r\n")
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)


class CoverageMissingProvinceTest(unittest.TestCase):
    def test_provincia_inteira_faltante_reprova_cobertura(self):
        root = tempfile.mkdtemp()
        in_dir = build_input_dir(root, provinces=("08", "28"))
        with contextlib.redirect_stdout(io.StringIO()):
            intake.run(extract_args(root, in_dir, provinces=["08", "28"]))
        partition = os.path.join(root, "ingestion_date=2026-01-01")
        manifest = part.read_manifest(partition)
        manifest["files"] = [f for f in manifest["files"] if f["province"] != "08"]
        write_json(part.manifest_path(partition), manifest)
        code, output = run_validate(partition)
        self.assertEqual(code, 1)
        self.assertIn("cobertura ........ 4/8", output)


if __name__ == "__main__":
    unittest.main()
