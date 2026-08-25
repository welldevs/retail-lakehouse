"""Validacao: cada modo de falha do snapshot precisa ser detectado, nunca silenciado."""

from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest

from ine_population_source import extract, partition as part, validate
from ine_population_source.canonical import read_json, write_json
from tests.support import extract_args, make_fetcher_factory, series, table_payload, validate_args

TABLE_31304 = table_payload(series("A", "Total. Madrid. Ambos sexos.", (2025, 6779888)))
TABLE_9689 = table_payload(series("B", "Total. Barcelona. Ambos sexos.", (2025, 5716544)))
RESPONSES = {"/ES/DATOS_TABLA/31304": TABLE_31304, "/ES/DATOS_TABLA/9689": TABLE_9689}


def build_partition(root, responses=None, errors=None, **overrides):
    factory, _ = make_fetcher_factory(dict(responses or RESPONSES), errors)
    overrides.setdefault("tables", ["31304", "9689"])
    with contextlib.redirect_stdout(io.StringIO()):
        extract.run(extract_args(root, **overrides), fetcher_factory=factory)
    return os.path.join(root, "ingestion_date=2026-01-01")


def run_validate(partition, strict=False):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        code = validate.run(validate_args(partition, strict))
    return code, buffer.getvalue()


class ValidPartitionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_particao_integra_e_aprovada(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("OK:", output)
        self.assertIn("orfaos ........... 0", output)

    def test_relata_series_e_pontos(self):
        _, output = run_validate(self.partition)
        self.assertIn("series ........... 2", output)
        self.assertIn("pontos ........... 2", output)


class IntegrityFailureTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp()
        self.partition = build_partition(self.root)

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
        os.unlink(part.table_path(self.partition, 31304))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("arquivo ausente", output)

    def test_checksum_divergente(self):
        payload, _ = read_json(part.table_path(self.partition, 31304))
        payload[0]["Nombre"] = "adulterado"
        write_json(part.table_path(self.partition, 31304), payload)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum divergente", output)

    def test_arquivo_corrompido_e_reportado_em_vez_de_derrubar_o_validador(self):
        with open(part.table_path(self.partition, 31304), "wb") as handle:
            handle.write(b'[{"COD": "A", "Nomb')
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("JSON invalido", output)
        self.assertIn("FALHOU", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_arquivo_de_tabela_nao_declarado_e_detectado(self):
        write_json(part.table_path(self.partition, 99999), [{"COD": "x", "Data": []}])
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
        partial = build_partition(
            tempfile.mkdtemp(), errors={"/ES/DATOS_TABLA/9689": "HTTP 500"}
        )
        with open(part.success_path(partial), "w", encoding="utf-8") as handle:
            handle.write("mentira\n")
        self.assertEqual(run_validate(partial)[0], 1)


class TotalsAndSchemaTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_manifest(self, mutate):
        manifest = part.read_manifest(self.partition)
        mutate(manifest)
        write_json(part.manifest_path(self.partition), manifest)

    def test_series_count_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(series_count=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.series_count", output)

    def test_bytes_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_table_files_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(table_files=7))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.table_files", output)

    def test_fingerprint_divergente(self):
        self._tamper_manifest(lambda m: m["schema_fingerprint"].update(sha256="0" * 64))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("schema_fingerprint divergente", output)

    def test_manifest_version_incompativel(self):
        self._tamper_manifest(lambda m: m.update(manifest_version=99))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("manifest_version", output)


class PerFileRecheckTest(unittest.TestCase):
    """Cada campo declarado por arquivo e reconferido, nao apenas os totais agregados."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def _tamper_entry(self, sufixo, campo, valor):
        manifest = part.read_manifest(self.partition)
        entrada = next(f for f in manifest["files"] if f["path"].endswith(sufixo))
        entrada[campo] = valor
        write_json(part.manifest_path(self.partition), manifest)

    def test_records_de_arquivo_de_tabela_e_reconferido(self):
        self._tamper_entry("table_id=31304.json", "records", 99)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("contagem divergente", output)

    def test_bytes_de_cada_arquivo_e_reconferido(self):
        self._tamper_entry("table_id=31304.json", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("tamanho divergente", output)

    def test_entrada_apontando_fora_da_raiz_do_snapshot_e_barrada(self):
        self._tamper_entry("table_id=31304.json", "path", "../../../../etc/passwd")
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


class CoverageAndFailuresTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_table_id_configurado_sem_arquivo_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [
            f for f in manifest["files"] if not f["path"].endswith("table_id=9689.json")
        ]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(part.table_path(self.partition, 9689))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("table_id(s) configurados sem arquivo", output)
        self.assertIn("cobertura ........ 1/2", output)

    def test_falha_registrada_no_manifesto_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [{"stage": "tables", "table_id": "99999", "error": "HTTP 500"}]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("registradas como falha no manifesto", output)


class RecursiveOrphanTest(unittest.TestCase):
    """A varredura precisa descer nos subdiretorios, senao o inventario fechado e falso."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_arquivo_em_subdiretorio_de_tables_e_detectado(self):
        destino = os.path.join(self.partition, "tables", "backup")
        os.makedirs(destino, exist_ok=True)
        write_json(os.path.join(destino, "old.json"), [{"COD": "x"}])
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("fora do manifesto", output)
        self.assertIn("backup", output)

    def test_diretorio_desconhecido_na_raiz_da_particao_e_detectado(self):
        destino = os.path.join(self.partition, "_scratch")
        os.makedirs(destino, exist_ok=True)
        with open(os.path.join(destino, "notas.txt"), "w", encoding="utf-8") as handle:
            handle.write("nota")
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("_scratch", output)

    def test_subdiretorio_vazio_nao_e_orfao(self):
        os.makedirs(os.path.join(self.partition, "tables", "vazio"), exist_ok=True)
        self.assertEqual(run_validate(self.partition)[0], 0)


class AnomaliesTest(unittest.TestCase):
    def test_anomalia_registrada_vira_aviso_e_nao_reprova(self):
        root = tempfile.mkdtemp()
        # 31304 e extraido; 9689 falha e fica sem arquivo (particao incompleta)
        partition = build_partition(root, errors={"/ES/DATOS_TABLA/9689": "HTTP 500"})
        # arquivo fora da forma canonica, escrito por fora da extracao
        alvo = part.table_path(partition, 9689)
        os.makedirs(os.path.dirname(alvo), exist_ok=True)
        with open(alvo, "w", encoding="utf-8") as handle:
            import json

            json.dump(TABLE_9689, handle, indent=8)
        # reexecucao sem erro: particao incompleta pode ser retomada sem --overwrite
        build_partition(root)
        manifest = part.read_manifest(partition)
        self.assertTrue(manifest["anomalies"])
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("AVISO: anomalia registrada", output)
        self.assertIn("anomalias ........ 1", output)


class QualityTest(unittest.TestCase):
    def setUp(self):
        responses = dict(RESPONSES)
        sem_valor = table_payload(
            {"COD": "B", "Nombre": "Total. Barcelona.", "Data": [{"Anyo": 2025, "Valor": None}]}
        )
        responses["/ES/DATOS_TABLA/9689"] = sem_valor
        self.partition = build_partition(tempfile.mkdtemp(), responses=responses)

    def test_qualidade_e_reportada_mas_nao_reprova_sem_strict(self):
        code, output = run_validate(self.partition)
        self.assertEqual(code, 0)
        self.assertIn("series sem valor . 1", output)

    def test_strict_reprova_registro_incompleto(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("--strict", output)


if __name__ == "__main__":
    unittest.main()
