"""Validacao: cada modo de falha do snapshot precisa ser detectado, nunca silenciado."""

from __future__ import annotations

import contextlib
import json
import io
import os
import tempfile
import unittest

from mercadona_catalog_source import extract, partition as part, validate
from mercadona_catalog_source.canonical import read_json, write_json
from tests.support import catalog, extract_args, make_fetcher_factory, tree, validate_args

TREE = tree((1, "Pai", [(10, "Alfa"), (11, "Beta")]))
RESPONSES = {
    "/categories/": TREE,
    "/categories/10/": catalog(10, "Alfa", ("g1", [(1, "p1", "1.00"), (2, "p2", "2.00")])),
    "/categories/11/": catalog(11, "Beta", ("g1", [(3, "p3", "3.00")])),
}


def build_partition(root, responses=None, **overrides):
    factory, _ = make_fetcher_factory(dict(responses or RESPONSES))
    with contextlib.redirect_stdout(io.StringIO()):
        extract.run(extract_args(root, **overrides), fetcher_factory=factory)
    return os.path.join(root, "ingestion_date=2026-01-01", "wh=mad1")


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

    def test_relata_linhas_e_produtos_unicos(self):
        _, output = run_validate(self.partition)
        self.assertIn("linhas ........... 3", output)
        self.assertIn("produtos unicos .. 3", output)


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
        os.unlink(part.catalog_path(self.partition, 10))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("arquivo ausente", output)

    def test_checksum_divergente(self):
        payload, _ = read_json(part.catalog_path(self.partition, 10))
        payload["name"] = "adulterado"
        write_json(part.catalog_path(self.partition, 10), payload)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("checksum divergente", output)

    def test_arquivo_corrompido_e_reportado_em_vez_de_derrubar_o_validador(self):
        with open(part.catalog_path(self.partition, 10), "wb") as handle:
            handle.write(b'{"id": 10, "categ')
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("JSON invalido", output)
        # o relatorio segue ate o fim, em vez de morrer no primeiro arquivo ruim
        self.assertIn("FALHOU", output)

    def test_arvore_de_categorias_ausente(self):
        os.unlink(part.categories_path(self.partition))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("arvore de categorias ausente", output)


class OrphanDetectionTest(unittest.TestCase):
    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_arquivo_de_catalogo_nao_declarado_e_detectado(self):
        write_json(part.catalog_path(self.partition, 99), {"id": 99, "categories": []})
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

    def test_success_indevido_em_particao_parcial(self):
        partial = build_partition(tempfile.mkdtemp(), limit=1)
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

    def test_product_rows_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(product_rows=999))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.product_rows", output)

    def test_bytes_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(bytes=1))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.bytes", output)

    def test_catalog_files_adulterado(self):
        self._tamper_manifest(lambda m: m["totals"].update(catalog_files=7))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("totals.catalog_files", output)

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

    def test_records_de_arquivo_de_catalogo_e_reconferido(self):
        self._tamper_entry("category_id=10.json", "records", 99)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("contagem divergente", output)

    def test_records_da_arvore_de_categorias_e_reconferido(self):
        self._tamper_entry("categories.json", "records", 42)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("contagem divergente", output)
        self.assertIn("categories.json", output)

    def test_bytes_de_cada_arquivo_e_reconferido(self):
        self._tamper_entry("category_id=10.json", "bytes", 7)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("tamanho divergente", output)

    def test_entrada_apontando_fora_da_raiz_do_snapshot_e_barrada(self):
        self._tamper_entry("category_id=10.json", "path", "../../../../etc/passwd")
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

    def test_categoria_de_nivel_2_sem_arquivo_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["files"] = [
            f for f in manifest["files"] if not f["path"].endswith("category_id=11.json")
        ]
        write_json(part.manifest_path(self.partition), manifest)
        os.unlink(part.catalog_path(self.partition, 11))
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("categorias de nivel 2 sem arquivo", output)
        self.assertIn("cobertura ........ 1/2", output)

    def test_falha_registrada_no_manifesto_reprova(self):
        manifest = part.read_manifest(self.partition)
        manifest["failures"] = [
            {"stage": "catalog", "category_id": 99, "name": "x", "error": "HTTP 500"}
        ]
        write_json(part.manifest_path(self.partition), manifest)
        code, output = run_validate(self.partition)
        self.assertEqual(code, 1)
        self.assertIn("registradas como falha no manifesto", output)


class RecursiveOrphanTest(unittest.TestCase):
    """A varredura precisa descer nos subdiretorios, senao o inventario fechado e falso."""

    def setUp(self):
        self.partition = build_partition(tempfile.mkdtemp())

    def test_arquivo_em_subdiretorio_de_catalog_e_detectado(self):
        destino = os.path.join(self.partition, "catalog", "backup")
        os.makedirs(destino, exist_ok=True)
        write_json(os.path.join(destino, "old.json"), {"id": 1})
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
        os.makedirs(os.path.join(self.partition, "catalog", "vazio"), exist_ok=True)
        self.assertEqual(run_validate(self.partition)[0], 0)


class AnomaliesTest(unittest.TestCase):
    def test_anomalia_registrada_vira_aviso_e_nao_reprova(self):
        root = tempfile.mkdtemp()
        partition = build_partition(root, limit=1)
        # arquivo fora da forma canonica: a extracao rebaixa e registra a anomalia
        alvo = part.catalog_path(partition, 11)
        os.makedirs(os.path.dirname(alvo), exist_ok=True)
        with open(alvo, "w", encoding="utf-8") as handle:
            json.dump({"id": 11, "name": "x", "categories": []}, handle, indent=8)
        build_partition(root)
        manifest = part.read_manifest(partition)
        self.assertTrue(manifest["anomalies"])
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("AVISO: anomalia registrada", output)
        self.assertIn("anomalias ........ 1", output)


class ShapeChangeDetectionTest(unittest.TestCase):
    def test_particao_sem_nenhum_produto_e_reprovada(self):
        responses = {
            "/categories/": TREE,
            "/categories/10/": {"id": 10, "name": "Alfa", "secoes": []},
            "/categories/11/": {"id": 11, "name": "Beta", "secoes": []},
        }
        partition = build_partition(tempfile.mkdtemp(), responses=responses)
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("nenhum produto lido", output)
        self.assertIn("n/a (nenhuma linha lida)", output)


class QualityTest(unittest.TestCase):
    def setUp(self):
        responses = dict(RESPONSES)
        sem_preco = catalog(11, "Beta", ("g1", [(3, "p3", "3.00")]))
        sem_preco["categories"][0]["products"][0]["price_instructions"]["unit_price"] = None
        responses["/categories/11/"] = sem_preco
        self.partition = build_partition(tempfile.mkdtemp(), responses=responses)

    def test_qualidade_e_reportada_mas_nao_reprova_sem_strict(self):
        code, output = run_validate(self.partition)
        self.assertEqual(code, 0)
        self.assertIn("sem preco ........ 1", output)

    def test_strict_reprova_registro_incompleto(self):
        code, output = run_validate(self.partition, strict=True)
        self.assertEqual(code, 1)
        self.assertIn("--strict", output)


class PartialPartitionTest(unittest.TestCase):
    def test_particao_parcial_avisa_mas_nao_reprova_por_cobertura(self):
        partition = build_partition(tempfile.mkdtemp(), limit=1)
        code, output = run_validate(partition, strict=True)
        self.assertEqual(code, 0)
        self.assertIn("AVISO", output)
        self.assertIn("particao parcial", output)


if __name__ == "__main__":
    unittest.main()
