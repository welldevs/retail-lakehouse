"""A leitura do manifesto impoe as obrigacoes do consumidor, e nao apenas as documenta.

Sem rede e sem boto3: tudo aqui exercita particoes sinteticas em disco.
"""

from __future__ import annotations

import json
import os
import tempfile
import unittest

from retail_platform import manifest as manifest_module
from retail_platform.manifest import ManifestError, read

from .support import build_partition, build_single_axis_partition, write_canonical


class TemporaryRoot(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def _patch_manifest(self, partition, mutate):
        path = os.path.join(partition, "_manifest.json")
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
        mutate(payload)
        write_canonical(path, payload)


class TestHappyPath(TemporaryRoot):
    def test_reads_a_valid_partition(self):
        partition = build_partition(self.root, category_ids=(112, 113))
        result = read(partition)
        self.assertEqual(result.ingestion_date, "2026-08-24")
        self.assertEqual(result.warehouse, "mad1")
        self.assertTrue(result.complete)
        self.assertEqual(len(result.files), 3)  # 1 arvore + 2 catalogos

    def test_resolves_paths_against_the_snapshot_root_not_the_partition(self):
        """Obrigacao 4.1: files[].path comeca em ingestion_date=.../wh=..., nao na particao.

        Resolver contra a particao produziria .../wh=mad1/ingestion_date=.../wh=mad1/...
        e o arquivo simplesmente nao existiria. E o erro mais facil de cometer aqui.
        """
        partition = build_partition(self.root)
        for entry in read(partition).files:
            self.assertTrue(
                os.path.exists(entry.local_path),
                f"caminho nao resolvido para a raiz do snapshot: {entry.local_path}",
            )

    def test_digest_on_disk_recomputes_instead_of_trusting(self):
        partition = build_partition(self.root)
        entry = next(e for e in read(partition).files if e.stage == "catalog")
        observed_sha, observed_size = entry.digest_on_disk()
        self.assertEqual(observed_sha, entry.sha256)
        self.assertEqual(observed_size, entry.bytes)

    def test_prefix_suffix_preserves_the_hive_layout(self):
        partition = build_partition(self.root, ingestion_date="2026-08-16", warehouse="bcn1")
        self.assertEqual(read(partition).prefix_suffix, "ingestion_date=2026-08-16/wh=bcn1")

    def test_undeclared_paths_are_the_three_the_contract_predicts(self):
        partition = build_partition(self.root)
        names = {name for _, name in read(partition).undeclared_paths()}
        self.assertEqual(names, {"_manifest.json", "_run.log", "_SUCCESS"})


class TestRefusals(TemporaryRoot):
    def test_missing_manifest(self):
        partition = build_partition(self.root)
        os.unlink(os.path.join(partition, "_manifest.json"))
        with self.assertRaisesRegex(ManifestError, "manifesto nao encontrado"):
            read(partition)

    def test_unreadable_manifest(self):
        partition = build_partition(self.root)
        with open(os.path.join(partition, "_manifest.json"), "w", encoding="utf-8") as handle:
            handle.write("{ nao e json")
        with self.assertRaisesRegex(ManifestError, "ilegivel"):
            read(partition)

    def test_unsupported_manifest_version(self):
        """CONTRACT.md secao 8: recusar e melhor do que interpretar por adivinhacao."""
        partition = build_partition(self.root, manifest_version=99)
        with self.assertRaisesRegex(ManifestError, "manifest_version"):
            read(partition)

    def test_success_without_complete_is_incoherent(self):
        """Garantia 10: _SUCCESS existe se e somente se complete."""
        partition = build_partition(self.root, complete=False, write_success=True)
        with self.assertRaisesRegex(ManifestError, "contradiz"):
            read(partition)

    def test_complete_without_success_is_incoherent(self):
        partition = build_partition(self.root, complete=True, write_success=False)
        with self.assertRaisesRegex(ManifestError, "contradiz"):
            read(partition)

    def test_incomplete_partition_is_refused(self):
        partition = build_partition(self.root, complete=False, write_success=False)
        with self.assertRaisesRegex(ManifestError, "incompleta"):
            read(partition)

    def test_failures_in_manifest_are_fatal(self):
        """Garantia 16: falha registrada reprova a particao."""
        partition = build_partition(self.root, failures=[{"category_id": 112}])
        with self.assertRaisesRegex(ManifestError, "failures"):
            read(partition)

    def test_path_escaping_the_snapshot_root_is_refused(self):
        partition = build_partition(self.root)
        self._patch_manifest(
            partition,
            lambda m: m["files"].insert(0, {"path": "../../../etc/passwd", "sha256": "x",
                                            "bytes": 0, "stage": "catalog"}),
        )
        with self.assertRaisesRegex(ManifestError, "fora da raiz"):
            read(partition)

    def test_manifest_entry_without_path_is_refused(self):
        partition = build_partition(self.root)
        self._patch_manifest(partition, lambda m: m["files"].insert(0, {"sha256": "x"}))
        with self.assertRaisesRegex(ManifestError, "malformada"):
            read(partition)

    def test_partition_moved_on_disk_is_refused(self):
        """O armazem so existe no caminho e no manifesto (obrigacao 4.3). Se os dois
        divergem, a particao foi movida e aceitar isso perderia a identidade."""
        partition = build_partition(self.root, warehouse="mad1")
        renamed = partition.replace("wh=mad1", "wh=bcn1")
        os.rename(partition, renamed)
        with self.assertRaisesRegex(ManifestError, "nao corresponde ao manifesto"):
            read(renamed)

    def test_manifest_without_files_list(self):
        partition = build_partition(self.root)
        self._patch_manifest(partition, lambda m: m.pop("files"))
        with self.assertRaisesRegex(ManifestError, "sem a lista 'files'"):
            read(partition)


class TestSingleAxisPartition(TemporaryRoot):
    """Formato de uma source sem segundo eixo, como o INE: so ingestion_date=..., sem
    wh=... nem qualquer outro eixo. Duas formas de particao sao suportadas hoje; nao ha
    um terceiro caso a generalizar."""

    def test_reads_a_partition_without_a_second_axis(self):
        partition = build_single_axis_partition(self.root)
        result = read(partition)
        self.assertEqual(result.ingestion_date, "2026-08-24")
        self.assertIsNone(result.axis_name)
        self.assertIsNone(result.axis_value)
        self.assertIsNone(result.warehouse)
        self.assertEqual(result.source_name, "ine_population_api")

    def test_prefix_suffix_has_no_second_segment(self):
        partition = build_single_axis_partition(self.root, ingestion_date="2026-09-01")
        self.assertEqual(read(partition).prefix_suffix, "ingestion_date=2026-09-01")

    def test_resolves_paths_against_the_snapshot_root(self):
        """Mesma obrigacao 4.1 da Mercadona, com um nivel a menos: a raiz do snapshot
        fica um diretorio acima da particao, nao dois."""
        partition = build_single_axis_partition(self.root)
        for entry in read(partition).files:
            self.assertTrue(
                os.path.exists(entry.local_path),
                f"caminho nao resolvido para a raiz do snapshot: {entry.local_path}",
            )

    def test_partition_moved_on_disk_is_refused(self):
        partition = build_single_axis_partition(self.root, ingestion_date="2026-08-24")
        renamed = partition.replace("ingestion_date=2026-08-24", "ingestion_date=2026-08-25")
        os.rename(partition, renamed)
        with self.assertRaisesRegex(ManifestError, "nao corresponde ao manifesto"):
            read(renamed)

    def test_unregistered_source_name_is_refused(self):
        partition = build_single_axis_partition(self.root, source_name="algo_desconhecido")
        with self.assertRaisesRegex(ManifestError, "desconhecida desta plataforma"):
            read(partition)


class TestContractConstants(unittest.TestCase):
    def test_undeclared_files_matches_what_the_source_leaves_undeclared(self):
        """A Source nao declara estes tres em files[]; tratar um deles como orfao
        reprovaria uma particao correta."""
        self.assertEqual(
            set(manifest_module.UNDECLARED_FILES),
            {"_manifest.json", "_run.log", "_SUCCESS"},
        )


if __name__ == "__main__":
    unittest.main()
