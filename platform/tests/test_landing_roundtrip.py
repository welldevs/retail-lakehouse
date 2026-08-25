"""As garantias de land e verify, exercitadas sem rede.

Estas verificacoes ja tinham sido feitas a mao contra o MinIO, e passaram. O problema e que
uma verificacao manual nao e reexecutavel: ninguem sabe se ela continua valendo depois da
proxima mudanca. Aqui elas viram teste.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest

from retail_platform import SOURCE_NAME
from retail_platform.land import LandingError, land
from retail_platform.verify import verify

from .fake_s3 import FakeConfig, FakeS3Client
from .support import build_partition, build_single_axis_partition


class LandingCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_partition(self.root, category_ids=(112, 113))
        self.prefix = f"{SOURCE_NAME}/ingestion_date=2026-08-24/wh=mad1"

    def _catalog_key(self, category_id: int = 112) -> str:
        return f"{self.prefix}/catalog/category_id={category_id}.json"


class TestLand(LandingCase):
    def test_lands_declared_files_plus_the_three_undeclared(self):
        result = land(self.config, self.partition)
        # 3 declarados (1 arvore + 2 catalogos) + _manifest.json + _run.log + _SUCCESS
        self.assertEqual(result.uploaded, 6)
        self.assertEqual(result.skipped, 0)
        self.assertEqual(
            self.client.keys(self.config.raw_bucket),
            {
                f"{self.prefix}/categories/categories.json",
                self._catalog_key(112),
                self._catalog_key(113),
                f"{self.prefix}/_manifest.json",
                f"{self.prefix}/_run.log",
                f"{self.prefix}/_SUCCESS",
            },
        )

    def test_success_is_the_last_object_written(self):
        """Um upload interrompido nunca pode parecer completo."""
        land(self.config, self.partition)
        keys = list(self.client.store)
        self.assertEqual(keys[-1][1], f"{self.prefix}/_SUCCESS")

    def test_is_idempotent(self):
        land(self.config, self.partition)
        puts_after_first = self.client.puts
        result = land(self.config, self.partition)
        self.assertEqual(result.uploaded, 0)
        self.assertEqual(result.skipped, 6)
        self.assertEqual(result.bytes_uploaded, 0)
        self.assertEqual(self.client.puts, puts_after_first, "reenviou objeto ja correto")

    def test_reuploads_only_what_diverged_at_the_destination(self):
        """Auto-correcao: objeto adulterado no destino volta a ser enviado, os outros nao."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, self._catalog_key(112))
        result = land(self.config, self.partition)
        self.assertEqual(result.uploaded, 1)
        self.assertEqual(result.skipped, 5)

    def test_refuses_to_upload_a_locally_corrupted_partition(self):
        """NAO PROPAGAR CORRUPCAO: o sha256 local e reconferido antes do PUT, sem assumir
        que `validate` rodou."""
        target = os.path.join(self.partition, "catalog", "category_id=112.json")
        with open(target, "ab") as handle:
            handle.write(b" ")
        with self.assertRaisesRegex(LandingError, "ANTES do upload"):
            land(self.config, self.partition)

    def test_aborts_before_writing_success(self):
        """A particao adulterada nao pode deixar _SUCCESS para tras."""
        with open(os.path.join(self.partition, "catalog", "category_id=113.json"), "ab") as h:
            h.write(b" ")
        with self.assertRaises(LandingError):
            land(self.config, self.partition)
        self.assertNotIn(
            (self.config.raw_bucket, f"{self.prefix}/_SUCCESS"), self.client.store
        )

    def test_refuses_a_partition_whose_declared_file_is_missing(self):
        os.unlink(os.path.join(self.partition, "catalog", "category_id=112.json"))
        with self.assertRaisesRegex(LandingError, "declarado e ausente"):
            land(self.config, self.partition)

    def test_declared_checksum_is_accepted_by_the_server(self):
        """Se a conversao hex->base64 estivesse errada, o duplo recusaria todo PUT — a
        mesma recusa que o servidor real faria."""
        land(self.config, self.partition)
        blob = self.client.store[(self.config.raw_bucket, self._catalog_key(112))]
        with open(os.path.join(self.partition, "catalog", "category_id=112.json"), "rb") as h:
            self.assertEqual(hashlib.sha256(blob).hexdigest(),
                             hashlib.sha256(h.read()).hexdigest())


class TestVerify(LandingCase):
    def test_passes_on_a_partition_just_landed(self):
        land(self.config, self.partition)
        errors, summary = verify(self.config, self.partition)
        self.assertEqual(errors, [])
        self.assertEqual(summary["declared"], 3)
        self.assertEqual(summary["checked"], 6)
        self.assertEqual(summary["orphans"], 0)

    def test_catches_a_tampered_object(self):
        """A verificacao que mais importa: o destino divergir do manifesto."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, self._catalog_key(112))
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("sha256 divergente" in e for e in errors), errors)

    def test_catches_a_missing_object(self):
        land(self.config, self.partition)
        self.client.drop(self.config.raw_bucket, self._catalog_key(113))
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("ausente ou ilegivel" in e for e in errors), errors)

    def test_catches_an_orphan_object(self):
        """Inventario fechado: objeto no prefixo que o manifesto nao declara e erro."""
        land(self.config, self.partition)
        self.client.store[(self.config.raw_bucket, f"{self.prefix}/catalog/extra.json")] = b"{}"
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("fora do manifesto" in e for e in errors), errors)

    def test_catches_a_manifest_that_differs_from_the_local_one(self):
        """O manifesto aterrissado tem de ser byte-identico ao local: sem isso, um
        inventario adulterado no destino passaria a ditar o que e 'correto' la."""
        land(self.config, self.partition)
        self.client.corrupt(self.config.raw_bucket, f"{self.prefix}/_manifest.json")
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("difere do arquivo local" in e for e in errors), errors)

    def test_catches_a_missing_success_marker(self):
        land(self.config, self.partition)
        self.client.drop(self.config.raw_bucket, f"{self.prefix}/_SUCCESS")
        errors, _ = verify(self.config, self.partition)
        self.assertTrue(any("_SUCCESS" in e for e in errors), errors)

    def test_verify_rereads_instead_of_trusting_head(self):
        """verify baixa o objeto e recalcula: um HEAD mentiroso nao o enganaria."""
        land(self.config, self.partition)
        gets_before = self.client.gets
        verify(self.config, self.partition)
        self.assertGreaterEqual(self.client.gets - gets_before, 6)


class TestLandSecondSourceWithoutAxis(unittest.TestCase):
    """A landing generica com uma segunda source (INE), sem eixo alem de ingestion_date,
    aterrissando ao lado da Mercadona no mesmo bucket sem nenhum codigo especifico."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_single_axis_partition(self.root, ingestion_date="2026-09-01")
        self.prefix = "ine_population_api/ingestion_date=2026-09-01"

    def test_lands_under_its_own_prefix_alongside_mercadona(self):
        result = land(self.config, self.partition)
        # 1 declarado (tables/table_id=31304.json) + _manifest.json + _run.log + _SUCCESS
        self.assertEqual(result.uploaded, 4)
        self.assertEqual(
            self.client.keys(self.config.raw_bucket),
            {
                f"{self.prefix}/tables/table_id=31304.json",
                f"{self.prefix}/_manifest.json",
                f"{self.prefix}/_run.log",
                f"{self.prefix}/_SUCCESS",
            },
        )

    def test_verify_passes_on_a_partition_just_landed(self):
        land(self.config, self.partition)
        errors, summary = verify(self.config, self.partition)
        self.assertEqual(errors, [])
        self.assertEqual(summary["declared"], 1)
        self.assertEqual(summary["checked"], 4)
        self.assertEqual(summary["orphans"], 0)


if __name__ == "__main__":
    unittest.main()
