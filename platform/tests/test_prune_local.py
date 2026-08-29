"""prune-local so apaga a copia local depois de PROVAR que ela esta intacta no destino.

A ordem e a garantia inteira: a mesma verificacao do verify-landing roda antes de qualquer
remocao. Nao ha modo --force de proposito — uma particao que nao esta integra no object
storage nao e redundante, e apagar exatamente essa seria o unico jeito de perder dado.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest import mock

from retail_platform.cli import EXIT_FAILED, EXIT_FATAL, EXIT_OK, main
from retail_platform.land import land

from .fake_s3 import FakeConfig, FakeS3Client
from .support import build_partition


class PruneLocalCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.client = FakeS3Client()
        self.config = FakeConfig(self.client)
        self.partition = build_partition(self.root, category_ids=(112, 113))
        patcher = mock.patch("retail_platform.cli.from_env", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def prune(self) -> int:
        return main(["prune-local", self.partition])


class TestPruneAfterLanding(PruneLocalCase):
    def setUp(self):
        super().setUp()
        land(self.config, self.partition)

    def test_apaga_a_copia_local_quando_o_destino_confere(self):
        self.assertTrue(os.path.isdir(self.partition))
        self.assertEqual(self.prune(), EXIT_OK)
        self.assertFalse(os.path.exists(self.partition))

    def test_o_object_storage_permanece_intacto(self):
        antes = dict(self.client.store)
        self.prune()
        self.assertEqual(self.client.store, antes)

    def test_nao_toca_particoes_vizinhas(self):
        outra = build_partition(self.root, warehouse="bcn1")
        self.prune()
        self.assertTrue(os.path.isdir(outra))


class TestPruneRecusa(PruneLocalCase):
    def test_recusa_quando_a_particao_nao_foi_aterrissada(self):
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "apagou sem ter destino")

    def test_recusa_quando_o_objeto_no_destino_foi_adulterado(self):
        land(self.config, self.partition)
        chave = next(k for k in self.client.store if k[1].endswith("category_id=112.json"))
        self.client.store[chave] = b"{}"
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "apagou com destino corrompido")

    def test_recusa_quando_falta_um_objeto_no_destino(self):
        land(self.config, self.partition)
        chave = next(k for k in self.client.store if k[1].endswith("category_id=113.json"))
        del self.client.store[chave]
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition))

    def test_recusa_quando_o_arquivo_local_diverge_do_proprio_manifesto(self):
        """verify-landing sozinho NAO pega este caso: ele compara o objeto com o
        manifesto, e o objeto continua certo. Sem a conferencia local, a remocao pareceria
        segura sem que ninguem tivesse olhado os bytes que estao sendo apagados."""
        land(self.config, self.partition)
        alvo = os.path.join(self.partition, "catalog", "category_id=112.json")
        with open(alvo, "a", encoding="utf-8") as handle:
            handle.write(" ")
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition), "apagou copia local corrompida")

    def test_recusa_quando_um_arquivo_local_sumiu(self):
        land(self.config, self.partition)
        os.unlink(os.path.join(self.partition, "catalog", "category_id=113.json"))
        self.assertEqual(self.prune(), EXIT_FAILED)
        self.assertTrue(os.path.isdir(self.partition))

    def test_recusa_caminho_que_nao_e_particao_sem_apagar_nada(self):
        """Guarda contra um argumento errado virar rm -rf em algo que nao e particao."""
        alvo = os.path.join(self.root, "nao-e-particao")
        os.makedirs(os.path.join(alvo, "coisas"), exist_ok=True)
        with open(os.path.join(alvo, "coisas", "arquivo.txt"), "w") as handle:
            handle.write("nao me apague")
        self.assertEqual(main(["prune-local", alvo]), EXIT_FATAL)
        self.assertTrue(os.path.isfile(os.path.join(alvo, "coisas", "arquivo.txt")))

    def test_recusa_diretorio_inexistente(self):
        self.assertEqual(
            main(["prune-local", os.path.join(self.root, "nao-existe")]), EXIT_FATAL
        )


if __name__ == "__main__":
    unittest.main()
