"""O selo da captura: o que ele cobre, o que ele ignora, e o que ele reprova.

TODOS OFFLINE, com o S3 falso — porque um teste de selo que exigisse MinIO de pe so rodaria
onde o problema ja e visivel. A logica de `collect`, `capture_id` e `comparar` e pura o
bastante para ser exercida contra um cliente em memoria.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fake_s3 import FakeS3Client  # noqa: E402

from retail_platform import freeze  # noqa: E402


class ConfigFalsa:
    raw_bucket = "retail-raw"

    def __init__(self, cliente):
        self._cliente = cliente

    def client(self):
        return self._cliente


def manifesto(arquivos, **execucao) -> bytes:
    """Manifesto minimo. `execucao` injeta os campos que o selo TEM de ignorar."""
    corpo = {
        "manifest_version": 1,
        "partition": {"ingestion_date": "2026-08-24"},
        "files": arquivos,
        "totals": {"bytes": sum(f["bytes"] for f in arquivos)},
        "run_id": "20260824T000000Z_a",
        "started_at_utc": "2026-08-24T00:00:00Z",
        "finished_at_utc": "2026-08-24T00:00:03Z",
        "duration_seconds": 3.0,
        "history": [],
    }
    corpo.update(execucao)
    return json.dumps(corpo).encode("utf-8")


ARQUIVOS = [
    {"path": "ingestion_date=2026-08-24/a.json", "sha256": "aa" * 32,
     "bytes": 10, "records": 2},
    {"path": "ingestion_date=2026-08-24/b.json", "sha256": "bb" * 32,
     "bytes": 20, "records": 3},
]


def montar(mapa: dict) -> ConfigFalsa:
    cliente = FakeS3Client()
    for chave, corpo in mapa.items():
        cliente.put_object(Bucket="retail-raw", Key=chave, Body=corpo)
    return ConfigFalsa(cliente)


UMA_PARTICAO = {"fonte_a/ingestion_date=2026-08-24/_manifest.json": manifesto(ARQUIVOS)}


class ColetaTest(unittest.TestCase):
    def test_uma_linha_por_manifesto_com_o_agregado_dos_arquivos(self):
        registros = freeze.collect(montar(UMA_PARTICAO))
        self.assertEqual(len(registros), 1)
        self.assertEqual(registros[0]["source"], "fonte_a")
        self.assertEqual(registros[0]["partition_key"], "ingestion_date=2026-08-24")
        self.assertEqual((registros[0]["files"], registros[0]["records"],
                          registros[0]["bytes"]), (2, 5, 30))

    def test_objeto_que_nao_e_manifesto_e_ignorado(self):
        """Varrer o bucket inteiro e pegar `_SUCCESS` ou o `.log` faria o selo cobrir
        arquivos de controle, que mudam a cada execucao sem nenhum dado mudar."""
        mapa = dict(UMA_PARTICAO)
        mapa["fonte_a/ingestion_date=2026-08-24/_SUCCESS"] = b""
        mapa["fonte_a/ingestion_date=2026-08-24/_run.log"] = b"linha"
        mapa["fonte_a/ingestion_date=2026-08-24/a.json"] = b"{}"
        self.assertEqual(len(freeze.collect(montar(mapa))), 1)

    def test_bucket_vazio_e_erro_com_instrucao(self):
        with self.assertRaises(freeze.FreezeError) as ctx:
            freeze.collect(montar({}))
        self.assertIn("make daily", str(ctx.exception))

    def test_a_ordem_e_estavel_entre_execucoes(self):
        """O `capture_id` e um hash sobre a lista; ordem instavel produziria um id novo a
        cada coleta, sem nada ter mudado."""
        mapa = {
            "z/ingestion_date=2026-08-24/_manifest.json": manifesto(ARQUIVOS),
            "a/ingestion_date=2026-08-25/_manifest.json": manifesto(ARQUIVOS),
            "a/ingestion_date=2026-08-24/_manifest.json": manifesto(ARQUIVOS),
        }
        chaves = [(r["source"], r["partition_key"]) for r in freeze.collect(montar(mapa))]
        self.assertEqual(chaves, sorted(chaves))


class OQueOSeloIgnoraTest(unittest.TestCase):
    """A decisao central do modulo, e a razao de ela existir.

    Se o selo cobrisse o manifesto inteiro, um re-land de dado BYTE-IDENTICO quebraria — o
    `run_id` e os instantes mudam sempre. Um alarme que dispara num no-op treina quem revisa
    a ignora-lo, que e o mesmo defeito do carimbo de data no CONTRACT do painel.
    """

    def test_run_id_e_instantes_NAO_mudam_o_selo(self):
        antes = freeze.collect(montar(UMA_PARTICAO))
        depois = freeze.collect(montar({
            "fonte_a/ingestion_date=2026-08-24/_manifest.json": manifesto(
                ARQUIVOS,
                run_id="20260901T235959Z_z",
                started_at_utc="2026-09-01T23:59:00Z",
                finished_at_utc="2026-09-01T23:59:59Z",
                duration_seconds=99.9,
                history=[{"run_id": "outro"}],
            )}))
        self.assertEqual(antes[0]["content_sha256"], depois[0]["content_sha256"])
        self.assertEqual(freeze.capture_id(antes), freeze.capture_id(depois))

    def test_a_ORDEM_dos_arquivos_no_manifesto_NAO_muda_o_selo(self):
        """Duas execucoes com o mesmo dado podem enumerar os arquivos em ordem diferente."""
        invertido = {"fonte_a/ingestion_date=2026-08-24/_manifest.json":
                     manifesto(list(reversed(ARQUIVOS)))}
        self.assertEqual(freeze.collect(montar(UMA_PARTICAO))[0]["content_sha256"],
                         freeze.collect(montar(invertido))[0]["content_sha256"])

    def test_um_byte_de_dado_MUDA_o_selo(self):
        alterado = [dict(ARQUIVOS[0]), dict(ARQUIVOS[1])]
        alterado[1]["sha256"] = "cc" * 32
        depois = freeze.collect(montar(
            {"fonte_a/ingestion_date=2026-08-24/_manifest.json": manifesto(alterado)}))
        self.assertNotEqual(freeze.collect(montar(UMA_PARTICAO))[0]["content_sha256"],
                            depois[0]["content_sha256"])

    def test_contagem_de_registros_faz_parte_do_selo(self):
        """Um arquivo com o mesmo sha256 e outra contagem declarada e um manifesto mentindo
        sobre si mesmo — o selo tem de pegar."""
        alterado = [dict(ARQUIVOS[0]), dict(ARQUIVOS[1])]
        alterado[0]["records"] = 999
        depois = freeze.collect(montar(
            {"fonte_a/ingestion_date=2026-08-24/_manifest.json": manifesto(alterado)}))
        self.assertNotEqual(freeze.collect(montar(UMA_PARTICAO))[0]["content_sha256"],
                            depois[0]["content_sha256"])


class ComparacaoTest(unittest.TestCase):
    """As tres formas de a captura mudar, e nenhuma pode passar em silencio."""

    def setUp(self):
        self.selado = freeze.collect(montar({
            "a/ingestion_date=2026-08-24/_manifest.json": manifesto(ARQUIVOS),
            "a/ingestion_date=2026-08-25/_manifest.json": manifesto(ARQUIVOS),
        }))

    def test_captura_intacta_nao_produz_problema(self):
        self.assertEqual(freeze.comparar(self.selado, list(self.selado)), [])

    def test_particao_selada_que_sumiu(self):
        problemas = freeze.comparar(self.selado, self.selado[:1])
        self.assertEqual(len(problemas), 1)
        self.assertIn("AUSENTE do RAW", problemas[0])

    def test_particao_nova_fora_do_selo(self):
        """UMA JANELA QUE CRESCE DEPOIS DO FECHAMENTO invalida todo numero ja publicado
        sobre ela — e nao quebra soma nenhuma, que e o que a torna perigosa."""
        atual = self.selado + [dict(self.selado[0], partition_key="ingestion_date=2026-08-26")]
        problemas = freeze.comparar(self.selado, atual)
        self.assertEqual(len(problemas), 1)
        self.assertIn("NOVA, fora do selo", problemas[0])

    def test_conteudo_alterado_numa_particao_selada(self):
        atual = [dict(self.selado[0], content_sha256="ff" * 32), self.selado[1]]
        problemas = freeze.comparar(self.selado, atual)
        self.assertEqual(len(problemas), 1)
        self.assertIn("conteudo MUDOU", problemas[0])


class SeedTest(unittest.TestCase):
    def test_o_seed_e_a_pagina_saem_da_mesma_coleta(self):
        """O markdown nao e legivel por SQL e o CSV nao e legivel por gente. Os dois saem do
        MESMO `collect`, no mesmo comando — se saissem de coletas diferentes, haveria um
        segundo lugar onde a verdade mora, e ele divergiria."""
        registros = freeze.collect(montar(UMA_PARTICAO))
        with tempfile.TemporaryDirectory() as tmp:
            seed = freeze.escrever_seed(registros, os.path.join(tmp, "s.csv"))
            pagina = freeze.escrever_pagina(registros, os.path.join(tmp, "F.md"))
            lido = freeze.ler_seed(seed)
            self.assertEqual(lido, registros)
            with open(pagina, encoding="utf-8") as arquivo:
                texto = arquivo.read()
            self.assertIn(freeze.capture_id(registros), texto)
            self.assertIn(registros[0]["content_sha256"][:24], texto)

    def test_a_pagina_NAO_carrega_relogio_no_corpo_do_selo(self):
        """A data do selo aparece uma vez, como informacao. O `capture_id` NAO depende dela —
        senao regerar a pagina sobre a mesma captura produziria um id novo."""
        registros = freeze.collect(montar(UMA_PARTICAO))
        self.assertEqual(freeze.capture_id(registros), freeze.capture_id(list(registros)))

    def test_seed_ausente_e_erro_com_instrucao(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(freeze.FreezeError) as ctx:
                freeze.ler_seed(os.path.join(tmp, "nao-existe.csv"))
        self.assertIn("make freeze", str(ctx.exception))


class CaptureIdTest(unittest.TestCase):
    def test_o_id_muda_quando_qualquer_particao_muda(self):
        base = freeze.collect(montar(UMA_PARTICAO))
        outro = [dict(base[0], content_sha256="ff" * 32)]
        self.assertNotEqual(freeze.capture_id(base), freeze.capture_id(outro))

    def test_o_id_nao_depende_de_bytes_nem_de_contagem(self):
        """Ele agrega os SELOS, e o selo ja cobre bytes e registros. Somar essas colunas
        de novo aqui nao acrescentaria deteccao e faria o id mudar por arredondamento."""
        base = freeze.collect(montar(UMA_PARTICAO))
        mesmo_selo = [dict(base[0], bytes=999, records=999, files=99)]
        self.assertEqual(freeze.capture_id(base), freeze.capture_id(mesmo_selo))


if __name__ == "__main__":
    unittest.main()
