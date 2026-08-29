"""A EVIDENCIA DO STREAM, conferida sem broker, sem Postgres e sem Iceberg.

O relatorio existe porque a metade em streaming e divida declarada: `make test` roda sem
rede, entao os motores so existem enquanto `make stream-up` estiver de pe. Isso torna o
relatorio o unico registro de que a SEMANTICA deles foi exercida — e o modo de falha que
importa nao e "quebrou", e "escreveu algo plausivel e errado".

Os testes abaixo miram exatamente nisso, e a assimetria com a evidencia do warehouse e
deliberada: la a captura ou acontece inteira ou nao acontece, porque e uma conexao so; aqui
sao TRES planos independentes, e o caso normal e um deles estar fora do ar. O que nao pode
acontecer e um plano ausente virar um zero — "o outbox tem 0 eventos" e uma afirmacao muito
diferente de "o OLTP nao respondeu", e as duas cabem na mesma celula de tabela.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from retail_platform.stream_evidence import render, write  # noqa: E402


def _oltp():
    return {
        "orders": 6400, "order_lines": 120693, "outbox_rows": 44456,
        "unpublished": 0, "orders_in_outbox": 6400,
        "published_from": "2026-08-28 18:38:48+00:00",
        "published_to": "2026-08-28 18:45:17+00:00",
        "by_event_type": {"order_placed": 6400, "order_delivered": 6046},
        "orders_by_status": {"DELIVERED": 5985, "CANCELLED": 196},
        "lines_by_status": {"fulfilled": 108194, "removed": 2321},
    }


def _broker():
    return {
        "topic": "retail.orders.events.v1",
        "bootstrap": "localhost:9092",
        "watermarks": {0: {"low": 0, "high": 11306}, 1: {"low": 0, "high": 11164}},
        "lags": {
            "orders-projector": {
                0: {"committed": 11306, "high": 11306, "lag": 0},
                1: {"committed": 11164, "high": 11164, "lag": 0},
            },
            "orders-projector-iceberg": {
                0: {"committed": 11000, "high": 11306, "lag": 306},
                1: {"committed": 11164, "high": 11164, "lag": 0},
            },
        },
    }


def _projecao():
    return {
        "table": "projection.live_order_state",
        "metadata_location": "s3://retail-lakehouse/iceberg/.../00052-abc.metadata.json",
        "rows": 6400, "snapshots": 105, "current_snapshot_id": 304920774543672206,
        "written_by": {"rebuild": 4800, "stream": 1600},
        "by_status": {"DELIVERED": 5985},
    }


def _reconciliacao(ok=True):
    if ok:
        return {"orders": {"iceberg": 6400, "silver": 6400, "oltp": 6400},
                "compared": 6400, "missing": {}, "divergences": [],
                "divergence_count": 0, "ok": True}
    return {
        "orders": {"iceberg": 6400, "silver": 6400, "oltp": 6399},
        "compared": 6399, "missing": {"oltp": ["ord_mad1_20260827_000010"]},
        "divergences": [{"order_id": "ord_vlc1_20260824_000001", "coluna": "net_amount",
                         "iceberg": "10.00", "silver": "10.50", "oltp": "10.00"}],
        "divergence_count": 1, "ok": False,
    }


def _dados(**substituir):
    base = {
        "capturado_em": "2026-08-29 13:32:28 UTC",
        "oltp": _oltp(),
        "broker": _broker(),
        "projecao": _projecao(),
        "reconciliacao": _reconciliacao(),
    }
    base.update(substituir)
    return base


class PlanoCompletoTest(unittest.TestCase):
    def setUp(self):
        self.texto = render(_dados())

    def test_a_data_da_captura_aparece(self):
        """Sem data, o relatorio nao e evidencia — e afirmacao."""
        self.assertIn("2026-08-29 13:32:28 UTC", self.texto)

    def test_os_tres_planos_aparecem(self):
        for cabecalho in ("Plano transacional", "Transporte — Kafka", "Projeção — Iceberg",
                          "Os três folds"):
            self.assertIn(cabecalho, self.texto)

    def test_o_total_do_topico_e_somado_e_nao_escrito(self):
        """11.306 + 11.164 = 22.470. Um total escrito a mao envelhece na primeira execucao."""
        self.assertIn("22,470 mensagens", self.texto)

    def test_a_proveniencia_dos_dois_escritores_e_publicada(self):
        """`written_by` e a unica prova CONSULTAVEL de que ha dois escritores. Sem ela, o
        gatilho do Iceberg ("um segundo engine precisar escrever a mesma tabela") vira
        anedota."""
        self.assertIn("`rebuild`", self.texto)
        self.assertIn("4,800", self.texto)
        self.assertIn("`stream`", self.texto)
        self.assertIn("1,600", self.texto)

    def test_o_lag_de_cada_grupo_e_separado(self):
        """Um lag somado esconderia justamente o ponto de desacoplamento: o sink Iceberg
        pode estar 306 mensagens atras sem que isso afete o sink Postgres."""
        self.assertIn("`orders-projector`", self.texto)
        self.assertIn("`orders-projector-iceberg`", self.texto)
        self.assertIn("Lag total: **306**", self.texto)
        self.assertIn("Lag total: **0**", self.texto)

    def test_o_metadado_corrente_e_publicado_com_o_caminho_completo(self):
        """E o que permite ao DuckDB ler a tabela sem adivinhar — e o que prova que o
        caminho veio do CATALOGO."""
        self.assertIn("00052-abc.metadata.json", self.texto)

    def test_o_acordo_dos_tres_folds_e_declarado_como_acordo(self):
        self.assertIn("os três concordam", self.texto)
        self.assertIn("6,400 pedidos", self.texto)


class DivergenciaTest(unittest.TestCase):
    def test_divergencia_aparece_com_a_linha_e_a_coluna(self):
        """O modo de falha mais caro deste relatorio seria uma divergencia virar um resumo.
        Quem le precisa do order_id e da coluna para ir olhar."""
        texto = render(_dados(reconciliacao=_reconciliacao(ok=False)))
        self.assertIn("**Divergências: 1.**", texto)
        self.assertIn("ord_vlc1_20260824_000001", texto)
        self.assertIn("net_amount", texto)
        self.assertNotIn("os três concordam", texto)

    def test_pedido_ausente_de_uma_fonte_aparece_nomeado(self):
        texto = render(_dados(reconciliacao=_reconciliacao(ok=False)))
        self.assertIn("ausentes de `oltp`", texto)
        self.assertIn("ord_mad1_20260827_000010", texto)


class PlanoDesligadoTest(unittest.TestCase):
    """O caso NORMAL, e nao o excepcional: os tres planos sobem sob demanda."""

    def test_plano_ausente_aparece_como_ausencia_e_nunca_como_zero(self):
        """"O outbox tem 0 eventos" e "o OLTP nao respondeu" cabem na mesma celula de
        tabela e significam coisas opostas. A primeira e uma medicao; a segunda e a falta
        de uma. Um relatorio que as confunde e pior que nenhum relatorio."""
        texto = render(_dados(oltp={"erro": "connection refused"}))
        self.assertIn("**OLTP nao observado.**", texto)
        self.assertIn("connection refused", texto)
        self.assertIn("make stream-up", texto)
        self.assertNotIn("| eventos no `outbox` | 0 |", texto)

    def test_um_plano_fora_nao_derruba_os_outros(self):
        """Evidencia parcial e util; evidencia que nao existe nao e."""
        texto = render(_dados(broker={"erro": "broker nao existe"}))
        self.assertIn("**Broker nao observado.**", texto)
        self.assertIn("| pedidos em `orders` | 6,400 |", texto)
        self.assertIn("`rebuild`", texto)

    def test_grupo_que_falhou_nao_se_disfarca_de_lag_zero(self):
        """Lag zero e a MELHOR noticia possivel; um grupo ilegivel nao pode se passar por
        ela. E a mesma armadilha da amostra vazia na evidencia do warehouse."""
        broker = _broker()
        broker["lags"]["orders-projector-iceberg"] = {"erro": "grupo desconhecido"}
        texto = render(_dados(broker=broker))
        self.assertIn("não observado: `grupo desconhecido`", texto)

    def test_todos_os_planos_fora_ainda_produz_um_relatorio_legivel(self):
        texto = render(_dados(oltp={"erro": "a"}, broker={"erro": "b"},
                              projecao={"erro": "c"}, reconciliacao={"erro": "d"}))
        self.assertEqual(texto.count("nao observado"), 4)
        self.assertIn("2026-08-29 13:32:28 UTC", texto)


class EscritaTest(unittest.TestCase):
    def test_escreve_atomicamente_e_nao_deixa_temporario(self):
        """Um relatorio truncado por interrupcao nao pode parecer completo — mesma
        disciplina do parquet do recorte e do landing."""
        with tempfile.TemporaryDirectory() as tmp:
            destino = os.path.join(tmp, "sub", "README.md")
            escrito = write(_dados(), destino)
            self.assertTrue(os.path.exists(escrito))
            self.assertEqual(
                [f for f in os.listdir(os.path.dirname(escrito)) if f.endswith(".tmp")], []
            )
            self.assertIn("Evidência do plano de stream",
                          open(escrito, encoding="utf-8").read())


if __name__ == "__main__":
    unittest.main()
