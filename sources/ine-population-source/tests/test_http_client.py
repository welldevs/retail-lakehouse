"""Cliente HTTP: throttle, retry, backoff e contadores — sem tocar a rede.

urlopen e time.sleep sao substituidos por duplos. Nenhum teste abre socket.
"""

from __future__ import annotations

import io
import json
import unittest
import urllib.error
from unittest import mock

from ine_population_source import http_client


def response(payload, status=200):
    body = json.dumps(payload).encode("utf-8")
    fake = mock.MagicMock()
    fake.read.return_value = body
    fake.__enter__ = lambda self: self
    fake.__exit__ = lambda self, *a: False
    fake.status = status
    return fake


def http_error(code):
    return urllib.error.HTTPError("u", code, "erro", {}, io.BytesIO(b""))


class UrlBuildingTest(unittest.TestCase):
    def test_parametros_sao_ordenados_e_codificados(self):
        capturado = {}

        def fake_urlopen(request, timeout=None):
            capturado["url"] = request.full_url
            return response({"ok": True})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/ES/DATOS_TABLA/31304", {"nult": "1", "det": "0"})
        self.assertTrue(capturado["url"].endswith("/ES/DATOS_TABLA/31304?det=0&nult=1"))

    def test_envia_user_agent_declarado(self):
        capturado = {}

        def fake_urlopen(request, timeout=None):
            capturado["ua"] = request.get_header("User-agent")
            return response({})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertEqual(capturado["ua"], http_client.USER_AGENT)

    def test_sem_parametros_nao_acrescenta_interrogacao(self):
        capturado = {}

        def fake_urlopen(request, timeout=None):
            capturado["url"] = request.full_url
            return response({})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertNotIn("?", capturado["url"])


class RetryPolicyTest(unittest.TestCase):
    def setUp(self):
        self.dormidas: list[float] = []

    def _client(self, max_retries=5):
        return http_client.Fetcher(0, 5, max_retries, lambda m: None)

    def test_status_nao_retentavel_falha_de_imediato(self):
        client = self._client()
        with mock.patch("urllib.request.urlopen", side_effect=http_error(404)):
            with mock.patch("time.sleep", self.dormidas.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertEqual(error, "HTTP 404")
        self.assertEqual(client.requests, 1)
        self.assertEqual(client.retries, 0)
        self.assertEqual(self.dormidas, [])

    def test_403_nao_e_retentavel_aqui(self):
        """Diferente da Mercadona: sem concorrencia medida contra o INE, nao ha
        evidencia de bloqueio por rajada que justifique tratar 403 como transitorio."""
        client = self._client()
        with mock.patch("urllib.request.urlopen", side_effect=http_error(403)):
            with mock.patch("time.sleep", self.dormidas.append):
                _, error = client.get_json("/x/", {})
        self.assertEqual(error, "HTTP 403")
        self.assertEqual(client.requests, 1)
        self.assertEqual(client.retries, 0)

    def test_backoff_exponencial_com_teto_de_60s(self):
        client = self._client(max_retries=5)
        with mock.patch("urllib.request.urlopen", side_effect=http_error(503)):
            with mock.patch("time.sleep", self.dormidas.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertEqual(error, "HTTP 503")
        self.assertEqual(self.dormidas, [5.0, 10.0, 20.0, 40.0, 60.0])
        self.assertEqual(client.retries, 5)
        self.assertEqual(client.requests, 6)  # 1 tentativa + 5 retries

    def test_todos_os_status_retentaveis_declarados_sao_retentados(self):
        for status in sorted(http_client.RETRYABLE_STATUS):
            with self.subTest(status=status):
                client = self._client(max_retries=1)
                with mock.patch("urllib.request.urlopen", side_effect=http_error(status)):
                    with mock.patch("time.sleep", lambda s: None):
                        client.get_json("/x/", {})
                self.assertEqual(client.retries, 1, f"status {status} nao foi retentado")

    def test_corpo_json_invalido_e_retentado(self):
        quebrado = mock.MagicMock()
        quebrado.read.return_value = b"<html>fora do ar</html>"
        quebrado.__enter__ = lambda self: self
        quebrado.__exit__ = lambda self, *a: False
        client = self._client(max_retries=2)
        with mock.patch("urllib.request.urlopen", return_value=quebrado):
            with mock.patch("time.sleep", self.dormidas.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertIn("JSONDecodeError", error)
        self.assertEqual(client.retries, 2)

    def test_sucesso_apos_falha_transitoria(self):
        client = self._client(max_retries=3)
        sequencia = [http_error(503), response({"ok": 1})]

        def fake_urlopen(request, timeout=None):
            item = sequencia.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            with mock.patch("time.sleep", self.dormidas.append):
                payload, error = client.get_json("/x/", {})
        self.assertEqual(payload, {"ok": 1})
        self.assertIsNone(error)
        self.assertEqual(client.requests, 2)
        self.assertEqual(client.retries, 1)

    def test_max_retries_zero_nao_dorme(self):
        client = self._client(max_retries=0)
        with mock.patch("urllib.request.urlopen", side_effect=http_error(503)):
            with mock.patch("time.sleep", self.dormidas.append):
                client.get_json("/x/", {})
        self.assertEqual(self.dormidas, [])
        self.assertEqual(client.requests, 1)


class ThrottleTest(unittest.TestCase):
    def test_primeira_requisicao_nao_dorme(self):
        dormidas: list[float] = []
        client = http_client.Fetcher(0.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", dormidas.append):
                client.get_json("/x/", {})
        self.assertEqual(dormidas, [])

    def test_segunda_requisicao_completa_o_intervalo(self):
        dormidas: list[float] = []
        relogio = iter([100.0, 100.0, 100.2, 100.2])
        client = http_client.Fetcher(0.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", dormidas.append):
                with mock.patch("time.monotonic", lambda: next(relogio)):
                    client.get_json("/a/", {})
                    client.get_json("/b/", {})
        self.assertEqual(len(dormidas), 1)
        self.assertAlmostEqual(dormidas[0], 0.3, places=6)

    def test_nao_dorme_quando_intervalo_ja_passou(self):
        dormidas: list[float] = []
        relogio = iter([100.0, 100.0, 110.0, 110.0])
        client = http_client.Fetcher(0.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", dormidas.append):
                with mock.patch("time.monotonic", lambda: next(relogio)):
                    client.get_json("/a/", {})
                    client.get_json("/b/", {})
        self.assertEqual(dormidas, [])

    def test_marca_o_relogio_no_inicio_da_requisicao(self):
        client = http_client.Fetcher(0.5, 5, 0, lambda m: None)
        marcas = []

        def fake_urlopen(request, timeout=None):
            marcas.append(client._last_request_at)
            return response({})

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertNotEqual(marcas[0], 0.0)


if __name__ == "__main__":
    unittest.main()
