"""HTTP client: throttle, retry, backoff and counters — without touching the network.

urlopen and time.sleep are replaced by doubles. No test opens a socket.
"""

from __future__ import annotations

import io
import json
import unittest
import urllib.error
from unittest import mock

from mercadona_catalog_source import http_client


def response(payload, status=200):
    body = json.dumps(payload).encode("utf-8")
    fake = mock.MagicMock()
    fake.read.return_value = body
    fake.__enter__ = lambda self: self
    fake.__exit__ = lambda self, *a: False
    fake.status = status
    return fake


def http_error(code):
    return urllib.error.HTTPError("u", code, "error", {}, io.BytesIO(b""))


class UrlBuildingTest(unittest.TestCase):
    def test_params_are_sorted_and_encoded(self):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["url"] = request.full_url
            captured["headers"] = request.headers
            return response({"ok": True})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/categories/", {"wh": "mad1", "lang": "es"})
        self.assertTrue(captured["url"].endswith("/categories/?lang=es&wh=mad1"))

    def test_sends_declared_user_agent(self):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["ua"] = request.get_header("User-agent")
            return response({})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertEqual(captured["ua"], http_client.USER_AGENT)

    def test_no_params_appends_no_question_mark(self):
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["url"] = request.full_url
            return response({})

        client = http_client.Fetcher(0, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertNotIn("?", captured["url"])


class RetryPolicyTest(unittest.TestCase):
    def setUp(self):
        self.sleeps: list[float] = []

    def _client(self, max_retries=5):
        return http_client.Fetcher(0, 5, max_retries, lambda m: None)

    def test_non_retryable_status_fails_immediately(self):
        client = self._client()
        with mock.patch("urllib.request.urlopen", side_effect=http_error(404)):
            with mock.patch("time.sleep", self.sleeps.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertEqual(error, "HTTP 404")
        self.assertEqual(client.requests, 1)
        self.assertEqual(client.retries, 0)
        self.assertEqual(self.sleeps, [])

    def test_410_also_fails_immediately(self):
        client = self._client()
        with mock.patch("urllib.request.urlopen", side_effect=http_error(410)):
            with mock.patch("time.sleep", self.sleeps.append):
                _, error = client.get_json("/x/", {})
        self.assertEqual(error, "HTTP 410")
        self.assertEqual(client.requests, 1)

    def test_exponential_backoff_capped_at_60s(self):
        client = self._client(max_retries=5)
        with mock.patch("urllib.request.urlopen", side_effect=http_error(503)):
            with mock.patch("time.sleep", self.sleeps.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertEqual(error, "HTTP 503")
        self.assertEqual(self.sleeps, [5.0, 10.0, 20.0, 40.0, 60.0])
        self.assertEqual(client.retries, 5)
        self.assertEqual(client.requests, 6)  # 1 attempt + 5 retries

    def test_every_declared_retryable_status_is_retried(self):
        for status in sorted(http_client.RETRYABLE_STATUS):
            with self.subTest(status=status):
                client = self._client(max_retries=1)
                with mock.patch("urllib.request.urlopen", side_effect=http_error(status)):
                    with mock.patch("time.sleep", lambda s: None):
                        client.get_json("/x/", {})
                self.assertEqual(client.retries, 1, f"status {status} was not retried")

    def test_invalid_json_body_is_retried(self):
        broken = mock.MagicMock()
        broken.read.return_value = b"<html>blocked</html>"
        broken.__enter__ = lambda self: self
        broken.__exit__ = lambda self, *a: False
        client = self._client(max_retries=2)
        with mock.patch("urllib.request.urlopen", return_value=broken):
            with mock.patch("time.sleep", self.sleeps.append):
                payload, error = client.get_json("/x/", {})
        self.assertIsNone(payload)
        self.assertIn("JSONDecodeError", error)
        self.assertEqual(client.retries, 2)

    def test_success_after_transient_failure(self):
        client = self._client(max_retries=3)
        sequence = [http_error(503), response({"ok": 1})]

        def fake_urlopen(request, timeout=None):
            item = sequence.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            with mock.patch("time.sleep", self.sleeps.append):
                payload, error = client.get_json("/x/", {})
        self.assertEqual(payload, {"ok": 1})
        self.assertIsNone(error)
        self.assertEqual(client.requests, 2)
        self.assertEqual(client.retries, 1)

    def test_max_retries_zero_does_not_sleep(self):
        client = self._client(max_retries=0)
        with mock.patch("urllib.request.urlopen", side_effect=http_error(503)):
            with mock.patch("time.sleep", self.sleeps.append):
                client.get_json("/x/", {})
        self.assertEqual(self.sleeps, [])
        self.assertEqual(client.requests, 1)


class ThrottleTest(unittest.TestCase):
    def test_first_request_does_not_sleep(self):
        sleeps: list[float] = []
        client = http_client.Fetcher(1.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", sleeps.append):
                client.get_json("/x/", {})
        self.assertEqual(sleeps, [])

    def test_second_request_completes_the_interval(self):
        sleeps: list[float] = []
        clock = iter([100.0, 100.0, 100.4, 100.4])
        client = http_client.Fetcher(1.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", sleeps.append):
                with mock.patch("time.monotonic", lambda: next(clock)):
                    client.get_json("/a/", {})
                    client.get_json("/b/", {})
        self.assertEqual(len(sleeps), 1)
        self.assertAlmostEqual(sleeps[0], 1.1, places=6)

    def test_does_not_sleep_when_interval_already_elapsed(self):
        sleeps: list[float] = []
        clock = iter([100.0, 100.0, 110.0, 110.0])
        client = http_client.Fetcher(1.5, 5, 0, lambda m: None)
        with mock.patch("urllib.request.urlopen", return_value=response({})):
            with mock.patch("time.sleep", sleeps.append):
                with mock.patch("time.monotonic", lambda: next(clock)):
                    client.get_json("/a/", {})
                    client.get_json("/b/", {})
        self.assertEqual(sleeps, [])

    def test_stamps_the_clock_at_request_start(self):
        # start-to-start: the timestamp is taken before urlopen, not after.
        client = http_client.Fetcher(1.5, 5, 0, lambda m: None)
        stamps = []

        def fake_urlopen(request, timeout=None):
            stamps.append(client._last_request_at)
            return response({})

        with mock.patch("urllib.request.urlopen", fake_urlopen):
            client.get_json("/x/", {})
        self.assertNotEqual(stamps[0], 0.0)


if __name__ == "__main__":
    unittest.main()
