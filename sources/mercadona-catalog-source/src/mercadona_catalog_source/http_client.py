"""Cliente HTTP sequencial contra a API publica da Mercadona.

Comportamento medido contra a fonte (ver README.md, secao "Restricoes da fonte"):
requisicoes sequenciais espacadas nao foram bloqueadas; rajadas concorrentes produziram
403 intermitente. Por isso o cliente e estritamente sequencial e limitado por taxa.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://tienda.mercadona.es/api"

# Identidade enviada a fonte. Ver README.md, secao "Restricoes da fonte": o host declara
# Disallow: /api em robots.txt e este User-Agent nao se identifica como robo.
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

RETRYABLE_STATUS = frozenset({403, 408, 429, 500, 502, 503, 504})

BACKOFF_BASE_SECONDS = 5.0
BACKOFF_CAP_SECONDS = 60.0


class Fetcher:
    """Cliente HTTP sequencial com throttle por taxa e backoff exponencial."""

    def __init__(
        self,
        delay: float,
        timeout: float,
        max_retries: int,
        log,
        base_url: str = BASE_URL,
    ) -> None:
        self.delay = delay
        self.timeout = timeout
        self.max_retries = max_retries
        self.log = log
        self.base_url = base_url
        self.requests = 0
        self.retries = 0
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        """Limita a taxa a 1/delay req/s, medindo do inicio da requisicao anterior."""
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def get_json(self, path: str, params: dict) -> tuple[object | None, str | None]:
        """Retorna (payload, None) em sucesso ou (None, motivo) apos esgotar as tentativas.

        O payload nao e validado aqui: qualquer JSON valido e devolvido como veio. A
        checagem de forma e responsabilidade de schema.py.
        """
        query = urllib.parse.urlencode(sorted(params.items()))
        url = f"{self.base_url}{path}?{query}" if query else f"{self.base_url}{path}"
        last_error = "sem tentativa"

        for attempt in range(self.max_retries + 1):
            self._throttle()
            request = urllib.request.Request(
                url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
            )
            try:
                self.requests += 1
                self._last_request_at = time.monotonic()
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    return json.loads(response.read().decode("utf-8")), None
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code}"
                retryable = exc.code in RETRYABLE_STATUS
            except Exception as exc:  # timeout, reset de conexao, corpo JSON invalido
                last_error = f"{type(exc).__name__}: {exc}"
                retryable = True

            if not retryable or attempt == self.max_retries:
                return None, last_error

            self.retries += 1
            backoff = min(BACKOFF_BASE_SECONDS * (2**attempt), BACKOFF_CAP_SECONDS)
            self.log(
                f"    retry {attempt + 1}/{self.max_retries} apos {last_error}; "
                f"aguardando {backoff:.0f}s"
            )
            time.sleep(backoff)

        return None, last_error
