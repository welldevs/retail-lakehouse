"""Sequential HTTP client for the INE's public Tempus3 API.

Unlike Mercadona, this API's rate-limit behavior was not measured: there is no concurrency
at all to measure (a run fetches a handful of table_ids, sequential by nature, no fan-out).
The throttle here exists as HTTP-client good practice, not as a response to an observed
block — see README.md, section "Source restrictions".
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://servicios.ine.es/wstempus/js"

USER_AGENT = "ine-population-source/1.0 (retail-lakehouse platform; contato via repositorio)"

RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})

BACKOFF_BASE_SECONDS = 5.0
BACKOFF_CAP_SECONDS = 60.0


class Fetcher:
    """Sequential HTTP client with rate throttling and exponential backoff."""

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
        """Limit the rate to 1/delay req/s, measured from the start of the previous request."""
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def get_json(self, path: str, params: dict) -> tuple[object | None, str | None]:
        """Return (payload, None) on success or (None, reason) after exhausting the attempts.

        The payload is not validated here: any valid JSON is returned as received. Shape
        checking is the responsibility of schema.py.
        """
        query = urllib.parse.urlencode(sorted(params.items()))
        url = f"{self.base_url}{path}?{query}" if query else f"{self.base_url}{path}"
        # Kept byte-identical: this reason can be persisted in the manifest's failures[].
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
            except Exception as exc:  # timeout, connection reset, invalid JSON body
                last_error = f"{type(exc).__name__}: {exc}"
                retryable = True

            if not retryable or attempt == self.max_retries:
                return None, last_error

            self.retries += 1
            backoff = min(BACKOFF_BASE_SECONDS * (2**attempt), BACKOFF_CAP_SECONDS)
            self.log(
                f"    retry {attempt + 1}/{self.max_retries} after {last_error}; "
                f"waiting {backoff:.0f}s"
            )
            time.sleep(backoff)

        return None, last_error
