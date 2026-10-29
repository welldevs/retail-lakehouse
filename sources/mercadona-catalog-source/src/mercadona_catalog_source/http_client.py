"""Sequential HTTP client for Mercadona's public API.

Behavior measured against the source (see README.md, section "Source restrictions"):
spaced sequential requests were not blocked; concurrent bursts produced intermittent
403s. That is why the client is strictly sequential and rate-limited.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = "https://tienda.mercadona.es/api"

# Identity sent to the source. See README.md, section "Source restrictions": the host declares
# Disallow: /api in robots.txt and this User-Agent does not identify itself as a robot.
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

RETRYABLE_STATUS = frozenset({403, 408, 429, 500, 502, 503, 504})

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
        """Limits the rate to 1/delay req/s, measured from the start of the previous request."""
        elapsed = time.monotonic() - self._last_request_at
        if self._last_request_at and elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def get_json(self, path: str, params: dict) -> tuple[object | None, str | None]:
        """Returns (payload, None) on success or (None, reason) after exhausting the attempts.

        The payload is not validated here: any valid JSON is returned as received. Shape
        checking is schema.py's responsibility.
        """
        query = urllib.parse.urlencode(sorted(params.items()))
        url = f"{self.base_url}{path}?{query}" if query else f"{self.base_url}{path}"
        # Kept verbatim ("no attempt"): it can be persisted into the manifest's failures[].error.
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
