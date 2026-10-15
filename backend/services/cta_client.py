"""HTTP client for the CTA Train Tracker Arrivals API.

Responsibilities:
  * build requests with documented parameters (key, mapid, outputType=JSON)
  * retry transient failures (network errors, timeouts, HTTP 5xx/429, CTA errCd 900)
  * translate HTTP and CTA-specific error codes into typed exceptions
  * never let the API key leak into logs or exception messages

Docs: https://www.transitchicago.com/developers/ttdocs/
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# urllib3 logs full request URLs (including ?key=...) at DEBUG level. Keep it quiet
# even when the application runs with LOG_LEVEL=DEBUG.
logging.getLogger("urllib3").setLevel(logging.WARNING)

USER_AGENT = "cta-transit-intelligence/0.1 (portfolio data collector)"

# CTA error codes from the Train Tracker docs (Arrivals API).
CTA_ERROR_CODES: dict[str, str] = {
    "0": "OK",
    "100": "Required parameter missing",
    "101": "Invalid API key",
    "102": "Maximum daily CTA Train Tracker API usage exceeded",
    "103": "Invalid mapid",
    "104": "mapid values must be integers",
    "105": "Maximum of 4 mapid values",
    "106": "Invalid route identifier",
    "107": "Maximum of 4 rt values",
    "108": "Invalid stpid",
    "109": "Maximum of 4 stpid values",
    "110": "Invalid max",
    "111": "Parameter 'max' must be a positive integer",
    "112": "stpid values must be integers",
    "500": "Invalid parameter",
    "900": "Server error",
}
RETRYABLE_CTA_ERROR_CODES = frozenset({"900"})


class CTAClientError(Exception):
    """Base class for all errors raised by the CTA client."""


class CTATransientError(CTAClientError):
    """A failure that may succeed if retried (network, timeout, 5xx, CTA server error)."""


class CTAResponseError(CTAClientError):
    """The response could not be interpreted (non-JSON, unexpected structure, HTTP 4xx)."""


class CTAApiError(CTAClientError):
    """CTA returned a well-formed response with a non-zero errCd."""

    def __init__(self, code: str, message: str, response: "ArrivalsResponse | None" = None):
        self.code = code
        self.response = response
        super().__init__(f"CTA API error {code}: {message}")


@dataclass(frozen=True)
class ArrivalsResponse:
    """A successfully received API response plus metadata about the request."""

    station_id: str
    payload: dict[str, Any]  # the parsed JSON body, unmodified
    http_status: int
    requested_at_utc: datetime
    received_at_utc: datetime
    elapsed_ms: int
    attempts: int
    request_params: dict[str, str]  # sanitized: never contains the API key

    @property
    def cta_error_code(self) -> str:
        return str(self.payload.get("ctatt", {}).get("errCd", ""))


def redact(text: str, secret: str | None) -> str:
    """Replace every occurrence of ``secret`` in ``text``."""
    if not secret:
        return text
    return text.replace(secret, "***REDACTED***")


class CTAClient:
    def __init__(
        self,
        api_key: str,
        *,
        base_url: str,
        timeout_seconds: float = 10.0,
        max_attempts: int = 3,
        session: requests.Session | None = None,
        backoff_initial_seconds: float = 2.0,
        backoff_max_seconds: float = 30.0,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self._api_key = api_key
        self._base_url = base_url
        self._timeout = timeout_seconds
        self._max_attempts = max_attempts
        self._backoff_initial = backoff_initial_seconds
        self._backoff_max = backoff_max_seconds
        self._session = session or requests.Session()
        self._session.headers.setdefault("User-Agent", USER_AGENT)

    def __repr__(self) -> str:  # never show the key
        return f"CTAClient(base_url={self._base_url!r})"

    def get_arrivals(self, station_id: str) -> ArrivalsResponse:
        """Fetch arrival predictions for one parent station (mapid).

        Raises CTAApiError for non-zero errCd (after retries, if retryable),
        CTATransientError when retries are exhausted, CTAResponseError for bad payloads.
        """
        public_params = {"mapid": station_id, "outputType": "JSON"}
        attempt_counter = {"n": 0}

        @retry(
            retry=retry_if_exception_type(CTATransientError),
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=self._backoff_initial, max=self._backoff_max),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )
        def _attempt() -> ArrivalsResponse:
            attempt_counter["n"] += 1
            return self._request_once(station_id, public_params, attempt_counter["n"])

        return _attempt()

    def _request_once(
        self, station_id: str, public_params: dict[str, str], attempt: int
    ) -> ArrivalsResponse:
        params = {"key": self._api_key, **public_params}
        requested_at = datetime.now(timezone.utc)
        started = time.perf_counter()
        try:
            resp = self._session.get(self._base_url, params=params, timeout=self._timeout)
        except requests.Timeout:
            raise CTATransientError(
                f"Timed out after {self._timeout}s requesting station {station_id}"
            ) from None
        except requests.RequestException as exc:
            # requests' messages can embed the full URL, query string included.
            raise CTATransientError(
                f"Network error requesting station {station_id}: "
                f"{redact(str(exc), self._api_key)}"
            ) from None
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        received_at = datetime.now(timezone.utc)

        if resp.status_code == 429 or resp.status_code >= 500:
            raise CTATransientError(f"HTTP {resp.status_code} for station {station_id}")
        if resp.status_code != 200:
            raise CTAResponseError(f"Unexpected HTTP {resp.status_code} for station {station_id}")

        try:
            payload = resp.json()
        except ValueError:
            raise CTAResponseError(
                f"Response for station {station_id} was not valid JSON "
                f"(content-type={resp.headers.get('Content-Type')!r})"
            ) from None

        if not isinstance(payload, dict) or not isinstance(payload.get("ctatt"), dict):
            raise CTAResponseError(f"Response for station {station_id} is missing 'ctatt' root")

        result = ArrivalsResponse(
            station_id=station_id,
            payload=payload,
            http_status=resp.status_code,
            requested_at_utc=requested_at,
            received_at_utc=received_at,
            elapsed_ms=elapsed_ms,
            attempts=attempt,
            request_params=dict(public_params),
        )

        code = result.cta_error_code
        if code != "0":
            message = payload["ctatt"].get("errNm") or CTA_ERROR_CODES.get(code, "Unknown error")
            if code in RETRYABLE_CTA_ERROR_CODES:
                raise CTATransientError(f"CTA errCd {code} ({message}) for station {station_id}")
            raise CTAApiError(code, redact(str(message), self._api_key), response=result)

        return result
