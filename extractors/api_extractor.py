"""
Generic HTTP API extractor — handles authentication, request lifecycle,
response validation, and provides hooks for pagination.

Used by concrete extractors:
  - OpenExchangeRatesExtractor  (currency rates)
  - RestCountriesExtractor      (country metadata)
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extractors.base_extractor import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)


class ApiExtractor(BaseExtractor):
    """
    Base class for REST API sources.

    Provides:
      - A pre-configured `requests.Session` with connection pooling,
        automatic TCP-level retries (separate from our tenacity retries),
        and configurable timeouts.
      - A `get()` helper that raises on HTTP errors and logs every request.
      - An optional `parse_response()` hook subclasses can override to
        transform the raw response before records are returned.

    Subclasses must implement:
        extract() -> list[dict]

    They typically call `self.get(url, params=...)` inside `extract()`.
    """

    #: Seconds to wait for connection + read (connect_timeout, read_timeout)
    DEFAULT_TIMEOUT: tuple[int, int] = (10, 30)

    def __init__(
        self,
        source_name: str,
        base_url: str,
        headers: dict[str, str] | None = None,
        timeout: tuple[int, int] | None = None,
    ) -> None:
        """
        Args:
            source_name: Identifier used in logs and metadata.
            base_url:    Root URL for the API (no trailing slash).
            headers:     Default headers sent on every request
                         (e.g., {"Authorization": "Bearer TOKEN"}).
            timeout:     (connect_timeout, read_timeout) in seconds.
        """
        super().__init__(source_name)
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout or self.DEFAULT_TIMEOUT
        self.session = self._build_session(headers or {})

    def _build_session(self, headers: dict[str, str]) -> requests.Session:
        """
        Build a requests.Session with:
          - Connection pooling (10 connections, 20 max pool size)
          - HTTP-level retry for transient 5xx / network errors
            (Note: these are TCP retries, not our tenacity retries)
          - Default headers (Content-Type, User-Agent, caller-supplied headers)
        """
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "etl-data-pipeline/1.0",
                **headers,
            }
        )
        return session

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        Perform a GET request and return the parsed JSON response.

        Args:
            endpoint: Path relative to base_url (leading slash is optional).
                      E.g., "/latest.json" or "v3.1/all"
            params:   Query parameters dict.

        Returns:
            Parsed JSON — could be a dict or a list depending on the API.

        Raises:
            ExtractionError: on HTTP error, timeout, or JSON parse failure.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self.logger.debug("GET %s | params=%s", url, params)

        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise ExtractionError(
                f"Request timed out: GET {url} — {exc}"
            ) from exc
        except requests.exceptions.HTTPError as exc:
            raise ExtractionError(
                f"HTTP error {response.status_code}: GET {url} — {exc}"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise ExtractionError(
                f"Request failed: GET {url} — {exc}"
            ) from exc

        try:
            data = response.json()
        except ValueError as exc:
            raise ExtractionError(
                f"Invalid JSON from {url}: {exc}"
            ) from exc

        self.logger.debug(
            "GET %s → %d | type=%s",
            url,
            response.status_code,
            type(data).__name__,
        )
        return data

    def parse_response(self, raw: Any) -> list[dict[str, Any]]:
        """
        Transform raw API response into a flat list of record dicts.

        Override this in subclasses when the API wraps records in a nested
        structure (e.g., {"rates": {...}} or {"data": [...]}).

        Default implementation handles:
          - list   → returned as-is
          - dict   → wrapped in a single-element list

        Args:
            raw: Parsed JSON from `get()`.

        Returns:
            Flat list of record dicts.
        """
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict):
            return [raw]
        raise ExtractionError(
            f"Unexpected response type from {self.source_name}: {type(raw)}"
        )

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        self.session.close()

    def __enter__(self) -> ApiExtractor:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
