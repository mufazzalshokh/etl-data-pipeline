"""
Abstract base class for all data extractors.

Design principles:
  - Template Method pattern: subclasses implement `extract()`, base class handles
    metadata enrichment, logging, retry logic, and error wrapping.
  - Every extractor is stateless — results are returned, not stored internally.
  - Data lineage metadata is injected at this layer so every record is traceable
    back to its source, extraction timestamp, and pipeline run.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised when an extractor fails to retrieve data after all retries."""


class BaseExtractor(ABC):
    """
    Abstract base for all pipeline extractors.

    Subclasses must implement:
        extract() -> list[dict]   — return raw records from the source

    Subclasses may override:
        validate_record(record)   — apply source-specific validation logic

    Usage:
        class MyExtractor(BaseExtractor):
            def extract(self) -> list[dict]:
                return [{"key": "value"}]

        extractor = MyExtractor(source_name="my_source")
        records = extractor.extract_with_metadata(pipeline_run_id="run_123")
    """

    #: How many times to retry a failed extraction before giving up
    MAX_RETRIES: int = 3

    #: Seconds to wait between the first and second retry attempt
    RETRY_WAIT_MIN: int = 2

    #: Maximum seconds to wait between retries (exponential backoff ceiling)
    RETRY_WAIT_MAX: int = 30

    def __init__(self, source_name: str) -> None:
        """
        Args:
            source_name: Human-readable identifier for the data source.
                         Used in logs, metadata fields, and audit tables.
                         Example: "open_exchange_rates", "rest_countries"
        """
        self.source_name = source_name
        self.logger = logging.getLogger(f"extractor.{source_name}")


    @abstractmethod
    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch raw records from the data source.

        Returns:
            A list of dicts, each representing one raw record.
            Do NOT add metadata here — `extract_with_metadata` handles that.

        Raises:
            ExtractionError: if the source is unreachable or returns invalid data.
        """


    def extract_with_metadata(
        self,
        pipeline_run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Extract records and enrich each one with pipeline lineage metadata.

        This is the method DAGs should call — never `extract()` directly.

        Args:
            pipeline_run_id: Airflow run_id passed in from the DAG context.
                             Stored on every record for full data lineage.

        Returns:
            List of records with the following metadata fields appended:
              _source         — source identifier (e.g., "open_exchange_rates")
              _extracted_at   — UTC ISO-8601 timestamp of this extraction
              _pipeline_run_id — Airflow run_id for lineage tracing

        Raises:
            ExtractionError: if extraction fails after all retries.
        """
        self.logger.info(
            "Starting extraction | source=%s | run_id=%s",
            self.source_name,
            pipeline_run_id,
        )

        records = self._extract_with_retry()
        extracted_at = datetime.now(tz=timezone.utc).isoformat()

        enriched = [
            {
                **record,
                "_source": self.source_name,
                "_extracted_at": extracted_at,
                "_pipeline_run_id": pipeline_run_id,
            }
            for record in records
        ]

        self.logger.info(
            "Extraction complete | source=%s | records=%d",
            self.source_name,
            len(enriched),
        )
        return enriched


    def _extract_with_retry(self) -> list[dict[str, Any]]:
        """
        Wraps `extract()` with exponential backoff retry logic via tenacity.

        Retries on any Exception (network timeouts, transient API errors, etc.).
        After MAX_RETRIES attempts, raises ExtractionError with context.
        """

        @retry(
            stop=stop_after_attempt(self.MAX_RETRIES),
            wait=wait_exponential(
                multiplier=1,
                min=self.RETRY_WAIT_MIN,
                max=self.RETRY_WAIT_MAX,
            ),
            retry=retry_if_exception_type(Exception),
            before_sleep=before_sleep_log(self.logger, logging.WARNING),
            reraise=False,
        )
        def _attempt() -> list[dict[str, Any]]:
            return self.extract()

        try:
            return _attempt()
        except Exception as exc:
            raise ExtractionError(
                f"Extraction failed for source '{self.source_name}' "
                f"after {self.MAX_RETRIES} attempts: {exc}"
            ) from exc


    def validate_record(self, record: dict[str, Any]) -> bool:
        """
        Optional per-record validation hook.

        Override in subclasses to drop or flag malformed records before load.

        Args:
            record: A single raw record dict from `extract()`.

        Returns:
            True if the record is valid and should be loaded.
            False if the record should be skipped.
        """
        return True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source_name={self.source_name!r})"