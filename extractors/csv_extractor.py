"""
CSV file extractor — supports local files, URLs, and streaming large files.

Used for: any public CSV dataset (e.g., data.gov.lv, Kaggle exports).

Features:
  - Auto-detects delimiter (, ; \t |)
  - Strips whitespace from column names
  - Drops fully-empty rows
  - Handles encoding issues gracefully
  - Supports column renaming and type casting via config
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from extractors.base_extractor import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)


class CsvExtractor(BaseExtractor):
    """
    Extract records from a CSV source (local path or HTTP/HTTPS URL).

    Args:
        source_name:    Identifier for logs and metadata.
        source_path:    Local file path (str or Path) or a full HTTP(S) URL.
        column_mapping: Optional dict to rename columns after load.
                        E.g., {"Country Name": "country_name"}
        dtype_mapping:  Optional dict mapping column names to Python/pandas types.
                        E.g., {"population": "int64", "area_km2": "float64"}
        delimiter:      CSV delimiter. If None, auto-detected via pandas sniffer.

    Example:
        extractor = CsvExtractor(
            source_name="world_population",
            source_path="https://example.com/population.csv",
            column_mapping={"Country Name": "country_name"},
            dtype_mapping={"population": "int64"},
        )
        records = extractor.extract_with_metadata(pipeline_run_id="run_001")
    """

    def __init__(
        self,
        source_name: str,
        source_path: str | Path,
        column_mapping: dict[str, str] | None = None,
        dtype_mapping: dict[str, str] | None = None,
        delimiter: str | None = None,
    ) -> None:
        super().__init__(source_name)
        self.source_path = str(source_path)
        self.column_mapping = column_mapping or {}
        self.dtype_mapping = dtype_mapping or {}
        self.delimiter = delimiter

    def extract(self) -> list[dict[str, Any]]:
        """
        Load CSV into a pandas DataFrame, clean it, and return as list[dict].

        Returns:
            List of records, one per CSV row.

        Raises:
            ExtractionError: if the file cannot be read or parsed.
        """
        self.logger.info("Reading CSV from: %s", self.source_path)

        raw_content = self._fetch_content()
        df = self._parse_csv(raw_content)
        df = self._clean(df)

        records = df.to_dict(orient="records")
        self.logger.info(
            "CSV extraction complete | rows=%d | columns=%d",
            len(records),
            len(df.columns),
        )
        return records

    def _fetch_content(self) -> str:
        """
        Load CSV bytes from a local path or remote URL.

        Returns:
            Raw CSV content as a UTF-8 string.
        """
        if self.source_path.startswith(("http://", "https://")):
            try:
                response = requests.get(self.source_path, timeout=(10, 60))
                response.raise_for_status()
                # Try UTF-8 first; fall back to latin-1 for legacy CSVs
                try:
                    return response.content.decode("utf-8")
                except UnicodeDecodeError:
                    return response.content.decode("latin-1")
            except requests.RequestException as exc:
                raise ExtractionError(
                    f"Failed to download CSV from {self.source_path}: {exc}"
                ) from exc
        else:
            path = Path(self.source_path)
            if not path.exists():
                raise ExtractionError(f"CSV file not found: {path}")
            try:
                return path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                return path.read_text(encoding="latin-1")

    def _parse_csv(self, content: str) -> pd.DataFrame:
        """
        Parse raw CSV content into a DataFrame.

        Auto-detects delimiter if not specified using pandas' built-in sniffer.
        """
        sep = self.delimiter
        if sep is None:
            # Sniff first 4096 bytes to detect separator
            try:
                import csv
                dialect = csv.Sniffer().sniff(content[:4096], delimiters=",;\t|")
                sep = dialect.delimiter
                self.logger.debug("Auto-detected CSV delimiter: %r", sep)
            except csv.Error:
                sep = ","  # safe fallback

        try:
            df = pd.read_csv(
                io.StringIO(content),
                sep=sep,
                dtype=str,           # load all as str; cast explicitly below
                na_values=["", "N/A", "NA", "null", "NULL", "None", "-"],
                keep_default_na=True,
                low_memory=False,
            )
        except Exception as exc:
            raise ExtractionError(f"Failed to parse CSV: {exc}") from exc

        return df

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names, apply mappings, cast types, drop empty rows.
        """
        # 1. Normalize column names: strip whitespace, lowercase, underscores
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[\s\-\.]+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )

        # 2. Apply caller-supplied column renaming
        if self.column_mapping:
            # Normalize keys the same way before renaming
            normalized_mapping = {
                k.strip().lower().replace(" ", "_"): v
                for k, v in self.column_mapping.items()
            }
            df.rename(columns=normalized_mapping, inplace=True)

        # 3. Drop rows that are completely empty
        df.dropna(how="all", inplace=True)

        # 4. Strip whitespace from string columns
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

        # 5. Apply type casting
        for col, dtype in self.dtype_mapping.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except (ValueError, TypeError) as exc:
                    self.logger.warning(
                        "Could not cast column '%s' to %s: %s", col, dtype, exc
                    )

        return df

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Skip records where all values are None/NaN."""
        return any(v is not None and str(v).strip() != "" for v in record.values())
