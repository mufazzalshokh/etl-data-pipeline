"""
Unit tests for the extractor layer.

Tests are fully isolated — no real API calls, no database connections.
All external dependencies are mocked.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from extractors.base_extractor import BaseExtractor, ExtractionError
from extractors.api_extractor import ApiExtractor
from extractors.csv_extractor import CsvExtractor


# Fixtures & helpers

class ConcreteExtractor(BaseExtractor):
    """Minimal concrete subclass for testing the abstract base."""
    def __init__(self, records=None, should_fail=False):
        super().__init__(source_name="test_source")
        self._records = records or [{"id": 1, "value": "test"}]
        self._should_fail = should_fail

    def extract(self):
        if self._should_fail:
            raise RuntimeError("Simulated extraction failure")
        return self._records


# BaseExtractor tests

class TestBaseExtractor:
    def test_extract_with_metadata_adds_lineage_fields(self):
        extractor = ConcreteExtractor()
        results = extractor.extract_with_metadata(pipeline_run_id="run_001")

        assert len(results) == 1
        record = results[0]

        # All original fields preserved
        assert record["id"] == 1
        assert record["value"] == "test"

        # Metadata injected
        assert record["_source"] == "test_source"
        assert record["_pipeline_run_id"] == "run_001"
        assert "_extracted_at" in record
        assert "T" in record["_extracted_at"]  # ISO 8601 format check

    def test_extract_with_metadata_handles_none_run_id(self):
        extractor = ConcreteExtractor()
        results = extractor.extract_with_metadata(pipeline_run_id=None)
        assert results[0]["_pipeline_run_id"] is None

    def test_extract_with_metadata_multiple_records(self):
        records = [{"n": i} for i in range(100)]
        extractor = ConcreteExtractor(records=records)
        results = extractor.extract_with_metadata(pipeline_run_id="run_batch")

        assert len(results) == 100
        # All records get same _source and _extracted_at
        sources = {r["_source"] for r in results}
        assert sources == {"test_source"}

    def test_extraction_error_raised_after_retries(self):
        """When extract() always fails, ExtractionError should be raised."""
        extractor = ConcreteExtractor(should_fail=True)
        # Override MAX_RETRIES to 1 to keep tests fast
        extractor.MAX_RETRIES = 1

        with pytest.raises(ExtractionError) as exc_info:
            extractor.extract_with_metadata()

        assert "test_source" in str(exc_info.value)

    def test_repr_contains_class_name_and_source(self):
        extractor = ConcreteExtractor()
        assert "ConcreteExtractor" in repr(extractor)
        assert "test_source" in repr(extractor)

    def test_default_validate_record_returns_true(self):
        extractor = ConcreteExtractor()
        assert extractor.validate_record({"any": "data"}) is True


# CsvExtractor tests

class TestCsvExtractor:
    CSV_CONTENT = "Country Name,Population,Area\nGermany,83000000,357114\nFrance,67000000,643801\n"

    def test_extract_from_string_content(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(self.CSV_CONTENT, encoding="utf-8")

        extractor = CsvExtractor(
            source_name="test_csv",
            source_path=str(csv_file),
        )
        records = extractor.extract()

        assert len(records) == 2
        # Column names normalized to lowercase with underscores
        assert "country_name" in records[0]
        assert records[0]["country_name"] == "Germany"

    def test_column_mapping_applied(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(self.CSV_CONTENT, encoding="utf-8")

        extractor = CsvExtractor(
            source_name="test_csv",
            source_path=str(csv_file),
            column_mapping={"Country Name": "country"},
        )
        records = extractor.extract()

        assert "country" in records[0]
        assert records[0]["country"] == "Germany"

    def test_file_not_found_raises_extraction_error(self):
        extractor = CsvExtractor(
            source_name="test_csv",
            source_path="/nonexistent/path/data.csv",
        )
        with pytest.raises(ExtractionError, match="not found"):
            extractor.extract()

    def test_empty_rows_dropped(self, tmp_path):
        csv_file = tmp_path / "empty_rows.csv"
        csv_file.write_text(
            "Name,Value\nAlpha,1\n,,\nBeta,2\n",
            encoding="utf-8",
        )
        extractor = CsvExtractor(source_name="test", source_path=str(csv_file))
        records = extractor.extract()
        # The fully-empty row should be dropped
        assert len(records) == 2

    def test_auto_delimiter_detection(self, tmp_path):
        csv_file = tmp_path / "semicolon.csv"
        csv_file.write_text("A;B;C\n1;2;3\n4;5;6\n", encoding="utf-8")

        extractor = CsvExtractor(source_name="test", source_path=str(csv_file))
        records = extractor.extract()

        assert len(records) == 2
        assert "a" in records[0]  # columns lowercased
        assert records[0]["a"] == "1"


# OpenExchangeRatesExtractor tests

class TestOpenExchangeRatesExtractor:
    MOCK_RESPONSE = {
        "disclaimer": "...",
        "license": "...",
        "timestamp": 1700000000,
        "base": "USD",
        "rates": {
            "EUR": 0.92,
            "GBP": 0.79,
            "JPY": 149.5,
        },
    }

    def test_extract_returns_one_record_per_currency(self, monkeypatch):
        monkeypatch.setenv("OPEN_EXCHANGE_RATES_APP_ID", "test_key")

        from extractors.open_exchange_rates_extractor import OpenExchangeRatesExtractor
        extractor = OpenExchangeRatesExtractor()

        with patch.object(extractor, "get", return_value=self.MOCK_RESPONSE):
            records = extractor.extract()

        assert len(records) == 3
        eur = next(r for r in records if r["target_currency"] == "EUR")
        assert eur["base_currency"] == "USD"
        assert eur["rate"] == 0.92

    def test_missing_app_id_raises_extraction_error(self, monkeypatch):
        monkeypatch.delenv("OPEN_EXCHANGE_RATES_APP_ID", raising=False)

        with pytest.raises(ExtractionError, match="OPEN_EXCHANGE_RATES_APP_ID"):
            from extractors.open_exchange_rates_extractor import OpenExchangeRatesExtractor
            OpenExchangeRatesExtractor()

    def test_validate_record_rejects_zero_rate(self, monkeypatch):
        monkeypatch.setenv("OPEN_EXCHANGE_RATES_APP_ID", "test_key")

        from extractors.open_exchange_rates_extractor import OpenExchangeRatesExtractor
        extractor = OpenExchangeRatesExtractor()

        assert extractor.validate_record({"rate": 0}) is False
        assert extractor.validate_record({"rate": -1.5}) is False
        assert extractor.validate_record({"rate": 1.23}) is True


# RestCountriesExtractor tests

class TestRestCountriesExtractor:
    MOCK_COUNTRY = {
        "cca2": "DE",
        "cca3": "DEU",
        "name": {"common": "Germany", "official": "Federal Republic of Germany"},
        "region": "Europe",
        "subregion": "Western Europe",
        "capital": ["Berlin"],
        "population": 83000000,
        "area": 357114.0,
        "currencies": {"EUR": {"name": "Euro", "symbol": "€"}},
        "languages": {"deu": "German"},
        "timezones": ["UTC+01:00"],
        "unMember": True,
        "flag": "🇩🇪",
    }

    def test_extract_flattens_nested_structure(self):
        from extractors.rest_countries_extractor import RestCountriesExtractor
        extractor = RestCountriesExtractor()

        with patch.object(extractor, "get", return_value=[self.MOCK_COUNTRY]):
            records = extractor.extract()

        assert len(records) == 1
        r = records[0]
        assert r["cca2"] == "DE"
        assert r["name_common"] == "Germany"
        assert r["capital"] == "Berlin"
        assert r["currencies"] == "EUR"
        assert r["languages"] == "German"
        assert r["is_un_member"] is True
        assert r["flag_emoji"] == "🇩🇪"

    def test_validate_record_rejects_missing_code(self):
        from extractors.rest_countries_extractor import RestCountriesExtractor
        extractor = RestCountriesExtractor()

        assert extractor.validate_record({"cca2": "", "cca3": ""}) is False
        assert extractor.validate_record({"cca2": "DE"}) is True