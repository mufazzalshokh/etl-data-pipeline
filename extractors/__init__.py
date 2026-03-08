"""
extractors/

Public interface for the ETL extractor layer.

Import from here to avoid deep package paths in DAGs:
    from extractors import OpenExchangeRatesExtractor, RestCountriesExtractor
"""

from extractors.base_extractor import BaseExtractor, ExtractionError
from extractors.api_extractor import ApiExtractor
from extractors.csv_extractor import CsvExtractor
from extractors.open_exchange_rates_extractor import OpenExchangeRatesExtractor
from extractors.rest_countries_extractor import RestCountriesExtractor

__all__ = [
    "BaseExtractor",
    "ExtractionError",
    "ApiExtractor",
    "CsvExtractor",
    "OpenExchangeRatesExtractor",
    "RestCountriesExtractor",
]