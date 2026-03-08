"""
Concrete extractor for the REST Countries API (no auth required).
Docs: https://restcountries.com/

Fetches all ~250 countries and flattens the rich nested JSON
into a clean tabular record per country.

Schema produced:
  cca2         : str   — ISO 3166-1 alpha-2 code (e.g., "DE")
  cca3         : str   — ISO 3166-1 alpha-3 code (e.g., "DEU")
  name_common  : str   — Common name ("Germany")
  name_official: str   — Official name ("Federal Republic of Germany")
  region       : str   — Geographic region ("Europe")
  subregion    : str   — Sub-region ("Western Europe") — nullable
  capital      : str   — Capital city — nullable
  population   : int   — Current population
  area_km2     : float — Area in square kilometres
  currencies   : str   — Comma-separated ISO 4217 codes ("EUR")
  languages    : str   — Comma-separated language codes ("deu")
  timezones    : str   — Comma-separated TZ identifiers ("UTC+01:00")
  is_un_member : bool  — Whether country is a UN member
  flag_emoji   : str   — Unicode flag emoji ("🇩🇪")
"""

from __future__ import annotations

from typing import Any

from extractors.api_extractor import ApiExtractor, ExtractionError


class RestCountriesExtractor(ApiExtractor):
    """
    Extract country metadata from restcountries.com.

    No API key required. Rate limit: generous for a pipeline.
    """

    BASE_URL = "https://restcountries.com"

    # Fields we request from the API to minimize payload size
    FIELDS = [
        "cca2", "cca3", "name", "region", "subregion",
        "capital", "population", "area", "currencies",
        "languages", "timezones", "unMember", "flag",
    ]

    def __init__(self) -> None:
        super().__init__(
            source_name="rest_countries",
            base_url=self.BASE_URL,
        )

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch all countries and flatten nested structures to a flat dict.
        """
        fields_param = ",".join(self.FIELDS)
        raw_list = self.get("/v3.1/all", params={"fields": fields_param})

        if not isinstance(raw_list, list):
            raise ExtractionError(
                f"Expected list from REST Countries API, got {type(raw_list)}"
            )

        records = [self._flatten(country) for country in raw_list]

        self.logger.info("Extracted %d country records", len(records))
        return records


    def _flatten(self, country: dict[str, Any]) -> dict[str, Any]:
        """
        Normalize a single country object from the API response.

        The API returns deeply nested structures:
          country["name"]["common"]     → name_common
          country["capital"][0]         → capital  (it's a list)
          country["currencies"]         → dict keyed by ISO code
          country["languages"]          → dict keyed by language code
        """
        name = country.get("name", {})
        capital_list = country.get("capital") or []

        return {
            "cca2": country.get("cca2", ""),
            "cca3": country.get("cca3", ""),
            "name_common": name.get("common", ""),
            "name_official": name.get("official", ""),
            "region": country.get("region", ""),
            "subregion": country.get("subregion") or None,
            "capital": capital_list[0] if capital_list else None,
            "population": country.get("population", 0),
            "area_km2": country.get("area") or None,
            "currencies": self._join_keys(country.get("currencies")),
            "languages": self._join_values(country.get("languages")),
            "timezones": ", ".join(country.get("timezones") or []),
            "is_un_member": country.get("unMember", False),
            "flag_emoji": country.get("flag", ""),
        }

    @staticmethod
    def _join_keys(mapping: dict | None) -> str | None:
        """Return dict keys joined by comma, or None if mapping is empty."""
        if not mapping:
            return None
        return ", ".join(sorted(mapping.keys()))

    @staticmethod
    def _join_values(mapping: dict | None) -> str | None:
        """Return dict values joined by comma, or None if mapping is empty."""
        if not mapping:
            return None
        return ", ".join(sorted(mapping.values()))

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Discard records missing a country code — they're unusable."""
        return bool(record.get("cca2") or record.get("cca3"))