"""
Concrete extractor for the Open Exchange Rates API.
Docs: https://docs.openexchangerates.org/

Free tier provides:
  - Latest rates relative to USD (base currency)
  - ~170 currencies updated hourly

Schema produced (one record per currency):
  base_currency  : str   — always "USD" on free tier
  target_currency: str   — ISO 4217 code (e.g., "EUR", "GBP")
  rate           : float — how many target units per 1 USD
  rate_date      : str   — YYYY-MM-DD date of the published rate
  timestamp      : int   — Unix epoch of rate publication
"""

from __future__ import annotations

import os
from typing import Any

from extractors.api_extractor import ApiExtractor, ExtractionError


class OpenExchangeRatesExtractor(ApiExtractor):
    """
    Extract latest currency exchange rates from openexchangerates.org.

    Requires env var: OPEN_EXCHANGE_RATES_APP_ID
    Free account: https://openexchangerates.org/signup/free
    """

    BASE_URL = "https://openexchangerates.org/api"

    def __init__(self) -> None:
        app_id = os.environ.get("OPEN_EXCHANGE_RATES_APP_ID", "")
        if not app_id:
            raise ExtractionError(
                "OPEN_EXCHANGE_RATES_APP_ID environment variable is not set. "
                "Get a free key at https://openexchangerates.org/signup/free"
            )
        super().__init__(
            source_name="open_exchange_rates",
            base_url=self.BASE_URL,
        )
        self.app_id = app_id

    def extract(self) -> list[dict[str, Any]]:
        """
        Fetch latest rates and flatten into one record per currency pair.

        API response shape:
            {
                "disclaimer": "...",
                "license": "...",
                "timestamp": 1700000000,
                "base": "USD",
                "rates": {
                    "EUR": 0.92,
                    "GBP": 0.79,
                    ...
                }
            }
        """
        raw = self.get("/latest.json", params={"app_id": self.app_id})

        # Validate required fields are present
        for required in ("base", "rates", "timestamp"):
            if required not in raw:
                raise ExtractionError(
                    f"Unexpected API response: missing field '{required}'"
                )

        base_currency = raw["base"]
        timestamp = raw["timestamp"]

        # Convert Unix timestamp to YYYY-MM-DD
        from datetime import datetime, timezone
        rate_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
            "%Y-%m-%d"
        )

        records = [
            {
                "base_currency": base_currency,
                "target_currency": currency_code,
                "rate": rate_value,
                "rate_date": rate_date,
                "timestamp": timestamp,
            }
            for currency_code, rate_value in raw["rates"].items()
        ]

        self.logger.info(
            "Extracted %d currency rates | base=%s | date=%s",
            len(records),
            base_currency,
            rate_date,
        )
        return records

    def validate_record(self, record: dict[str, Any]) -> bool:
        """Discard records with a zero or negative rate."""
        rate = record.get("rate")
        return rate is not None and float(rate) > 0