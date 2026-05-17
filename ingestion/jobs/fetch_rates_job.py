import logging
from typing import Optional

from utils import fetch_from_api

logger = logging.getLogger("Currency_data_fetch")


def get_rates(params: dict) -> Optional[dict]:
    """Fetches latest rates for the given base currency; returns None if API returns empty or invalid data."""
    data = fetch_from_api("latest", params)
    base_currency = params.get("base")
    if not data:
        return None
    # Handle empty rates list from API
    if isinstance(data.get("response", {}).get("rates"), list):
        logger.warning(f"API returned empty rates list for base currency: {base_currency}")
        return None
    if isinstance(data.get("rates"), list):
        logger.warning(f"API returned empty rates list for base currency: {base_currency}")
        return None

    try:
        return data.get("response")
    except Exception as e:
        print(f"Failed to validate rates payload: {e}")
        logger.exception(f"Failed to validate rates for {base_currency}")
        return None
