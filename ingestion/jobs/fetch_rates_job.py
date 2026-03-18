import logging
from typing import Optional
from utils import fetch_from_api

logger = logging.getLogger("Currency_data_fetch")


def get_rates(base_currency: str) -> Optional[dict]:
    params = {"base": base_currency} if base_currency else {}
    data = fetch_from_api("latest", params)

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
        logger.exception(f"Failed to validate rates for {base_currency}: {e}")
        return None


if __name__ == "__main__":
    print(get_rates("USD"))  # for testing
