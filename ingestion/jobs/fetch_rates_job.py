import logging
from typing import Optional

from utils import fetch_from_api

logger = logging.getLogger("Currency_data_fetch")


def get_rates(params: dict) -> Optional[dict]:
    """
    Fetches and processes exchange rate data based on the provided parameters.

    This function retrieves the latest exchange rates from an API, validates the
    response, and ensures data integrity. If the response contains invalid or
    unexpected data such as empty rates lists, appropriate warnings are logged.
    Any errors during validation are also logged.

    :param params: A dictionary containing query parameters for the API. The key
        "base" can specify the base currency for the conversion rates.
    :type params: dict
    :return: A dictionary containing the response data if the fetch and validation
        are successful, otherwise None.
    :rtype: Optional[dict]
    """
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
        logger.exception(f"Failed to validate rates for {base_currency}: {e}")
        return None
