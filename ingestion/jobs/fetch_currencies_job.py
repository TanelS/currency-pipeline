import logging
from typing import Optional

from utils import fetch_from_api

logger = logging.getLogger("Currency_symbols_fetch")


def get_currencies() -> Optional[dict]:
    """
    Fetches a list of currencies from an external API and returns the data in dictionary format.

    This function retrieves the currency data by making a request to an external API. If
    the API does not return any data, it returns None. In case of an issue while accessing
    the "response" key from the returned data, it logs the exception and also returns None.

    :return: A dictionary containing currency data or None if no data is available or an
        error occurs.
    :rtype: Optional[dict]
    """
    data = fetch_from_api("currencies")
    if not data:
        return None

    try:
        return data.get("response")
    except Exception as e:
        logger.exception(f"Failed to validate currencies response: {e}")
        return None
