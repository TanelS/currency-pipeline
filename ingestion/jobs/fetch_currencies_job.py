import logging
from typing import Optional

from utils import fetch_from_api

logger = logging.getLogger("Currency_symbols_fetch")


def get_currencies() -> Optional[dict]:
    """Returns the 'response' list from the CurrencyBeacon currencies endpoint, or None on failure."""
    data = fetch_from_api("currencies")
    if not data:
        return None

    try:
        return data.get("response")
    except Exception:
        logger.exception("Failed to validate currencies response")
        return None
