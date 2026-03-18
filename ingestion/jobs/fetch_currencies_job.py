import logging
from typing import Optional

from utils import fetch_from_api

logger = logging.getLogger("Currency_symbols_fetch")


def get_currencies() -> Optional[dict]:
    data = fetch_from_api("currencies")
    if not data:
        return None

    try:
        return data.get('response')
    except Exception as e:
        logger.exception(f"Failed to validate currencies response: {e}")
        return None


if __name__ == "__main__":
    print(get_currencies())  # for testing
