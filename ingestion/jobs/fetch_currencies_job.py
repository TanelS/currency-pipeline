import logging

from ingestion.io import fetch_from_api

logger = logging.getLogger("Currency_symbols_fetch")


def get_currencies() -> dict | None:
    data = fetch_from_api("currencies")
    print(data)
    if not data:
        return None

    try:
        return data
    except Exception as e:
        logger.exception(f"Failed to validate currencies response: {e}")
        return None


if __name__ == "__main__":
    get_currencies()   # for testing
