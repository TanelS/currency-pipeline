import logging
from typing import Optional

import requests

import config

logger = logging.getLogger('Currency_raw_data_fetch')


def fetch_from_api(endpoint: str, params: dict = None) -> Optional[dict]:
    """
    Fetches data from a specified API endpoint using HTTP GET request.

    This function uses the configured API root and API key to send requests to the
    CurrencyBeacon API. It handles optional query parameters, logs errors if the
    request fails, and returns the parsed JSON data if the response is successful.

    :param endpoint: The specific API endpoint to fetch data from.
    :param params: A dictionary of query parameters to include in the request.
                   Optional and defaults to None.
    :return: The parsed JSON response as a dictionary if the request is
             successful, otherwise None.
    """
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {config.CURRENCYBEACON_API_KEY}'
    }

    url = f'{config.CURRENCYBEACON_API_ROOT}/{endpoint}'

    try:
        r = requests.get(url, headers=headers, params=params or {}, timeout=50, verify=True)
        r.raise_for_status()
        r_data = r.json()

        return r_data

    except Exception as e:
        print(f'API request failed: {e}')
        logger.exception(f'API request failed for endpoint {endpoint}: {e}')
        return None
