import logging
from typing import Optional
import requests

import config

logger = logging.getLogger('Currency_raw_data_fetch')


def fetch_from_api(endpoint: str, params: dict = None) -> Optional[dict]:
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
