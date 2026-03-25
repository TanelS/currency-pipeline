import logging.config
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import (
    dotenv_values,
    load_dotenv,
    find_dotenv
)

load_dotenv(find_dotenv('./.env'))

config_environment = {
    **dotenv_values("./.env"),  # load local file development variables
    **os.environ,  # override loaded values with system environment variables
}

# logfile location and size
LOG_FILENAME = os.path.join(Path(__file__).parent, 'logs', 'pipedrive_test.log')
LOG_FILESIZE = 1  # logfile size in Megabytes, in total there will be two files (for a test 1MB is enough=

# Ensure UTC timestamps in logs
logging.Formatter.converter = time.gmtime

logging.basicConfig(
    level=logging.INFO,
    format="$asctime - $name - ${levelname}: $message",
    style="$",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        RotatingFileHandler(
            filename=LOG_FILENAME,
            maxBytes=LOG_FILESIZE * 1024 * 1024,
            backupCount=1,
            encoding="utf-8"
        )
    ]
)

DATABASE_USERNAME = config_environment["DB_USERNAME"]
DATABASE_PASSWORD = config_environment["DB_PASSWORD"]
DATABASE_HOST = config_environment["DB_HOST"]
DATABASE_PORT = config_environment["DB_PORT"]
DATABASE_NAME = config_environment["DB_DATABASE"]

CURRENCYBEACON_API_KEY = config_environment["CURRENCYBEACON_API_KEY"]
CURRENCYBEACON_API_ROOT = config_environment["CURRENCYBEACON_API_ROOT"]
RUNNING_LOCAL = config_environment["RUNNING_LOCAL"]

DBT_POSTGRES_HOST = config_environment["DBT_POSTGRES_HOST"]
DBT_POSTGRES_PORT = config_environment["DBT_POSTGRES_PORT"]

API_V1_STR = config_environment["API_V1_STR"]

# CORS Origins configuration
CORS_ORIGINS = [origin.strip() for origin in config_environment.get("CORS_ORIGINS", "").split(",") if origin.strip()]


