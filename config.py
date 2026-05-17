import logging.config
import os
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import dotenv_values, find_dotenv, load_dotenv

load_dotenv(find_dotenv("./.env"))

config_environment = {
    **dotenv_values("./.env"),  # load local file development variables
    **os.environ,  # override loaded values with system environment variables
}

# logfile location and size
LOG_FILENAME = os.path.join(Path(__file__).parent, "logs", "currency_pipeline.log")
LOG_FILESIZE = 3  # logfile size in Megabytes, two rotating files total

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
            encoding="utf-8",
        )
    ],
)

DATABASE_USERNAME = config_environment["DB_USERNAME"]
DATABASE_PASSWORD = config_environment["DB_PASSWORD"]
DATABASE_NAME = config_environment["DB_DATABASE"]

CURRENCYBEACON_API_KEY = config_environment["CURRENCYBEACON_API_KEY"]
CURRENCYBEACON_API_ROOT = config_environment["CURRENCYBEACON_API_ROOT"]
RUNNING_LOCAL = config_environment["RUNNING_LOCAL"].lower() in ("1", "true", "yes")
RUNNING_AWS = config_environment["RUNNING_AWS"].lower() in ("1", "true", "yes")

AWS_ACCESS_KEY_ID = config_environment["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = config_environment["AWS_SECRET_ACCESS_KEY"]
AWS_S3_BUCKET = config_environment["AWS_S3_BUCKET"]

# Actually Spark code uses those too, so the prefix DBT_ is just arbitrary:
DBT_POSTGRES_HOST = config_environment["DBT_POSTGRES_HOST"]
DBT_POSTGRES_PORT = config_environment["DBT_POSTGRES_PORT"]

if RUNNING_LOCAL and RUNNING_AWS:  # to handle the possible misconfiguration in .env
    raise ValueError("RUNNING_LOCAL and RUNNING_AWS cannot both be True")
