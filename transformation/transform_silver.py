import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from decimal import Decimal
import logging
from ingestion.jobs import get_currencies, get_rates
from spark.config.spark_config import BRONZE_OUT_DIR
from spark.session.builder import get_spark

from spark.schemas.rate_schema import RATES_SCHEMA
from spark.schemas.currency_schema import CURRENCY_SCHEMA

logger = logging.getLogger('transform-silver')


