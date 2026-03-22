import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from decimal import Decimal
import logging
from ingestion.jobs import get_currencies, get_rates
from spark.config.spark_config import BRONZE_OUT_DIR
from spark.session.builder import get_spark

from spark.schemas.rate_schema import RATES_SCHEMA
from spark.schemas.currency_schema import CURRENCY_SCHEMA
from db import jdbc_props

from spark.utils.cleaner import clean_string

logger = logging.getLogger('transform-silver')


def transform_rates_staging(spark: SparkSession):
    silver_path_currencies = os.path.join(BRONZE_OUT_DIR, 'currencies')
    silver_path_rates = os.path.join(BRONZE_OUT_DIR, 'rates')
    print(f'Reading Silver currencies from: {silver_path_currencies}')

    df_curr = spark.read.format('delta').load(silver_path_currencies)

    print(f'==== Read {df_curr.count()} currency rows')
    df_curr.show()





if __name__ == '__main__':
    spark = get_spark("silver_currency_stuff")
    spark.sparkContext.setLogLevel("WARN")

    transform_rates_staging(spark)