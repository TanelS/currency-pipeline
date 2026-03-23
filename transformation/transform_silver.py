import datetime
import os
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
    StructField,
    StructType,
)

from decimal import Decimal
import logging
from ingestion.jobs import get_currencies, get_rates
from spark.config.spark_config import BRONZE_OUT_DIR
from spark.session.builder import get_spark
from spark.utils.validation import (
    clean_string_df,
    validate_int_df,
    validate_timestamp_df
)

from spark.schemas.rate_schema import RATES_SCHEMA
from spark.schemas.currency_schema import CURRENCY_SCHEMA
from db import jdbc_props

from spark.utils.validation import clean_string

logger = logging.getLogger('transform-silver')

with open('./conf/base/parameters.yml', 'r') as file:
    validation_params = yaml.safe_load(file)

if validation_params is None:
    raise ValueError("Validation parameters cannot be None")

currency_rules = validation_params['validation']['currencies']['columns']
rates_rules = validation_params['validation']['rates']['columns']



def transform_currencies(spark: SparkSession):
    silver_path_currencies = os.path.join(BRONZE_OUT_DIR, 'currencies')
    print(f'Reading Silver currencies from: {silver_path_currencies}')

    df_curr = spark.read.format('delta').load(silver_path_currencies)

    curr_string_cols = [f.name for f in df_curr.schema.fields if f.dataType == StringType()]
    curr_int_cols = [f.name for f in df_curr.schema.fields if f.dataType == IntegerType()]
    curr_bool_cols = [f.name for f in df_curr.schema.fields if f.dataType == BooleanType()]


    print(f'==== Read {df_curr.count()} currency rows')
    df_curr.show()
    print('==== Cleaning currencies dataframe ...')

    df_curr_cleaned = clean_string_df(df_curr, curr_string_cols)

    print('Showing cleaned currencies')
    df_curr_cleaned.show()

    df_curr_int_validated = validate_int_df(df_curr_cleaned, curr_int_cols, currency_rules)
    print('Showing integer validated currencies')
    df_curr_int_validated.show()



def transform_rates(spark: SparkSession):
    silver_path_rates = os.path.join(BRONZE_OUT_DIR, 'rates')
    print(f'Reading Silver rates from: {silver_path_rates}')

    df_rates = spark.read.format('delta').load(silver_path_rates)

    rates_string_cols = [f.name for f in df_rates.schema.fields if f.dataType == StringType()]
    rates_timestamp_cols = [f.name for f in df_rates.schema.fields if f.dataType == TimestampType()]

    df_rates_cleaned = clean_string_df(df_rates, rates_string_cols)
    print('Showing cleaned rates')
    df_rates_cleaned.show()

    df_rates_timestamp_validated = validate_timestamp_df(df_rates_cleaned, rates_timestamp_cols, rates_rules)
    print('Showing timestamp cleaned rates')
    df_rates_timestamp_validated.show()


if __name__ == '__main__':
    spark = get_spark("silver_currency_stuff")
    spark.sparkContext.setLogLevel("WARN")

    # transform_rates_staging(spark)
    transform_currencies(spark)
    transform_rates(spark)
