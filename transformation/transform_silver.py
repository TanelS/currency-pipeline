import logging
import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)

from spark.config.spark_config import BRONZE_OUT_DIR, SILVER_DIR
from spark.session.builder import get_spark
from spark.utils.validation import (
    clean_string_df,
    validate_boolean_df,
    validate_decimal_df,
    validate_int_df,
    validate_string_df,
    validate_timestamp_df,
)

logger = logging.getLogger('transform-silver')

with open('./conf/base/parameters.yml', 'r') as file:
    validation_params = yaml.safe_load(file)

if validation_params is None:
    raise ValueError("Validation parameters cannot be None")

currency_rules = validation_params['validation']['currencies']['columns']
rates_rules = validation_params['validation']['rates']['columns']

bronze_path_currencies = os.path.join(BRONZE_OUT_DIR, 'currencies')
silver_path_currencies = os.path.join(SILVER_DIR, 'currencies')
silver_path_quarantine_currencies = os.path.join(SILVER_DIR, 'currencies_quarantine')

bronze_path_rates = os.path.join(BRONZE_OUT_DIR, 'rates')
silver_path_rates = os.path.join(SILVER_DIR, 'rates')
silver_path_quarantine_rates = os.path.join(SILVER_DIR, 'rates_quarantine')


def transform_currencies(spark: SparkSession):
    """
    Transforms and validates the currencies data from a Bronze Delta table, cleans the data, and
    saves valid records to a Silver Delta table while placing invalid records into a quarantine.

    This function performs the following:
    1. Reads the Bronze Delta table for currencies.
    2. Cleans string columns within the dataframe.
    3. Applies validations to integer, boolean, and string columns based on predefined rules.
    4. Splits the data into valid and quarantined subsets.
    5. Writes the valid data to the designated Silver Delta table.
    6. Writes the quarantined data to a separate Silver Delta quarantine table if any invalid records exist.

    :param spark: The SparkSession instance used for interacting with Delta tables.
    :type spark: SparkSession

    :return: None
    """
    print(f'Reading Bronze currencies from: {bronze_path_currencies}')

    df_curr = spark.read.format('delta').load(bronze_path_currencies)

    curr_string_cols = [f.name for f in df_curr.schema.fields if f.dataType == StringType()]
    curr_int_cols = [f.name for f in df_curr.schema.fields if f.dataType == IntegerType()]
    curr_bool_cols = [f.name for f in df_curr.schema.fields if f.dataType == BooleanType()]

    print(f'Read {df_curr.count()} currency rows')
    print('Cleaning currencies dataframe ...')

    df_curr_cleaned = clean_string_df(df_curr, curr_string_cols)
    df_curr_cleaned = df_curr_cleaned.withColumn('code', F.lpad(F.col('code'), 3, '0'))
    df_curr_int_validated = validate_int_df(df_curr_cleaned, curr_int_cols, currency_rules)
    df_curr_bool_validated = validate_boolean_df(df_curr_int_validated, curr_bool_cols, currency_rules)
    df_curr_validated = validate_string_df(df_curr_bool_validated, curr_string_cols, currency_rules)
    df_curr_validated.show()

    df_currencies_valid = df_curr_validated.filter(col('_validation_errors') == '')
    df_currencies_quarantine = df_curr_validated.filter(col('_validation_errors') != '')
    print('Valid currencies:')
    df_currencies_valid.show()
    print('Quarantined currencies:')
    df_currencies_quarantine.show()

    (
        df_currencies_valid
        .drop('_validation_errors')
        .write.format("delta")
        .mode("overwrite")
        .save(silver_path_currencies)
    )
    print(f'Valid {df_currencies_valid.count()} currencies saved to: {silver_path_currencies}')

    quar_curr_count = df_currencies_quarantine.count()

    if quar_curr_count > 0:
        (
            df_currencies_quarantine
            .write.format("delta")
            .mode("overwrite")
            .save(silver_path_quarantine_currencies)
        )
        print(f'Quarantined {quar_curr_count} currencies saved to: {silver_path_quarantine_currencies}')


def transform_rates(spark: SparkSession):
    """
    Transforms and validates currency exchange rates data from the bronze layer to the silver layer.

    This function reads currency rates from a bronze Delta table, filters out rates corresponding to non-ISO 4217 currencies 
    by joining with a validated list of ISO 4217 currencies, cleans up string data, validates timestamp and decimal fields, 
    and performs further string-specific validation. The valid and invalid rows are then separated. The valid rows are saved 
    to the silver Delta table, while invalid rows (quarantined records) are saved to a separate quarantine table.

    :param spark: SparkSession object to interact with Spark APIs.
    :type spark: SparkSession
    :return: None
    """
    bronze_path_rates = os.path.join(BRONZE_OUT_DIR, 'rates')
    print(f'Reading Bronze rates from: {bronze_path_rates}')

    iso_4217_currencies = (
        spark.read
        .format('delta')
        .load(silver_path_currencies)
        .select('short_code')
    )

    df_rates = spark.read.format('delta').load(bronze_path_rates)
    df_rates = df_rates.repartition(4)

    # We discard all non ISO 4217 currencies:
    df_rates = df_rates.join(iso_4217_currencies, df_rates.curr_base == iso_4217_currencies.short_code, 'inner').drop('short_code')
    df_rates = df_rates.join(iso_4217_currencies, df_rates.currency == iso_4217_currencies.short_code, 'inner').drop('short_code')


    print(f'Read {df_rates.count()} currency rates rows')

    rates_string_cols = [f.name for f in df_rates.schema.fields if f.dataType == StringType()]
    rates_timestamp_cols = [f.name for f in df_rates.schema.fields if f.dataType == TimestampType()]
    rates_decimal_cols = [f.name for f in df_rates.schema.fields if f.dataType == DecimalType()]

    df_rates_cleaned = clean_string_df(df_rates, rates_string_cols)
    df_rates_timestamp_validated = validate_timestamp_df(df_rates_cleaned, rates_timestamp_cols, rates_rules)
    df_rates_decimal_validated = validate_decimal_df(df_rates_timestamp_validated, rates_decimal_cols, rates_rules)
    df_rates_validated = validate_string_df(df_rates_decimal_validated, rates_string_cols, rates_rules)

    df_rates_valid = df_rates_validated.filter(col('_validation_errors') == '')
    df_rates_quarantine = df_rates_validated.filter(col('_validation_errors') != '')

    print('Valid rates:')
    df_rates_valid.show()

    (
        df_rates_valid
        .drop('_validation_errors')
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("curr_base")
        .save(silver_path_rates)
    )

    print(f'Valid {df_rates_valid.count()} rates written to {silver_path_rates}')

    print('Quarantined rates:')
    df_rates_quarantine.show()

    quar_rates_count = df_rates_quarantine.count()

    if quar_rates_count > 0:
        (
            df_rates_quarantine
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("curr_base")
            .save(silver_path_quarantine_currencies)
        )
        print(f'Quarantined {quar_rates_count} rates saved to: {silver_path_quarantine_rates}')



if __name__ == '__main__':
    spark = get_spark("silver_currency_stuff")
    spark.sparkContext.setLogLevel("WARN")

    transform_currencies(spark)
    transform_rates(spark)

