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


def transform_currencies(spark: SparkSession) -> None:
    """
    Transforms and validates currency data from the Bronze layer, cleans string columns,
    ensures integer and boolean column rules are met, and segregates the valid and
    quarantined records into corresponding Silver layer paths.

    During the execution, the method performs the following tasks:
    - Reads the Bronze currency data
    - Cleans string columns to remove unwanted formatting or inconsistencies
    - Validates integer and boolean columns based on predefined rules
    - Identifies and separates valid and quarantined records
    - Writes valid records to the specified Silver path for validated currencies
    - Writes quarantined records (if any) to the quarantine path for further inspection

    :param spark: An instance of the SparkSession used for reading and writing data.
    :type spark: SparkSession

    :return: This function does not return any value. The output is saved to Delta tables
             at the Silver layer for valid and quarantined currencies.
    :rtype: None
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

    try:
        (
            df_currencies_valid
            .drop('_validation_errors')
            .write.format("delta")
            .mode("overwrite")
            .save(silver_path_currencies)
        )
    except Exception as e:
        logger.exception(f'Failed to save valid currencies to {silver_path_currencies}: {e}')
        return

    print(f'Valid {df_currencies_valid.count()} currencies saved to: {silver_path_currencies}')

    quar_curr_count = df_currencies_quarantine.count()

    if quar_curr_count > 0:
        try:
            (
                df_currencies_quarantine
                .write.format("delta")
                .mode("overwrite")
                .save(silver_path_quarantine_currencies)
            )
        except Exception as e:
            logger.exception(f'Failed to save quarantined currencies to {silver_path_quarantine_currencies}: {e}')

        print(f'Quarantined {quar_curr_count} currencies saved to: {silver_path_quarantine_currencies}')


def transform_rates(spark: SparkSession) -> None:
    """
    Transforms currency rates data while ensuring only ISO 4217 currencies are processed. It validates, partitions, and
    writes the cleaned data to appropriate layers, distinguishing valid and quarantined rates for further processing
    or review.

    :param spark: The SparkSession object used to perform the transformations.
    :type spark: SparkSession

    :return: None
    """
    bronze_path_rates = os.path.join(BRONZE_OUT_DIR, 'rates')
    print(f'Reading Bronze rates from: {bronze_path_rates}')

    # We deal only with ISO 4217 currencies
    iso_4217_currencies = (
        spark.read
        .format('delta')
        .load(silver_path_currencies)
        .select('short_code')
    )

    if not iso_4217_currencies.count() == 0:
        logger.error('No ISO 4217 currencies found in Silver layer. Cannot proceed with rate transformation.')
        print('No ISO 4217 currencies found in Silver layer. Cannot proceed with rate transformation.')
        return

    df_rates = spark.read.format('delta').load(bronze_path_rates)

    # We use only latest rates
    max_ingested_at = df_rates.select(F.max('_ingested_at')).collect()[0][0]
    df_rates = df_rates.filter(F.col('_ingested_at') == max_ingested_at)

    print(f'Latest ingested_at: {max_ingested_at}')

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

    try:
        (
            df_rates_valid
            .drop('_validation_errors')
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("curr_base")
            .save(silver_path_rates)
        )
    except Exception as e:
        logger.exception(f'Error writing valid rates to {silver_path_rates}: {e}')
        print(f'Error writing valid rates to {silver_path_rates}: {e}')

    print(f'Valid {df_rates_valid.count()} rates written to {silver_path_rates}')

    print('Quarantined rates:')
    df_rates_quarantine.show()

    quar_rates_count = df_rates_quarantine.count()

    if quar_rates_count > 0:
        try:
            (
                df_rates_quarantine
                .write.format("delta")
                .mode("overwrite")
                .partitionBy("curr_base")
                .save(silver_path_quarantine_rates)
            )
        except Exception as e:
            logger.exception(f'Error writing quarantined rates to {silver_path_quarantine_rates}: {e}')
            print(f'Error writing quarantined rates to {silver_path_quarantine_rates}: {e}')

        print(f'Quarantined {quar_rates_count} rates saved to: {silver_path_quarantine_rates}')



if __name__ == '__main__':
    spark = get_spark("silver_currency_stuff")
    spark.sparkContext.setLogLevel("WARN")

    transform_currencies(spark)
    transform_rates(spark)

