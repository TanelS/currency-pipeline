import datetime
import logging
import os
from decimal import Decimal
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.jobs import get_currencies, get_rates
from spark.config.spark_config import BRONZE_OUT_DIR
from spark.schemas.currency_schema import CURRENCY_SCHEMA
from spark.schemas.rate_schema import RATES_SCHEMA
from spark.session.builder import get_spark

logger = logging.getLogger('ingest-bronze')


def ingest_curr_codes(spark: SparkSession) -> Optional[DataFrame]:
    """
    Ingests currency codes data from the CurrencyBeacon API into a Delta table in the Bronze
    layer. The data is read from the API, transformed to include metadata columns, and then
    written to a Delta table.

    The method starts by fetching raw currency symbols from the CurrencyBeacon API. If no data
    is available, the ingestion process is aborted. The fetched data is transformed into a
    Spark DataFrame according to a predefined schema. Metadata, such as ingestion timestamp,
    source file URL, and batch ID, is added to the DataFrame.

    The transformed DataFrame is written as a Delta table into a predefined Bronze layer
    directory. The writing process ensures partitioning and overwriting to maintain data
    coherence for subsequent processing layers.

    :param spark: A SparkSession instance used for creating and managing Spark DataFrames.
    :type spark: SparkSession
    :return: The ingested DataFrame with metadata added, or None if the ingestion process fails.
    :rtype: Optional[DataFrame]
    """
    print('Reading raw currency symbols data from CurrenyBeacon API')
    raw_curr_symbols = get_currencies()

    if not raw_curr_symbols:
        logger.error('No currency data received from API. Skipping ingestion.')
        return

    df_read = spark.createDataFrame(raw_curr_symbols, CURRENCY_SCHEMA)

    print("Preview of ingested currency data:")
    df_read.show()

    row_count = df_read.count()
    print(f"  Rows read: {row_count:,}")

    df_with_meta = df_read.withColumns(
        {
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.lit("https://api.currencybeacon.com/v1/currencies"),
            "_batch_id": F.lit(datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")),
        }
    )

    out_path = os.path.join(BRONZE_OUT_DIR, "currencies")
    print(f"  Writing Bronze Delta to: {out_path}")

    try:
        (
            df_with_meta
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .save(out_path)
        )
    except Exception as e:
        logger.exception(f'En error occurred during currency codes ingestion from CurrenyBeacon API: {e}')
        print('Currency codes ingestion failed. Check logs for details.')
        return None

    print("  Done. Bronze table written.")
    return df_with_meta


def ingest_rates(spark: SparkSession) -> Optional[DataFrame]:
    """
    Reads currency rates from the CurrencyBeacon API and processes them into a Spark DataFrame
    for storage and analysis. Extracts rates for configured base currencies, calculates rates
    against target currencies, and persists the results into a Delta table within the bronze
    data layer. The function incorporates metadata such as ingestion timestamp, source file
    URL, and batch ID for traceability.

    :param spark: A SparkSession instance used to interact with the Spark cluster.
    :type spark: SparkSession

    :return: A Spark DataFrame containing the ingested and processed currency rates,
        including metadata such as ingestion timestamp, source file URL, and batch ID.
        Returns None if no data is ingested or if ingestion fails.
    :rtype: Optional[DataFrame]
    """
    print('Reading currency rates from CurrenyBeacon API')

    df_symbols = (
        spark.read.format("Delta")
        .load(f"{BRONZE_OUT_DIR}/currencies")
        .select("short_code")
        .collect()
    )

    if not df_symbols:
        logger.error('No currency symbols found in Bronze table. Skipping rates ingestion.')
        return

    codes = {df_symbol["short_code"] for df_symbol in df_symbols}

    rate_rows = []

    for base_c in codes:
        target_symbols = codes - {base_c}
        target_symbols_str = ",".join(target_symbols)

        print(f"Processing base currency: {base_c}")

        params = {"base": base_c, "symbols": target_symbols_str}

        rates_with_meta = get_rates(params)

        if not rates_with_meta:
            logger.error(f'There are no rates for base currency "{base_c}"')
            continue

        rate_date = rates_with_meta.get("date")
        curr_base = rates_with_meta.get("base")
        rates = rates_with_meta.get("rates")

        if rates:
            for curr_code, rate in rates.items():
                rate_rows.append(
                    {
                        "curr_base": curr_base,
                        "rate_date": datetime.datetime.strptime(rate_date, "%Y-%m-%dT%H:%M:%SZ") if rate_date else None,
                        "currency": curr_code,
                        "rate": Decimal(str(rate)) if rate is not None else None,
                    }
                )
        else:
            logger.warning(f'No rates for base currency "{base_c}"')

    if not rate_rows:
        logger.error('No rates to store')
        print('No rates to store, exiting ...')
        return None

    df_read = spark.createDataFrame(rate_rows, RATES_SCHEMA)

    print("Preview of ingested rates:")
    df_read.show()

    df_with_meta = df_read.withColumns(
        {
            "_ingested_at": F.current_timestamp(),
            "_source_file": F.lit("https://api.currencybeacon.com/v1/latest"),
            "_batch_id": F.lit(datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")),
        }
    )

    out_path = os.path.join(BRONZE_OUT_DIR, "rates")
    print(f"  Writing Bronze Delta to: {out_path}")

    try:
        (
            df_with_meta.select(
                "curr_base",
                "currency",
                "rate",
                "rate_date",
                "_ingested_at",
                "_source_file",
                "_batch_id",
            )
            .write.format("delta")
            .mode("append")
            .partitionBy("curr_base")
            .save(out_path)
        )

        print("  Done. Bronze currency rates table written.")
        return df_with_meta
    except Exception as e:
        logger.exception(f'Failed to write Bronze Delta to {out_path}: {str(e)}')
        print('Currency rates ingestion failed. Check logs for details.')
        return None


if __name__ == "__main__":
    spark = get_spark("bronze_currency_rates")
    spark.sparkContext.setLogLevel("WARN")

    df_addresses = ingest_curr_codes(spark)
    df_rates = ingest_rates(spark)

    spark.stop()
    print("Ingestion complete.")
