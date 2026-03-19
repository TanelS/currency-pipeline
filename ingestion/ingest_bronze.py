import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType,
)

from decimal import Decimal

from ingestion.jobs import get_currencies, get_rates
from spark.config.spark_config import BRONZE_OUT_DIR
from spark.session.builder import get_spark

CURRENCY_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("short_code", StringType(), True),
        StructField("code", StringType(), True),
        StructField("precision", IntegerType(), True),
        StructField("subunit", IntegerType(), True),
        StructField("symbol", StringType(), True),  # TODO rename downstream to curr_symbol
        StructField("symbol_first", BooleanType(), True),
        StructField("decimal_mark", StringType(), True),
        StructField("thousands_separator", StringType(), True),
    ]
)

RATES_SCHEMA = StructType(
    [
        StructField("curr_base", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("rate_date", TimestampType(), nullable=True),
        StructField("rate", DecimalType(20, 10), nullable=True),
    ]
)


def ingest_curr_codes(spark: SparkSession):
    print("Reading raw currency symbols data from API")
    raw_curr_symbols = get_currencies()

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

    (df_with_meta.coalesce(1).write.format("delta").mode("overwrite").save(out_path))

    print("  Done. Bronze table written.")
    return df_with_meta


def ingest_rates(spark: SparkSession):
    print("Reading currency rates from API")

    df_symbols = (
        spark.read.format("Delta")
        .load(f"{BRONZE_OUT_DIR}/currencies")
        .select("short_code")
        .collect()
    )

    codes = {df_symbol["short_code"] for df_symbol in df_symbols}

    rate_rows = []

    for base_c in codes:
        target_symbols = codes - {base_c}
        target_symbols_str = ",".join(target_symbols)

        print(f"Processing base currency: {base_c}")

        params = {"base": base_c, "symbols": target_symbols_str}

        rates_with_meta = get_rates(params)

        rate_date = rates_with_meta.get("date") if rates_with_meta else None
        curr_base = rates_with_meta.get("base") if rates_with_meta else None
        rates = rates_with_meta.get("rates") if rates_with_meta else None

        if rates:
            for curr_code, rate in rates.items():
                rate_rows.append(
                    {
                        "curr_base": curr_base,
                        "rate_date": datetime.datetime.strptime(rate_date, "%Y-%m-%dT%H:%M:%SZ"),
                        "currency": curr_code,
                        "rate": Decimal(str(rate)) if rate is not None else None,
                    }
                )

    print(f'{len(rate_rows) = }')  # TODO delete

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


if __name__ == "__main__":
    spark = get_spark("bronze_currency_rates")
    spark.sparkContext.setLogLevel("WARN")

    # df_addresses = ingest_curr_codes(spark)
    df_rates = ingest_rates(spark)

    spark.stop()
    print("Ingestion complete.")
