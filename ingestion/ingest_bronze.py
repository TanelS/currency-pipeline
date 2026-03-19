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
)

from ingestion.jobs import get_currencies
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
            "_batch_id": F.lit(
                datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
            ),
        }
    )

    out_path = os.path.join(BRONZE_OUT_DIR, "currencies")
    print(f"  Writing Bronze Delta to: {out_path}")

    (
        df_with_meta.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .save(out_path)
    )

    print("  Done. Bronze table written.")
    return df_with_meta


if __name__ == "__main__":
    spark = get_spark("bronze_currency_symbols")
    spark.sparkContext.setLogLevel("WARN")

    df_addresses = ingest_curr_codes(spark)

    spark.stop()
    print("Ingestion complete.")
