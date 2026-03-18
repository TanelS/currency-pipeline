from spark.session.builder import get_spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ingestion.jobs import get_currencies
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)


CURRENCY_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("short_code", StringType(), True),
    StructField("code", StringType(), True),
    StructField("precision", IntegerType(), True),
    StructField("subunit", IntegerType(), True),
    StructField("symbol", StringType(), True),  # TODO rename downstream to curr_symbol
    StructField("symbol_first", BooleanType(), True),
    StructField("decimal_mark", StringType(), True),
    StructField("thousands_separator", StringType(), True)
])


def ingest_curr_codes(spark: SparkSession):
    print('Reading raw currency symbols data from API')
    currency_records = []
    raw_curr_symbols = get_currencies()

    df_read = (spark.createDataFrame(currency_records, CURRENCY_SCHEMA))

    print('Preview of ingested currency data:')
    df_read.show()  # Call show() separately to display the data

    return df_read

if __name__ == '__main__':
    spark = get_spark('bronze_currency_symbols')
    spark.sparkContext.setLogLevel("WARN")

    df_addresses = ingest_curr_codes(spark)

    spark.stop()
    print("Ingestion complete.")