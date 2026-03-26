from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


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
