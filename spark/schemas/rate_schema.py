from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RATES_SCHEMA = StructType(
    [
        StructField("curr_base", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("rate_date", TimestampType(), nullable=True),
        StructField("rate", DecimalType(20, 10), nullable=True),
    ]
)