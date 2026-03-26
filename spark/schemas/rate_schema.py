from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    DecimalType,
)

RATES_SCHEMA = StructType(
    [
        StructField("curr_base", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("rate_date", TimestampType(), nullable=True),
        StructField("rate", DecimalType(20, 10), nullable=True),
    ]
)