import os
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import psycopg
from spark.config.spark_config import BRONZE_OUT_DIR, SILVER_DIR
from spark.session.builder import get_spark
from db import jdbc_props, JDBC_URL

silver_path_currencies = os.path.join(SILVER_DIR, 'currencies')
silver_path_rates = os.path.join(SILVER_DIR, 'rates')


def load_currencies_to_stage(spark):
    print(f'Reading Silver currencues from: {silver_path_currencies}')
    df = spark.read.format('delta').load(silver_path_currencies)
    print(f"  Rows: {df.count():,}")
    df.show()

    print("Writing currencies to stage table...")
    writer = df.write.mode('overwrite').option('truncate', 'true')
    writer.jdbc(JDBC_URL, 'public.currencies_stage', properties=jdbc_props)
    print('  Currencies stage table loaded.')




if __name__ == '__main__':
    spark = get_spark("to_postgres_stage")
    spark.sparkContext.setLogLevel("WARN")

    load_currencies_to_stage(spark)