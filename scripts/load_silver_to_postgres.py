import os

import psycopg

from db import JDBC_URL, conn_string, jdbc_props
from spark.config.spark_config import SILVER_DIR
from spark.session.builder import get_spark

silver_path_currencies = os.path.join(SILVER_DIR, 'currencies')
silver_path_rates = os.path.join(SILVER_DIR, 'rates')


def load_currencies_to_stage(spark):
    """
    Loads currency data from a Delta table into a stage table in a PostgreSQL database.

    This function reads data from a Silver Delta table containing currency information,
    displays the row count and table preview, and then writes the data into a stage table.
    Finally, it adds a primary key constraint to the stage table.

    :param spark: A SparkSession object used to interact with Spark.
    :type spark: pyspark.sql.session.SparkSession
    :return: None
    """
    print(f'Reading Silver currencies from: {silver_path_currencies}')
    df = spark.read.format('delta').load(silver_path_currencies)
    print(f"  Rows: {df.count():,}")
    df.show()

    print("Writing currencies to stage table...")
    writer = df.write.mode('overwrite').option('truncate', 'true')
    writer.jdbc(JDBC_URL, 'public.currencies_stage', properties=jdbc_props)
    print('===  Currencies stage table loaded.')

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute('ALTER TABLE public.currencies_stage ADD PRIMARY KEY (short_code)')
        conn.commit()


def load_rates_to_stage(spark):
    """
    Load exchange rates data from a Delta table into a staging table in a PostgreSQL database.

    This function reads data from the Delta table specified by the path `silver_path_rates`,
    writes it to a staging table (`public.rates_stage`) in the database while overwriting
    existing content, and sets up primary and foreign key constraints on the staging table.

    :param spark: Spark session used for reading from the Delta table and processing data.
    :type spark: pyspark.sql.SparkSession

    :return: None
    """
    print(f'Reading Silver rates from: {silver_path_rates}')

    df = spark.read.format('delta').load(silver_path_rates)
    print(f"  Rows: {df.count():,}")
    df.show()

    print("Writing rates to stage table...")
    writer = df.write.mode('overwrite').option('truncate', 'true').option('batchsize', 10000)
    writer.jdbc(JDBC_URL, 'public.rates_stage', properties=jdbc_props)

    print('===  Rates stage table loaded.')

    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cursor:
            cursor.execute('ALTER TABLE public.rates_stage ADD PRIMARY KEY (curr_base, currency, rate_date)')
            cursor.execute('ALTER TABLE public.rates_stage ADD CONSTRAINT base_fk FOREIGN KEY (curr_base) REFERENCES public.currencies_stage (short_code)')
            cursor.execute('ALTER TABLE public.rates_stage ADD CONSTRAINT curr_fk FOREIGN KEY (currency) REFERENCES public.currencies_stage (short_code)')

        conn.commit()


if __name__ == '__main__':
    spark = get_spark("to_postgres_stage")
    spark.sparkContext.setLogLevel("WARN")

    load_currencies_to_stage(spark)
    load_rates_to_stage(spark)