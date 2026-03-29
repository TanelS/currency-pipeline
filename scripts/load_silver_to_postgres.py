import logging
import os

import psycopg

from db import JDBC_URL, conn_string, jdbc_props
from spark.config.spark_config import SILVER_DIR
from spark.session.builder import get_spark

silver_path_currencies = os.path.join(SILVER_DIR, "currencies")
silver_path_rates = os.path.join(SILVER_DIR, "rates")

logger = logging.getLogger("Currencies-rates-to-db-staging")


def load_currencies_to_stage(spark) -> None:
    """
    Loads currency data from the Silver table into the Staging table, performing necessary
    pre-processing steps including row counting, foreign key constraint management, and
    primary key assignment.

    The function reads data from a Silver Delta table and writes it to a staging PostgreSQL
    table, managing table constraints before and after the data load to ensure consistency.

    :param spark: Apache Spark session instance used to interact with the data source.
    :type spark: pyspark.sql.SparkSession
    :return: None
    """
    print(f"Reading Silver currencies from: {silver_path_currencies}")
    df = spark.read.format("delta").load(silver_path_currencies)
    print(f"  Rows: {df.count():,}")
    df.show()

    print("Writing currencies to staging table...")

    # Drop FK-s before jdbc write
    try:
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE public.rates_stage DROP CONSTRAINT IF EXISTS base_fk"
                )  # noqa
                cursor.execute(
                    "ALTER TABLE public.rates_stage DROP CONSTRAINT IF EXISTS curr_fk"
                )  # noqa
            conn.commit()
    except Exception as e:
        logger.exception(f"Failed to drop foreign key constraints: {e}")

    writer = df.write.mode("overwrite").option("truncate", "true")
    writer.jdbc(JDBC_URL, "public.currencies_stage", properties=jdbc_props)
    print("===  Currencies stage table loaded.")

    with psycopg.connect(conn_string) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE public.currencies_stage DROP CONSTRAINT IF EXISTS currencies_stage_pkey"
                )  # noqa
                cursor.execute(
                    "ALTER TABLE public.currencies_stage ADD PRIMARY KEY (short_code)"
                )  # noqa
            conn.commit()
        except Exception as e:
            logger.exception(
                f"Failed to add primary key to currencies stage table: {e}"
            )


def load_rates_to_stage(spark) -> None:
    """
    Loads rate data from the Silver Delta table into a staging table in a database. The function reads data
    from a Delta table using the provided Spark session, writes it into a staging table, and updates
    database constraints to update primary and foreign key relationships for the staging table.

    :param spark: Spark session object used to interact with Spark for reading the Delta table.
    :type spark: pyspark.sql.SparkSession
    :return: None
    """
    print(f"Reading Silver rates from: {silver_path_rates}")

    df = spark.read.format("delta").load(silver_path_rates)
    print(f"  Rows: {df.count():,}")
    df.show()

    print("Writing rates to stage table...")
    writer = (
        df.write.mode("overwrite").option("truncate", "true").option("batchsize", 10000)
    )
    writer.jdbc(JDBC_URL, "public.rates_stage", properties=jdbc_props)

    print("===  Rates stage table loaded.")

    with psycopg.connect(conn_string) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "ALTER TABLE public.rates_stage DROP CONSTRAINT IF EXISTS base_fk"
                )
                cursor.execute(
                    "ALTER TABLE public.rates_stage DROP CONSTRAINT IF EXISTS curr_fk"
                )
                cursor.execute(
                    "ALTER TABLE public.rates_stage DROP CONSTRAINT IF EXISTS rates_stage_pkey"
                )
                cursor.execute(
                    "ALTER TABLE public.rates_stage ADD PRIMARY KEY (curr_base, currency, rate_date)"
                )
                cursor.execute(
                    "ALTER TABLE public.rates_stage ADD CONSTRAINT base_fk FOREIGN KEY (curr_base) REFERENCES public.currencies_stage (short_code)"
                )
                cursor.execute(
                    "ALTER TABLE public.rates_stage ADD CONSTRAINT curr_fk FOREIGN KEY (currency) REFERENCES public.currencies_stage (short_code)"
                )
            conn.commit()
        except Exception as e:
            logger.exception(f"Failed to add PK and FK keyt to rates stage table: {e}")


if __name__ == "__main__":
    spark = get_spark("to_postgres_stage")
    spark.sparkContext.setLogLevel("WARN")

    load_currencies_to_stage(spark)
    load_rates_to_stage(spark)
