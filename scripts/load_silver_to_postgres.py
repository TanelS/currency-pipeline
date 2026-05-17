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
    """Overwrites currencies_stage from Silver; drops FK constraints before write, reassigns PK after."""
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
    except Exception:
        logger.exception("Failed to drop foreign key constraints")

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
        except Exception:
            logger.exception("Failed to add primary key to currencies stage table")


def load_rates_to_stage(spark) -> None:
    """Overwrites rates_stage from Silver; reassigns PK and FK constraints referencing currencies_stage after write."""
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
        except Exception:
            logger.exception("Failed to add PK and FK to rates stage table")


if __name__ == "__main__":
    spark = get_spark("to_postgres_stage")
    spark.sparkContext.setLogLevel("WARN")

    load_currencies_to_stage(spark)
    load_rates_to_stage(spark)
