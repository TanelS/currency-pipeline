from pyspark.sql import SparkSession
import config


def get_spark(appname: str):
    builder = (
        SparkSession.builder
        .appName(appname)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if config.RUNNING_LOCAL:
        builder = builder.master("local[*]").config(
            "spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0"
        )
    return builder.getOrCreate()
