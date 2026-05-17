from pyspark.sql import SparkSession

import config


def get_spark(appname: str) -> SparkSession:
    """Builds SparkSession with Delta + S3 jars for AWS mode, or Delta + JDBC only for local mode."""
    builder = (
        SparkSession.builder.appName(appname)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if config.RUNNING_AWS:
        builder = (
            builder.master("local[*]")
            .config(
                "spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.7.3",
            )
            .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
            .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.region", "eu-north-1")
        )

    elif config.RUNNING_LOCAL:
        builder = builder.master("local[*]").config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.3",
        )
    return builder.getOrCreate()
