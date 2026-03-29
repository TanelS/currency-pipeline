from pyspark.sql import SparkSession

import config


def get_spark(appname: str) -> SparkSession:
    """
    Initializes and returns a SparkSession configured for use with Delta Lake.

    This function creates a SparkSession with Delta Lake support enabled,
    configures Spark SQL extensions, and sets the catalog to DeltaCatalog. When
    running in a local environment, it additionally configures the session with
    appropriate settings for local execution and required dependencies.

    :param appname: The name of the Spark application to set for the session.
    :type appname: str
    :return: A SparkSession instance configured with Delta Lake support.
    :rtype: SparkSession
    """
    builder = (
        SparkSession.builder.appName(appname)
        .config(
            'spark.sql.extensions',
            'io.delta.sql.DeltaSparkSessionExtension'
        )
        .config(
            'spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        )
    )
    if config.RUNNING_LOCAL:
        builder = builder.master('local[*]').config(
            'spark.jars.packages',
            'io.delta:delta-spark_2.12:3.1.0,'
            'org.postgresql:postgresql:42.7.3',
        )
    return builder.getOrCreate()
