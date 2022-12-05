from pyspark.sql import SparkSession


def start_spark():
    """
    Create Spark application using Delta Lake dependencies.

    :param app_name: Name of the Spark App
    :return: SparkSession
    """

    spark = (
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-core:1.0.1")
        .getOrCreate()
    )
    return spark
