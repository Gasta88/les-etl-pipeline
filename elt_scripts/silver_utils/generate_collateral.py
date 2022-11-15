import logging
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["SOURCE_DIR"] = None
    # TODO: pass number of cores to the SPark application parametrically
    config["SPARK"] = SparkSession.builder.master("local").getOrCreate()
    return config


def process_collateral_info(df):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.withColumn(
            "tmp_CS11", F.unix_timestamp(F.to_timestamp(F.col("CS11"), "yyyy-MM"))
        )
        .drop("CS11")
        .withColumnRenamed("tmp_CS11", "CS11")
        .withColumn(
            "tmp_CS12", F.unix_timestamp(F.to_timestamp(F.col("CS12"), "yyyy-MM"))
        )
        .drop("CS12")
        .withColumnRenamed("tmp_CS12", "CS12")
        .withColumn(
            "tmp_CS22", F.unix_timestamp(F.to_timestamp(F.col("CS22"), "yyyy-MM"))
        )
        .drop("CS22")
        .withColumnRenamed("tmp_CS22", "CS22")
    )
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start COLLATERAL SILVER job.")
    run_props = set_job_params()
    bronze_df = run_props["SPARK"].read.parquet(
        f'{run_props["SOURCE_DIR"]}/bronze/collateral_bronze.parquet'
    )
    logger.info("Generate collateral info dataframe")
    info_df = process_collateral_info(bronze_df)

    logger.info("Write dataframe")

    (
        info_df.format("parquet")
        .mode("append")
        .save("../../data/output/silver/collaterals/info_table.parquet")
    )

    return


if __name__ == "__main__":
    main()
