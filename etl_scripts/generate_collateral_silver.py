import logging
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

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
    config["SOURCE_DIR"] = os.environ["SOURCE_DIR"]
    config["SPARK"] = SparkSession.builder.master(
        f'local[{int(os.environ["WORKERS"])}]'
    ).getOrCreate()
    config["DATE_COLUMNS"] = ["CS11", "CS12", "CS22"]
    return config


def process_dates(df, date_cols_list):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param date_cols_list: list of date columns.
    :return new_df: silver type Spark dataframe.
    """
    date_cols = [c for c in date_cols_list if c in df.columns]

    new_df = (
        df.select(F.explode(F.array(date_cols)).alias("date_col"))
        .dropDuplicates()
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


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
        f'{run_props["SOURCE_DIR"]}/bronze/collaterals.parquet'
    )
    logger.info("Generate collateral info dataframe")
    info_df = process_collateral_info(bronze_df)
    logger.info("Generate time dataframe")
    date_df = process_dates(bronze_df, run_props["DATE_COLUMNS"])

    logger.info("Write dataframe")

    (
        info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/collaterals/info_table.parquet")
    )
    (
        date_df.format("parquet")
        .mode("append")
        .save("../dataoutput/silver/collaterals/date_table.parquet")
    )

    return


if __name__ == "__main__":
    main()
