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
    return config


def process_dates(df):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select("DATE_VALUE")
        .alias("date_col")
        .dropDuplicates()
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


def process_info(df):
    """
    Extract amortisation values dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.withColumn(
        "tmp_DATE_VALUE", F.unix_timestamp(F.col("DATE_VALUE"))
    ).drop("DATE_VALUE")
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start AMORTISATION SILVER job.")
    run_props = set_job_params()
    bronze_df = run_props["SPARK"].read.parquet(
        f'{run_props["SOURCE_DIR"]}/bronze/amortisation.parquet'
    )
    logger.info("Generate time dataframe")
    date_df = process_dates(bronze_df, run_props["DATE_COLUMNS"])
    logger.info("Generate info dataframe")
    info_df = process_info(bronze_df)
    logger.info("Write dataframe")

    (
        date_df.format("parquet")
        .mode("append")
        .save("../dataoutput/silver/amortisation/date_table.parquet")
    )
    (
        info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/amortisation/info_table.parquet")
    )

    return


if __name__ == "__main__":
    main()
