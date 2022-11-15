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
    config["DATE_COLUMNS"] = ["BS1"]
    return config


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "bond_info": ["ID", "year", "month", "day"]
        + [f"BS{i}" for i in range(1, 11) if f"BS{i}" in df.columns],
        "collateral_info": ["ID", "year", "month", "day"]
        + [f"BS{i}" for i in range(11, 19) if f"BS{i}" in df.columns],
        "contact_info": ["ID", "year", "month", "day"]
        + [f"BS{i}" for i in range(19, 25) if f"BS{i}" in df.columns],
    }
    return cols_dict


def process_dates(df, col_types_dict):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param col_types_dict: collection of columns and their types.
    :return new_df: silver type Spark dataframe.
    """
    date_cols = [c for c in col_types_dict["date"] if c in df.columns]

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


def process_bond_info(df, cols_dict):
    """
    Extract bond info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["bond_info"])
        .withColumn(
            "tmp_BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd"))
        )
        .drop("BS1")
        .withColumnRenamed("tmp_BS1", "BS1")
    )
    return new_df


def process_collateral_info(df, cols_dict):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(["BS1", "BS2"] + cols_dict["collateral_info"])
        .withColumn(
            "tmp_BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd"))
        )
        .drop("BS1")
        .withColumnRenamed("tmp_BS1", "BS1")
    )
    return new_df


def process_contact_info(df, cols_dict):
    """
    Extract contact info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(["BS1", "BS2"] + cols_dict["contact_info"])
        .withColumn(
            "tmp_BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd"))
        )
        .drop("BS1")
        .withColumnRenamed("tmp_BS1", "BS1")
    )
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    bronze_df = run_props["SPARK"].read.parquet(
        f'{run_props["SOURCE_DIR"]}/bronze/bond_info.parquet'
    )
    bond_info_columns = get_columns_collection(bronze_df)
    logger.info("Generate time dataframe")
    date_df = process_dates(bronze_df, run_props["DATE_COLUMNS"])
    logger.info("Generate bond info dataframe")
    info_df = process_bond_info(bronze_df, bond_info_columns)
    logger.info("Generate collateral info dataframe")
    collateral_df = process_collateral_info(bronze_df, bond_info_columns)
    logger.info("Generate contact info dataframe")
    contact_df = process_contact_info(bronze_df, bond_info_columns)

    logger.info("Write dataframe")

    (
        date_df.format("parquet")
        .mode("append")
        .save("../dataoutput/silver/bond_info/date_table.parquet")
    )
    (
        info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/bond_info/info_table.parquet")
    )
    (
        collateral_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/bond_info/collaterals_table.parquet")
    )
    (
        contact_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/bond_info/contacts_table.parquet")
    )

    return


if __name__ == "__main__":
    main()
