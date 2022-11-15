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
    config["DATE_COLUMNS"] = [
        "AS1",
        "AS19",
        "AS20",
        "AS31",
        "AS50",
        "AS51",
        "AS67",
        "AS70",
        "AS71",
        "AS87",
        "AS91",
        "AS112",
        "AS124",
        "AS127",
        "AS130",
        "AS133",
        "AS134",
        "AS137",
    ]
    return config


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["ID", "year", "month", "day"]
        + [f"AS{i}" for i in range(1, 15) if f"AS{i}" in df.columns],
        "obligor_info": [f"AS{i}" for i in range(15, 50) if f"AS{i}" in df.columns],
        "loan_info": [f"AS{i}" for i in range(50, 80) if f"AS{i}" in df.columns],
        "interest_rate": [f"AS{i}" for i in range(80, 100) if f"AS{i}" in df.columns],
        "financial_info": [f"AS{i}" for i in range(100, 115) if f"AS{i}" in df.columns],
        "performance_info": [
            f"AS{i}" for i in range(115, 146) if f"AS{i}" in df.columns
        ],
    }
    return cols_dict


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


def process_obligor_info(df, cols_dict):
    """
    Extract obligor info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["obligor_info"])
        .withColumn("tmp_AS1", F.unix_timestamp(F.col("AS1")))
        .drop("AS1")
        .withColumnRenamed("tmp_AS1", "AS1")
        .withColumn("tmp_AS19", F.unix_timestamp(F.col("AS19")))
        .drop("AS19")
        .withColumnRenamed("tmp_AS19", "AS19")
        .withColumn("tmp_AS20", F.unix_timestamp(F.col("AS20")))
        .drop("AS20")
        .withColumnRenamed("tmp_AS20", "AS20")
        .withColumn("tmp_AS31", F.unix_timestamp(F.col("AS31")))
        .drop("AS31")
        .withColumnRenamed("tmp_AS31", "AS31")
    )
    return new_df


def process_loan_info(df, cols_dict):
    """
    Extract loan info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["loan_info"])
        .withColumn("tmp_AS1", F.unix_timestamp(F.col("AS1")))
        .drop("AS1")
        .withColumnRenamed("tmp_AS1", "AS1")
        .withColumn("tmp_AS50", F.unix_timestamp(F.col("AS50")))
        .drop("AS50")
        .withColumnRenamed("tmp_AS50", "AS50")
        .withColumn("tmp_AS51", F.unix_timestamp(F.col("AS51")))
        .drop("AS51")
        .withColumnRenamed("tmp_AS51", "AS51")
        .withColumn("tmp_AS67", F.unix_timestamp(F.col("AS67")))
        .drop("AS67")
        .withColumnRenamed("tmp_AS67", "AS67")
        .withColumn("tmp_AS70", F.unix_timestamp(F.col("AS70")))
        .drop("AS70")
        .withColumnRenamed("tmp_AS70", "AS70")
        .withColumn("tmp_AS71", F.unix_timestamp(F.col("AS71")))
        .drop("AS71")
        .withColumnRenamed("tmp_AS71", "AS71")
    )
    return new_df


def process_interest_rate(df, cols_dict):
    """
    Extract interest rate dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["interest_rate"])
        .withColumn("tmp_AS1", F.unix_timestamp(F.col("AS1")))
        .drop("AS1")
        .withColumnRenamed("tmp_AS1", "AS1")
        .withColumn("tmp_AS87", F.unix_timestamp(F.col("AS87")))
        .drop("AS87")
        .withColumnRenamed("tmp_AS87", "AS87")
        .withColumn("tmp_AS91", F.unix_timestamp(F.col("AS91")))
        .drop("AS91")
        .withColumnRenamed("tmp_AS91", "AS91")
    )
    return new_df


def process_financial_info(df, cols_dict):
    """
    Extract financial info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["financial_info"])
        .withColumn("tmp_AS1", F.unix_timestamp(F.col("AS1")))
        .drop("AS1")
        .withColumnRenamed("tmp_AS1", "AS1")
        .withColumn("tmp_AS112", F.unix_timestamp(F.col("AS112")))
        .drop("AS112")
        .withColumnRenamed("tmp_AS112", "AS112")
    )
    return new_df


def process_performance_info(df, cols_dict):
    """
    Extract performance info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["performance_info"])
        .withColumn("tmp_AS1", F.unix_timestamp(F.col("AS1")))
        .drop("AS1")
        .withColumnRenamed("tmp_AS1", "AS1")
        .withColumn("tmp_AS124", F.unix_timestamp(F.col("AS124")))
        .drop("AS124")
        .withColumnRenamed("tmp_AS124", "AS124")
        .withColumn("tmp_AS127", F.unix_timestamp(F.col("AS127")))
        .drop("AS127")
        .withColumnRenamed("tmp_AS127", "AS127")
        .withColumn("tmp_AS130", F.unix_timestamp(F.col("AS130")))
        .drop("AS130")
        .withColumnRenamed("tmp_AS130", "AS130")
        .withColumn("tmp_AS133", F.unix_timestamp(F.col("AS133")))
        .drop("AS133")
        .withColumnRenamed("tmp_AS133", "AS133")
        .withColumn("tmp_AS134", F.unix_timestamp(F.col("AS134")))
        .drop("AS134")
        .withColumnRenamed("tmp_AS134", "AS134")
        .withColumn("tmp_AS137", F.unix_timestamp(F.col("AS137")))
        .drop("AS137")
        .withColumnRenamed("tmp_AS137", "AS137")
    )
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    bronze_df = run_props["SPARK"].read.parquet(
        f'{run_props["SOURCE_DIR"]}/bronze/assets.parquet'
    )
    assets_columns = get_columns_collection(bronze_df)
    logger.info("Generate time dataframe")
    date_df = process_dates(bronze_df, run_props["DATE_COLUMNS"])
    logger.info("Generate obligor info dataframe")
    obligor_info_df = process_obligor_info(bronze_df, assets_columns)
    logger.info("Generate loan info dataframe")
    loan_info_df = process_loan_info(bronze_df, assets_columns)
    logger.info("Generate interest rate dataframe")
    interest_rate_df = process_interest_rate(bronze_df, assets_columns)
    logger.info("Generate financial info dataframe")
    financial_info_df = process_financial_info(bronze_df, assets_columns)
    logger.info("Generate performace info dataframe")
    performance_info_df = process_performance_info(bronze_df, assets_columns)

    logger.info("Write dataframe")

    (
        date_df.format("parquet")
        .mode("append")
        .save("../dataoutput/silver/assets/date_table.parquet")
    )
    (
        loan_info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/assets/loan_info_table.parquet")
    )
    (
        obligor_info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/assets/obligor_info_table.parquet")
    )
    (
        financial_info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/assets/financial_info_table.parquet")
    )
    (
        interest_rate_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/assets/interest_rate_table.parquet")
    )
    (
        performance_info_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/silver/assets/performance_info_table.parquet")
    )
    return


if __name__ == "__main__":
    main()
