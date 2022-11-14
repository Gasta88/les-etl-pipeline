import logging
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, BooleanType, DoubleType
import csv
from functools import reduce

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
    config["ASSET_COLUMNS_TYPE"] = {
        "date": [
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
        ],
        "string": [
            "AS2",
            "AS3",
            "AS4",
            "AS5",
            "AS6",
            "AS7",
            "AS8",
            "AS15",
            "AS16",
            "AS17",
            "AS18",
            "AS21",
            "AS22",
            "AS24",
            "AS25",
            "AS26",
            "AS32",
            "AS33",
            "AS34",
            "AS35",
            "AS36",
            "AS42",
            "AS43",
            "AS45",
            "AS52",
            "AS57",
            "AS58",
            "AS59",
            "AS62",
            "AS65",
            "AS68",
            "AS83",
            "AS84",
            "AS89",
            "AS92",
            "AS94",
            "AS111",
            "AS123",
            "AS129",
        ],
        "boolean": [
            "AS23",
            "AS27",
            "AS28",
            "AS29",
            "AS30",
            "AS37",
            "AS38",
            "AS39",
            "AS40",
            "AS41",
            "AS44",
            "AS53",
            "AS54",
            "AS55",
            "AS56",
            "AS60",
            "AS61",
            "AS63",
            "AS64",
            "AS66",
            "AS69",
            "AS80",
            "AS81",
            "AS82",
            "AS85",
            "AS86",
            "AS88",
            "AS90",
            "AS93",
            "AS100",
            "AS101",
            "AS102",
            "AS103",
            "AS104",
            "AS105",
            "AS106",
            "AS107",
            "AS108",
            "AS109",
            "AS110",
            "AS115",
            "AS116",
            "AS117",
            "AS118",
            "AS119",
            "AS120",
            "AS121",
            "AS122",
            "AS125",
            "AS126",
            "AS128",
            "AS131",
            "AS132",
            "AS135",
            "AS136",
            "AS138",
        ],
    }
    return config


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


# Continue with obligor info method


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start asset data job.")
    run_props = set_job_params()
    bronze_df = run_props["SPARK"].read.parquet(
        f'{run_props["SOURCE_DIR"]}/bronze/asset_bronze.parquet'
    )
    assets_columns = split_columns_by_topic(bronze_df)
    date_df = process_dates(bronze_df, run_props["ASSET_COLUMNS_TYPE"])
    obligor_info_df = process_obligor_info(bronze_df, assets_columns)
    loan_info_df = process_loan_info(bronze_df, assets_columns)
    interest_rate_df = process_interest_rate(bronze_df, assets_columns)
    financial_info_df = process_financial_info(bronze_df, assets_columns)
    performance_info_df = process_performance_info(bronze_df, assets_columns)

    # (
    #     final_df.format("parquet")
    #     .partitionBy("year", "month", "day")
    #     .mode("append")
    #     .save("../data/output/bronze/asset_bronze.parquet")
    # )
    return


if __name__ == "__main__":
    main()
