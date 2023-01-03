import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from delta import *
from utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
    cast_to_datatype,
    return_write_mode,
)

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
    config["DATE_COLUMNS"] = ["CS11", "CS12", "CS22"]
    config["COLLATERAL_COLUMNS"] = {
        "CS1": StringType(),
        "CS2": StringType(),
        "CS3": StringType(),
        "CS4": DoubleType(),
        "CS5": DoubleType(),
        "CS6": StringType(),
        "CS7": BooleanType(),
        "CS8": BooleanType(),
        "CS9": BooleanType(),
        "CS10": DoubleType(),
        "CS11": DateType(),
        "CS12": DateType(),
        "CS13": StringType(),
        "CS14": StringType(),
        "CS15": DoubleType(),
        "CS16": StringType(),
        "CS17": StringType(),
        "CS18": DoubleType(),
        "CS19": DoubleType(),
        "CS20": StringType(),
        "CS21": DoubleType(),
        "CS22": DateType(),
        "CS23": StringType(),
        "CS24": StringType(),
        "CS25": StringType(),
        "CS26": StringType(),
        "CS27": StringType(),
        "CS28": DoubleType(),
    }
    return config


def process_collateral_info(df):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.dropDuplicates()
    return new_df


def generate_collateral_silver(spark, bucket_name, bronze_prefix, silver_prefix, pcds):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze data.
    :param silver_prefix: specific bucket prefix from where to deposit silver data.
    :param pcds: list of PCDs that have been elaborated in the previous Bronze layer.
    :return status: 0 if successful.
    """
    logger.info("Start COLLATERAL SILVER job.")
    run_props = set_job_params()
    if pcds == "":
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )
    else:
        truncated_pcds = ["-".join(pcd.split("-")[:2]) for pcd in pcds.split(",")]
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(truncated_pcds))
            .drop("valid_from", "valid_to", "checksum", "iscurrent", "lookup")
        )
    logger.info("Remove ND values.")
    tmp_df1 = replace_no_data(bronze_df)
    logger.info("Replace Y/N with boolean flags.")
    tmp_df2 = replace_bool_data(tmp_df1)
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(tmp_df2, run_props["COLLATERAL_COLUMNS"])
    logger.info("Generate collateral info dataframe")
    info_df = process_collateral_info(cleaned_df)

    logger.info("Write dataframe")
    write_mode = return_write_mode(bucket_name, silver_prefix, pcds)

    (
        info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/info_table")
    )
