import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from delta import *
from google.cloud import storage
import datetime
from src.loan_etl_pipeline.utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
    cast_to_datatype,
    return_write_mode,
    get_all_pcds,
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


def generate_collateral_silver(spark, bucket_name, source_prefix, target_prefix):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :return status: 0 if successful.
    """
    logger.info("Start COLLATERAL SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    clean_dump_csv = bucket.blob(
        f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_clean_collaterals.csv'
    )
    if not (clean_dump_csv.exists()):
        logger.info(
            f"Could not find clean CSV dump file from COLLATERAL BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
    else:
        pcds = get_all_pcds(bucket_name, "collaterals")
        ed_code = source_prefix.split("/")[-1]
        logger.info(f"Processing data for deal {ed_code}")
        for pcd in pcds:
            part_pcd = pcd.replace("-", "")
            logger.info(f"Processing {pcd} data from bronze to silver. ")
            bronze_df = (
                spark.read.format("delta")
                .load(f"gs://{bucket_name}/{source_prefix}")
                .where(f"part={ed_code}_{part_pcd}")
                .filter(F.col("iscurrent") == 1)
                .drop("valid_from", "valid_to", "checksum", "iscurrent")
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
            write_mode = return_write_mode(bucket_name, target_prefix, pcds)

            (
                info_df.write.format("delta")
                .partitionBy("ed_code", "year", "month")
                .mode(write_mode)
                .save(f"gs://{bucket_name}/{target_prefix}/info_table")
            )
    logger.info("End COLLATERAL SILVER job.")
    return 0
