import logging
import sys
import pyspark.sql.functions as F
from src.les_etl_pipeline.utils.silver_funcs import (
    cast_to_datatype,
    BOND_COLUMNS,
    profile_data,
)
import pandas as pd


# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Bond Info bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    general_cols = ["ed_code", "pcd_year", "pcd_month", "BL1", "BL2"]
    cols_dict = {
        "bond_info": general_cols
        + [f"BL{i}" for i in range(3, 19) if f"BL{i}" in df.columns],
        "transaction_info": general_cols
        + [f"BL{i}" for i in range(19, 25) if f"BL{i}" in df.columns],
        "tranche_info": general_cols
        + [f"BL{i}" for i in range(25, 51) if f"BL{i}" in df.columns],
    }
    return cols_dict


def generate_bond_info_silver(
    spark, bucket_name, source_prefix, target_prefix, ed_code, ingestion_date, tries=5
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param ed_code: deal code to process.
    :param ingestion_date: date of the ETL ingestion.
    :param tries: number of times to retry the job.
    :return status: 0 if successful.
    """
    data_type = "bond_info"
    logger.info(f"Start {data_type.upper()} SILVER job.")
    clean_dump_uri = (
        f"gs://{bucket_name}/clean_dump/{data_type}/{ingestion_date}_{ed_code}.csv"
    )
    clean_dump = pd.read_csv(clean_dump_uri)
    if clean_dump.empty:
        logger.info(
            f"Could not find clean CSV dump file from {data_type.upper()} BRONZE job. Workflow stopped!"
        )
        return 0
    for _, row in clean_dump.iterrows():
        pcd = row["pcd"]
        logger.info(f"Processing data for deal {ed_code}:{pcd}")
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{source_prefix}")
            .where(F.col("part") == f"{ed_code}_{pcd.replace('-', '')}")
            .filter(F.col("iscurrent") == 1)
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )
        logger.info("Profile table")
        good_df, bad_df = profile_data(spark, bronze_df, data_type)
        if bad_df.isEmpty():
            logger.info("No bad records found")
        else:
            logger.info("Writing bad records on GCS..")
            bad_df.write.format("parquet").partitionBy("part").mode("overwrite").save(
                f"gs://{bucket_name}/dirty_dumps/{data_type}"
            )
        if good_df.isEmpty():
            logger.warning("No good record found. Skip!")
            continue
        logger.info("Cleaning values.")
        cleaned_df = cast_to_datatype(good_df, BOND_COLUMNS)
        columns = get_columns_collection(bronze_df)
        for table_name, cols_list in columns.items():
            logger.info(f"Generate {table_name} dataframe")
            for i in range(tries):
                try:
                    cleaned_df.select(cols_list).dropDuplicates().write.format(
                        "parquet"
                    ).partitionBy("part").mode("overwrite").save(
                        f"gs://{bucket_name}/{target_prefix}/{table_name}"
                    )
                except Exception as e:
                    logger.error(f"Writing exception: {e}.Try again.")
                    continue
                break
    logger.info(f"End {data_type.upper()} SILVER job.")
    return 0
