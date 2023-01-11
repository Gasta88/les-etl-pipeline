import logging
import sys
from src.loan_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
)
from delta import *

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def generate_bronze_tables(
    spark, raw_bucketname, data_bucketname, bronze_prefix, all_files, data_type
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param all_files: list of clean CSV files from EDW.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")

    logger.info("Create NEW dataframe")
    if len(all_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_files)} {data_type} data files.")
        pcds, new_df = create_dataframe(spark, raw_bucketname, all_files, data_type)
        if new_df is None:
            logger.error("No dataframes were extracted from files. Exit process!")
            sys.exit(1)
        else:
            logger.info(f"Retrieve OLD dataframe. Use following PCDs: {pcds}")
            old_df = get_old_df(spark, data_bucketname, bronze_prefix, pcds, data_type)
            if old_df is None:
                logger.info(f"Initial load into {data_type.upper()} BRONZE")
                (
                    new_df.write.partitionBy("year", "month")
                    .format("delta")
                    .mode("append")
                    .save(f"gs://{data_bucketname}/{bronze_prefix}")
                )
            else:
                logger.info(f"Upsert data into {data_type.upper()} BRONZE")
                perform_scd2(spark, old_df, new_df, data_type)

    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
