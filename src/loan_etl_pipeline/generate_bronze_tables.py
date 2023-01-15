import logging
import sys
from google.cloud import storage
import datetime
from src.loan_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
    get_all_files,
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
    spark,
    raw_bucketname,
    data_bucketname,
    source_prefix,
    target_prefix,
    data_type,
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze new data.
    :param target_prefix: specific bucket prefix from where to collect bronze old data.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bucketname)
    clean_dump_csv = bucket.blob(
        f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_clean_{data_type}.csv'
    )
    if not (clean_dump_csv.exists()):
        logger.info(
            f"Could not find clean CSV dump file from {data_type.upper()} BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
    else:
        ed_code = source_prefix.split("/")[-1]
        all_files = get_all_files(data_bucketname, data_type)
        logger.info(f"Retrieved {len(all_files)} {data_type} data files.")
        for new_file_name in all_files:
            logger.info(f"Create NEW {ed_code} dataframe")
            pcds, new_df = create_dataframe(
                spark, raw_bucketname, new_file_name, data_type
            )
            if new_df is None:
                logger.error("No dataframes were extracted from file. Exit process!")
                continue
            else:
                logger.info(
                    f"Retrieve OLD {ed_code} dataframe. Use following PCDs: {pcds}"
                )
                old_df = get_old_df(
                    spark, data_bucketname, target_prefix, pcds, ed_code
                )
                if old_df is None:
                    logger.info(f"Initial load into {data_type.upper()} BRONZE")
                    (
                        new_df.write.partitionBy("ed_code", "year", "month")
                        .format("delta")
                        .mode("append")
                        .save(f"gs://{data_bucketname}/{target_prefix}")
                    )
                else:
                    logger.info(f"Upsert data into {data_type.upper()} BRONZE")
                    perform_scd2(spark, old_df, new_df, data_type)

    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
