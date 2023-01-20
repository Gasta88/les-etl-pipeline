import logging
import sys
from google.cloud import storage
import pandas as pd
import datetime
from src.loan_etl_pipeline.utils.bronze_profile_funcs import (
    get_csv_files,
    get_profiling_rules,
    profile_data,
)

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def profile_bronze_data(
    spark, raw_bucketname, data_bucketname, source_prefix, file_key, data_type
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect source files.
    :param file_key: label for file name that helps with the cherry picking with data_type.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE PROFILING job.")
    ed_code = source_prefix.split("/")[-1]
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(data_bucketname)
    clean_dump_csv = bucket.blob(
        f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_clean_{data_type}.csv'
    )
    if clean_dump_csv.exists():
        logger.info(
            f"{data_type.upper()} BRONZE PROFILING job has been already profiled. Continue!"
        )
        return 0
    else:
        table_rules = get_profiling_rules(data_type)

        logger.info(f"Create NEW {ed_code} dataframe")
        all_new_files = get_csv_files(
            raw_bucketname, source_prefix, file_key, data_type
        )
        if len(all_new_files) == 0:
            logger.warning("No new CSV files to retrieve. Workflow stopped!")
            sys.exit(1)
        else:
            logger.info(f"Retrieved {len(all_new_files)} {data_type} data CSV files.")
            dirty_files = []
            clean_files = []
            for new_file_name in all_new_files:
                logger.info(f"Checking {new_file_name}..")
                profile_flag, error_text = profile_data(
                    spark, raw_bucketname, new_file_name, data_type, table_rules
                )
                if profile_flag == "dirty":
                    dirty_files.append((new_file_name, error_text))
                else:
                    clean_files.append(new_file_name)
            if dirty_files == []:
                logger.info("No failed CSV found.")
            else:
                logger.info(f"Found {len(dirty_files)} failed CSV found.")
                dirty_data = {
                    "csv_uri": [el[0] for el in dirty_files],
                    "error_text": [el[1] for el in dirty_files],
                }
                dirty_df = pd.DataFrame(data=dirty_data)
                bucket.blob(
                    f'dirty_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_dirty_{data_type}.csv'
                ).upload_from_string(dirty_df.to_csv(), "text/csv")
            if clean_files == []:
                logger.info("No passed CSV found. Workflow stopped!")
                sys.exit(1)
            else:
                logger.info(f"Found {len(clean_files)} clean CSV found.")
                clean_data = {
                    "csv_uri": clean_files,
                }
                clean_df = pd.DataFrame(data=clean_data)
                bucket.blob(
                    f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_clean_{data_type}.csv'
                ).upload_from_string(clean_df.to_csv(), "text/csv")
    logger.info(f"End {data_type.upper()} BRONZE PROFILING job.")
    return 0
