import logging
import sys
from google.cloud import storage
import pandas as pd
import datetime
from utils.bronze_profile_funcs import get_csv_files, get_profiling_rules, profile_data

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def profile_bronze_data(spark, bucket_name, upload_prefix, file_key, data_type):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect source files.
    :param file_key: label for file name that helps with the cherry picking with data_type.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return clean_files: asset CSV files that pass the profile rules.
    """
    logger.info(f"Start {data_type.upper()} BRONZE PROFILING job.")
    table_rules = get_profiling_rules(bucket_name, data_type)

    logger.info("Create NEW dataframe")
    all_new_files = get_csv_files(bucket_name, upload_prefix, file_key, data_type)
    if len(all_new_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_new_files)} {data_type} data CSV files.")
        clean_files, dirty_files = profile_data(
            spark, bucket_name, all_new_files, data_type, table_rules
        )
        if dirty_files == []:
            logger.info("No failed CSV found.")
        else:
            logger.info(f"Found {len(dirty_files)} failed CSV found.")
            dirty_data = {
                "csv_uri": [el[0] for el in dirty_files],
                "error_text": [el[1] for el in dirty_files],
            }
            dirty_df = pd.DataFrame(data=dirty_data)
            storage_client = storage.Client()
            bucket = storage_client.get_bucket(bucket_name)
            bucket.blob(
                f'dirty_dump/{datetime.date.today().strftime("%Y-%m-%d")}dirty_{data_type}.csv'
            ).upload_from_string(dirty_df.to_csv(), "text/csv")
        if clean_files == []:
            logger.info("No passed CSV found. Workflow stopped!")
            sys.exit(1)
    logger.info(f"End {data_type.upper()} BRONZE PROFILING job.")
    return clean_files
