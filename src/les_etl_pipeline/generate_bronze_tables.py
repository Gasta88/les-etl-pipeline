import logging
import sys
from google.cloud import storage
from src.les_etl_pipeline.utils.bronze_funcs import (
    get_old_table,
    create_dataframe,
    perform_scd2,
    get_csv_files,
    get_clean_dumps,
)
from delta import *
import pandas as pd

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
    file_key,
    ingestion_date,
    tries=5,
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze new data.
    :param target_prefix: specific bucket prefix from where to collect bronze old data.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param file_key: label for file name that helps with the cherry picking with data_type.
    :param ingestion_date: date of the ETL ingestion.
    :param tries: number of tries to write the data on GCS.
    :return status: 0 if successful.
    """
    logger.info(f"Start {data_type.upper()} BRONZE job.")
    ed_code = source_prefix.split("/")[-1]
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(data_bucketname)
    logger.info(f"Check for clean_dumps on the {ingestion_date}e.")
    all_clean_dumps = get_clean_dumps(data_bucketname, data_type, ingestion_date)
    if all_clean_dumps:
        logger.warning(f"Data already processesed on {ingestion_date}. Stop!")
        return 0
    all_new_files = get_csv_files(raw_bucketname, source_prefix, file_key, data_type)
    if not all_new_files:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        return 0
    logger.info(f"Retrieved {len(all_new_files)} {data_type} data CSV files.")
    clean_lookup = []
    for new_file_name in all_new_files:
        logger.info(f"Checking {new_file_name}..")
        pcd = "-".join(new_file_name.split("/")[-1].split("_")[1:4])
        logger.info(f"Processing data for deal {ed_code}:{pcd}")
        logger.info(f"Retrieve OLD {ed_code} dataframe. Use following PCD: {pcd}")
        old_table = get_old_table(spark, data_bucketname, target_prefix, pcd, ed_code)
        new_df, err = create_dataframe(spark, raw_bucketname, new_file_name, data_type)
        if err:
            logger.error(err)
            continue
        if not new_df:
            logger.error("No dataframes were extracted from file. Skip!")
            continue
        for i in range(tries):
            try:
                logger.info(f"Load into {data_type.upper()} BRONZE")
                if old_table is not None:
                    # TODO: fix SCD2 when subsequent data loads are performed
                    # logger.info(f"Upsert data into {data_type.upper()} BRONZE")
                    # new_df = perform_scd2(new_df, old_table, data_type)
                    continue
                # No SCD Type2 performed
                (
                    new_df.write.partitionBy("part")
                    .format("delta")
                    .mode("overwrite")
                    .save(f"gs://{data_bucketname}/{target_prefix}")
                )
            except Exception as e:
                logger.error(f"Writing exception: {e}.Try again.")
                continue
            break
        clean_lookup.append({"ed_code": ed_code, "pcd": pcd})
    if clean_lookup:
        clean_df = pd.DataFrame(data=clean_lookup)
        bucket.blob(
            f"clean_dump/{data_type}/{ingestion_date}_{ed_code}.csv"
        ).upload_from_string(clean_df.to_csv(index=False), "text/csv")
        logger.info(f"Found {len(clean_lookup)} clean CSV found.")
    logger.info(f"End {data_type.upper()} BRONZE job.")
    return 0
