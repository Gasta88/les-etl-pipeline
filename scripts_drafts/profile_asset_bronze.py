import logging
import sys
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv
from google.cloud import storage

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_csv_files(bucket_name, prefix, file_key):
    """
    Return list of CSV files that satisfy the file_key parameter from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project="dataops-369610")
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name, prefix=prefix)
        if (b.name.endswith(".csv"))
        and (file_key in b.name)
        and not ("Labeled0M" in b.name)
    ]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {bucket_name}. Exit process!"
        )
        sys.exit(1)
    else:
        return all_files


def _get_checks_dict(df):
    """
    Run quality check on PySpark dataframe.

    :param df: PySpark dataframe.
    :return check_dict: collection of checks run on the PySpark dataframe.
    """
    mandatory_cols = [
        "AS1",
        "AS2",
        "AS3",
        "AS4",
        "AS5",
        "AS6",
        "AS7",
        "AS15",
        "AS16",
        "AS18",
        "AS22",
        "AS23",
        "AS25",
        "AS26",
        "AS37",
        "AS42",
        "AS50",
        "AS51",
        "AS52",
        "AS53",
        "AS54",
        "AS55",
        "AS56",
        "AS58",
        "AS59",
        "AS62",
        "AS65",
        "AS66",
        "AS68",
        "AS80",
        "AS81",
        "AS82",
        "AS83",
        "AS84",
        "AS85",
        "AS94",
        "AS115",
        "AS116",
        "AS117",
        "AS118",
        "AS121",
        "AS122",
        "AS123",
        "AS124",
        "AS125",
        "AS128",
        "AS132",
        "AS134",
    ]
    n_rows = df.count()
    checks_dict = {
        "IsNotEmpty": n_rows > 0,
        "hasColumns": mandatory_cols.issubset(set(df.columns)),
        "AS3_IdUnique": df.select("AS3").distinct().count() == n_rows,
        "AS3_IsComplete": (
            df.select(F.concat("AS3").alias("combined"))
            .where(F.col("combined").isNull())
            .count()
            == 0
        ),
    }


def profile_data(spark, bucket_name, all_files):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param all_files: list of files to be read to generate the dtaframe.
    :return clean_files: CSV files that passes the bronze profiling rules.
    """
    storage_client = storage.Client(project="dataops-369610")
    clean_files = []
    bucket = storage_client.get_bucket(bucket_name)
    for csv_f in all_files:
        blob = bucket.blob(csv_f)
        dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
        blob.download_to_filename(dest_csv_f)
        col_names = []
        content = []
        with open(dest_csv_f, "r") as f:
            for i, line in enumerate(csv.reader(f)):
                if i == 0:
                    col_names = line
                    col_names[0] = "AS1"
                elif i == 1:
                    continue
                else:
                    if len(line) == 0:
                        continue
                    content.append(line)
            df = spark.createDataFrame(content, col_names)
    return clean_files


def profile_asset_bronze(spark, bucket_name, upload_prefix, file_key):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect CSV files.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :return clean_files: asset CSV files that pass the profile rules.
    """
    logger.info("Start ASSETS BRONZE PROFILING job.")

    logger.info("Create NEW dataframe")
    all_new_files = get_csv_files(bucket_name, upload_prefix, file_key)
    if len(all_new_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_new_files)} asset data CSV files.")
        new_asset_df = create_dataframe(spark, bucket_name, all_new_files)

        logger.info(f"Profile dataframe.")

    logger.info("End ASSETS BRONZE PROFILING job.")
    return clean_files
