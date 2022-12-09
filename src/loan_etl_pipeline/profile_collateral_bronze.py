import logging
import sys
import csv
from google.cloud import storage
import pandas as pd
import datetime
import pyspark.sql.functions as F

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
        if (b.name.endswith(".csv")) and (file_key in b.name)
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
    mandatory_cols = set(
        [
            "CS1",
            "CS2",
            "CS3",
            "CS6",
            "CS10",
            "CS11",
            "CS12",
            "CS13",
            "CS14",
            "CS16",
            "CS23",
            "CS24",
            "CS28",
        ]
    )
    n_rows = df.count()
    checks_dict = {
        "IsNotEmpty": n_rows > 0,
        "hasColumns": mandatory_cols.issubset(set(df.columns)),
        "CS1+CS2_IdUnique": df.select("CS1", "CS2").distinct().count() == n_rows,
        "CS1+CS2_IsComplete": (
            df.select(F.concat(F.col("CS1"), F.col("CS2")).alias("combined"))
            .where(F.col("combined").isNull())
            .count()
            == 0
        ),
    }
    return checks_dict


def profile_data(spark, bucket_name, all_files):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param all_files: list of files to be read to generate the dataframe.
    :return clean_files: CSV files that passes the bronze profiling rules.
    :return dirty_files: list of tuples with CSV file and relative error text that fails the bronze profiling rules.
    """
    storage_client = storage.Client(project="dataops-369610")
    clean_files = []
    dirty_files = []
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
                    col_names[0] = "CS1"
                elif i == 1:
                    continue
                else:
                    if len(line) == 0:
                        continue
                    content.append(line)
            df = spark.createDataFrame(content, col_names)
            checks = _get_checks_dict(df)
            if False in checks.values():
                error_list = []
                for k, v in checks.items():
                    if not v:
                        error_list.append(k)
                        logger.error(f"Failed CSV: {csv_f}")
                        logger.error(f"Caused by: {k}")
                error_text = ";".join(error_list)
                dirty_files.append((csv_f, error_text))
            else:
                clean_files.append(csv_f)
    return (clean_files, dirty_files)


def profile_collateral_bronze(spark, bucket_name, upload_prefix, file_key):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect CSV files.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :return clean_files: asset CSV files that pass the profile rules.
    """
    logger.info("Start COLLATERALS BRONZE PROFILING job.")

    logger.info("Create NEW dataframe")
    all_new_files = get_csv_files(bucket_name, upload_prefix, file_key)
    if len(all_new_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_new_files)} collateral data CSV files.")
        clean_files, dirty_files = profile_data(spark, bucket_name, all_new_files)
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
                f'dirty_dump/{datetime.date.today().strftime("%Y-%m-%d")}dirty_collaterals.csv'
            ).upload_from_string(dirty_df.to_csv(), "text/csv")
        if clean_files == []:
            logger.info("No passed CSV found. Workflow stopped!")
            sys.exit(1)
    logger.info("End COLLATERALS BRONZE PROFILING job.")
    return clean_files
