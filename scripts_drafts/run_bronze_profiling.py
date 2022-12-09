import logging
import sys
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv
from functools import reduce
from google.cloud import storage

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_csv_files(bucket_name, prefix):
    """
    Return list of CSV files from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :return dict_files: collection of desired files from source_dir indexed by type.
    """
    storage_client = storage.Client(project="dataops-369610")
    dict_files = {"assets": [], "collaterals": [], "bond_info": [], "amortisation": []}
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name, prefix=prefix)
        if (b.name.endswith(".csv")) and not ("Labeled0M" in b.name)
    ]
    if len(all_files) == 0:
        logger.error(f"No files found in {bucket_name}. Exit process!")
        sys.exit(1)
    else:
        for f in all_files:
            if "Loan_Data" in f and not ("Labeled0M" in f):
                dict_files["assets"].append(f)
            if "Collateral" in f:
                dict_files["collaterals"].append(f)
            if "Bond_Info" in f:
                dict_files["bond_info"].append(f)
            if "Amortization" in f:
                dict_files["amortisation"].append(f)
        return dict_files


def create_dataframe(spark, bucket_name, all_files):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param all_files: list of files to be read to generate the dtaframe.
    :return df: PySpark datafram for loan asset data.
    """
    list_dfs = []
    pcds = []
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    for csv_f in all_files:
        blob = bucket.blob(csv_f)
        dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
        blob.download_to_filename(dest_csv_f)
        col_names = []
        content = []
        with open(dest_csv_f, "r") as f:
            csv_id = dest_csv_f.split("/")[-1].split("_")[0]
            pcds.append("-".join(dest_csv_f.split("/")[-1].split("_")[1:4]))
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
            df = (
                spark.createDataFrame(content, col_names)
                .withColumn("ed_code", F.lit(csv_id))
                .replace("", None)
                .withColumn("year", F.year(F.col("AS1")))
                .withColumn("month", F.month(F.col("AS1")))
                .withColumn(
                    "valid_from", F.lit(F.current_timestamp()).cast(TimestampType())
                )
                .withColumn("valid_to", F.lit("").cast(TimestampType()))
                .withColumn("iscurrent", F.lit(1).cast("int"))
                .withColumn(
                    "checksum",
                    F.md5(
                        F.concat(
                            F.col("ed_code"),
                            F.col("AS3"),
                        )
                    ),
                )
            )
            list_dfs.append(df)
    if list_dfs == []:
        logger.error("No dataframes were extracted from files. Exit process!")
        sys.exit(1)
    return (pcds, reduce(DataFrame.union, list_dfs))


def run_bronze_profiling(spark, bucket_name, upload_prefix, bronze_prefix, file_key):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect CSV files.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :return status: 0 if successful.
    """
    logger.info("Start BRONZE PROFILING job.")

    logger.info("Create NEW dataframe")
    all_new_files = get_csv_files(bucket_name, upload_prefix, file_key)
    if len(all_new_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_new_files)} asset data CSV files.")
        pcds, new_asset_df = create_dataframe(spark, bucket_name, all_new_files)

        logger.info(f"Retrieve OLD dataframe. Use following PCDs: {pcds}")
        old_asset_df = get_old_df(
            spark,
            bucket_name,
            bronze_prefix,
            pcds,
        )
        if old_asset_df is None:
            logger.info("Initial load into ASSET BRONZE")
            (
                new_asset_df.write.partitionBy("year", "month")
                .format("delta")
                .mode("overwrite")
                .save(f"gs://{bucket_name}/{bronze_prefix}")
            )
        else:
            logger.info("Upsert data into ASSET BRONZE")
            perform_scd2(spark, old_asset_df, new_asset_df)

    logger.info("End ASSETS BRONZE job.")
    return 0
