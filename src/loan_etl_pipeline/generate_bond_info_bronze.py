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
from delta import *

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_old_df(spark, bucket_name, prefix, pcds):
    """
    Return BRONZE BOND_INFO table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :params pcds: list of PCD from source files (valid only when generating TARGET dataframe).
    :return df: Spark dataframe with the Asset information.
    """
    storage_client = storage.Client(project="dataops-369610")
    check_list = []
    for pcd in pcds:
        year = pcd.split("-")[0]
        month = pcd.split("-")[1]
        partition_prefix = f"{prefix}/year={year}/month={month}"
        check_list.append(
            len(
                [
                    b.name
                    for b in storage_client.list_blobs(
                        bucket_name, prefix=partition_prefix
                    )
                ]
            )
        )
    if sum(check_list) > 0:
        df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{prefix}/bond_info")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(pcds))
            .drop("lookup")
        )
        return df
    else:
        logger.info("No old files for legacy dataframe.")
        return None


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
            csv_date = "-".join(dest_csv_f.split("/")[-1].split("_")[1:4])
            pcds.append(csv_date)
            for i, line in enumerate(csv.reader(f)):
                if i == 0:
                    col_names = line
                    col_names[0] = "BS1"
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
                .withColumn("year", F.year(F.col("BS1")))
                .withColumn("month", F.month(F.col("BS1")))
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
                            F.col("BS1"),
                            F.col("BS2"),
                        )
                    ),
                )
            )
            list_dfs.append(df)
    if list_dfs == []:
        logger.error("No dataframes were extracted from files. Exit process!")
        sys.exit(1)
    return (pcds, reduce(DataFrame.union, list_dfs))


def perform_scd2(spark, source_df, target_df):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param source_df: Pyspark dataframe with data from most recent filset.
    :param target_df: Pyspark dataframe with data from legacy filset.
    :param spark: SparkSession object.
    """
    source_df.createOrReplaceTempView("delta_table_bond_info")
    target_df.createOrReplaceTempView("staged_update")
    update_qry = """
        SELECT NULL AS mergeKey, source.*
        FROM delta_table_bond_info AS target
        INNER JOIN staged_update as source
        ON target.id = source.id
        WHERE target.checksum != source.checksum
        AND target.iscurrent = 1
    UNION
        SELECT id AS mergeKey, *
        FROM staged_update
    """
    # Upsert
    spark.sql(
        f"""
        MERGE INTO delta_table_bond_info tgt
        USING ({update_qry}) src
        ON tgt.id = src.mergeKey
        WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
        THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    return


def generate_bond_info_bronze(spark, bucket_name, bronze_prefix, all_files):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param all_files: list of clean CSV files from EDW.
    :return status: 0 if successful.
    """
    logger.info("Start BOND INFO BRONZE job.")

    logger.info("Create NEW dataframe")
    if len(all_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_files)} bond info data CSV files.")
        pcds, new_bond_info_df = create_dataframe(spark, bucket_name, all_files)

        logger.info(f"Retrieve OLD dataframe. Use following PCDs: {pcds}")
        old_bond_info_df = get_old_df(
            spark,
            bucket_name,
            bronze_prefix,
            pcds,
        )
        if old_bond_info_df is None:
            logger.info("Initial load into BOND INFO BRONZE")
            (
                new_bond_info_df.write.partitionBy("year", "month")
                .format("delta")
                .mode("overwrite")
                .save(f"gs://{bucket_name}/{bronze_prefix}")
            )
        else:
            logger.info("Upsert data into BOND INFO BRONZE")
            perform_scd2(spark, old_bond_info_df, new_bond_info_df)

    logger.info("End BOND INFO BRONZE job.")
    return 0
