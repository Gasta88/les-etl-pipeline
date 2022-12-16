import re
import logging
import sys
from lxml import objectify
import pandas as pd
from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
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


def get_raw_files(bucket_name, prefix, file_key):
    """
    Return list of XML files that satisfy the file_key parameter from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project="dataops-369610")
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name, prefix=prefix)
        if (b.name.endswith(".xml")) and (file_key in b.name)
    ]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {bucket_name}. Exit process!"
        )
        sys.exit(1)
    else:
        return all_files


def get_old_df(spark, bucket_name, prefix, pcds):
    """
    Return BRONZE DEAL DETAILS table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :params pcds: list of PCD from source files (valid only when generating TARGET dataframe).
    :return df: Spark dataframe with the Deal Details information.
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
        truncated_pcds = ["-".join(pcd.split("-")[:2]) for pcd in pcds]
        df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{prefix}/deal_details")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(truncated_pcds))
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
    :return df: PySpark dataframe.
    """
    list_dfs = []
    pcds = []
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    for xml_f in all_files:
        blob = bucket.blob(xml_f)
        dest_xml_f = f'/tmp/{xml_f.split("/")[-1]}'
        blob.download_to_filename(dest_xml_f)
        xml_data = objectify.parse(dest_xml_f)  # Parse XML data
        root = xml_data.getroot()  # Root element

        data = []
        cols = []
        for i in range(
            len(
                root.getchildren()[1]
                .getchildren()[0]
                .getchildren()[1]
                .getchildren()[0]
                .getchildren()
            )
        ):
            child = (
                root.getchildren()[1]
                .getchildren()[0]
                .getchildren()[1]
                .getchildren()[0]
                .getchildren()[i]
            )
            tag = re.sub(r"{[^}]*}", "", child.tag)
            if tag == "ISIN":
                # is array
                data.append(";".join(map(str, child.getchildren())))
                cols.append(tag)
            elif tag in ["Country", "DealVisibleToOrg", "DealVisibleToUser"]:
                # usually null values
                continue
            elif tag == "Submissions":
                # get Submission metadata
                submission_children = child.getchildren()[0].getchildren()
                for submission_child in submission_children:
                    tag = re.sub(r"{[^}]*}", "", submission_child.tag)
                    if tag in ["MetricData", "IsProvisional", "IsRestructured"]:
                        # Not needed
                        continue
                    data.append(submission_child.text)
                    cols.append(tag)
            else:
                data.append(child.text)
                cols.append(tag)
        # Convert None in empty strings if only one file is loaded. This should help to create a Spark dataframe
        data = ["" if v is None else v for v in data]
        df = pd.DataFrame(data).T  # Create DataFrame and transpose it
        df.columns = cols  # Update column names
        pcds.append(df["PoolCutOffDate"].values[0].split("T")[0])
        list_dfs.append(df)
    # Conver from Pandas to Spark dataframe and add SCD-2 columns
    spark_df = (
        spark.createDataFrame(pd.concat(list_dfs, ignore_index=True))
        .withColumnRenamed("EDCode", "ed_code")
        .replace("", None)
        .withColumn("year", F.year(F.col("PoolCutOffDate")))
        .withColumn("month", F.month(F.col("PoolCutOffDate")))
        .withColumn("valid_from", F.lit(F.current_timestamp()).cast(TimestampType()))
        .withColumn("valid_to", F.lit("").cast(TimestampType()))
        .withColumn("iscurrent", F.lit(1).cast("int"))
        .withColumn(
            "checksum",
            F.md5(
                F.concat(
                    F.col("ed_code"),
                    F.col("PoolCutOffDate"),
                )
            ),
        )
    )
    return (pcds, spark_df)


def perform_scd2(spark, source_df, target_df):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param source_df: Pyspark dataframe with data from most recent filset.
    :param target_df: Pyspark dataframe with data from legacy filset.
    :param spark: SparkSession object.
    """
    source_df.createOrReplaceTempView("delta_table_deal_details")
    target_df.createOrReplaceTempView("staged_update")
    update_qry = """
        SELECT NULL AS mergeKey, source.*
        FROM delta_table_deal_details AS target
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
        MERGE INTO delta_table_deal_details tgt
        USING ({update_qry}) src
        ON tgt.id = src.mergeKey
        WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
        THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
        WHEN NOT MATCHED THEN INSERT *
    """
    )


def generate_deal_details_bronze(
    spark, bucket_name, source_prefix, target_prefix, file_key
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect XML files.
    :param target_prefix: specific bucket prefix from where to save Delta Lake files.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :return status: 0 is successful.
    """
    logger.info("Start DEAL DETAILS BRONZE job.")
    all_xml_files = get_raw_files(bucket_name, source_prefix, file_key)
    logger.info("Create NEW dataframe")
    if len(all_xml_files) == 0:
        logger.warning("No new XML files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_xml_files)} deal details data XML files.")
        pcds, new_deal_details_df = create_dataframe(spark, bucket_name, all_xml_files)

        logger.info(f"Retrieve OLD dataframe. Use following PCDs: {pcds}")
        old_deal_details_df = get_old_df(
            spark,
            bucket_name,
            target_prefix,
            pcds,
        )
        if old_deal_details_df is None:
            logger.info("Initial load into DEAL DETAILS BRONZE")
            (
                new_deal_details_df.write.partitionBy("year", "month")
                .format("delta")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}")
            )
        else:
            logger.info("Upsert data into DEAL DETAILS BRONZE")
            perform_scd2(spark, old_deal_details_df, new_deal_details_df)

    logger.info("End DEAL DETAILS BRONZE job.")
    return 0
