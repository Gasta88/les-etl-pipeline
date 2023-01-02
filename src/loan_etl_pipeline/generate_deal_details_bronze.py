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
from utils.bronze_funcs import get_old_df, perform_scd2
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


def generate_deal_details_bronze(
    spark, bucket_name, source_prefix, target_prefix, file_key
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect XML files.
    :param target_prefix: specific bucket prefix from where to save Delta Lake files.
    :param file_key: label for file name that helps with the cherry picking with Deal_Details.
    :return status: 0 is successful.
    """
    # deal Details behaves differently and it has function on its own.
    # This is just a hack to use bronze_funcs.get_old_df and bronze_funcs.perform_scd2
    data_type = "deal_details"
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
            spark, bucket_name, target_prefix, pcds, data_type
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
            perform_scd2(spark, old_deal_details_df, new_deal_details_df, data_type)

    logger.info("End DEAL DETAILS BRONZE job.")
    return 0
