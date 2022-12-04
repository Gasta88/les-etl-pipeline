import logging
import sys
from pyspark.sql import SparkSession, DataFrame
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


def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["BUCKET_NAME"] = "fgasta_test"
    config["UPLOAD_PREFIX"] = "mini_source/"
    config["BRONZE_PREFIX"] = "SME/bronze/assets"
    config["FILE_KEY"] = "Loan_Data"
    config["SPARK"] = (
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-core:1.0.1")
        .getOrCreate()
    )
    return config


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


def get_source_df(spark, bucket_name, prefix, pcds):
    """
    Return BRONZE ASSET table, but only the partitions from the specified pcds.

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
            .load(f"gs://{bucket_name}/{prefix}/assets")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(pcds))
            .drop("lookup")
        )
        return df
    else:
        return None


def create_dataframe(spark, all_files):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param all_files: list of files to be read to generate the dtaframe.
    :return df: PySpark datafram for loan asset data.
    """
    list_dfs = []
    pcds = []
    for csv_f in all_files:
        col_names = []
        content = []
        with open(csv_f, "r") as f:
            csv_id = csv_f.split("/")[-1].split("_")[0]
            pcds.append("_".join(csv_f.split("/")[-1].split("_")[1:4]))
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


def perform_scd2(spark, source_df, target_df):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param source_df: Pyspark dataframe with data from most recent filset.
    :param target_df: Pyspark dataframe with data from legacy filset.
    :param spark: SparkSession object.
    """
    source_df.createOrReplaceTempView("delta_table_asset")
    target_df.createOrReplaceTempView("staged_update")
    update_qry = """
        SELECT NULL AS mergeKey, source.*
        FROM delta_table_asset AS target
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
        MERGE INTO delta_table_asset tgt
        USING ({update_qry}) src
        ON tgt.id = src.mergeKey
        WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
        THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    return


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start ASSETS BRONZE job.")
    run_props = set_job_params()

    logger.info("Create TARGET dataframe")
    all_target_files = get_csv_files(
        run_props["BUCKET_NAME"],
        run_props["UPLOAD_PREFIX"],
        run_props["FILE_KEY"],
    )
    if len(all_target_files) == 0:
        logger.warning("No new CSV files to retrieve. Workflow stopped!")
        sys.exit(1)
    else:
        logger.info(f"Retrieved {len(all_target_files)} asset data CSV files.")
        pcds, target_asset_df = create_dataframe(run_props["SPARK"], all_target_files)

        logger.info("Retrieve SOURCE dataframe")
        source_asset_df = get_source_df(
            run_props["SPARK"],
            run_props["BUCKET_NAME"],
            run_props["BRONZE_PREFIX"],
            pcds,
        )
        if source_asset_df is None:
            logger.info("Initial load into ASSET BRONZE")
            (
                target_asset_df.write.partitionBy("year", "month")
                .format("delta")
                .mode("overwrite")
                .save(f'gs://{run_props["BUCKET_NAME"]}/{run_props["BRONZE_PREFIX"]}')
            )
        else:
            logger.info("Upsert data into ASSET BRONZE")
            perform_scd2(run_props["SPARK"], source_asset_df, target_asset_df)

    logger.info("End ASSETS BRONZE job.")
    return


if __name__ == "__main__":
    main()
