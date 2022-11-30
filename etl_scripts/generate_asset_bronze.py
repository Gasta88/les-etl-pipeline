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
    config["FILE_KEY"] = "Loan_Data"
    config["SPARK"] = SparkSession.builder.master("local[*]").getOrCreate()
    return config


def get_files(bucket_name, file_key, pcds=None):
    """
    Return list of files that satisfy the file_key parameter.
    Works only on local machine so far.

    :param bucket_name: GS bucket where files are stored.
    :param file_key: label for file name that helps with the cherry picking.
    :params pcds: list of PCD from source files (valid only when generating TARGET dataframe).
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project="dataops-369610")
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name)
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
        if pcds is None:
            # These are for SOURCE dataframe
            return all_files
        # These are for TARGET dataframe
        target_files = []
        for f in all_files:
            pcd = "_".join(f.split("/")[-1].split("_")[1:4])
            if pcd in pcds:
                target_files.append(f)
        return target_files


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
    logger.info("Create SOURCE dataframe")
    all_source_files = get_files(run_props["BUCKET_NAME"], run_props["FILE_KEY"])
    logger.info(f"Retrieved {len(all_source_files)} asset data source files.")
    pcds, source_asset_df = create_dataframe(run_props["SPARK"], all_source_files)
    logger.info("Create TARGET dataframe")
    all_target_files = get_files(
        run_props["BUCKET_NAME"], run_props["FILE_KEY"], pcds=pcds
    )
    logger.info(f"Retrieved {len(all_target_files)} asset data source files.")
    if len(all_target_files) > 0:
        logger.info("Legacy data to update")
        _, target_asset_df = create_dataframe(run_props["SPARK"], all_target_files)
        perform_scd2(run_props["SPARK"], source_asset_df, target_asset_df)
    else:
        logger.info("No legacy data to update")
        (
            source_asset_df.write.partitionBy("year", "month")
            .mode("append")
            .parquet(f'gs://{run_props["BUCKET_NAME"]}/bronze/assets.parquet')
        )
    return


if __name__ == "__main__":
    main()
