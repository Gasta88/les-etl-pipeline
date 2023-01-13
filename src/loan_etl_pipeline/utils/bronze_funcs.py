from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    TimestampType,
)
import csv
from functools import reduce

PRIMARY_COLS = {
    "assets": ["AS1", "AS3"],
    "collaterals": ["CS1"],
    "amortisation": ["AS3"],
    "bond_info": ["BS1", "BS2"],
}


def get_old_df(spark, bucket_name, prefix, pcds, data_type):
    """
    Return BRONZE table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcds: list of PCD from source files (valid only when generating TARGET dataframe).
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return df: Spark dataframe.
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
            # .load(f"gs://{bucket_name}/{prefix}/{data_type}")
            .load(f"gs://{bucket_name}/{prefix}")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(truncated_pcds))
            .drop("lookup")
        )
        return df
    else:
        # logger.info("No old files for legacy dataframe.")
        return None


def create_dataframe(spark, bucket_name, all_files, data_type):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param all_files: list of files to be read to generate the dataframe.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
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
                    col_names[0] = PRIMARY_COLS[data_type][0]
                elif i == 1:
                    continue
                else:
                    if len(line) == 0:
                        continue
                    content.append(line)
            # Prep array of primary cols to use in checksum column
            checksum_cols = [F.col("ed_code")] + [
                F.col(col_name) for col_name in PRIMARY_COLS[data_type]
            ]
            df = (
                spark.createDataFrame(content, col_names)
                .withColumn("ed_code", F.lit(csv_id))
                .replace("", None)
                .withColumn("ImportDate", F.lit(csv_date))
                .withColumn("year", F.year(F.col("ImportDate")))
                .withColumn("month", F.month(F.col("ImportDate")))
                .withColumn(
                    "valid_from", F.lit(F.current_timestamp()).cast(TimestampType())
                )
                .withColumn("valid_to", F.lit("").cast(TimestampType()))
                .withColumn("iscurrent", F.lit(1).cast("int"))
                .withColumn(
                    "checksum",
                    F.md5(F.concat(*checksum_cols)),
                )
                .drop("ImportDate")
            )
            list_dfs.append(df)
    if list_dfs == []:
        return None
    return (pcds, reduce(DataFrame.union, list_dfs))


def perform_scd2(spark, source_df, target_df, data_type):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param spark: SparkSession object.
    :param source_df: Pyspark dataframe with data from most recent filset.
    :param target_df: Pyspark dataframe with data from legacy filset.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    """
    source_df.createOrReplaceTempView(f"delta_table_{data_type}")
    target_df.createOrReplaceTempView("staged_update")
    update_join_condition = " AND ".join(
        [f"target.{col} = source.{col}" for col in PRIMARY_COLS[data_type]]
    )
    update_col_selection = " ,".join(
        [f"{col} AS mergeKey_{i}" for i, col in enumerate(PRIMARY_COLS[data_type])]
    )
    update_qry = f"""
        SELECT NULL AS mergeKey, source.*
        FROM delta_table_{data_type} AS target
        INNER JOIN staged_update as source
        ON ({update_join_condition})
        WHERE target.checksum != source.checksum
        AND target.iscurrent = 1
    UNION
        SELECT {update_col_selection}, *
        FROM staged_update
    """
    # Upsert
    upsert_join_condition = " AND ".join(
        [
            f"target.{col} = source.mergeKey_{i}"
            for i, col in enumerate(PRIMARY_COLS[data_type])
        ]
    )
    spark.sql(
        f"""
        MERGE INTO delta_table_{data_type} tgt
        USING ({update_qry}) src
        ON (({upsert_join_condition}))
        WHEN MATCHED AND src.checksum != tgt.checksum AND tgt.iscurrent = 1 
        THEN UPDATE SET valid_to = src.valid_from, iscurrent = 0
        WHEN NOT MATCHED THEN INSERT *
    """
    )
    return


def get_file_sets(all_files):
    """
    Return collection of files indexed by ed_code to process.

    :param all_files: all suitable files to process from the profiled raw layer.
    :return file_sets: collection indexed by ed_code of suitable raw files.
    """
    file_sets = {}
    for ed_code in list(set([file_name.split("/")[-2] for file_name in all_files])):
        file_sets[ed_code] = [
            file_name for file_name in all_files if ed_code in file_name
        ]
    return file_sets
