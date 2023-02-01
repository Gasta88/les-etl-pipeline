from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv
import datetime
from functools import reduce

PRIMARY_COLS = {
    "assets": ["AS1", "AS3"],
    "collaterals": ["CS1"],
    "amortisation": ["AS3"],
    "bond_info": ["BS1", "BS2"],
    "deal_details": ["ed_code", "PoolCutOffDate"],
}


def get_old_df(spark, bucket_name, prefix, pcds, ed_code):
    """
    Return BRONZE table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcds: list of PCD from source files (valid only when generating TARGET dataframe).
    :param ed_code: deal code to look up for legacy data.
    :return df: Spark dataframe.
    """
    storage_client = storage.Client(project="dataops-369610")
    check_list = []
    for pcd in pcds:
        part_pcd = pcd.replace("-", "")
        partition_prefix = f"{prefix}/part={ed_code}_{part_pcd}"
        files_in_partition = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=partition_prefix)
        ]
        if len(files_in_partition) > 0:
            check_list.append(pcd)
    if check_list == []:
        return None
    else:
        df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{prefix}")
            .where(f"part={ed_code}_{part_pcd}")
        )
        return df


def create_dataframe(spark, bucket_name, csv_f, data_type):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param csv_f: list of files to be read to generate the dataframe.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return df: PySpark datafram for loan asset data.
    """
    pcds = []
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
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
        checksum_cols = [F.col("ed_code"), F.col("ImportDate")] + [
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
            .withColumn(
                "part",
                F.concat(F.col("ed_code"), F.lit("_"), F.col("year"), F.col("month")),
            )
            .drop("ImportDate", "year", "month")
        )
        # repartition = 4 instances * 8 cores each * 3 for replication factor
        df = df.repartition(96)
    if len(df.head(1)) == 0:
        return None
    return (pcds, df)


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


def get_all_files(bucket_name, data_type, ed_code):
    """
    Return list of files inside the deal to process.

    :param bucket_name: GS bucket where files are stored.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param ed_code: deal code that refers to the data to transform.
    :return file_sets: collection indexed by ed_code of suitable raw files.
    """
    all_files = []
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    csv_f = f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_clean_{data_type}.csv'
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    with open(dest_csv_f, "r") as f:
        for i, line in enumerate(csv.reader(f)):
            if i == 0:
                continue
            else:
                if len(line) == 0:
                    continue
                all_files.append(line[1])
    return all_files
