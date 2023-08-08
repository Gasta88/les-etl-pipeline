from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, StructType, StructField, StringType
import csv
from io import StringIO
from unidecode import unidecode
from delta.tables import *


PRIMARY_COLS = {
    "assets": ["AL1", "AL2"],
    "bond_info": ["BL1", "BL2"],
    "deal_details": ["ed_code", "PoolCutOffDate"],
}

INITIAL_COL = {
    "assets": "AL1",
    "bond_info": "BL1",
}


def _correct_file_coding(raw_file_obj):
    """
    Check the content of the CSV for encoding issues and fix them.


    :param raw_file_obj: file handler from the raw CSV.
    :return clean_file_obj: file object without encoding issues.
    """
    content = raw_file_obj.read()
    fixed_content = content.replace("\ufeff", "").replace("\0", "").replace("\x00", "")
    clean_file_obj = StringIO(unidecode(fixed_content))
    return clean_file_obj


def get_old_table(spark, bucket_name, prefix, pcd, ed_code):
    """
    Return BRONZE table, but only the partitions from the specified pcds.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcd: PCD in part format to retrieve old data.
    :param ed_code: deal code to look up for legacy data.
    :return df: Spark dataframe.
    """
    storage_client = storage.Client(project="dataops-369610")
    pcd = pcd.replace("-", "")
    partition_prefix = f"{prefix}/part={ed_code}_{pcd}"
    files_in_partition = [
        b.name for b in storage_client.list_blobs(bucket_name, prefix=partition_prefix)
    ]
    return (
        spark.read.format("delta")
        .load(f"gs://{bucket_name}/{prefix}")
        .where(F.col("part") == f"{ed_code}_{pcd}")
        if len(files_in_partition) > 0
        else None
    )


def create_dataframe(spark, bucket_name, csv_f, data_type):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param csv_f: CSV file to be read and profile.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return df: PySpark datafram for loan asset data.
    """
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    errors = []
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    with open(dest_csv_f, "rU", errors="ignore") as f:
        clean_f = _correct_file_coding(f)
        reader = csv.reader(clean_f, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        content = []
        for i, line in enumerate(reader):
            if data_type == "amortisation":
                # Just check that AS3 is present instead of the hundreds of columns that the file has.
                curr_line = line[:121]
            else:
                curr_line = line
            if i == 0:
                col_names = curr_line
                col_names[0] = INITIAL_COL[data_type]
            elif i == 1:
                continue
            elif not curr_line:
                continue
            else:
                try:
                    clean_line = [
                        None
                        if (el.strip() == "")
                        or (el.startswith("ND"))
                        or (el == "No Collateral")
                        else el.lower().strip()
                        for el in curr_line
                    ]
                    record = {
                        col_names[i]: clean_line[i] for i in range(len(clean_line))
                    }
                    record["filename"] = csv_f
                    record["pcd"] = "-".join(csv_f.split("/")[-1].split("_")[1:4])
                    record["ed_code"] = csv_f.split("/")[-1].split("_")[0]
                    content.append(record)
                except Exception as e:
                    errors.append(f"Error in line {i}: {line}")
                    continue
    if not content:
        return (None, "ERROR: empty dataframe")
    checksum_cols = [F.col("ed_code"), F.col("pcd")] + [
        F.col(col_name) for col_name in PRIMARY_COLS[data_type]
    ]
    # Cast all columns as string in Bronze Layer so that we can store the data for the first round.
    schema = StructType(
        [
            StructField(col_name, StringType(), True)
            for col_name in col_names + ["filename", "pcd", "ed_code"]
        ]
    )
    df = (
        spark.createDataFrame(content, schema=schema)
        .withColumn("valid_from", F.lit(F.current_timestamp()).cast(TimestampType()))
        .withColumn("valid_to", F.lit("").cast(TimestampType()))
        .withColumn("iscurrent", F.lit(1).cast("int"))
        .withColumn(
            "checksum",
            F.md5(F.concat(*checksum_cols)),
        )
        .withColumn(
            "part",
            F.concat(F.col("ed_code"), F.lit("_"), F.col("pcd")),
        )
        .withColumn("part", F.regexp_replace("part", "-", ""))
    )
    errors = "\n".join(errors) if errors else None
    return (df, errors)


def perform_scd2(source_df, target_table, data_type):
    """
    Perform SCD-2 to update legacy data at the bronze level tables.

    :param source_df: Pyspark dataframe with data from most recent fileset.
    :param target_table: Delta Lake table with data from old fileset.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return new_target_df: PySpark dataframe with new data.
    """
    target_df = target_table.toDF()
    new_data_to_insert = (
        source_df.alias("updates")
        .join(target_df.alias("target"), PRIMARY_COLS[data_type])
        .where("target.iscurrent=1 AND updates.checksum <> target.checksum")
    )

    merge_cols = [
        f"{col} AS mergeKey_{i}" for i, col in enumerate(PRIMARY_COLS[data_type])
    ]
    null_merge_cols = [
        f"NULL AS mergeKey_{i}" for i, _ in enumerate(PRIMARY_COLS[data_type])
    ]
    stage_updates = new_data_to_insert.selectExpr(*null_merge_cols, "updates.*").union(
        source_df.selectExpr(*merge_cols, "*")
    )
    upsert_join_condition = " AND ".join(
        [
            f"target.{col} = mergeKey_{i}"
            for i, col in enumerate(PRIMARY_COLS[data_type])
        ]
    )
    target_table.alias("target").merge(
        stage_updates.alias("staged_updates"), upsert_join_condition
    ).whenMatchedUpdate(
        condition="target.iscurrent = true AND target.checksum <> staged_updates.checksum",
        set={  # Set current to false and endDate to source's effective date.
            "iscurrent": 0,
            "valid_to": "staged_updates.valid_from",
        },
    ).whenNotMatchedInsert(
        # TODO: SET ALL VALUE TO BE WRITTEN
        values={
            "customerid": "staged_updates.customerId",
            "name": "staged_updates.name",
            "address": "staged_updates.address",
            "current": "true",
            "effectiveDate": "staged_updates.effectiveDate",  # Set current to true along with the new address and its effective date.
            "endDate": "null",
        }
    ).execute()
    new_target_df = target_table.toDF()
    return new_target_df


def get_csv_files(bucket_name, prefix, file_key, data_type):
    """
    Return list of source files that satisfy the file_key parameter from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project="dataops-369610")
    all_files = [
        b.name
        for b in storage_client.list_blobs(bucket_name, prefix=prefix)
        if b.name.endswith(".csv")
        and file_key in b.name
        and (data_type != "assets" or "Labeled" not in b.name)
    ]
    return all_files


def get_clean_dumps(bucket_name, data_type, ingestion_date):
    """
    Returns a list of all clean dumps in a given bucket for a specific data type and ingestion date.

    :param bucket_name: The name of the bucket.
    :param data_type: The type of data.
    :param ingestion_date: The date of ingestion.
    :return all_clean_dumps: A list of clean dump names.
    """
    storage_client = storage.Client(project="dataops-369610")
    all_clean_dumps = [
        b.name
        for b in storage_client.list_blobs(
            bucket_name, prefix=f"clean_dump/{data_type}"
        )
        if b.name.startswith(ingestion_date)
    ]
    return all_clean_dumps
