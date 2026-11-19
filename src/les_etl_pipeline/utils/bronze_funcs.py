from google.cloud import storage
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType, StructType, StructField, StringType
import csv
from io import StringIO
from unidecode import unidecode
from delta.tables import *
from src.les_etl_pipeline.utils.config import get_project_id


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
    storage_client = storage.Client(project=get_project_id())
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


def parse_csv_file(spark, csv_file_obj, data_type, csv_f):
    """
    Parse a CSV file object into a bronze-level Spark DataFrame.

    Kept separate from GCS download so the parsing logic can be exercised
    offline against a local file or fixture.

    :param spark: SparkSession object.
    :param csv_file_obj: open text file object with the raw CSV.
    :param data_type: type of data to handle, ex: assets, bond_info.
    :param csv_f: original object name, used to derive ``pcd`` and ``ed_code``.
    :return df: PySpark dataframe for the raw data.
    :return errors: newline-joined parsing errors, or None.
    """
    errors = []
    clean_f = _correct_file_coding(csv_file_obj)
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
            except Exception:
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


def create_dataframe(spark, bucket_name, csv_f, data_type):
    """
    Download a CSV file from GCS and parse it into a bronze-level DataFrame.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param csv_f: CSV file to be read and profile.
    :param data_type: type of data to handle, ex: assets, bond_info.
    :return df: PySpark dataframe for the raw data.
    :return errors: newline-joined parsing errors, or None.
    """
    storage_client = storage.Client(project=get_project_id())
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    with open(dest_csv_f, "r", errors="ignore") as f:
        return parse_csv_file(spark, f, data_type, csv_f)


def perform_scd2(spark, old_df, new_df, data_type):
    """
    Merge a new fileset into the existing bronze data using SCD-2 semantics.

    Rows whose business key already exists with a different checksum are closed
    (``iscurrent`` set to 0 and ``valid_to`` set to the new ``valid_from``) and
    the new version is inserted. Rows with a new key are inserted. Unchanged
    rows are left untouched.

    This function is pure with respect to storage: it returns the merged
    DataFrame and never writes it. The caller decides where to persist it.

    :param spark: SparkSession object (kept for call-site symmetry).
    :param old_df: DataFrame with the current bronze table.
    :param new_df: DataFrame with the most recent fileset.
    :param data_type: type of data to handle, ex: assets, bond_info, deal_details.
    :return merged_df: DataFrame with the SCD-2 merged data.
    """
    key_cols = PRIMARY_COLS[data_type]

    old_current = old_df.filter(F.col("iscurrent") == 1)
    old_history = old_df.filter(F.col("iscurrent") == 0)

    old_keys = old_current.select(*key_cols).distinct()

    changed_keys = (
        old_current.select(*key_cols, F.col("checksum").alias("old_checksum"))
        .join(
            new_df.select(*key_cols, F.col("checksum").alias("new_checksum")),
            on=key_cols,
            how="inner",
        )
        .filter(F.col("old_checksum") != F.col("new_checksum"))
        .select(*key_cols)
        .distinct()
    )

    new_valid_from = new_df.select(
        *key_cols, F.col("valid_from").alias("new_valid_from")
    )
    closed = (
        old_current.alias("old")
        .join(changed_keys.alias("changed"), on=key_cols, how="inner")
        .join(new_valid_from.alias("new"), on=key_cols, how="inner")
        .select(*[F.col(f"old.{col}") for col in old_df.columns], "new.new_valid_from")
        .withColumn("iscurrent", F.lit(0))
        .withColumn("valid_to", F.col("new_valid_from"))
        .drop("new_valid_from")
    )

    kept = old_current.join(changed_keys, on=key_cols, how="left_anti")

    unchanged_keys = old_keys.join(changed_keys, on=key_cols, how="left_anti")
    inserted = new_df.join(unchanged_keys, on=key_cols, how="left_anti")

    return (
        old_history.unionByName(closed)
        .unionByName(kept)
        .unionByName(inserted)
    )


def get_csv_files(bucket_name, prefix, file_key, data_type):
    """
    Return list of source files that satisfy the file_key parameter from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project=get_project_id())
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
    storage_client = storage.Client(project=get_project_id())
    all_clean_dumps = [
        b.name
        for b in storage_client.list_blobs(
            bucket_name, prefix=f"clean_dump/{data_type}"
        )
        if b.name.startswith(ingestion_date)
    ]
    return all_clean_dumps
