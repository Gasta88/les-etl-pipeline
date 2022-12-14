import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from delta import *
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
    config["DATE_COLUMNS"] = ["BS1", "BS27", "BS28", "BS38", "BS39"]
    config["BOND_COLUMNS"] = {
        "BS1": DateType(),
        "BS2": StringType(),
        "BS3": DoubleType(),
        "BS4": DoubleType(),
        "BS5": BooleanType(),
        "BS6": StringType(),
        "BS11": DoubleType(),
        "BS12": BooleanType(),
        "BS13": DoubleType(),
        "BS19": StringType(),
        "BS20": StringType(),
        "BS25": StringType(),
        "BS26": StringType(),
        "BS27": DateType(),
        "BS28": DateType(),
        "BS29": StringType(),
        "BS30": DoubleType(),
        "BS31": DoubleType(),
        "BS32": StringType(),
        "BS33": DoubleType(),
        "BS34": DoubleType(),
        "BS35": DoubleType(),
        "BS36": DoubleType(),
        "BS37": DoubleType(),
        "BS38": DateType(),
        "BS39": DateType(),
    }
    return config


def replace_no_data(df):
    """
    Replace ND values inside the dataframe
    TODO: ND are associated with labels that explain why the vaue is missing.
          Should handle this information better in future releases.
    :param df: Spark dataframe with loan asset data.
    :return df: Spark dataframe without ND values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).startswith("ND"), None).otherwise(F.col(col_name)),
        )
    return df


def replace_bool_data(df):
    """
    Replace Y/N with boolean flags in the dataframe.

    :param df: Spark dataframe with loan asset data.
    :return df: Spark dataframe without Y/N values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "Y", "True")
            .when(F.col(col_name) == "N", "False")
            .otherwise(F.col(col_name)),
        )
    return df


def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan asset data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            df = df.withColumn(col_name, F.col(col_name).contains("True"))
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
    return df


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "bond_info": ["ed_code", "year", "month"]
        + [f"BS{i}" for i in range(1, 11) if f"BS{i}" in df.columns],
        "collateral_info": ["ed_code", "year", "month"]
        + [f"BS{i}" for i in range(11, 19) if f"BS{i}" in df.columns],
        "contact_info": ["ed_code", "year", "month"]
        + [f"BS{i}" for i in range(19, 25) if f"BS{i}" in df.columns],
        "tranche_info": ["ed_code", "year", "month"]
        + [f"BS{i}" for i in range(25, 40) if f"BS{i}" in df.columns],
    }
    return cols_dict


def process_dates(df, date_cols_list):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param date_cols_list: list of date columns.
    :return new_df: silver type Spark dataframe.
    """
    date_cols = [c for c in date_cols_list if c in df.columns]

    new_df = (
        df.select(F.explode(F.array(date_cols)).alias("date_col"))
        .dropDuplicates()
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


def process_bond_info(df, cols_dict):
    """
    Extract bond info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["bond_info"])
        .dropDuplicates()
        .withColumn("BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd")))
    )
    return new_df


def process_collateral_info(df, cols_dict):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(["BS1", "BS2"] + cols_dict["collateral_info"])
        .dropDuplicates()
        .withColumn("BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd")))
    )
    return new_df


def process_contact_info(df, cols_dict):
    """
    Extract contact info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(["BS1", "BS2"] + cols_dict["contact_info"])
        .dropDuplicates()
        .withColumn("BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd")))
    )
    return new_df


def process_tranche_info(df, cols_dict):
    """
    Extract tranche info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(["BS1", "BS2"] + cols_dict["tranche_info"])
        .dropDuplicates()
        .withColumn("BS1", F.unix_timestamp(F.to_timestamp(F.col("BS1"), "yyyy-MM-dd")))
        .withColumn(
            "BS27", F.unix_timestamp(F.to_timestamp(F.col("BS27"), "yyyy-MM-dd"))
        )
        .withColumn(
            "BS28", F.unix_timestamp(F.to_timestamp(F.col("BS28"), "yyyy-MM-dd"))
        )
        .withColumn(
            "BS38", F.unix_timestamp(F.to_timestamp(F.col("BS38"), "yyyy-MM-dd"))
        )
        .withColumn(
            "BS39", F.unix_timestamp(F.to_timestamp(F.col("BS39"), "yyyy-MM-dd"))
        )
    )
    return new_df


def return_write_mode(bucket_name, prefix, pcds):
    """
    If PCDs are already presents as partition return "overwrite", otherwise "append" mode.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcds: list of PCDs that have been elaborated in the previous Silver layer.
    :return write_mode: label that express how data should be written on storage.
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
        return "overwrite"
    else:
        return "append"


def generate_bond_info_silver(spark, bucket_name, bronze_prefix, silver_prefix, pcds):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze data.
    :param silver_prefix: specific bucket prefix from where to deposit silver data.
    :param pcds: list of PCDs that have been elaborated in the previous Bronze layer.
    :return status: 0 if successful.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    if pcds == "":
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )
    else:
        truncated_pcds = ["-".join(pcd.split("-")[:2]) for pcd in pcds.split(",")]
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(truncated_pcds))
            .drop("valid_from", "valid_to", "checksum", "iscurrent", "lookup")
        )
    logger.info("Remove ND values.")
    tmp_df1 = replace_no_data(bronze_df)
    logger.info("Replace Y/N with boolean flags.")
    tmp_df2 = replace_bool_data(tmp_df1)
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(tmp_df2, run_props["BOND_COLUMNS"])
    bond_info_columns = get_columns_collection(cleaned_df)
    logger.info("Generate time dataframe")
    date_df = process_dates(cleaned_df, run_props["DATE_COLUMNS"])
    logger.info("Generate bond info dataframe")
    info_df = process_bond_info(cleaned_df, bond_info_columns)
    logger.info("Generate collateral info dataframe")
    collateral_df = process_collateral_info(cleaned_df, bond_info_columns)
    logger.info("Generate contact info dataframe")
    contact_df = process_contact_info(cleaned_df, bond_info_columns)
    logger.info("Generate tranche info dataframe")
    tranche_df = process_tranche_info(cleaned_df, bond_info_columns)

    logger.info("Write dataframe")
    write_mode = return_write_mode(bucket_name, silver_prefix, pcds)

    (
        date_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/date_table")
    )
    (
        info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/info_table")
    )
    (
        collateral_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/collaterals_table")
    )
    (
        contact_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/contact_table")
    )
    (
        tranche_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/tranche_info_table")
    )
    logger.info("End BOND INFO SILVER job.")
    return 0
