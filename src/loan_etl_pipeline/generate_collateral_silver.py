import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
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
    config["DATE_COLUMNS"] = ["CS11", "CS12", "CS22"]
    config["COLLATERAL_COLUMNS"] = {
        "CS1": StringType(),
        "CS2": StringType(),
        "CS3": StringType(),
        "CS4": DoubleType(),
        "CS5": DoubleType(),
        "CS6": StringType(),
        "CS7": BooleanType(),
        "CS8": BooleanType(),
        "CS9": BooleanType(),
        "CS10": DoubleType(),
        "CS11": DateType(),
        "CS12": DateType(),
        "CS13": StringType(),
        "CS14": StringType(),
        "CS15": DoubleType(),
        "CS16": StringType(),
        "CS17": StringType(),
        "CS18": DoubleType(),
        "CS19": DoubleType(),
        "CS20": StringType(),
        "CS21": DoubleType(),
        "CS22": DateType(),
        "CS23": StringType(),
        "CS24": StringType(),
        "CS25": StringType(),
        "CS26": StringType(),
        "CS27": StringType(),
        "CS28": DoubleType(),
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


def process_collateral_info(df):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.dropDuplicates()
        .withColumn("CS11", F.unix_timestamp(F.to_timestamp(F.col("CS11"), "yyyy-MM")))
        .withColumn("CS12", F.unix_timestamp(F.to_timestamp(F.col("CS12"), "yyyy-MM")))
        .withColumn("CS22", F.unix_timestamp(F.to_timestamp(F.col("CS22"), "yyyy-MM")))
    )
    return new_df


def generate_collateral_silver(spark, bucket_name, bronze_prefix, silver_prefix, pcds):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze data.
    :param silver_prefix: specific bucket prefix from where to deposit silver data.
    :param pcds: list of PCDs that have been elaborated in the previous Bronze layer.
    :return status: 0 if successful.
    """
    logger.info("Start COLLATERAL SILVER job.")
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
    cleaned_df = cast_to_datatype(tmp_df2, run_props["COLLATERAL_COLUMNS"])
    logger.info("Generate collateral info dataframe")
    info_df = process_collateral_info(cleaned_df)
    logger.info("Generate time dataframe")
    date_df = process_dates(cleaned_df, run_props["DATE_COLUMNS"])

    logger.info("Write dataframe")

    (
        info_df.write.format("delta")
        .mode("overwrite")
        .save(f"gs://{bucket_name}/{silver_prefix}/info_table")
    )
    (
        date_df.write.format("delta")
        .mode("overwrite")
        .save(f"gs://{bucket_name}/{silver_prefix}/date_table")
    )
