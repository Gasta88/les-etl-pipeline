import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
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
    config["DATE_COLUMNS"] = [
        "PoolCreationDate",
        "RestructureDates",
        "InterestPaymentDate",
        "PoolCutOffDate",
        "SubmissionTimestamp",
    ]
    config["DEAL_DETAILS_COLUMNS"] = {
        "AssetClassCode": StringType(),
        "AssetClassName": StringType(),
        "CountryCodeOfPrimaryExchange": StringType(),
        "CountryCodeOfSecuritisedAsset": StringType(),
        "CountryCodeOfSpvIncorporation": StringType(),
        "CountryOfPrimaryExchange": StringType(),
        "CountryOfSecuritisedAsset": StringType(),
        "CountryOfSpvIncorporation": StringType(),
        "DataOwner": StringType(),
        "DataProvider": StringType(),
        "DealSize": DoubleType(),
        "DealVersion": IntegerType(),
        "ed_code": StringType(),
        "ISIN": StringType(),
        "IsActiveDeal": BooleanType(),
        "IsECBEligible": BooleanType(),
        "IsMasterTrust": BooleanType(),
        "IsProvisional": BooleanType(),
        "IsRestructured": BooleanType(),
        "PoolCreationDate": DateType(),
        "RestructureDates": DateType(),
        "SpvName": StringType(),
        "ContactInformation": StringType(),
        "CurrentLLPDUploadStatus": StringType(),
        "CurrentPoolBalance": DoubleType(),
        "ECBDataQualityScore": StringType(),
        "HasSuccessfulSubmission": BooleanType(),
        "InterestPaymentDate": DateType(),
        "NumberOfActiveAssets": IntegerType(),
        "OriginalPoolBalance": DoubleType(),
        "PoolCutOffDate": DateType(),
        "RequestId": StringType(),
        "SubmissionTimestamp": DateType(),
        "TotalNumberOfAssets": IntegerType(),
        "TotalResubmissionCount": IntegerType(),
        "TotalNotionalValue": DoubleType(),
        "Vintage": IntegerType(),
    }
    return config


def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan deal details data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            df = df.withColumn(col_name, F.col(col_name).contains("true"))
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
        if data_type == IntegerType():
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
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
        .filter("date_col IS NOT NULL")  # some dates can be NULL
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


def process_deal_info(df):
    """
    Extract deal info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.dropDuplicates()
        # .withColumn("PoolCreationDate", F.unix_timestamp(F.col("PoolCreationDate")))
        # .withColumn("RestructureDates", F.unix_timestamp(F.col("RestructureDates")))
        # .withColumn(
        #     "InterestPaymentDate", F.unix_timestamp(F.col("InterestPaymentDate"))
        # )
        # .withColumn("PoolCutOffDate", F.unix_timestamp(F.col("PoolCutOffDate")))
        # .withColumn(
        #     "SubmissionTimestamp", F.unix_timestamp(F.col("SubmissionTimestamp"))
        # )
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


def generate_deal_details_silver(
    spark, bucket_name, bronze_prefix, silver_prefix, pcds
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze data.
    :param silver_prefix: specific bucket prefix from where to deposit silver data.
    :param pcds: list of PCDs that have been elaborated in the previous Bronze layer.
    :return status: 0 if successful.
    """
    logger.info("Start DEAL DETAILS SILVER job.")
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
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(bronze_df, run_props["DEAL_DETAILS_COLUMNS"])
    # logger.info("Generate time dataframe")
    # date_df = process_dates(cleaned_df, run_props["DATE_COLUMNS"])
    logger.info("Generate deal info dataframe")
    deal_info_df = process_deal_info(cleaned_df)

    logger.info("Write dataframe")
    write_mode = return_write_mode(bucket_name, silver_prefix, pcds)

    # (
    #     date_df.write.format("delta")
    #     .partitionBy("year", "month")
    #     .mode(write_mode)
    #     .save(f"gs://{bucket_name}/{silver_prefix}/date_table")
    # )
    (
        deal_info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/deal_info_table")
    )
    logger.info("End DEAL DETAILS SILVER job.")
    return 0
