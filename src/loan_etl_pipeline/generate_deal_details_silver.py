import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
from delta import *
from src.loan_etl_pipeline.utils.silver_funcs import (
    cast_to_datatype,
    return_write_mode,
)

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


def process_deal_info(df):
    """
    Extract deal info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.dropDuplicates()
    return new_df


def generate_deal_details_silver(spark, bucket_name, source_prefix, target_prefix):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :return status: 0 if successful.
    """
    logger.info("Start DEAL DETAILS SILVER job.")
    run_props = set_job_params()
    bronze_df = (
        spark.read.format("delta")
        .load(f"gs://{bucket_name}/{source_prefix}")
        .where(F.col("iscurrent") == 1)
        .drop("valid_from", "valid_to", "checksum", "iscurrent")
    )
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(bronze_df, run_props["DEAL_DETAILS_COLUMNS"])
    logger.info("Generate deal info dataframe")
    deal_info_df = process_deal_info(cleaned_df)

    logger.info("Write dataframe")
    write_mode = return_write_mode(bucket_name, target_prefix)

    (
        deal_info_df.write.format("delta")
        .partitionBy("ed_code", "year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{target_prefix}/deal_info_table")
    )
    logger.info("End DEAL DETAILS SILVER job.")
    return 0
