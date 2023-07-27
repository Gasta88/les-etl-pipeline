import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType, IntegerType
from src.les_etl_pipeline.utils.silver_funcs import cast_to_datatype

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
        "part": StringType(),
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


def generate_deal_details_silver(
    spark, bucket_name, source_prefix, target_prefix, tries=5
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param tries: number of tries before giving up
    :return status: 0 if successful.
    """
    logger.info("Start DEAL DETAILS SILVER job.")
    run_props = set_job_params()
    bronze_df = (
        spark.read.format("delta")
        .load(f"gs://{bucket_name}/{source_prefix}")
        .filter(F.col("iscurrent") == 1)
        .drop("valid_from", "valid_to", "checksum", "iscurrent")
    )
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(bronze_df, run_props["DEAL_DETAILS_COLUMNS"])
    logger.info("Generate deal info dataframe")
    deal_info_df = cleaned_df.dropDuplicates()

    logger.info("Write dataframe")
    for i in range(tries):
        try:
            (
                deal_info_df.write.format("parquet")
                .partitionBy("part")
                # Only one ed_code+part per job, so it's best to overwrite than append
                .mode("overwrite")
                .save(f"gs://{bucket_name}/{target_prefix}/deal_info_table")
            )
        except Exception as e:
            logger.error(f"Writing exception: {e}.Try again.")
            continue
        break
    logger.info("End DEAL DETAILS SILVER job.")
    return 0
