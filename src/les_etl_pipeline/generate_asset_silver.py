import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from google.cloud import storage
from src.les_etl_pipeline.utils.silver_funcs import (
    replace_no_data,
    replace_bool_data,
    cast_to_datatype,
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
        "AL1",
        "AL19",
        "AL20",
        "AL31",
        "AL48",
        "AL50",
        "AL51",
        "AL52",
        "AL69",
        "AL95",
        "AL100",
        "AL101",
        "AL107",
        "AL110",
        "AL113",
        "AL116",
        "AL117",
        "AL120",
        "AL136",
        "AL145",
        "AL148",
    ]
    config["ASSET_COLUMNS"] = {
        "AL1": DateType(),
        "AL2": StringType(),
        "AL3": StringType(),
        "AL4": StringType(),
        "AL5": StringType(),
        "AL6": StringType(),
        "AL7": BooleanType(),
        "AL8": StringType(),
        "AL9": StringType(),
        "AL10": StringType(),
        "AL15": StringType(),
        "AL16": StringType(),
        "AL17": StringType(),
        "AL18": StringType(),
        "AL19": DateType(),
        "AL20": DateType(),
        "AL21": StringType(),
        "AL22": StringType(),
        "AL23": BooleanType(),
        "AL29": BooleanType(),
        "AL30": DoubleType(),
        "AL31": DateType(),
        "AL32": StringType(),
        "AL33": StringType(),
        "AL34": StringType(),
        "AL35": StringType(),
        "AL36": StringType(),
        "AL37": DoubleType(),
        "AL38": DoubleType(),
        "AL39": DoubleType(),
        "AL40": DoubleType(),
        "AL41": DoubleType(),
        "AL42": StringType(),
        "AL43": StringType(),
        "AL44": DoubleType(),
        "AL45": StringType(),
        "AL46": StringType(),
        "AL47": BooleanType(),
        "AL48": DateType(),
        "AL50": DateType(),
        "AL51": DateType(),
        "AL52": DateType(),
        "AL53": DoubleType(),
        "AL54": DoubleType(),
        "AL55": DoubleType(),
        "AL56": DoubleType(),
        "AL57": DoubleType(),
        "AL58": StringType(),
        "AL59": StringType(),
        "AL60": StringType(),
        "AL61": DoubleType(),
        "AL62": DoubleType(),
        "AL63": DoubleType(),
        "AL64": StringType(),
        "AL66": StringType(),
        "AL67": StringType(),
        "AL68": DoubleType(),
        "AL69": DateType(),
        "AL70": StringType(),
        "AL74": DoubleType(),
        "AL75": DoubleType(),
        "AL76": StringType(),
        "AL77": DoubleType(),
        "AL78": DoubleType(),
        "AL79": DoubleType(),
        "AL80": DoubleType(),
        "AL83": DoubleType(),
        "AL84": DoubleType(),
        "AL85": DoubleType(),
        "AL86": DoubleType(),
        "AL87": DoubleType(),
        "AL88": DoubleType(),
        "AL89": DoubleType(),
        "AL90": DoubleType(),
        "AL91": DoubleType(),
        "AL92": DoubleType(),
        "AL93": DoubleType(),
        "AL94": StringType(),
        "AL95": DateType(),
        "AL98": DoubleType(),
        "AL99": DoubleType(),
        "AL100": DateType(),
        "AL101": DateType(),
        "AL102": DoubleType(),
        "AL103": DoubleType(),
        "AL104": BooleanType(),
        "AL105": BooleanType(),
        "AL106": StringType(),
        "AL107": DateType(),
        "AL108": DoubleType(),
        "AL109": DoubleType(),
        "AL110": DateType(),
        "AL111": DoubleType(),
        "AL112": StringType(),
        "AL113": DateType(),
        "AL114": BooleanType(),
        "AL115": DoubleType(),
        "AL116": DateType(),
        "AL117": DateType(),
        "AL118": DoubleType(),
        "AL119": DoubleType(),
        "AL120": DateType(),
        "AL121": DoubleType(),
        "AL122": StringType(),
        "AL123": BooleanType(),
        "AL124": DoubleType(),
        "AL125": DoubleType(),
        "AL126": BooleanType(),
        "AL127": DoubleType(),
        "AL128": DoubleType(),
        "AL129": DoubleType(),
        "AL133": StringType(),
        "AL134": StringType(),
        "AL135": StringType(),
        "AL136": DateType(),
        "AL137": StringType(),
        "AL138": DoubleType(),
        "AL139": StringType(),
        "AL140": BooleanType(),
        "AL141": StringType(),
        "AL142": DoubleType(),
        "AL143": DoubleType(),
        "AL144": StringType(),
        "AL145": DateType(),
        "AL146": DoubleType(),
        "AL147": StringType(),
        "AL148": DateType(),
    }
    return config


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["ed_code", "pcd_year", "pcd_month"]
        + [f"AL{i}" for i in range(1, 5) if f"AL{i}" in df.columns],
        "lease_info": [f"AL{i}" for i in range(5, 50) if f"AL{i}" in df.columns],
        "lease_features": [f"AL{i}" for i in range(50, 74) if f"AL{i}" in df.columns],
        "interest_rate": [f"AL{i}" for i in range(74, 83) if f"AL{i}" in df.columns],
        "financial_info": [f"AL{i}" for i in range(83, 98) if f"AL{i}" in df.columns],
        "performance_info": [
            f"AL{i}" for i in range(98, 133) if f"AL{i}" in df.columns
        ],
        "collateral_info": [
            f"AL{i}" for i in range(133, 154) if f"AL{i}" in df.columns
        ],
    }
    return cols_dict


def process_lease_info(df, cols_dict):
    """
    Extract lease info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(cols_dict["general"] + cols_dict["lease_info"]).dropDuplicates()
    return new_df


def process_lease_features(df, cols_dict):
    """
    Extract lease features dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["lease_features"]
    ).dropDuplicates()
    return new_df


def process_interest_rate(df, cols_dict):
    """
    Extract interest rate dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["interest_rate"]
    ).dropDuplicates()
    return new_df


def process_financial_info(df, cols_dict):
    """
    Extract financial info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["financial_info"]
    ).dropDuplicates()
    return new_df


def process_performance_info(df, cols_dict):
    """
    Extract performance info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["performance_info"]
    ).dropDuplicates()
    return new_df


def process_collateral_info(df, cols_dict):
    """
    Extract collateral info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.select(
        cols_dict["general"] + cols_dict["collateral_info"]
    ).dropDuplicates()
    return new_df


def generate_asset_silver(
    spark, bucket_name, source_prefix, target_prefix, ed_code, ingestion_date
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param ed_code: deal code to process.
    :param ingestion_date: date of the ETL ingestion.
    :return status: 0 if successful.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client(project="dataops-369610")
    all_clean_dumps = [
        b
        for b in storage_client.list_blobs(bucket_name, prefix="clean_dump/assets")
        if f"{ingestion_date}_{ed_code}" in b.name
    ]
    if all_clean_dumps == []:
        logger.info(
            "Could not find clean CSV dump file from ASSETS BRONZE PROFILING BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
    else:
        for clean_dump_csv in all_clean_dumps:
            pcd = "_".join(clean_dump_csv.name.split("/")[-1].split("_")[2:4])
            logger.info(f"Processing data for deal {ed_code}:{pcd}")
            part_pcd = pcd.replace("_0", "").replace("_", "")
            bronze_df = (
                spark.read.format("delta")
                .load(f"gs://{bucket_name}/{source_prefix}")
                .where(F.col("part") == f"{ed_code}_{part_pcd}")
                .filter(F.col("iscurrent") == 1)
                .drop("valid_from", "valid_to", "checksum", "iscurrent")
            )
            assets_columns = get_columns_collection(bronze_df)
            logger.info("Remove ND values.")
            tmp_df1 = replace_no_data(bronze_df)
            logger.info("Replace Y/N with boolean flags.")
            tmp_df2 = replace_bool_data(tmp_df1)
            logger.info("Cast data to correct types.")
            cleaned_df = cast_to_datatype(tmp_df2, run_props["ASSET_COLUMNS"])
            logger.info("Generate lease info dataframe")
            lease_info_df = process_lease_info(cleaned_df, assets_columns)
            logger.info("Generate lease features dataframe")
            lease_features_df = process_lease_features(cleaned_df, assets_columns)
            logger.info("Generate interest rate dataframe")
            interest_rate_df = process_interest_rate(cleaned_df, assets_columns)
            logger.info("Generate financial info dataframe")
            financial_info_df = process_financial_info(cleaned_df, assets_columns)
            logger.info("Generate performace info dataframe")
            performance_info_df = process_performance_info(cleaned_df, assets_columns)
            logger.info("Generate collateral info dataframe")
            collateral_info_df = process_collateral_info(cleaned_df, assets_columns)

            logger.info("Write dataframe")

            (
                lease_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/lease_info_table")
            )
            (
                lease_features_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/lease_features_table")
            )
            (
                financial_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/financial_info_table")
            )
            (
                interest_rate_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/interest_rate_table")
            )
            (
                performance_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/performance_info_table")
            )
            (
                collateral_info_df.write.format("parquet")
                .partitionBy("pcd_year", "pcd_month")
                .mode("append")
                .save(f"gs://{bucket_name}/{target_prefix}/collateral_info_table")
            )
    logger.info("Remove clean dumps.")
    for clean_dump_csv in all_clean_dumps:
        clean_dump_csv.delete()
    logger.info("End ASSET SILVER job.")
    return 0
