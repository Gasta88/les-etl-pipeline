import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType
from delta import *
from google.cloud import storage
import datetime
from src.loan_etl_pipeline.utils.silver_funcs import return_write_mode, get_all_pcds

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
    config["AMORTISATION_MANDATORY_COLUMNS"] = {
        "AS3": StringType(),
    }
    config["AMORTISATION_OPTIONAL_COLUMNS"] = {
        "AS3": StringType(),
    }
    for i in range(150, 390):
        if i % 2 == 0:
            config["AMORTISATION_MANDATORY_COLUMNS"][f"AS{i}"] = DoubleType()
        else:
            config["AMORTISATION_MANDATORY_COLUMNS"][f"AS{i}"] = DateType()
    for i in range(390, 1350):
        if i % 2 == 0:
            config["AMORTISATION_OPTIONAL_COLUMNS"][f"AS{i}"] = DoubleType()
        else:
            config["AMORTISATION_OPTIONAL_COLUMNS"][f"AS{i}"] = DateType()
    return config


def _melt(df, id_vars, value_vars, var_name="FEATURE_NAME", value_name="FEATURE_VALUE"):
    """Convert DataFrame from wide to long format."""
    # Ref:https://stackoverflow.com/a/41673644
    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name))
            for c in value_vars
        )
    )
    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))
    cols = id_vars + [
        F.col("_vars_and_vals")[x].cast("string").alias(x)
        for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)


def unpivot_dataframe(df, columns):
    """
    Convert dataframe from wide to long table.

    :param df: raw Spark dataframe.
    :param columns: data columns with respective datatype.
    :return new_df: unpivot Spark dataframe.
    """
    df = df.withColumn(
        "AS3", F.concat_ws("_", F.col("AS3"), F.monotonically_increasing_id())
    )
    date_columns = [
        k for k, v in columns.items() if v == DateType() and k in df.columns
    ]
    double_columns = [
        k for k, v in columns.items() if v == DoubleType() and k in df.columns
    ]

    date_df = _melt(
        df,
        id_vars=["AS3"],
        value_vars=date_columns,
        var_name="DATE_COLUMNS",
        value_name="DATE_VALUE",
    ).filter(F.col("DATE_VALUE").isNotNull())
    double_df = _melt(
        df,
        id_vars=["AS3"],
        value_vars=double_columns,
        var_name="DOUBLE_COLUMNS",
        value_name="DOUBLE_VALUE",
    )
    scd2_df = df.select("AS3", "part")
    new_df = (
        date_df.join(double_df, on="AS3", how="inner")
        .join(scd2_df, on="AS3", how="inner")
        .withColumn("AS3", F.split(F.col("AS3"), "_").getItem(0))
    )
    return new_df


def generate_amortisation_silver(
    spark, bucket_name, source_prefix, target_prefix, ed_code
):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param source_prefix: specific bucket prefix from where to collect bronze data.
    :param target_prefix: specific bucket prefix from where to deposit silver data.
    :param ed_code: deal code to process.
    :return status: 0 if successful.
    """
    logger.info("Start AMORTISATION SILVER job.")
    run_props = set_job_params()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    clean_dump_csv = bucket.blob(
        f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_clean_amortisation.csv'
    )
    if not (clean_dump_csv.exists()):
        logger.info(
            f"Could not find clean CSV dump file from AMORTISATION BRONZE PROFILING job. Workflow stopped!"
        )
        sys.exit(1)
    else:
        pcds = get_all_pcds(bucket_name, "amortisation", ed_code)
        logger.info(f"Processing data for deal {ed_code}")
        for pcd in pcds:
            part_pcd = pcd.replace("-", "")
            logger.info(f"Processing {pcd} data from bronze to silver. ")
            bronze_df = (
                spark.read.format("delta")
                .load(f"gs://{bucket_name}/{source_prefix}")
                .where(F.col("part") == f"{ed_code}_{part_pcd}")
                .filter(F.col("iscurrent") == 1)
                .drop("valid_from", "valid_to", "checksum", "iscurrent")
                .repartition(96)
            )
            logger.info("Cast data to correct types.")
            tmp_df1 = unpivot_dataframe(
                bronze_df, run_props["AMORTISATION_MANDATORY_COLUMNS"]
            )
            tmp_df2 = unpivot_dataframe(
                bronze_df, run_props["AMORTISATION_OPTIONAL_COLUMNS"]
            )
            mandatory_info_df = tmp_df1.withColumn(
                "DATE_VALUE", F.to_date(F.col("DATE_VALUE"))
            ).withColumn(
                "DOUBLE_VALUE", F.round(F.col("DOUBLE_VALUE").cast(DoubleType()), 2)
            )
            optional_info_df = tmp_df2.withColumn(
                "DATE_VALUE", F.to_date(F.col("DATE_VALUE"))
            ).withColumn(
                "DOUBLE_VALUE", F.round(F.col("DOUBLE_VALUE").cast(DoubleType()), 2)
            )

            logger.info("Write mandatory dataframe")
            write_mode = return_write_mode(bucket_name, target_prefix, pcds)
            (
                mandatory_info_df.write.format("delta")
                .partitionBy("part")
                .mode(write_mode)
                .save(f"gs://{bucket_name}/{target_prefix}/mandatory_info_table")
            )

            logger.info("Write optional dataframe")
            write_mode = return_write_mode(bucket_name, target_prefix, pcds)
            (
                optional_info_df.write.format("delta")
                .partitionBy("part")
                .mode(write_mode)
                .save(f"gs://{bucket_name}/{target_prefix}/optional_info_table")
            )
    logger.info("End AMORTISATION SILVER job.")
    return 0
