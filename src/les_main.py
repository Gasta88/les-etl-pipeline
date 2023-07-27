import argparse

# Bronze layer packages
from src.les_etl_pipeline.generate_bronze_tables import generate_bronze_tables
from src.les_etl_pipeline.generate_deal_details_bronze import (
    generate_deal_details_bronze,
)

# Silver layer packages
from src.les_etl_pipeline.generate_asset_silver import generate_asset_silver
from src.les_etl_pipeline.generate_collateral_silver import generate_collateral_silver
from src.les_etl_pipeline.generate_bond_info_silver import generate_bond_info_silver
from src.les_etl_pipeline.generate_amortisation_silver import (
    generate_amortisation_silver,
)
from src.les_etl_pipeline.generate_deal_details_silver import (
    generate_deal_details_silver,
)
from pyspark.sql import SparkSession
from delta import *


def start_spark():
    """
    Create Spark application using Delta Lake dependencies.

    :param app_name: Name of the Spark App
    :return: SparkSession
    """

    spark = (
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-core:2.1.0")
        .config(
            "spark.delta.logStore.gs.impl",
            "io.delta.storage.GCSLogStore",
        )
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.parquet.enable.summary-metadata", "false")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.hive.metastorePartitionPruning", "true")
        .getOrCreate()
    )
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    return spark


def run(
    raw_bucketname,
    data_bucketname,
    source_prefix,
    target_prefix,
    ed_code,
    file_key,
    stage_name,
    ingestion_date,
):
    """
    :param raw_bucketname: GS bucket where original files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect CSV files.
    :param target_prefix: specific bucket prefix from where to collect bronze old data.
    :param ed_code: deal code used for Silver layer ETL.
    :param file_key: label for file name that helps with the cherry picking with file type.
    :param stage_name: name of the ETL stage.
    :param ingestion_date: date of the ETL ingestion.
    :return: None
    """
    spark = start_spark()

    bronze_tables = {"bronze_asset": "assets", "bronze_bond_info": "bond_info"}

    silver_tables = {
        "silver_asset": generate_asset_silver,
        "silver_bond_info": generate_bond_info_silver,
    }

    if stage_name in bronze_tables:
        status = generate_bronze_tables(
            spark,
            raw_bucketname,
            data_bucketname,
            source_prefix,
            target_prefix,
            bronze_tables[stage_name],
            file_key,
            ingestion_date,
        )
    elif stage_name == "bronze_deal_details":
        status = generate_deal_details_bronze(
            spark,
            raw_bucketname,
            data_bucketname,
            source_prefix,
            target_prefix,
            file_key,
        )
    elif stage_name == "silver_deal_details":
        status = generate_deal_details_silver(
            spark,
            data_bucketname,
            source_prefix,
            target_prefix,
        )
    elif stage_name in silver_tables:
        status = silver_tables[stage_name](
            spark,
            data_bucketname,
            source_prefix,
            target_prefix,
            ed_code,
            ingestion_date,
        )
    else:
        raise ValueError(f"Invalid stage_name: {stage_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source-prefix",
        type=str,
        dest="source_prefix",
        required=True,
        help="Prefix on GCS where new source data is allocated",
    )

    parser.add_argument(
        "--target-prefix",
        type=str,
        dest="target_prefix",
        required=False,
        help="Prefix on GCS where layer tables are stored",
    )

    parser.add_argument(
        "--file-key",
        type=str,
        dest="file_key",
        required=False,
        help="File key to filter out the CSV to load.",
    )

    parser.add_argument(
        "--data-bucketname",
        type=str,
        dest="data_bucketname",
        required=True,
        help="Name of the GCS Bucket where transformed data can be found -- DO NOT add the gs:// Prefix",
    )

    parser.add_argument(
        "--raw-bucketname",
        type=str,
        dest="raw_bucketname",
        required=True,
        help="Name of the GCS Bucket where original data can be found -- DO NOT add the gs:// Prefix",
    )

    parser.add_argument(
        "--ed-code",
        type=str,
        dest="ed_code",
        required=False,
        help="Deal code used in Silver layer ETL",
    )

    parser.add_argument(
        "--stage-name",
        type=str,
        dest="stage_name",
        required=True,
        help="Name of the ETL stage, like bronze_asset or silver_collateral",
    )

    parser.add_argument(
        "--ingestion-date",
        type=str,
        dest="ingestion_date",
        required=False,
        help="Date of the ETL ingestion",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        raw_bucketname=known_args.raw_bucketname,
        data_bucketname=known_args.data_bucketname,
        source_prefix=known_args.source_prefix,
        target_prefix=known_args.target_prefix,
        ed_code=known_args.ed_code,
        file_key=known_args.file_key,
        stage_name=known_args.stage_name,
        ingestion_date=known_args.ingestion_date,
    )
