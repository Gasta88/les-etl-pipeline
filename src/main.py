import argparse
from src.utils.spark_setup import start_spark

# Bronze profile packages
from src.loan_etl_pipeline.profile_bronze_data import profile_bronze_data

# Bronze layer packages
from src.loan_etl_pipeline.generate_bronze_tables import generate_bronze_tables
from src.loan_etl_pipeline.generate_deal_details_bronze import (
    generate_deal_details_bronze,
)

# Silver layer packages
from src.loan_etl_pipeline.generate_asset_silver import generate_asset_silver
from src.loan_etl_pipeline.generate_collateral_silver import generate_collateral_silver
from src.loan_etl_pipeline.generate_bond_info_silver import generate_bond_info_silver
from src.loan_etl_pipeline.generate_amortisation_silver import (
    generate_amortisation_silver,
)
from src.loan_etl_pipeline.generate_deal_details_silver import (
    generate_deal_details_silver,
)

# External sources
from src.loan_etl_pipeline.generate_quandl_silver import generate_quandl_silver


def run(
    raw_bucketname,
    data_bucketname,
    source_prefix,
    target_prefix,
    file_key,
    stage_name,
    pcds,
):
    """
    :param raw_bucketname: GS bucket where original files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param source_prefix: specific bucket prefix from where to collect CSV files.
    :param target_prefix: specific bucket prefix from where to collect bronze old data.
    :param file_key: label for file name that helps with the cherry picking with file type.
    :param stage_name: name of the ETL stage.
    :param pcds: list of PCDs to be elaborated in Silver layer.
    :return: None
    """
    spark = start_spark()
    # ----------------Bronze Quality layer ETL
    if stage_name == "profile_bronze_asset":
        status = profile_bronze_data(
            spark, raw_bucketname, data_bucketname, source_prefix, file_key, "assets"
        )
    if stage_name == "profile_bronze_collateral":
        status = profile_bronze_data(
            spark,
            raw_bucketname,
            data_bucketname,
            source_prefix,
            file_key,
            "collaterals",
        )
    if stage_name == "profile_bronze_bond_info":
        status = profile_bronze_data(
            spark, raw_bucketname, data_bucketname, source_prefix, file_key, "bond_info"
        )
    if stage_name == "profile_bronze_amortisation":
        status = profile_bronze_data(
            spark,
            raw_bucketname,
            data_bucketname,
            source_prefix,
            file_key,
            "amortisation",
        )

    # ----------------Bronze layer ETL
    if stage_name == "bronze_asset":
        status = generate_bronze_tables(
            spark, raw_bucketname, data_bucketname, target_prefix, clean_files, "assets"
        )

    if stage_name == "bronze_collateral":
        status = generate_bronze_tables(
            spark,
            raw_bucketname,
            data_bucketname,
            target_prefix,
            clean_files,
            "collaterals",
        )

    if stage_name == "bronze_bond_info":
        status = generate_bronze_tables(
            spark,
            raw_bucketname,
            data_bucketname,
            target_prefix,
            clean_files,
            "bond_info",
        )

    if stage_name == "bronze_amortisation":
        status = generate_bronze_tables(
            spark,
            raw_bucketname,
            data_bucketname,
            target_prefix,
            clean_files,
            "amortisation",
        )
    if stage_name == "bronze_deal_details":
        status = generate_deal_details_bronze(
            spark,
            raw_bucketname,
            data_bucketname,
            source_prefix,
            target_prefix,
            file_key,
        )

    # ----------------Silver layer ETL
    if stage_name == "silver_asset":
        status = generate_asset_silver(
            spark, data_bucketname, source_prefix, target_prefix, pcds
        )

    if stage_name == "silver_collateral":
        status = generate_collateral_silver(
            spark, data_bucketname, source_prefix, target_prefix, pcds
        )

    if stage_name == "silver_bond_info":
        status = generate_bond_info_silver(
            spark, data_bucketname, source_prefix, target_prefix, pcds
        )

    if stage_name == "silver_amortisation":
        status = generate_amortisation_silver(
            spark, data_bucketname, source_prefix, target_prefix, pcds
        )

    if stage_name == "silver_deal_details":
        status = generate_deal_details_silver(
            spark, data_bucketname, source_prefix, target_prefix, pcds
        )

    # ----------------External sources ETL
    if stage_name == "silver_quandl":
        status = generate_quandl_silver(
            raw_bucketname, data_bucketname, source_prefix, target_prefix
        )


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
        "--pcds",
        type=str,
        dest="pcds",
        required=False,
        help="List of PCDs ingested in the Bronze layer",
    )

    parser.add_argument(
        "--stage-name",
        type=str,
        dest="stage_name",
        required=True,
        help="Name of the ETL stage, like bronze_asset or silver_collateral",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        raw_bucketname=known_args.raw_bucketname,
        data_bucketname=known_args.data_bucketname,
        source_prefix=known_args.source_prefix,
        target_prefix=known_args.target_prefix,
        file_key=known_args.file_key,
        pcds=known_args.pcds,
        stage_name=known_args.stage_name,
    )
