import argparse
from src.utils.spark_setup import start_spark
from src.loan_etl_pipeline.profile_asset_bronze import profile_asset_bronze
from src.loan_etl_pipeline.generate_asset_bronze import generate_asset_bronze
from src.loan_etl_pipeline.profile_collateral_bronze import profile_collateral_bronze
from src.loan_etl_pipeline.generate_collateral_bronze import generate_collateral_bronze
from src.loan_etl_pipeline.profile_bond_info_bronze import profile_bond_info_bronze
from src.loan_etl_pipeline.generate_bond_info_bronze import generate_bond_info_bronze
from src.loan_etl_pipeline.profile_amortisation_bronze import (
    profile_amortisation_bronze,
)
from src.loan_etl_pipeline.generate_amortisation_bronze import (
    generate_amortisation_bronze,
)


def run(bucket_name, upload_prefix, bronze_prefix, file_key, stage_name):
    """
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect CSV files.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :param stage_name: name of the ETL stage.
    :return: None
    """
    spark = start_spark()
    if stage_name == "bronze_asset":
        clean_files = profile_asset_bronze(spark, bucket_name, upload_prefix, file_key)
        status = generate_asset_bronze(spark, bucket_name, bronze_prefix, clean_files)

    if stage_name == "bronze_collateral":
        clean_files = profile_collateral_bronze(
            spark, bucket_name, upload_prefix, file_key
        )
        status = generate_collateral_bronze(
            spark, bucket_name, bronze_prefix, clean_files
        )

    if stage_name == "bronze_bond_info":
        clean_files = profile_bond_info_bronze(
            spark, bucket_name, upload_prefix, file_key
        )
        status = generate_bond_info_bronze(
            spark, bucket_name, bronze_prefix, clean_files
        )

    if stage_name == "bronze_amortisation":
        clean_files = profile_amortisation_bronze(
            spark, bucket_name, upload_prefix, file_key
        )
        status = generate_amortisation_bronze(
            spark, bucket_name, bronze_prefix, clean_files
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--upload-prefix",
        type=str,
        dest="upload_prefix",
        required=True,
        help="Prefix on GCS where new CSV are allocated",
    )

    parser.add_argument(
        "--bronze-prefix",
        type=str,
        dest="bronze_prefix",
        required=True,
        help="Prefix on GCS where bronze tableis stored",
    )

    parser.add_argument(
        "--file-key",
        type=str,
        dest="file_key",
        required=True,
        help="File key to filter out the CSV to load.",
    )

    parser.add_argument(
        "--bucket-name",
        type=str,
        dest="bucket_name",
        required=True,
        help="Name of the GCS Bucket -- DO NOT add the gs:// Prefix",
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
        bucket_name=known_args.bucket_name,
        upload_prefix=known_args.upload_prefix,
        bronze_prefix=known_args.bronze_prefix,
        file_key=known_args.file_key,
        stage_name=known_args.stage_name,
    )
