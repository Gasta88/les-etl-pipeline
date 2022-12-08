# Import Python libraries.
import argparse
from src.utils.spark_setup import start_spark
from src.loan_etl_pipeline.generate_asset_bronze import generate_asset_bronze

# from src.spark_serverless_repo_exemplar.save_to_bq import save_file_to_bq


def run(bucket_name, upload_prefix, bronze_prefix, file_key):
    """
    :param bucket_name: GS bucket where files are stored.
    :param upload_prefix: specific bucket prefix from where to collect CSV files.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param file_key: label for file name that helps with the cherry picking with Asset.
    :return: None
    """
    spark = start_spark()
    status = generate_asset_bronze(
        spark, bucket_name, upload_prefix, bronze_prefix, file_key
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

    # parser.add_argument(
    #     "--project", type=str, dest="project_id", required=True, help="GCP Project ID"
    # )

    parser.add_argument(
        "--bucket-name",
        type=str,
        dest="bucket_name",
        required=True,
        help="Name of the GCS Bucket -- DO NOT add the gs:// Prefix",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        bucket_name=known_args.bucket_name,
        upload_prefix=known_args.upload_prefix,
        bronze_prefix=known_args.bronze_prefix,
        file_key=known_args.file_key,
    )
