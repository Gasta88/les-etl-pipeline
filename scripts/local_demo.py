"""Run the bronze -> silver pipeline locally on synthetic fixtures.

No GCS, BigQuery or Dataproc access is required. Requires Python 3.9/3.10 and
Java 11+. Output is written to ``local_data/`` (gitignored).
"""

import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.les_etl_pipeline.generate_asset_silver import get_columns_collection
from src.les_etl_pipeline.generate_deal_details_bronze import parse_deal_details_xml
from src.les_etl_pipeline.utils.bronze_funcs import parse_csv_file, perform_scd2
from src.les_etl_pipeline.utils.silver_funcs import (
    ASSET_COLUMNS,
    cast_to_datatype,
    profile_data,
)

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
OUTPUT = ROOT / "local_data"
SCD2_COLUMNS = ["valid_from", "valid_to", "checksum", "iscurrent"]


def start_spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("les-etl-local-demo")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def read_csv(spark, filename, data_type):
    with open(FIXTURES / filename, "r", encoding="utf-8") as handle:
        df, errors = parse_csv_file(spark, handle, data_type, f"edw_data/{filename}")
    if errors:
        raise RuntimeError(errors)
    return df


def main():
    if OUTPUT.exists():
        shutil.rmtree(OUTPUT)
    OUTPUT.mkdir(parents=True)

    spark = start_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        assets = read_csv(spark, "LESSES0001_2023_08_01_000000.csv", "assets")
        bond_info = read_csv(
            spark, "LESSES0001_2023_08_01_bond_000000.csv", "bond_info"
        )

        assets.write.format("delta").mode("overwrite").save(
            str(OUTPUT / "bronze" / "assets")
        )
        bond_info.write.format("delta").mode("overwrite").save(
            str(OUTPUT / "bronze" / "bond_info")
        )

        _, deal_details = parse_deal_details_xml(
            spark, str(FIXTURES / "deal_details_LESSES0001.xml")
        )
        merged = perform_scd2(
            spark, deal_details.limit(0), deal_details, "deal_details"
        )
        merged.write.format("delta").mode("overwrite").save(
            str(OUTPUT / "bronze" / "deal_details")
        )

        bronze_assets = assets.drop(*SCD2_COLUMNS)
        good_df, bad_df = profile_data(spark, bronze_assets, "assets")
        cleaned_df = cast_to_datatype(good_df, ASSET_COLUMNS)
        primary_columns, secondary_columns = get_columns_collection(bronze_assets)

        for table_name, columns in secondary_columns.items():
            if not columns:
                continue
            (
                cleaned_df.na.drop(how="all", subset=columns)
                .select(primary_columns + columns)
                .dropDuplicates()
                .write.parquet(str(OUTPUT / "silver" / "assets" / table_name))
            )

        print("Local demo complete")
        print(f"  bronze assets rows : {assets.count()}")
        print(f"  bronze bond rows   : {bond_info.count()}")
        print(f"  bronze deal rows   : {merged.count()}")
        print(f"  silver good rows   : {good_df.count()}")
        print(f"  silver bad rows    : {bad_df.count()}")
        print(f"  output             : {OUTPUT}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
