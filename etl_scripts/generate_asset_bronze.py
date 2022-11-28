import glob
import logging
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    TimestampType,
)
import csv
from functools import reduce

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
    config["SOURCE_DIR"] = "../data/mini_source"
    config["FILE_KEY"] = "Loan_Data"
    config["SPARK"] = SparkSession.builder.master("local[*]").getOrCreate()
    return config


def get_raw_files(source_dir, file_key):
    """
    Return list of files that satisfy the file_key parameter.
    Works only on local machine so far.

    :param source_dir: folder path where files are stored.
    :param file_key: label for file name that helps with the cherry picking.
    :return all_files: listof desired files from source_dir.
    """
    all_files = [
        f for f in glob.glob(f"{source_dir}/*/*{file_key}*.csv") if "Labeled0M" not in f
    ]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {source_dir}. Exit process!"
        )
        sys.exit(1)
    else:
        return all_files


def create_dataframe(spark, all_files):
    """
    Read files and generate one PySpark DataFrame from them.

    :param spark: SparkSession object.
    :param all_files: list of files to be read to generate the dtaframe.
    :return df: PySpark datafram for loan asset data.
    """
    list_dfs = []
    for csv_f in all_files:
        col_names = []
        content = []
        with open(csv_f, "r") as f:
            csv_id = csv_f.split("/")[-1].split("_")[0]
            csv_date = "-".join(csv_f.split("/")[-1].split("_")[1:4])
            for i, line in enumerate(csv.reader(f)):
                if i == 0:
                    col_names = line
                    col_names[0] = "AS1"
                elif i == 1:
                    continue
                else:
                    content.append(line)
            df = (
                spark.createDataFrame(content, col_names)
                .withColumn("ed_code", F.lit(csv_id))
                .replace("", None)
                .withColumn("ImportDate", F.lit(csv_date))
                .withColumn("year", F.year(F.col("ImportDate")))
                .withColumn("month", F.month(F.col("ImportDate")))
                .withColumn(
                    "valid_from", F.lit(F.current_timestamp()).cast(TimestampType())
                )
                .withColumn("valid_to", F.lit("").cast(TimestampType()))
                .withColumn("iscurrent", F.lit(1).cast("int"))
                .withColumn(
                    "checksum",
                    F.md5(
                        F.concat(
                            F.col("ed_code"),
                            F.col("AS1"),
                            F.col("AS2"),
                            F.col("AS3"),
                            F.col("AS4"),
                            F.col("AS5"),
                            F.col("AS6"),
                            F.col("AS7"),
                        )
                    ),
                )
                .drop("ImportDate")
            )
            list_dfs.append(df)
    if list_dfs == []:
        logger.error("No dataframes were extracted from files. Exit process!")
        sys.exit(1)
    return reduce(DataFrame.union, list_dfs)


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start ASSETS BRONZE job.")
    run_props = set_job_params()
    all_asset_files = get_raw_files(run_props["SOURCE_DIR"], run_props["FILE_KEY"])
    logger.info(f"Retrieved {len(all_asset_files)} asset data files.")
    raw_asset_df = create_dataframe(run_props["SPARK"], all_asset_files)
    (
        raw_asset_df.write.partitionBy("year", "month")
        .mode("append")
        .parquet("../data/output/bronze/assets.parquet")
    )
    return


if __name__ == "__main__":
    main()
