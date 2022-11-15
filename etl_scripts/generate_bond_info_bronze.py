import glob
import logging
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, BooleanType, DoubleType
import csv
from functools import reduce
import os

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
    config["SOURCE_DIR"] = os.environ["SOURCE_DIR"]
    config["FILE_KEY"] = "Bond_Info"
    config["SPARK"] = SparkSession.builder.master(
        f'local[{int(os.environ["WORKERS"])}]'
    ).getOrCreate()
    config["BOND_COLUMNS"] = {
        "BS1": DateType(),
        "BS2": StringType(),
        "BS3": DoubleType(),
        "BS4": DoubleType(),
        "BS5": BooleanType(),
        "BS6": StringType(),
        "BS11": DoubleType(),
        "BS12": BooleanType(),
        "BS13": DoubleType(),
        "BS19": StringType(),
        "BS20": StringType(),
    }
    return config


def get_raw_files(source_dir, file_key):
    """
    Return list of files that satisfy the file_key parameter.
    Works only on local machine so far.

    :param source_dir: folder path where files are stored.
    :param file_key: label for file name that helps with the cherry picking.
    :return all_files: listof desired files from source_dir.
    """
    all_files = [f for f in glob.glob(f"{source_dir}/*/{file_key}/*.csv")]
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
            portfolio_id = csv_f.split("/")[-2]
            for i, line in enumerate(csv.reader(f)):
                if i == 0:
                    col_names = line
                elif i == 1:
                    continue
                else:
                    content.append(line)
            df = spark.createDataFrame(content, col_names).withColumn(
                "ID", F.lit(portfolio_id)
            )
            list_dfs.append(df)
    if list_dfs == []:
        logger.error("No dataframes were extracted from files. Exit process!")
        sys.exit(1)
    return reduce(DataFrame.union, list_dfs)


def replace_no_data(df):
    """
    Replace ND values inside the dataframe
    TODO: ND are associated with labels that explain why the vaue is missing.
          Should handle this information better in future releases.
    :param df: Spark dataframe with loan asset data.
    :return df: Spark dataframe without ND values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name).startswith("ND"), None).otherwise(F.col(col_name)),
        )
    return df


def replace_bool_data(df):
    """
    Replace Y/N with boolean flags in the dataframe.

    :param df: Spark dataframe with loan asset data.
    :return df: Spark dataframe without Y/N values.
    """
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(F.col(col_name) == "Y", "True")
            .when(F.col(col_name) == "N", "False")
            .otherwise(F.col(col_name)),
        )
    return df


def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan asset data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            df = (
                df.withColumn("tmp_col_name", F.col(col_name).contains("True"))
                .drop(col_name)
                .withColumnRenamed("tmp_col_name", col_name)
            )
        if data_type == DateType():
            df = (
                df.withColumn("tmp_col_name", F.to_date(F.col(col_name)))
                .drop(col_name)
                .withColumnRenamed("tmp_col_name", col_name)
            )
        if data_type == DoubleType():
            df = (
                df.withColumn(
                    "tmp_col_name", F.round(F.col(col_name).cast(DoubleType()), 2)
                )
                .drop(col_name)
                .withColumnRenamed("tmp_col_name", col_name)
            )
    df = (
        df.withColumn("year", F.year(F.col("BS1")))
        .withColumn("month", F.month(F.col("BS1")))
        .withColumn("day", F.dayofmonth(F.col("BS1")))
    )
    return df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start BOND INFO BRONZE job.")
    run_props = set_job_params()
    all_bond_info_files = get_raw_files(run_props["SOURCE_DIR"], run_props["FILE_KEY"])
    logger.info(f"Retrieved {len(all_bond_info_files)} bond info data files.")
    raw_bond_info_df = create_dataframe(run_props["SPARK"], all_bond_info_files)
    logger.info("Remove ND values.")
    tmp_df1 = replace_no_data(raw_bond_info_df)
    logger.info("Replace Y/N with boolean flags.")
    tmp_df2 = replace_bool_data(tmp_df1)
    logger.info("Cast data to correct types.")
    final_df = cast_to_datatype(tmp_df2, run_props["BOND_COLUMNS"])
    (
        final_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../dataoutput/bronze/bond_info.parquet")
    )
    return


if __name__ == "__main__":
    main()
