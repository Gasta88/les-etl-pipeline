import glob
import logging
import sys
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType
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
    config["SOURCE_DIR"] = None
    config["FILE_KEY"] = "Amortization"
    # TODO: pass number of cores to the SPark application parametrically
    config["SPARK"] = SparkSession.builder.master("local").getOrCreate()
    config["AMORTISATION_COLUMNS"] = {
        "AS3": StringType(),
        "AS150": DoubleType(),
        "AS151": DateType(),
        "AS1348": DoubleType(),
        "AS1349": DateType(),
    }
    for i in range(152, 1348):
        if i % 2 == 0:
            config["BOND_COLUMNS"][f"AS{i}"] = DoubleType()
        else:
            config["BOND_COLUMNS"][f"AS{i}"] = DateType()
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
                    content.append(
                        list(filter(None, [None if x == "" else x for x in line]))
                    )
            df = spark.createDataFrame(content, col_names).withColumn(
                "ID", F.lit(portfolio_id)
            )
            list_dfs.append(df)
    if list_dfs == []:
        logger.error("No dataframes were extracted from files. Exit process!")
        sys.exit(1)
    return reduce(DataFrame.union, list_dfs)


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
    )
    double_df = _melt(
        df,
        id_vars=["AS3"],
        value_vars=double_columns,
        var_name="DOUBLE_COLUMNS",
        value_name="DOUBLE_VALUE",
    )
    new_df = date_df.join(double_df, on="AS3", how="inner")
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start AMORTISATION BRONZE job.")
    run_props = set_job_params()
    all_amortisation_files = get_raw_files(
        run_props["SOURCE_DIR"], run_props["FILE_KEY"]
    )
    logger.info(f"Retrieved {len(all_amortisation_files)} amortisation data files.")
    raw_amortisation_df = create_dataframe(run_props["SPARK"], all_amortisation_files)
    logger.info("Unpivot amortisation dataframe.")
    unpivot_df = unpivot_dataframe(
        raw_amortisation_df, run_props["AMORTISATION_COLUMNS"]
    )
    logger.info("Cast data to correct types.")
    final_df = (
        unpivot_df.withColumn("tmp_col_name", F.to_date(F.col("DATE_VALUE")))
        .drop("DATE_VALUE")
        .withColumnRenamed("tmp_col_name", "DATE_VALUE")
        .withColumn(
            "tmp_col_name", F.round(F.col("DOUBLE_VALUE").cast(DoubleType()), 2)
        )
        .drop("DOUBLE_VALUE")
        .withColumnRenamed("tmp_col_name", "DOUBLE_VALUE")
        .withColumn("year", F.year(F.col("DATE_VALUE")))
        .withColumn("month", F.month(F.col("DATE_VALUE")))
        .withColumn("day", F.dayofmonth(F.col("DATE_VALUE")))
    )
    (
        final_df.format("parquet")
        .partitionBy("year", "month", "day")
        .mode("append")
        .save("../../data/output/bronze/amortisation_bronze.parquet")
    )
    return


if __name__ == "__main__":
    main()
