import logging
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType

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
    config["SOURCE_DIR"] = "../data/output/bronze"
    config["AMORTISATION_COLUMNS"] = {
        "AS3": StringType(),
        "AS150": DoubleType(),
        "AS151": DateType(),
        "AS1348": DoubleType(),
        "AS1349": DateType(),
    }
    for i in range(152, 1348):
        if i % 2 == 0:
            config["AMORTISATION_COLUMNS"][f"AS{i}"] = DoubleType()
        else:
            config["AMORTISATION_COLUMNS"][f"AS{i}"] = DateType()
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
    scd2_df = df.select("AS3", "ed_code", "year", "month")
    new_df = (
        date_df.join(double_df, on="AS3", how="inner")
        .join(scd2_df, on="AS3", how="inner")
        .withColumn("AS3", F.split(F.col("AS3"), "_").getItem(0))
    )
    return new_df


def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan asset data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
    return df


def process_dates(df):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select("DATE_VALUE")
        .withColumnRenamed("DATE_VALUE", "date_col")
        .dropDuplicates()
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


def process_info(df):
    """
    Extract amortisation values dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :return new_df: silver type Spark dataframe.
    """
    new_df = df.withColumn("DATE_VALUE", F.unix_timestamp(F.col("DATE_VALUE")))
    return new_df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start AMORTISATION SILVER job.")
    run_props = set_job_params()
    bronze_df = (
        run_props["SPARK"]
        .read.parquet(f'{run_props["SOURCE_DIR"]}/amortisation.parquet')
        .filter("iscurrent == 1")
        .drop("valid_from", "valid_to", "checksum", "iscurrent")
    )
    logger.info("Cast data to correct types.")
    tmp_df1 = unpivot_dataframe(bronze_df, run_props["AMORTISATION_COLUMNS"])
    cleaned_df = tmp_df1.withColumn(
        "DATE_VALUE", F.to_date(F.col("DATE_VALUE"))
    ).withColumn("DOUBLE_VALUE", F.round(F.col("DOUBLE_VALUE").cast(DoubleType()), 2))
    logger.info("Generate time dataframe")
    date_df = process_dates(cleaned_df)
    logger.info("Generate info dataframe")
    info_df = process_info(cleaned_df)
    logger.info("Write dataframe")

    (
        date_df.write.mode("overwrite").parquet(
            "../data/output/SME/silver/amortisation/date_table.parquet"
        )
    )
    (
        info_df.write.partitionBy("year", "month")
        .mode("overwrite")
        .parquet("../data/output/SME/silver/amortisation/info_table.parquet")
    )

    return


if __name__ == "__main__":
    main()
