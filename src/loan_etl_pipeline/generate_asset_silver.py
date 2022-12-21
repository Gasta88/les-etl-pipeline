import logging
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StringType, DoubleType, BooleanType
from delta import *
from google.cloud import storage

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
    config["DATE_COLUMNS"] = [
        "AS1",
        "AS19",
        "AS20",
        "AS31",
        "AS50",
        "AS51",
        "AS67",
        "AS70",
        "AS71",
        "AS87",
        "AS91",
        "AS112",
        "AS124",
        "AS127",
        "AS130",
        "AS133",
        "AS134",
        "AS137",
    ]
    config["ASSET_COLUMNS"] = {
        "AS1": DateType(),
        "AS2": StringType(),
        "AS3": StringType(),
        "AS4": StringType(),
        "AS5": StringType(),
        "AS6": StringType(),
        "AS7": StringType(),
        "AS8": StringType(),
        "AS15": StringType(),
        "AS16": StringType(),
        "AS17": StringType(),
        "AS18": StringType(),
        "AS19": DateType(),
        "AS20": DateType(),
        "AS21": StringType(),
        "AS22": StringType(),
        "AS23": BooleanType(),
        "AS24": StringType(),
        "AS25": StringType(),
        "AS26": StringType(),
        "AS27": DoubleType(),
        "AS28": DoubleType(),
        "AS29": BooleanType(),
        "AS30": DoubleType(),
        "AS31": DateType(),
        "AS32": StringType(),
        "AS33": StringType(),
        "AS34": StringType(),
        "AS35": StringType(),
        "AS36": StringType(),
        "AS37": DoubleType(),
        "AS38": DoubleType(),
        "AS39": DoubleType(),
        "AS40": DoubleType(),
        "AS41": DoubleType(),
        "AS42": StringType(),
        "AS43": StringType(),
        "AS44": DoubleType(),
        "AS45": StringType(),
        "AS50": DateType(),
        "AS51": DateType(),
        "AS52": StringType(),
        "AS53": BooleanType(),
        "AS54": DoubleType(),
        "AS55": DoubleType(),
        "AS56": DoubleType(),
        "AS57": StringType(),
        "AS58": StringType(),
        "AS59": StringType(),
        "AS60": DoubleType(),
        "AS61": DoubleType(),
        "AS62": StringType(),
        "AS63": DoubleType(),
        "AS64": DoubleType(),
        "AS65": StringType(),
        "AS66": DoubleType(),
        "AS67": DateType(),
        "AS68": StringType(),
        "AS69": DoubleType(),
        "AS70": DateType(),
        "AS71": DateType(),
        "AS80": DoubleType(),
        "AS81": DoubleType(),
        "AS82": DoubleType(),
        "AS83": StringType(),
        "AS84": StringType(),
        "AS85": DoubleType(),
        "AS86": DoubleType(),
        "AS87": DateType(),
        "AS88": DoubleType(),
        "AS89": StringType(),
        "AS90": DoubleType(),
        "AS91": DateType(),
        "AS92": StringType(),
        "AS93": DoubleType(),
        "AS94": StringType(),
        "AS100": DoubleType(),
        "AS101": DoubleType(),
        "AS102": DoubleType(),
        "AS103": DoubleType(),
        "AS104": DoubleType(),
        "AS105": DoubleType(),
        "AS106": DoubleType(),
        "AS107": DoubleType(),
        "AS108": DoubleType(),
        "AS109": DoubleType(),
        "AS110": DoubleType(),
        "AS111": StringType(),
        "AS112": DateType(),
        "AS115": DoubleType(),
        "AS116": DoubleType(),
        "AS117": DoubleType(),
        "AS118": DoubleType(),
        "AS119": DoubleType(),
        "AS120": DoubleType(),
        "AS121": BooleanType(),
        "AS122": BooleanType(),
        "AS123": StringType(),
        "AS124": DateType(),
        "AS125": DoubleType(),
        "AS126": DoubleType(),
        "AS127": DateType(),
        "AS128": DoubleType(),
        "AS129": StringType(),
        "AS130": DateType(),
        "AS131": BooleanType(),
        "AS132": DoubleType(),
        "AS133": DateType(),
        "AS134": DateType(),
        "AS135": DoubleType(),
        "AS136": DoubleType(),
        "AS137": DateType(),
        "AS138": DoubleType(),
    }
    return config


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
            df = df.withColumn(col_name, F.col(col_name).contains("True"))
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
    return df


def get_columns_collection(df):
    """
    Get collection of dataframe columns divided by topic.

    :param df: Asset bronze Spark dataframe.
    :return cols_dict: collection of columns labelled by topic.
    """
    cols_dict = {
        "general": ["ed_code", "year", "month"]
        + [f"AS{i}" for i in range(1, 15) if f"AS{i}" in df.columns],
        "obligor_info": [f"AS{i}" for i in range(15, 50) if f"AS{i}" in df.columns],
        "loan_info": [f"AS{i}" for i in range(50, 80) if f"AS{i}" in df.columns],
        "interest_rate": [f"AS{i}" for i in range(80, 100) if f"AS{i}" in df.columns],
        "financial_info": [f"AS{i}" for i in range(100, 115) if f"AS{i}" in df.columns],
        "performance_info": [
            f"AS{i}" for i in range(115, 146) if f"AS{i}" in df.columns
        ],
    }
    return cols_dict


def process_dates(df, date_cols_list):
    """
    Extract dates dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param date_cols_list: list of date columns.
    :return new_df: silver type Spark dataframe.
    """
    date_cols = [c for c in date_cols_list if c in df.columns]

    new_df = (
        df.select(F.explode(F.array(date_cols)).alias("date_col"))
        .dropDuplicates()
        .withColumn("unix_date", F.unix_timestamp(F.col("date_col")))
        .withColumn("year", F.year(F.col("date_col")))
        .withColumn("month", F.month(F.col("date_col")))
        .withColumn("quarter", F.quarter(F.col("date_col")))
        .withColumn("WoY", F.weekofyear(F.col("date_col")))
        .withColumn("day", F.dayofmonth(F.col("date_col")))
    )
    return new_df


def process_obligor_info(df, cols_dict):
    """
    Extract obligor info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["obligor_info"]).dropDuplicates()
        # .withColumn("AS1", F.unix_timestamp(F.col("AS1")))
        # .withColumn("AS19", F.unix_timestamp(F.col("AS19")))
        # .withColumn("AS20", F.unix_timestamp(F.col("AS20")))
        # .withColumn("AS31", F.unix_timestamp(F.col("AS31")))
    )
    return new_df


def process_loan_info(df, cols_dict):
    """
    Extract loan info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["loan_info"]).dropDuplicates()
        # .withColumn("AS1", F.unix_timestamp(F.col("AS1")))
        # .withColumn("AS50", F.unix_timestamp(F.col("AS50")))
        # .withColumn("AS51", F.unix_timestamp(F.col("AS51")))
        # .withColumn("AS67", F.unix_timestamp(F.col("AS67")))
        # .withColumn("AS70", F.unix_timestamp(F.col("AS70")))
        # .withColumn("AS71", F.unix_timestamp(F.col("AS71")))
    )
    return new_df


def process_interest_rate(df, cols_dict):
    """
    Extract interest rate dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["interest_rate"]).dropDuplicates()
        # .withColumn("AS1", F.unix_timestamp(F.col("AS1")))
        # .withColumn("AS87", F.unix_timestamp(F.col("AS87")))
        # .withColumn("AS91", F.unix_timestamp(F.col("AS91")))
    )
    return new_df


def process_financial_info(df, cols_dict):
    """
    Extract financial info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["financial_info"]).dropDuplicates()
        # .withColumn("AS1", F.unix_timestamp(F.col("AS1")))
        # .withColumn("AS112", F.unix_timestamp(F.col("AS112")))
    )
    return new_df


def process_performance_info(df, cols_dict):
    """
    Extract performance info dimension from bronze Spark dataframe.

    :param df: Spark bronze dataframe.
    :param cols_dict: collection of columns labelled by their topic.
    :return new_df: silver type Spark dataframe.
    """
    new_df = (
        df.select(cols_dict["general"] + cols_dict["performance_info"]).dropDuplicates()
        # .withColumn("AS1", F.unix_timestamp(F.col("AS1")))
        # .withColumn("AS124", F.unix_timestamp(F.col("AS124")))
        # .withColumn("AS127", F.unix_timestamp(F.col("AS127")))
        # .withColumn("AS130", F.unix_timestamp(F.col("AS130")))
        # .withColumn("AS133", F.unix_timestamp(F.col("AS133")))
        # .withColumn("AS134", F.unix_timestamp(F.col("AS134")))
        # .withColumn("AS137", F.unix_timestamp(F.col("AS137")))
    )
    return new_df


def return_write_mode(bucket_name, prefix, pcds):
    """
    If PCDs are already presents as partition return "overwrite", otherwise "append" mode.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param pcds: list of PCDs that have been elaborated in the previous Silver layer.
    :return write_mode: label that express how data should be written on storage.
    """
    storage_client = storage.Client(project="dataops-369610")
    check_list = []
    for pcd in pcds:
        year = pcd.split("-")[0]
        month = pcd.split("-")[1]
        partition_prefix = f"{prefix}/year={year}/month={month}"
        check_list.append(
            len(
                [
                    b.name
                    for b in storage_client.list_blobs(
                        bucket_name, prefix=partition_prefix
                    )
                ]
            )
        )
    if sum(check_list) > 0:
        return "overwrite"
    else:
        return "append"


def generate_asset_silver(spark, bucket_name, bronze_prefix, silver_prefix, pcds):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze data.
    :param silver_prefix: specific bucket prefix from where to deposit silver data.
    :param pcds: list of PCDs that have been elaborated in the previous Bronze layer.
    :return status: 0 if successful.
    """
    logger.info("Start ASSET SILVER job.")
    run_props = set_job_params()
    if pcds == "":
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .drop("valid_from", "valid_to", "checksum", "iscurrent")
        )
    else:
        truncated_pcds = ["-".join(pcd.split("-")[:2]) for pcd in pcds.split(",")]
        bronze_df = (
            spark.read.format("delta")
            .load(f"gs://{bucket_name}/{bronze_prefix}")
            .filter("iscurrent == 1")
            .withColumn("lookup", F.concat_ws("-", F.col("year"), F.col("month")))
            .filter(F.col("lookup").isin(truncated_pcds))
            .drop("valid_from", "valid_to", "checksum", "iscurrent", "lookup")
        )
    assets_columns = get_columns_collection(bronze_df)
    logger.info("Remove ND values.")
    tmp_df1 = replace_no_data(bronze_df)
    logger.info("Replace Y/N with boolean flags.")
    tmp_df2 = replace_bool_data(tmp_df1)
    logger.info("Cast data to correct types.")
    cleaned_df = cast_to_datatype(tmp_df2, run_props["ASSET_COLUMNS"])
    # logger.info("Generate time dataframe")
    # date_df = process_dates(cleaned_df, run_props["DATE_COLUMNS"])
    logger.info("Generate obligor info dataframe")
    obligor_info_df = process_obligor_info(cleaned_df, assets_columns)
    logger.info("Generate loan info dataframe")
    loan_info_df = process_loan_info(cleaned_df, assets_columns)
    logger.info("Generate interest rate dataframe")
    interest_rate_df = process_interest_rate(cleaned_df, assets_columns)
    logger.info("Generate financial info dataframe")
    financial_info_df = process_financial_info(cleaned_df, assets_columns)
    logger.info("Generate performace info dataframe")
    performance_info_df = process_performance_info(cleaned_df, assets_columns)

    logger.info("Write dataframe")
    write_mode = return_write_mode(bucket_name, silver_prefix, pcds)

    # (
    #     date_df.write.format("delta")
    #     .partitionBy("year", "month")
    #     .mode(write_mode)
    #     .save(f"gs://{bucket_name}/{silver_prefix}/date_table")
    # )
    (
        loan_info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/loan_info_table")
    )
    (
        obligor_info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/obligor_info_table")
    )
    (
        financial_info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/financial_info_table")
    )
    (
        interest_rate_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/interest_rate_table")
    )
    (
        performance_info_df.write.format("delta")
        .partitionBy("year", "month")
        .mode(write_mode)
        .save(f"gs://{bucket_name}/{silver_prefix}/performance_info_table")
    )
    logger.info("End ASSET SILVER job.")
    return 0
