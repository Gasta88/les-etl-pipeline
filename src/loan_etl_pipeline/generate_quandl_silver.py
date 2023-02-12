import logging
import sys
import pandas as pd
from google.cloud import storage
import unicodedata
from collections import OrderedDict
import pyspark.sql.functions as F
from google.cloud import bigquery

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


COUNTRY_MAPPER = OrderedDict(
    {
        "AND": "AD",
        "ALB": "AL",
        "AUT": "AT",
        "BIH": "BA",
        "BEL": "BE",
        "BGR": "BG",
        "BLR": "BY",
        "CHE": "CH",
        "SRB": "CS",
        "MNE": "CS",
        "CYP": "CY",
        "CZE": "CZ",
        "DEU": "DE",
        "DNK": "FO",
        "EST": "EE",
        "ESP": "ES",
        "FIN": "FI",
        "FRA": "FR",
        "GBR": "UK",
        "GRC": "GR",
        "HRV": "HR",
        "HUN": "HU",
        "IRL": "IE",
        "ISL": "IS",
        "ITA": "VA",
        "LIE": "LI",
        "LTU": "LT",
        "LUX": "LU",
        "LVA": "LV",
        "MCO": "MC",
        "MDA": "MD",
        "MKD": "MK",
        "MLT": "MT",
        "NLD": "NL",
        "NOR": "SJ",
        "POL": "PL",
        "PRT": "PT",
        "ROU": "RO",
        "RUS": "RU",
        "SWE": "SE",
        "SVN": "SI",
        "SVK": "SK",
        "TUR": "TR",
        "UKR": "UA",
        "EUR": "XC",
    }
)


def prepare_dataset(ds_code, data):
    """
    Clean up key values and make a dataframe out of it for storage.

    :param ds_code: QUANDL code of the dataset.
    :param data: dictionary with data.
    :return df: final dataframe to be stored
    """
    new_data = []
    if len(data) == 0:
        # Some datasets are empty
        return None
    for k, v in data.items():
        tmp_dict = {}
        tmp_dict["name"] = ds_code
        # tmp_dict["date_time"] = k.to_pydatetime().strftime("%Y-%m-%d")
        tmp_dict["date_time"] = k.to_pydatetime().date()
        tmp_dict["value"] = v
        new_data.append(tmp_dict)
    return pd.DataFrame(new_data)


def prepare_manifest(manifest_df):
    """
    Create a nice CSV file where there is the mapping between QUANDL code and its description.

    :param manifest_df: raw manifest data associated to QUANDL data.
    :return df: final dataframe to be stored.
    """
    char_to_replace = {
        "-": "_",
        " ": "_",
        "(": "",
        ")": "",
    }
    manifest_df["name"] = manifest_df["name"].apply(
        lambda x: unicodedata.normalize("NFKD", x)
        .lower()
        .replace(" - ", "-")
        .replace(" -", "")
        .translate(str.maketrans(char_to_replace))
    )
    manifest_df["quandl_country"] = manifest_df["code"].str[:3]
    manifest_df["ed_country"] = manifest_df["code"].str[:3]
    manifest_df.replace({"ed_country": COUNTRY_MAPPER}, inplace=True)
    manifest_df.drop(columns=["refreshed_at", "from_date", "to_date"], inplace=True)
    return manifest_df


def prep_bigquery_env():
    """
    If needed prepare dataset and table definition in BigQuery.

    :return manifest_table: BigQuery manifest table object.
    :return data_table: BigQuery data table object.
    """
    client = bigquery.Client(project="dataops-369610")
    project = "dataops-369610"
    dataset_name = "external_data"
    data_table_name = "quandl_datasets"
    manifest_table_name = "quandl_manifest"
    try:
        dataset = client.get_dataset(f"{project}.{dataset_name}")
    except Exception as e:
        print("No dataset. Create one.")
        dataset = bigquery.Dataset(f"{project}.{dataset_name}")
        dataset.location = "EU"
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset: {project}.{dataset_name}")
    # Manifest table
    try:
        manifest_table = client.get_table(
            f"{project}.{dataset_name}.{manifest_table_name}"
        )
    except Exception as e:
        print("No manifest table. Create one.")
        schema = [
            bigquery.SchemaField("code", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("quandl_country", "STRING"),
            bigquery.SchemaField("ed_country", "STRING"),
        ]
        manifest_table = bigquery.Table(
            f"{project}.{dataset_name}.{manifest_table_name}", schema=schema
        )
        manifest_table = client.create_table(manifest_table)
        print(f"Created table: {project}.{dataset_name}.{manifest_table_name}")
    # Data table
    try:
        data_table = client.get_table(f"{project}.{dataset_name}.{data_table_name}")
    except Exception as e:
        print("No data table. Create one.")
        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("date_time", "DATE"),
            bigquery.SchemaField("value", "FLOAT64"),
        ]
        data_table = bigquery.Table(
            f"{project}.{dataset_name}.{data_table_name}", schema=schema
        )
        data_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.YEAR,
            field="date_time",  # name of column to use for partitioning
        )
        data_table.clustering_fields = ["name"]
        data_table = client.create_table(data_table)
        print(f"Created table: {project}.{dataset_name}.{data_table_name}")
    return (manifest_table, data_table)


def generate_quandl_silver(spark, raw_bucketname, data_bucketname, bronze_prefix):
    """
    Run main steps of the module.

    :param spark: SparkSession object.
    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :return status: 0 when succesful.
    """
    logger.info("Start QUANDL UPLOAD job.")
    storage_client = storage.Client(project="dataops-369610")
    raw_bucket = storage_client.get_bucket(raw_bucketname)

    logger.info("Prepare BigQuery env.")
    manifest_table, data_table = prep_bigquery_env()

    logger.info("Prepare manifest table.")
    maifest_file = f"{bronze_prefix}/SGE_TradingEconomics_Metadata.csv"
    blob = raw_bucket.blob(maifest_file)
    dest_csv_f = f'/tmp/{maifest_file.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    manifest_df = pd.read_csv(dest_csv_f)
    new_manifest_df = prepare_manifest(manifest_df)
    spark_df = spark.createDataFrame(new_manifest_df)
    (
        spark_df.write.format("bigquery")
        .option("temporaryGcsBucket", data_bucketname)
        .option("table", f"{manifest_table.dataset_id}.{manifest_table.table_id}")
        .mode("overwrite")
        .save()
    )

    logger.info("Create NEW dataframe")
    quandl_file = f"{bronze_prefix}/quandl_datasets.pkl"

    blob = raw_bucket.blob(quandl_file)
    dest_pkl_f = f'/tmp/{quandl_file.split("/")[-1]}'
    blob.download_to_filename(dest_pkl_f)
    macro_econo_df = pd.read_pickle(dest_pkl_f)

    list_dfs = []
    for k, d in macro_econo_df.items():
        # TODO: The SGE label could not be always applicable. Need to re-engineer this part when/if new data from QUANDL is retrieved.
        code = k.replace("SGE/", "").replace(" - Value", "")
        if code in ["ALBRATING", "DZARATING"]:
            # Skip these datasets since there is no description for them.
            continue
        tmp_df = prepare_dataset(code, d)
        if tmp_df is None:
            continue
        list_dfs.append(tmp_df)
    df = pd.concat(list_dfs)
    spark_df = spark.createDataFrame(df)
    (
        spark_df.write.format("bigquery")
        .option("temporaryGcsBucket", data_bucketname)
        .option("partitionField", "date_time")
        .option("partitionType", "YEAR")
        .option("clusteredFields", "name")
        .option("table", f"{data_table.dataset_id}.{data_table.table_id}")
        .mode("overwrite")
        .save()
    )

    logger.info("End QUANDL UPLOAD job.")
    return 0
