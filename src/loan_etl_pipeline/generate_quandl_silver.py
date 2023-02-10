import logging
import sys
import pandas as pd
from google.cloud import storage
import unicodedata
from collections import OrderedDict

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
    for k, v in data.items():
        tmp_dict = {}
        tmp_dict["name"] = ds_code
        tmp_dict["date_time"] = k.to_pydatetime().strftime("%Y-%m-%d")
        tmp_dict["year"] = k.to_pydatetime().strftime("%Y")
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
    return manifest_df


def generate_quandl_silver(
    raw_bucketname, data_bucketname, bronze_prefix, target_prefix
):
    """
    Run main steps of the module.

    :param raw_bucketname: GS bucket where raw files are stored.
    :param data_bucketname: GS bucket where transformed files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param target_prefix: specific bucket prefix from where to store silver new data.
    :return status: 0 when succesful.
    """
    logger.info("Start QUANDL UPLOAD job.")
    storage_client = storage.Client(project="dataops-369610")
    raw_bucket = storage_client.get_bucket(raw_bucketname)
    data_bucket = storage_client.get_bucket(data_bucketname)

    logger.info("Prepare manifest table.")
    maifest_file = f"{bronze_prefix}/SGE_TradingEconomics_Metadata.csv"
    blob = raw_bucket.blob(maifest_file)
    dest_csv_f = f'/tmp/{maifest_file.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    manifest_df = pd.read_csv(dest_csv_f)
    new_manifest_df = prepare_manifest(manifest_df)
    data_bucket.blob(f"{target_prefix}/quandl_manifest.csv").upload_from_string(
        new_manifest_df.to_csv(), "text/csv"
    )
    logger.info("Create NEW dataframe")
    quandl_file = f"{bronze_prefix}/quandl_datasets.pkl"

    blob = raw_bucket.blob(quandl_file)
    dest_pkl_f = f'/tmp/{quandl_file.split("/")[-1]}'
    blob.download_to_filename(dest_pkl_f)
    macro_econo_df = pd.read_pickle(dest_pkl_f)

    for k, d in macro_econo_df.items():
        # TODO: The SGE label could not be always applicable. Need to re-engineer this part when/if new data from QUANDL is retrieved.
        code = k.replace("SGE/", "").replace(" - Value", "")
        if code in ["ALBRATING", "DZARATING"]:
            # Skip these datasets since there is no description for them.
            continue
        df = prepare_dataset(code, d)
        data_bucket.blob(f"{target_prefix}/{code}.csv").upload_from_string(
            df.to_csv(), "text/csv"
        )

    logger.info("End QUANDL UPLOAD job.")
    return 0
