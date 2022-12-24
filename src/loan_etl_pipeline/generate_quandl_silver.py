import logging
import sys
import pandas as pd
from google.cloud import storage

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


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


def generate_quandl_silver(bucket_name, bronze_prefix, silver_prefix):
    """
    Run main steps of the module.

    :param bucket_name: GS bucket where files are stored.
    :param bronze_prefix: specific bucket prefix from where to collect bronze old data.
    :param silver_prefix: specific bucket prefix from where to store silver new data.
    :return status: 0 when succesful.
    """
    logger.info("Start QUANDL UPLOAD job.")

    logger.info("Create NEW dataframe")
    quandl_file = f"{bronze_prefix}/quandl_datasets.pkl"
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(quandl_file)
    dest_pkl_f = f'/tmp/{quandl_file.split("/")[-1]}'
    blob.download_to_filename(dest_pkl_f)
    macro_econo_df = pd.read_pickle(dest_pkl_f)

    for k, d in macro_econo_df.items():
        # TODO: The SGE label could not be always applicable. Need to re-engineer this part when/if new data from QUANDL is retrieved.
        code = k.replace("SGE/", "").replace(" - Value", "")
        df = prepare_dataset(code, d)
        bucket.blob(f"{silver_prefix}/{code}.csv").upload_from_string(
            df.to_csv(), "text/csv"
        )

    logger.info("End QUANDL UPLOAD job.")
    return 0
