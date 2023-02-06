from google.cloud import storage
import csv
from validation_rules import (
    asset_schema,
    collateral_schema,
    bond_info_schema,
    amortisation_schema,
)
from cerberus import Validator


INITIAL_COL = {
    "assets": "AS1",
    "collaterals": "CS1",
    "amortisation": "AS3",
    "bond_info": "BS1",
}


def get_profiling_rules(data_type):
    """
    Return QA rules for data_type.

    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return validator: Cerberus validator with specific schema for profiling.
    """
    if data_type == "assets":
        return Validator(asset_schema, allow_unknown=True)
    if data_type == "collaterals":
        return Validator(collateral_schema, allow_unknown=True)
    if data_type == "bond_info":
        return Validator(bond_info_schema, allow_unknown=True)
    if data_type == "amortisation":
        return Validator(amortisation_schema, allow_unknown=True)


def get_csv_files(bucket_name, prefix, file_key, data_type):
    """
    Return list of source files that satisfy the file_key parameter from EDW.

    :param bucket_name: GS bucket where files are stored.
    :param prefix: specific bucket prefix from where to collect files.
    :param file_key: label for file name that helps with the cherry picking.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :return all_files: list of desired files from source_dir.
    """
    storage_client = storage.Client(project="dataops-369610")
    if data_type == "assets":
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv"))
            and (file_key in b.name)
            and not ("Labeled0M" in b.name)  # This is generated internally by Cal
        ]
    else:
        all_files = [
            b.name
            for b in storage_client.list_blobs(bucket_name, prefix=prefix)
            if (b.name.endswith(".csv")) and (file_key in b.name)
        ]
    if len(all_files) == 0:
        return []
    else:
        return all_files


def profile_data(spark, bucket_name, csv_f, data_type, validator):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param csv_f: CSV file to be read and profile.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param validator: Cerberus validator object.
    :return profile_flag: CSV files is dirty or clean.
    :return error_text: if CSV is dirty provide reason, None otherwise.
    """
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    col_names = []
    clean_content = []
    dirty_content = []
    try:
        with open(dest_csv_f, "r") as f:
            for i, line in enumerate(csv.reader(f)):
                if data_type == "amortisation":
                    # Just check that AS3 is present instead of the hundreds of columns that the file has.
                    curr_line = line[:1]
                else:
                    curr_line = line
                if i == 0:
                    col_names = curr_line
                    col_names[0] = INITIAL_COL[data_type]
                elif i == 1:
                    continue
                else:
                    if len(curr_line) == 0:
                        continue
                    clean_line = [
                        None if (el == "") or (el.startswith("ND")) else el
                        for el in curr_line
                    ]
                    record = {
                        col_names[i]: clean_line[i] for i in range(len(clean_line))
                    }
                    flag = validator.validate(record)
                    errors = None if flag else validator.errors
                    record["filename"] = csv_f
                    if not flag:
                        # Does not pass validation
                        record["qc_errors"] = errors
                        dirty_content.append(record)
                    else:
                        clean_content.append(record)
    except Exception as e:
        dirty_content.append({"filename": csv_f, "qc_errors": e})
    return (clean_content, dirty_content)
