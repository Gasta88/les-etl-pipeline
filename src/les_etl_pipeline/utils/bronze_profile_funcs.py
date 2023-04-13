from google.cloud import storage
import csv


INITIAL_COL = {
    "assets": "AL1",
    "bond_info": "BL1",
}


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
            and not ("Labeled" in b.name)  # This is generated internally by Cal
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


def profile_data(bucket_name, csv_f, data_type, validator):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

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
                    record["pcd"] = "-".join(csv_f.split("/")[-1].split("_")[1:4])
                    record["ed_code"] = csv_f.split("/")[-1].split("_")[0]
                    if not flag:
                        # Does not pass validation
                        record["qc_errors"] = errors
                        dirty_content.append(record)
                    else:
                        clean_content.append(record)
    except Exception as e:
        dirty_content.append({"filename": csv_f, "qc_errors": e})
    return (clean_content, dirty_content)
