from google.cloud import storage
import json
import csv

# import pyspark.sql.functions as F

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
    :return rules: collection of profiling rules for data_type.
    """
    rules = None
    bucket_name = "data-lake-code-847515094398"
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    profile_file = [
        b.name
        for b in storage_client.list_blobs(bucket_name)
        if (b.name.endswith(".json")) and ("bronze" in b.name)
    ][0]
    blob = bucket.blob(profile_file)
    rules = json.loads(blob.download_as_string())
    return rules[data_type]


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


def _get_checks_dict(df, table_rules):
    """
    Return collection of constrains specific by table name.

    :param df: Spark dataframe that holds the SQL data.
    :param table_rules: collection of profiling rules for the table.
    :return constrains_dict: collection of constrains to be tested on the SQL dump.
    """
    split_table_rules_cols = [c.split("+") for c in table_rules.keys()]
    table_rules_cols = []
    for c in split_table_rules_cols:
        table_rules_cols += c
    expected_columns = set(table_rules_cols)
    constrains_dict = {
        "hasSize": len(df.head(1)) > 0,
        "hasColumns": expected_columns.issubset(set(df.columns)),
    }
    # for col_names, rules_set in table_rules.items():
    #     list_col_names = col_names.split("+")
    #     for rule_set in rules_set:
    #         rule_set_name = f'{col_names}|{rule_set.split("|")[0]}'
    #         if rule_set == "IsComplete":
    #             constrains_dict[rule_set_name] = (
    #                 df.select(F.concat(*list_col_names).alias("combined"))
    #                 .where(F.col("combined").isNull())
    #                 .count()
    #                 == 0
    #             )
    #         if rule_set == "IsUnique":
    #             constrains_dict[rule_set_name] = (
    #                 df.select(*list_col_names).distinct().count() == n_rows
    #             )
    #         # Assumption that IsContained rule runs always on one column only
    #         if rule_set.startswith("IsContainedInt"):
    #             allowed_values = list(map(int, rule_set.split("|")[-1].split(",")))
    #             constrains_dict[rule_set_name] = set(
    #                 [r[0] for r in df.select(*list_col_names).distinct().collect()]
    #             ).issubset(set(allowed_values))
    #         if rule_set.startswith("IsContainedStr"):
    #             allowed_values = rule_set.split("|")[-1].split(",")
    #             constrains_dict[rule_set_name] = set(
    #                 [r[0] for r in df.select(*list_col_names).distinct().collect()]
    #             ).issubset(set(allowed_values))
    return constrains_dict


# def profile_data(spark, bucket_name, all_files, data_type, table_rules):
#     """
#     Check whether the file is ok to be stored in the bronze layer or not.

#     :param spark: SparkSession object.
#     :param bucket_name: GS bucket where files are stored.
#     :param all_files: list of files to be read to generate the dataframe.
#     :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
#     :param table_rules: collection of profiling rules for the table.
#     :return clean_files: CSV files that passes the bronze profiling rules.
#     :return dirty_files: list of tuples with CSV file and relative error text that fails the bronze profiling rules.
#     """
#     storage_client = storage.Client(project="dataops-369610")
#     clean_files = []
#     dirty_files = []
#     bucket = storage_client.get_bucket(bucket_name)
#     for csv_f in all_files:
#         blob = bucket.blob(csv_f)
#         dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
#         blob.download_to_filename(dest_csv_f)
#         col_names = []
#         content = []
#         try:
#             with open(dest_csv_f, "r") as f:
#                 for i, line in enumerate(csv.reader(f)):
#                     if i == 0:
#                         col_names = line
#                         col_names[0] = INITIAL_COL[data_type]
#                     elif i == 1:
#                         continue
#                     else:
#                         if len(line) == 0:
#                             continue
#                         content.append(line)
#                 df = spark.createDataFrame(content, col_names)
#                 checks = _get_checks_dict(df, table_rules)
#                 if False in checks.values():
#                     error_list = []
#                     for k, v in checks.items():
#                         if not v:
#                             error_list.append(k)
#                     error_text = ";".join(error_list)
#                     dirty_files.append((csv_f, error_text))
#                 else:
#                     clean_files.append(csv_f)
#         except Exception as e:
#             dirty_files.append((csv_f, e))
#     return (clean_files, dirty_files)
def profile_data(spark, bucket_name, csv_f, data_type, table_rules):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param spark: SparkSession object.
    :param bucket_name: GS bucket where files are stored.
    :param csv_f: CSV file to be read and profile.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param table_rules: collection of profiling rules for the table.
    :return profile_flag: CSV files is dirty or clean.
    :return error_text: if CSV is dirty provide reason, None otherwise.
    """
    storage_client = storage.Client(project="dataops-369610")
    error_text = None
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    col_names = []
    content = []
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
                    content.append(curr_line)
                    # repartition = 4 instances * 8 cores each * 3 for replication factor
            df = spark.createDataFrame(content, col_names).repartition(96)
            checks = _get_checks_dict(df, table_rules)
            if False in checks.values():
                error_list = []
                for k, v in checks.items():
                    if not v:
                        error_list.append(k)
                error_text = ";".join(error_list)
                profile_flag = "dirty"
            else:
                profile_flag = "clean"
    except Exception as e:
        error_text = e
        profile_flag = "dirty"
    return (profile_flag, error_text)
