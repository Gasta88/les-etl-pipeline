import pyspark.sql.functions as F
from pyspark.sql.types import DateType, DoubleType, BooleanType, IntegerType
from google.cloud import storage
import datetime
import csv


def replace_no_data(df):
    """
    Replace ND values inside the dataframe
    TODO: ND are associated with labels that explain why the vaue is missing.
          Should handle this information better in future releases.
    :param df: Spark dataframe with data.
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

    :param df: Spark dataframe with loan deal details data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            df = df.withColumn(col_name, F.col(col_name).contains("true"))
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
        if data_type == IntegerType():
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    return df


# TO BE REMOVED??
def get_all_pcds(bucket_name, data_type, ed_code):
    """
    Return list of PCDs inside CSV profiling output file.

    :param bucket_name: GS bucket where files are stored.
    :param data_type: type of data to handle, ex: amortisation, assets, collaterals.
    :param ed_code: deal code that rfers to the data to transform.
    :return pcds: list of PCDs to be elaborated.
    """
    pcds = []
    filename_idx = 0
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(bucket_name)
    csv_f = f'clean_dump/{datetime.date.today().strftime("%Y-%m-%d")}_{ed_code}_clean_{data_type}.csv'
    blob = bucket.blob(csv_f)
    dest_csv_f = f'/tmp/{csv_f.split("/")[-1]}'
    blob.download_to_filename(dest_csv_f)
    with open(dest_csv_f, "r") as f:
        for i, line in enumerate(csv.reader(f)):
            if i == 0:
                filename_idx = line.index("filename")
                continue
            else:
                if len(line) == 0:
                    continue
                pcd = "-".join(line[filename_idx].split("/")[-1].split("_")[1:3])
                if pcd not in pcds:
                    pcds.append(pcd)
    return pcds
