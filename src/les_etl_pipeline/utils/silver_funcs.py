import pyspark.sql.functions as F
from pyspark.sql.types import (
    DateType,
    StringType,
    DoubleType,
    BooleanType,
    IntegerType,
    StructType,
    StructField,
)
from cerberus import Validator
from src.les_etl_pipeline.utils.validation_rules import asset_schema, bond_info_schema


def cast_to_datatype(df, columns):
    """
    Cast data to the respective datatype.

    :param df: Spark dataframe with loan deal details data.
    :param columns: collection of column names and respective data types.
    :return df: Spark dataframe with correct values.
    """
    for col_name, data_type in columns.items():
        if data_type == BooleanType():
            df = df.withColumn(
                col_name, F.when(F.col(col_name) == ("y"), True).otherwise(False)
            )
        if data_type == DateType():
            df = df.withColumn(col_name, F.to_date(F.col(col_name)))
        if data_type == DoubleType():
            df = df.withColumn(col_name, F.round(F.col(col_name).cast(DoubleType()), 2))
        if data_type == IntegerType():
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    return df


def profile_data(spark, df, data_type):
    """
    Check whether the file is ok to be stored in the bronze layer or not.

    :param spark: SparkSession object.
    :param df: Spark dataframe with bronze level data to be profiled.
    :param data_type: data type to inspect for QC.
    :return good_df: Spark dataframe with data that passed the validation rules.
    :return bad_df: Spark dataframe with data that didn't pass the validation rules.
    """

    def _run_qc_check_func(partition_data, validator):
        """
        Perform checks with Cerberus validator.

        :param partiton_data: dataframe partiton data.
        :param validator: Cerberus Validator objet for respective data_type.
        """
        for row in partition_data:
            row_dict = row.asDict(recursive=True)
            flag = validator.validate(row_dict)
            qc_errors = None if flag else validator.errors
            row_dict["flag"] = flag
            row_dict["qc_errors"] = qc_errors
            yield list(row_dict.values())

    validator_map = {
        "assets": Validator(asset_schema()),
        "bond_info": Validator(bond_info_schema()),
    }
    validator = validator_map[data_type]
    col_names = df.columns
    schema = StructType(
        [StructField(col_name, StringType(), True) for col_name in col_names]
    )
    schema = schema.add("flag", BooleanType(), False)
    schema = schema.add("qc_errors", StringType(), True)
    new_rdd = df.rdd.mapPartitions(lambda ptd: _run_qc_check_func(ptd, validator))
    new_df = spark.createDataFrame(new_rdd, schema=schema)
    good_df = new_df.filter(F.col("flag"))
    bad_df = new_df.filter(~F.col("flag"))
    return good_df, bad_df


ASSET_COLUMNS = {
    "AL1": DateType(),
    "AL2": StringType(),
    "AL3": StringType(),
    "AL4": StringType(),
    "AL5": StringType(),
    "AL6": StringType(),
    "AL7": BooleanType(),
    "AL8": StringType(),
    "AL9": StringType(),
    "AL10": StringType(),
    "AL15": StringType(),
    "AL16": StringType(),
    "AL17": StringType(),
    "AL18": StringType(),
    "AL19": DateType(),
    "AL20": DateType(),
    "AL21": StringType(),
    "AL22": StringType(),
    "AL23": BooleanType(),
    "AL29": BooleanType(),
    "AL30": DoubleType(),
    "AL31": DateType(),
    "AL32": StringType(),
    "AL33": StringType(),
    "AL34": StringType(),
    "AL35": StringType(),
    "AL36": StringType(),
    "AL37": DoubleType(),
    "AL38": DoubleType(),
    "AL39": DoubleType(),
    "AL40": DoubleType(),
    "AL41": DoubleType(),
    "AL42": StringType(),
    "AL43": StringType(),
    "AL44": DoubleType(),
    "AL45": StringType(),
    "AL46": StringType(),
    "AL47": BooleanType(),
    "AL48": DateType(),
    "AL50": DateType(),
    "AL51": DateType(),
    "AL52": DateType(),
    "AL53": DoubleType(),
    "AL54": DoubleType(),
    "AL55": DoubleType(),
    "AL56": DoubleType(),
    "AL57": DoubleType(),
    "AL58": StringType(),
    "AL59": StringType(),
    "AL60": StringType(),
    "AL61": DoubleType(),
    "AL62": DoubleType(),
    "AL63": DoubleType(),
    "AL64": StringType(),
    "AL66": StringType(),
    "AL67": StringType(),
    "AL68": DoubleType(),
    "AL69": DateType(),
    "AL70": StringType(),
    "AL74": DoubleType(),
    "AL75": DoubleType(),
    "AL76": StringType(),
    "AL77": DoubleType(),
    "AL78": DoubleType(),
    "AL79": DoubleType(),
    "AL80": DoubleType(),
    "AL83": DoubleType(),
    "AL84": DoubleType(),
    "AL85": DoubleType(),
    "AL86": DoubleType(),
    "AL87": DoubleType(),
    "AL88": DoubleType(),
    "AL89": DoubleType(),
    "AL90": DoubleType(),
    "AL91": DoubleType(),
    "AL92": DoubleType(),
    "AL93": DoubleType(),
    "AL94": StringType(),
    "AL95": DateType(),
    "AL98": DoubleType(),
    "AL99": DoubleType(),
    "AL100": DateType(),
    "AL101": DateType(),
    "AL102": DoubleType(),
    "AL103": DoubleType(),
    "AL104": BooleanType(),
    "AL105": BooleanType(),
    "AL106": StringType(),
    "AL107": DateType(),
    "AL108": DoubleType(),
    "AL109": DoubleType(),
    "AL110": DateType(),
    "AL111": DoubleType(),
    "AL112": StringType(),
    "AL113": DateType(),
    "AL114": BooleanType(),
    "AL115": DoubleType(),
    "AL116": DateType(),
    "AL117": DateType(),
    "AL118": DoubleType(),
    "AL119": DoubleType(),
    "AL120": DateType(),
    "AL121": DoubleType(),
    "AL122": StringType(),
    "AL123": BooleanType(),
    "AL124": DoubleType(),
    "AL125": DoubleType(),
    "AL126": BooleanType(),
    "AL127": DoubleType(),
    "AL128": DoubleType(),
    "AL129": DoubleType(),
    "AL133": StringType(),
    "AL134": StringType(),
    "AL135": StringType(),
    "AL136": DateType(),
    "AL137": StringType(),
    "AL138": DoubleType(),
    "AL139": StringType(),
    "AL140": BooleanType(),
    "AL141": StringType(),
    "AL142": DoubleType(),
    "AL143": DoubleType(),
    "AL144": StringType(),
    "AL145": DateType(),
    "AL146": DoubleType(),
    "AL147": StringType(),
    "AL148": DateType(),
}

BOND_COLUMNS = {
    "BL1": DateType(),
    "BL2": StringType(),
    "BL4": BooleanType(),
    "BL5": BooleanType(),
    "BL11": DoubleType(),
    "BL12": BooleanType(),
    "BL13": DoubleType(),
    "BL14": DoubleType(),
    "BL15": DoubleType(),
    "BL16": DoubleType(),
    "BL17": DoubleType(),
    "BL18": DateType(),
    "BL19": StringType(),
    "BL20": StringType(),
    "BL25": StringType(),
    "BL26": StringType(),
    "BL27": DateType(),
    "BL28": DateType(),
    "BL29": StringType(),
    "BL30": DoubleType(),
    "BL31": DoubleType(),
    "BL32": StringType(),
    "BL33": DoubleType(),
    "BL34": DoubleType(),
    "BL35": DoubleType(),
    "BL36": DoubleType(),
    "BL37": DoubleType(),
    "BL38": DateType(),
    "BL39": DateType(),
    "BL40": DateType(),
    "BL41": StringType(),
    "BL42": DateType(),
    "BL43": DoubleType(),
    "BL44": DoubleType(),
    "BL45": DoubleType(),
    "BL46": DoubleType(),
}
