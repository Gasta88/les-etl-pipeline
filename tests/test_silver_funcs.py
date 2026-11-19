from pyspark.sql.types import BooleanType, DateType, DoubleType, IntegerType

from src.les_etl_pipeline.utils.silver_funcs import cast_to_datatype, profile_data


def test_cast_to_datatype(spark):
    df = spark.createDataFrame(
        [("y", "2023-08-01", "1.2345", "7")],
        ["flag", "dt", "amount", "count"],
    )

    row = cast_to_datatype(
        df,
        {
            "flag": BooleanType(),
            "dt": DateType(),
            "amount": DoubleType(),
            "count": IntegerType(),
        },
    ).first()

    assert row["flag"] is True
    assert str(row["dt"]) == "2023-08-01"
    assert row["amount"] == 1.23
    assert row["count"] == 7


def test_cast_to_datatype_maps_non_y_to_false(spark):
    df = spark.createDataFrame([("n",)], ["flag"])
    row = cast_to_datatype(df, {"flag": BooleanType()}).first()
    assert row["flag"] is False


def test_profile_data_splits_good_and_bad_rows(spark):
    good = {
        "ed_code": "LESSES0001",
        "part": "LESSES0001_20230801",
        "filename": "bond_info_LESSES0001_2023_08_01_000000.csv",
        "pcd": "2023-08-01",
        "BL1": "2023-08-01",
        "BL2": "issuer",
        "BL19": "contact",
        "BL20": "info",
        "BL29": "eur",
        "BL38": "2023-08-01",
        "BL39": "2023-08-01",
    }
    bad = dict(good, pcd="1999-01-01")

    df = spark.createDataFrame([good, bad])

    good_df, bad_df = profile_data(spark, df, "bond_info")

    assert good_df.count() == 1
    assert bad_df.count() == 1
    assert good_df.first()["flag"] is True
    assert bad_df.first()["flag"] is False
