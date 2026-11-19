import datetime
from io import StringIO
from pathlib import Path

import pyspark.sql.functions as F

from src.les_etl_pipeline.utils.bronze_funcs import (
    _correct_file_coding,
    parse_csv_file,
    perform_scd2,
)

FIXTURES = Path(__file__).parent / "fixtures"

SCD2_SCHEMA = (
    "ed_code string, PoolCutOffDate string, DealSize int, "
    "valid_from timestamp, valid_to timestamp, iscurrent int, "
    "checksum string, part string"
)


def test_correct_file_coding_strips_bom_nul_and_accents():
    raw = StringIO("\ufeffh\x00ello \u00e9t\u00e9")
    cleaned = _correct_file_coding(raw).read()
    assert cleaned == "hello ete"


def test_parse_csv_file_builds_bronze_columns(spark):
    path = FIXTURES / "LESSES0001_2023_08_01_000000.csv"
    object_name = "edw_data/LESSES0001_2023_08_01_000000.csv"

    with open(path, "r", encoding="utf-8") as handle:
        df, errors = parse_csv_file(spark, handle, "assets", object_name)

    assert errors is None
    assert df.count() == 2

    rows = {row["AL2"]: row for row in df.collect()}
    first = rows["pool001"]
    assert first["ed_code"] == "LESSES0001"
    assert first["pcd"] == "2023-08-01"
    assert first["part"] == "LESSES0001_20230801"
    assert first["AL4"] is None
    assert rows["pool002"]["AL3"] is None
    assert first["checksum"]


def test_parse_csv_file_returns_error_when_empty(spark):
    handle = StringIO("AL1,AL2\nDescription,Pool\n")
    df, errors = parse_csv_file(spark, handle, "assets", "LESSES0001_x.csv")

    assert df is None
    assert errors == "ERROR: empty dataframe"


def test_perform_scd2_inserts_new_keys_and_versions_changed_ones(spark):
    old = spark.createDataFrame(
        [
            (
                "E1",
                "2023-08-01",
                100,
                datetime.datetime(2023, 8, 1),
                None,
                1,
                "aaa",
                "E1_20230801",
            ),
            (
                "E1",
                "2023-09-01",
                200,
                datetime.datetime(2023, 9, 1),
                None,
                1,
                "bbb",
                "E1_20230901",
            ),
        ],
        SCD2_SCHEMA,
    )
    new = spark.createDataFrame(
        [
            (
                "E1",
                "2023-08-01",
                150,
                datetime.datetime(2023, 8, 2),
                None,
                1,
                "ccc",
                "E1_20230801",
            ),
            (
                "E1",
                "2023-10-01",
                300,
                datetime.datetime(2023, 10, 1),
                None,
                1,
                "ddd",
                "E1_20231001",
            ),
        ],
        SCD2_SCHEMA,
    )

    merged = perform_scd2(spark, old, new, "deal_details")

    assert merged.count() == 4

    current = merged.filter(F.col("iscurrent") == 1)
    assert current.count() == 3
    assert {row["checksum"] for row in current.collect()} == {"bbb", "ccc", "ddd"}

    closed = merged.filter(F.col("iscurrent") == 0).collect()
    assert len(closed) == 1
    assert closed[0]["checksum"] == "aaa"
    assert closed[0]["valid_to"] == datetime.datetime(2023, 8, 2)


def test_perform_scd2_is_a_noop_for_unchanged_keys(spark):
    row = (
        "E1",
        "2023-08-01",
        100,
        datetime.datetime(2023, 8, 1),
        None,
        1,
        "aaa",
        "E1_20230801",
    )
    old = spark.createDataFrame([row], SCD2_SCHEMA)
    new = spark.createDataFrame([row], SCD2_SCHEMA)

    merged = perform_scd2(spark, old, new, "deal_details")

    assert merged.count() == 1
    assert merged.first()["checksum"] == "aaa"
