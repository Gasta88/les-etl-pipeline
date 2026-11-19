import datetime

from src.les_etl_pipeline.utils.validation_rules import (
    TO_DATE,
    asset_schema,
    bond_info_schema,
)


def test_to_date_parses_all_supported_formats():
    assert TO_DATE("2023-08-01") == datetime.datetime(2023, 8, 1)
    assert TO_DATE("2023-08") == datetime.datetime(2023, 8, 1)
    assert TO_DATE("2023") == datetime.datetime(2023, 1, 1)


def test_asset_schema_contains_common_and_asset_fields():
    schema = asset_schema()
    assert {"ed_code", "part", "filename", "pcd", "AL1", "AL2"} <= schema.keys()


def test_bond_info_schema_contains_common_and_bond_fields():
    schema = bond_info_schema()
    assert {"ed_code", "part", "filename", "pcd", "BL1", "BL2"} <= schema.keys()
