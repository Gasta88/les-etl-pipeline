from pathlib import Path

from src.les_etl_pipeline.generate_deal_details_bronze import parse_deal_details_xml

FIXTURES = Path(__file__).parent / "fixtures"


def test_parse_deal_details_xml(spark):
    path = FIXTURES / "deal_details_LESSES0001.xml"

    pcd, df = parse_deal_details_xml(spark, str(path))

    assert pcd == "20230801"
    assert df.count() == 1

    row = df.first()
    assert row["ed_code"] == "LESSES0001"
    assert row["PoolCutOffDate"] == "2023-08-01T00:00:00"
    assert row["ISIN"] == "XS0000000001;XS0000000002"
    assert row["RequestId"] == "REQ-1"
    assert row["part"] == "LESSES0001_20230801"
    assert row["iscurrent"] == 1
    assert row["checksum"]
