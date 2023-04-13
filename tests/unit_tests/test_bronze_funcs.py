import unittest
import warnings
from src.les_etl_pipeline.utils.bronze_funcs import perform_scd2
from pyspark.sql import SparkSession


class BronzeTablesTestCase(unittest.TestCase):
    """Test suite for bronze data tables on ETL."""

    maxDiff = None

    def setUp(self):
        """Initialize the test settings."""
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        self.spark = SparkSession.builder.master("local").getOrCreate()
        self.spark.sparkContext.setLogLevel("FATAL")

    def tearDown(self):
        """Remove test settings."""
        self.spark.catalog.clearCache()
        self.spark.stop()

    def test_perform_scd2(self):
        """Test src.loan_etl_pipeline.utils.bronze_funcs.perform_scd2 method."""
        data_type = "amortisation"
        cols = [
            "AS3",
            "AS150",
            "AS151",
            "year",
            "month",
            "valid_from",
            "valid_to",
            "iscurrent",
            "checksum",
            "ed_code",
        ]
        source_data = [
            [
                "000807376530886",
                593.08,
                "2013-01-05",
                2012,
                12,
                "2022-11-26 10:45:14.361954",
                None,
                1,
                "4a21110eab65f99d6850542e52c19db0",
                "SMESES000176100320040",
            ],
            [
                "000807376574695",
                536.31,
                "2012-12-08",
                2012,
                12,
                "2022-11-26 10:45:14.361954",
                None,
                1,
                "b74583db3a5ae82bb5665900bae62fd6",
                "SMESES000176100320040",
            ],
        ]
        update_data = [
            [
                "000807376530886",
                666,
                "2018-01-05",
                2018,
                12,
                "2022-11-26 10:45:14.361954",
                None,
                1,
                "4a21110eab65f99d6850542e52c19db0",
                "SMESES000176100320040",
            ],
            [
                "000807376574696",
                667,
                "2012-12-08",
                2012,
                12,
                "2022-11-26 10:45:14.361954",
                None,
                1,
                "b74583db3a5ae82bb5665900bae62fd6",
                "SMESES000176100320040",
            ],
        ]
        source_df = self.spark.createDataFrame(source_data, cols)
        target_df = self.spark.createDataFrame(update_data, cols)
        perform_scd2(self.spark, source_df, target_df, data_type)
        res = self.spark.sql(
            "SELECT COUNT(*) AS result FROM delta_table_amortisation"
        ).collect()
        self.assertTrue(res[0].result == 3)


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
