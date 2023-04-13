import unittest
import warnings
from src.les_etl_pipeline.utils.bronze_profile_funcs import (
    _get_checks_dict,
)
import json
from pyspark.sql import SparkSession


class BronzeProfilingTestCase(unittest.TestCase):
    """Test suite for bronze data profiling on ETL."""

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

    def test_get_get_checks_dict(self):
        """Test src.loan_etl_pipeline.utils.bronze_profile_funcs. _get_checks_dict method."""
        with open("../../dependencies/bronze_profiling_rules.json", "r") as f:
            table_rules = json.load(f)
        cols = "AS3"
        good_data = [
            [1],
            [2],
            [3],
        ]
        df = self.spark.createDataFrame(good_data, cols)
        res = _get_checks_dict(df, table_rules["amortisation"])
        self.assertTrue(False not in [v for v in res.values()])

        bad_data = [
            [1],
            [2],
            [2],
        ]
        df = self.spark.createDataFrame(bad_data, cols)
        res = _get_checks_dict(df, table_rules["amortisation"])
        self.assertFalse(False not in [v for v in res.values()])


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
