import unittest
import warnings
from src.les_etl_pipeline.utils.silver_funcs import replace_no_data, replace_bool_data
from pyspark.sql import SparkSession


class SilverTablesTestCase(unittest.TestCase):
    """Test suite for silver data tables on ETL."""

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

    def test_replace_no_data(self):
        """Test src.loan_etl_pipeline.utils.silver_funcs.replace_no_data method."""
        cols = ["col1"]
        data = [
            ["ND,1"],
            ["000807376530883"],
            ["ND,3"],
        ]
        df = self.spark.createDataFrame(data, cols)
        new_df = replace_no_data(df)
        self.assertTrue(new_df.filter("col1 IS NOT NULL").count() == 1)

    def test_replace_bool_data(self):
        """Test src.loan_etl_pipeline.utils.silver_funcs.replace_bool_data method."""
        cols = ["col1"]
        data = [
            ["Y"],
            ["N"],
            ["N"],
        ]
        df = self.spark.createDataFrame(data, cols)
        new_df = replace_bool_data(df)
        self.assertTrue(new_df.filter("col1 = 'True'").count() == 1)


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
