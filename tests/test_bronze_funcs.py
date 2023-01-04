import unittest
import warnings
from src.loan_etl_pipeline.utils.bronze_funcs import (
    get_old_df,
    create_dataframe,
    perform_scd2,
)
import json


class BronzeTablesTestCase(unittest.TestCase):
    """Test suite for bronze data tables on ETL."""

    maxDiff = None

    def setUp(self):
        """Initialize the test settings."""
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

    def tearDown(self):
        """Remove test settings."""
        pass

    def test_get_profiling_rules(self):
        pass


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=3)
    unittest.main(testRunner=runner)
