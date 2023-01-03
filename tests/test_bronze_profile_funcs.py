import unittest
import warnings
from src.loan_etl_pipeline.utils.bronze_profile_funcs import (
    get_profiling_rules,
    get_csv_files,
    _get_checks_dict,
    profile_data,
)
import json


class BronzeProfilingTestCase(unittest.TestCase):
    """Test suite for bronze data profiling on ETL."""

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
