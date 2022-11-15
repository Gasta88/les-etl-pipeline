import configparser
import logging
import os
import sys
from etl_scripts import *


# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


config = configparser.ConfigParser()
config.read("etl.cfg")


def main():
    """Run main steps in the ETL pipeline."""
    os.environ["SOURCE_DIR"] = config["GENERAL"]["SOURCE_DIR"]
    os.environ["WORKERS"] = config["GENERAL"]["WORKERS"]

    logger.info("Generate BRONZE level tables.")
    etl_scripts.generate_asset_bronze.main()


if __name__ == "__main__":
    main()
