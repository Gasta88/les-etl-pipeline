import glob
import logging
import sys
from lxml import objectify
import pandas as pd
import os

# Setup logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def set_job_params():
    """
    Setup parameters used for this module.

    :return config: dictionary with properties used in this job.
    """
    config = {}
    config["SOURCE_DIR"] = os.environ["SOURCE_DIR"]
    config["FILE_KEY"] = "Deal_Details"
    return config


def get_raw_files(source_dir, file_key):
    """
    Return list of files that satisfy the file_key parameter.
    Works only on local machine so far.

    :param source_dir: folder path where files are stored.
    :param file_key: label for file name that helps with the cherry picking.
    :return all_files: listof desired files from source_dir.
    """
    all_files = [f for f in glob.glob(f"{source_dir}/*/{file_key}/*.xml")]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {source_dir}. Exit process!"
        )
        sys.exit(1)
    else:
        return all_files[0]


def create_dataframe(deal_detail_file):
    """
    Read files and generate one PySpark DataFrame from them.

    :param deal_detail_file: file to be read to generate the dataframe.
    :return df: PySpark datafram for loan asset data.
    """
    xml_data = objectify.parse(deal_detail_file)  # Parse XML data
    root = xml_data.getroot()  # Root element

    data = []
    cols = []
    for i in range(
        len(
            root.getchildren()[1]
            .getchildren()[0]
            .getchildren()[1]
            .getchildren()[0]
            .getchildren()
        )
    ):
        child = (
            root.getchildren()[1]
            .getchildren()[0]
            .getchildren()[1]
            .getchildren()[0]
            .getchildren()[i]
        )
        data.append(child.text)
        cols.append(child.tag.replace("{http://edwin.eurodw.eu/EDServices/2.3}", ""))

    df = pd.DataFrame(data).T  # Create DataFrame and transpose it
    df.columns = cols  # Update column names
    return df


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start DEAL DETAILS BRONZE job.")
    run_props = set_job_params()
    xml_file = get_raw_files(run_props["SOURCE_DIR"], run_props["FILE_KEY"])
    final_df = create_dataframe(xml_file)
    (
        final_df.format("parquet")
        .mode("append")
        .save("../dataoutput/bronze/deal_details/info.parquet")
    )
    return


if __name__ == "__main__":
    main()
