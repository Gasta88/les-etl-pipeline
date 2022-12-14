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
    config["SOURCE_DIR"] = "../data/mini_source"
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
    all_files = [f for f in glob.glob(f"{source_dir}/*/*{file_key}*.xml")]
    if len(all_files) == 0:
        logger.error(
            f"No files with key {file_key.upper()} found in {source_dir}. Exit process!"
        )
        sys.exit(1)
    else:
        return all_files


def create_source_dataframe(deal_detail_files):
    """
    Read files and generate one PySpark DataFrame from them.

    :param deal_detail_files: files to be read to generate the dataframe.
    :return df: PySpark datafram for loan asset data.
    """
    list_dfs = []
    for xml_f in deal_detail_files:
        xml_data = objectify.parse(xml_f)  # Parse XML data
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
            tag = child.tag.replace("{http://edwin.eurodw.eu/EDServices/2.3}", "")
            if tag == "ISIN":
                # is array
                data.append(";".join(map(str, child.getchildren())))
            elif tag in ["Country", "DealVisibleToOrg", "DealVisibleToUser"]:
                # usually null values
                # TODO: Submissions might have interesting stuff. Ask to Luca.
                continue
            elif tag == "Submissions":
                # get ECBDataQualityScore
                data.append(child.getchildren()[0].getchildren()[3].text)
                tag = "ECBDataQualityScore"
            else:
                data.append(child.text)
            cols.append(tag)

        df = pd.DataFrame(data).T  # Create DataFrame and transpose it
        df.columns = cols  # Update column names
        df["valid_from"] = pd.Timestamp.now()
        df["valid_to"] = None
        df["iscurrent"] = 1
        df["checksum"] = pd.util.hash_pandas_object(
            df["EDCode"]
        )  # Check if you can add "year" and "month"
        list_dfs.append(df)

    return pd.concat(list_dfs, ignore_index=True)


def main():
    """
    Run main steps of the module.
    """
    logger.info("Start DEAL DETAILS BRONZE job.")
    run_props = set_job_params()
    all_xml_files = get_raw_files(run_props["SOURCE_DIR"], run_props["FILE_KEY"])
    raw_deal_details_df = create_source_dataframe(all_xml_files)

    (
        raw_deal_details_df.to_csv(
            "../data/output/bronze/deal_details.csv",
            mode="a",
            header=not os.path.exists("../data/output/SME/bronze/deal_details.csv"),
        )
    )


if __name__ == "__main__":
    main()
