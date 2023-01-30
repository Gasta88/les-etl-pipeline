from google.cloud import storage
from uuid import uuid1
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import XCom

# Var definitions
PROJECT_ID = "dataops-369610"
REGION = "europe-west3"
CODE_BUCKET = "data-lake-code-847515094398"
RAW_BUCKET = "fgasta_test_raw"
DATA_BUCKET = "fgasta_data_lake_test"
PHS_CLUSTER = "spark-hist-srv-dataops-369610"
METASTORE_CLUSTER = "data-catalog-dataops-369610"

SUBNETWORK_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
PYTHON_FILE_LOCATION = f"gs://{CODE_BUCKET}/dist/main.py"
PHS_CLUSTER_PATH = f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{PHS_CLUSTER}"
SPARK_DELTA_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-core_2.13-2.1.0.jar"
SPARK_DELTA_STORE_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar"
PY_FILES = f"gs://{CODE_BUCKET}/dist/loan_etl_pipeline_0.1.0.zip"
METASTORE_SERVICE_LOCATION = (
    f"projects/{PROJECT_ID}/locations/{REGION}/services/{METASTORE_CLUSTER}"
)

ENVIRONMENT_CONFIG = {
    "execution_config": {"subnetwork_uri": "default"},
    "peripherals_config": {
        "metastore_service": METASTORE_SERVICE_LOCATION,
        "spark_history_server_config": {
            "dataproc_cluster": PHS_CLUSTER_PATH,
        },
    },
}

RUNTIME_CONFIG = {
    "properties": {
        "spark.app.name": "loan_etl_pipeline",
        "spark.executor.instances": "4",
        "spark.driver.cores": "8",
        "spark.executor.cores": "8",
        "spark.executor.memory": "16g",
    },
    "version": "2.0",
}


@provide_session
def cleanup_xcom(session=None, **kwargs):
    dag = kwargs["dag"]
    dag_id = dag.dag_id
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


def get_raw_prefixes():
    """
    Retrive refixes from raw bucket to start a DAG in it.
    """
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(RAW_BUCKET)
    raw_prefixes = list(
        set(
            [
                "/".join(b.name.split("/")[:-1])
                for b in storage_client.list_blobs(
                    # bucket.name, prefix="edw_data/downloaded-data/SME"
                    bucket.name,
                    prefix="mini_source",
                )
                if b.name.endswith(".csv")
            ]
        )
    )
    return raw_prefixes


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
}
with models.DAG(
    "delta_lake_etl",  # The id you will see in the DAG airflow page
    default_args=default_args,  # The interval with which to schedule the DAG
    schedule_interval=None,  # Override to match your needs
    on_success_callback=cleanup_xcom,
) as dag:
    run_id = str(uuid1())
    raw_prefixes = get_raw_prefixes()
    for rp in raw_prefixes:
        ed_code = rp.split("/")[-1]
        # # DEBUG
        # if ed_code == "SMEMBE000095100220092":
        start = EmptyOperator(task_id=f"{ed_code}_start")
        with TaskGroup(group_id=f"{ed_code}_assets") as assets_tg:
            assets_bronze_profile_task = DataprocCreateBatchOperator(
                task_id=f"assets_bronze_profile_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--file-key=Loan_Data",
                            "--stage-name=profile_bronze_asset",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"profile-bronze-asset-{ed_code.lower()}-{run_id}",
            )
            assets_bronze_task = DataprocCreateBatchOperator(
                task_id=f"assets_bronze_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--target-prefix=SME/bronze/assets",
                            "--file-key=Loan_Data",
                            "--stage-name=bronze_asset",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"bronze-assets-{ed_code.lower()}-{run_id}",
            )
            assets_silver_task = DataprocCreateBatchOperator(
                task_id=f"assets_silver_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            "--source-prefix=SME/bronze/assets",
                            "--target-prefix=SME/silver/assets",
                            f"--ed-code={ed_code}",
                            "--stage-name=silver_asset",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"silver-assets-{ed_code.lower()}-{run_id}",
            )
            assets_bronze_profile_task >> assets_bronze_task >> assets_silver_task
        with TaskGroup(group_id=f"{ed_code}_collaterals") as collaterals_tg:
            collateral_bronze_profile_task = DataprocCreateBatchOperator(
                task_id=f"collateral_bronze_profile_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--file-key=Collateral",
                            "--stage-name=profile_bronze_collateral",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"profile-bronze-collateral-{ed_code.lower()}-{run_id}",
            )
            collateral_bronze_task = DataprocCreateBatchOperator(
                task_id=f"collateral_bronze_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--target-prefix=SME/bronze/collaterals",
                            "--file-key=Collateral",
                            "--stage-name=bronze_collateral",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"create-bronze-collaterals-{ed_code.lower()}-{run_id}",
            )
            collateral_silver_task = DataprocCreateBatchOperator(
                task_id=f"collateral_silver_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            "--source-prefix=SME/bronze/collaterals",
                            "--target-prefix=SME/silver/collaterals",
                            f"--ed-code={ed_code}",
                            "--stage-name=silver_collateral",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"create-silver-collaterals-{ed_code.lower()}-{run_id}",
            )
            (
                collateral_bronze_profile_task
                >> collateral_bronze_task
                >> collateral_silver_task
            )
        with TaskGroup(group_id=f"{ed_code}_bond_info") as bond_info_tg:
            bond_info_bronze_profile_task = DataprocCreateBatchOperator(
                task_id=f"bond_info_bronze_profile_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--file-key=Bond_Info",
                            "--stage-name=profile_bronze_bond_info",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"profile-bronze-bond-info-{ed_code.lower()}-{run_id}",
            )
            bond_info_bronze_task = DataprocCreateBatchOperator(
                task_id=f"bond_info_bronze_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            f"--source-prefix=mini_source/{ed_code}",
                            "--target-prefix=SME/bronze/bond_info",
                            "--file-key=Bond_Info",
                            "--stage-name=bronze_bond_info",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"create-bronze-bond-info-{ed_code.lower()}-{run_id}",
            )
            bond_info_silver_task = DataprocCreateBatchOperator(
                task_id=f"bond_info_silver_{ed_code}",
                batch={
                    "pyspark_batch": {
                        "main_python_file_uri": PYTHON_FILE_LOCATION,
                        "jar_file_uris": [
                            SPARK_DELTA_JAR_FILE,
                            SPARK_DELTA_STORE_JAR_FILE,
                        ],
                        "python_file_uris": [PY_FILES],
                        "args": [
                            f"--project={PROJECT_ID}",
                            f"--raw-bucketname={RAW_BUCKET}",
                            f"--data-bucketname={DATA_BUCKET}",
                            "--source-prefix=SME/bronze/bond_info",
                            "--target-prefix=SME/silver/bond_info",
                            f"--ed-code={ed_code}",
                            "--stage-name=silver_bond_info",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"create-silver-bond-info-{ed_code.lower()}-{run_id}",
            )
            (
                bond_info_bronze_profile_task
                >> bond_info_bronze_task
                >> bond_info_silver_task
            )
        end = EmptyOperator(task_id=f"{ed_code}_end")
        start >> [assets_tg, collaterals_tg, bond_info_tg] >> end
