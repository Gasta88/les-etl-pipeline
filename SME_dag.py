from google.cloud import storage
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.decorators import task

# Prod Var definitions
PROJECT_ID = "dataops-369610"
REGION = "europe-west3"
CODE_BUCKET = "data-lake-code-847515094398"
RAW_BUCKET = "algoritmica_data"
DATA_BUCKET = "algoritmica_data_lake"
# DATA_BUCKET = "fgasta_data_lake_test"
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


@task.python(trigger_rule="all_done")
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
                    bucket.name,
                    prefix="edw_data/downloaded-data/SME",
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
    "retries": 1,
}
with models.DAG(
    "delta_lake_etl",  # The id you will see in the DAG airflow page
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    on_success_callback=cleanup_xcom,
    on_failure_callback=cleanup_xcom,
    max_active_tasks=11,
) as dag:
    from uuid import uuid1
    import datetime

    raw_prefixes = get_raw_prefixes()
    for rp in raw_prefixes:
        ed_code = rp.split("/")[-1]
        ingestion_date = datetime.date.today().strftime("%Y-%m-%d")
        # Unique batch ids per tasks
        assets_profile_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        assets_bronze_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        assets_silver_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        bond_info_profile_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        bond_info_bronze_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        bond_info_silver_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        collaterals_profile_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        collaterals_bronze_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        collaterals_silver_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        deal_details_bronze_batch_id = f"{ed_code.lower()}-{str(uuid1())}"
        deal_details_silver_batch_id = f"{ed_code.lower()}-{str(uuid1())}"

        # DEBUG
        if "SMESES" not in ed_code:
            continue
        start = EmptyOperator(task_id=f"{ed_code}_start")
        # assets TaskGroup
        with TaskGroup(group_id=f"{ed_code}_assets") as assets_tg:
            assets_profile_task = DataprocCreateBatchOperator(
                task_id=f"assets_profile_{ed_code}",
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--file-key=Loan_Data",
                            "--stage-name=profile_bronze_asset",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=assets_profile_batch_id,
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--target-prefix=SME/bronze/assets",
                            "--file-key=Loan_Data",
                            "--stage-name=bronze_asset",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=assets_bronze_batch_id,
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
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=assets_silver_batch_id,
            )
            assets_profile_task >> assets_bronze_task >> assets_silver_task
        # collaterlas TaskGroup
        with TaskGroup(group_id=f"{ed_code}_collaterals") as collaterals_tg:
            collateral_profile_task = DataprocCreateBatchOperator(
                task_id=f"collateral_profile_{ed_code}",
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--file-key=Collateral",
                            "--stage-name=profile_bronze_collateral",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=collaterals_profile_batch_id,
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--target-prefix=SME/bronze/collaterals",
                            "--file-key=Collateral",
                            "--stage-name=bronze_collateral",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=collaterals_bronze_batch_id,
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
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=collaterals_silver_batch_id,
            )
            (
                collateral_profile_task
                >> collateral_bronze_task
                >> collateral_silver_task
            )
        # bond_info TaskGroup
        with TaskGroup(group_id=f"{ed_code}_bond_info") as bond_info_tg:
            bond_info_profile_task = DataprocCreateBatchOperator(
                task_id=f"bond_info_profile_{ed_code}",
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--file-key=Bond_Info",
                            "--stage-name=profile_bronze_bond_info",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=bond_info_profile_batch_id,
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--target-prefix=SME/bronze/bond_info",
                            "--file-key=Bond_Info",
                            "--stage-name=bronze_bond_info",
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=bond_info_bronze_batch_id,
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
                            f"--ingestion-date={ingestion_date}",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=bond_info_silver_batch_id,
            )
            (bond_info_profile_task >> bond_info_bronze_task >> bond_info_silver_task)
        # deal details TaskGroup
        with TaskGroup(group_id=f"{ed_code}_deal_details") as deal_details_tg:
            deal_details_bronze_task = DataprocCreateBatchOperator(
                task_id=f"deal_details_bronze_{ed_code}",
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
                            f"--source-prefix=edw_data/downloaded-data/SME/{ed_code}",
                            "--target-prefix=SME/bronze/deal_details",
                            "--file-key=Deal_Details",
                            "--stage-name=bronze_deal_details",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=deal_details_bronze_batch_id,
            )
            deal_details_silver_task = DataprocCreateBatchOperator(
                task_id=f"deal_details_silver_{ed_code}",
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
                            "--source-prefix=SME/bronze/deal_details",
                            "--target-prefix=SME/silver/deal_details",
                            f"--ed-code={ed_code}",
                            "--stage-name=silver_deal_details",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=deal_details_silver_batch_id,
            )
            deal_details_bronze_task >> deal_details_silver_task
        # clean-up TaskGroup
        # with TaskGroup(group_id=f"{ed_code}_clean_up") as clean_up_tg:
        #     delete_assets_profile = DataprocDeleteBatchOperator(
        #         task_id=f"delete_assets_profile_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=assets_profile_batch_id,
        #     )
        #     delete_assets_bronze = DataprocDeleteBatchOperator(
        #         task_id=f"delete_assets_bronze_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=assets_bronze_batch_id,
        #     )
        #     delete_assets_silver = DataprocDeleteBatchOperator(
        #         task_id=f"delete_assets_silver_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=assets_silver_batch_id,
        #     )
        #     delete_collaterals_profile = DataprocDeleteBatchOperator(
        #         task_id=f"delete_collaterals_profile_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=collaterals_profile_batch_id,
        #     )
        #     delete_collaterals_bronze = DataprocDeleteBatchOperator(
        #         task_id=f"delete_collaterals_bronze_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=collaterals_bronze_batch_id,
        #     )
        #     delete_collaterals_silver = DataprocDeleteBatchOperator(
        #         task_id=f"delete_collaterals_silver_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=collaterals_silver_batch_id,
        #     )
        #     delete_bond_info_profile = DataprocDeleteBatchOperator(
        #         task_id=f"delete_bond_info_profile_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=bond_info_profile_batch_id,
        #     )
        #     delete_bond_info_bronze = DataprocDeleteBatchOperator(
        #         task_id=f"delete_bond_info_bronze_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=bond_info_bronze_batch_id,
        #     )
        #     delete_bond_info_silver = DataprocDeleteBatchOperator(
        #         task_id=f"delete_bond_info_silver_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=bond_info_silver_batch_id,
        #     )
        #     delete_deal_details_bronze = DataprocDeleteBatchOperator(
        #         task_id=f"delete_deal_details_bronze_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=deal_details_bronze_batch_id,
        #     )
        #     delete_deal_details_silver = DataprocDeleteBatchOperator(
        #         task_id=f"delete_deal_details_silver_{ed_code}",
        #         project_id=PROJECT_ID,
        #         #region=REGION,
        #         batch_id=deal_details_silver_batch_id,
        #     )
        end = EmptyOperator(task_id=f"{ed_code}_end")
        (
            start
            >> [assets_tg, collaterals_tg, bond_info_tg, deal_details_tg]
            # >> clean_up_tg
            >> end
        )
