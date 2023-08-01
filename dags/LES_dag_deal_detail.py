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
CODE_BUCKET = "data-lake-code-v2-847515094398"
RAW_BUCKET = "algoritmica_data"
DATA_BUCKET = "algoritmica_data_lake_v2"
PHS_CLUSTER = "spark-hist-srv-dataops-369610"
METASTORE_CLUSTER = "data-catalog-dataops-369610"

SUBNETWORK_URI = f"projects/{PROJECT_ID}/regions/{REGION}/subnetworks/default"
PYTHON_FILE_LOCATION = f"gs://{CODE_BUCKET}/dist/les_main.py"
PHS_CLUSTER_PATH = f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{PHS_CLUSTER}"
SPARK_DELTA_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-core_2.13-2.1.0.jar"
SPARK_DELTA_STORE_JAR_FILE = f"gs://{CODE_BUCKET}/dependencies/delta-storage-2.2.0.jar"
PY_FILES = f"gs://{CODE_BUCKET}/dist/les_etl_pipeline_0.1.0.zip"
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
        "spark.app.name": "les_etl_pipeline",
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
    Retrieve prefixes from raw bucket to start a DAG in it.
    """
    storage_client = storage.Client(project="dataops-369610")
    bucket = storage_client.get_bucket(RAW_BUCKET)
    blobs = bucket.list_blobs(prefix="edw_data/downloaded-data/LES")
    raw_prefixes = list(
        set([b.name.split("/")[-2] for b in blobs if b.name.endswith(".csv")])
    )
    return raw_prefixes


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
    "region": REGION,
    "retries": 0,
}
with models.DAG(
    "les_deal_details",  # The id you will see in the DAG airflow page
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    on_success_callback=cleanup_xcom,
    on_failure_callback=cleanup_xcom,
    max_active_tasks=20,
) as dag:
    raw_prefixes = get_raw_prefixes()
    for rp in raw_prefixes:
        ed_code = rp.split("/")[-1]

        # # DEBUG
        # if "LESSES" not in ed_code:
        #     continue
        start = EmptyOperator(task_id=f"{ed_code}_start")
        # deal details TaskGroup
        with TaskGroup(group_id=f"{ed_code}_deal_details") as tg:
            bronze_task = DataprocCreateBatchOperator(
                task_id=f"bronze_{ed_code}",
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
                            f"--source-prefix=edw_data/downloaded-data/LES/{ed_code}",
                            "--target-prefix=LES/bronze/deal_details",
                            "--file-key=Deal_Details",
                            "--stage-name=bronze_deal_details",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"{ed_code.lower()}-deal-details-bronze",
            )
            silver_task = DataprocCreateBatchOperator(
                task_id=f"silver_{ed_code}",
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
                            "--source-prefix=LES/bronze/deal_details",
                            "--target-prefix=LES/silver/deal_details",
                            f"--ed-code={ed_code}",
                            "--stage-name=silver_deal_details",
                        ],
                    },
                    "environment_config": ENVIRONMENT_CONFIG,
                    "runtime_config": RUNTIME_CONFIG,
                },
                batch_id=f"{ed_code.lower()}-deal-details-silver",
            )
            (bronze_task >> silver_task)
        # clean-up TaskGroup
        with TaskGroup(group_id=f"{ed_code}_clean_up") as clean_up_tg:
            delete_bronze = DataprocDeleteBatchOperator(
                task_id=f"delete_bronze_{ed_code}",
                project_id=PROJECT_ID,
                region=REGION,
                batch_id=f"{ed_code.lower()}-deal-details-bronze",
            )
            delete_silver = DataprocDeleteBatchOperator(
                task_id=f"delete_silver_{ed_code}",
                project_id=PROJECT_ID,
                region=REGION,
                batch_id=f"{ed_code.lower()}-deal-details-silver",
            )
        end = EmptyOperator(task_id=f"{ed_code}_end")
        (start >> tg >> clean_up_tg >> end)
