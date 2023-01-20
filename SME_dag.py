"""
Examples below show how to use operators for managing Dataproc Serverless batch workloads.
 You use these operators in DAGs that create, delete, list, and get a Dataproc Serverless Spark batch workload.
https://airflow.apache.org/docs/apache-airflow/stable/concepts/variables.html
* project_id is the Google Cloud Project ID to use for the Cloud Dataproc Serverless.
* bucket_name is the URI of a bucket where the main python file of the workload (spark-job.py) is located.
* phs_cluster is the Persistent History Server cluster name.
* image_name is the name and tag of the custom container image (image:tag).
* metastore_cluster is the Dataproc Metastore service name.
* region_name is the region where the Dataproc Metastore service is located.
"""

import datetime
from google.cloud import storage

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
    DataprocDeleteBatchOperator,
    DataprocGetBatchOperator,
    DataprocListBatchesOperator,
)
from airflow.utils.dates import days_ago

# Var definitions
PROJECT_ID = "{{ var.value.project_id }}"
REGION = "{{ var.value.region_name}}"
CODE_BUCKET = "{{ var.value.code_bucketname }}"
RAW_BUCKET = "{{ var.value.raw_bucketname }}"
DATA_BUCKET = "{{ var.value.data_bucketname }}"
PHS_CLUSTER = "{{ var.value.phs_cluster }}"
METASTORE_CLUSTER = "{{var.value.metastore_cluster}}"
SUBNETWORK_URI = "projects/{{ var.value.project_id }}/regions/{{ var.value.region_name}}/subnetworks/default"

PYTHON_FILE_LOCATION = "gs://{{var.value.bucket_name }}/dist/main.py"
PHS_CLUSTER_PATH = "projects/{{ var.value.project_id }}/regions/{{ var.value.region_name}}/clusters/{{ var.value.phs_cluster }}"
SPARK_DELTA_JAR_FILE = (
    "gs://{{ var.value.code_bucketname }}/dependencies/delta-core_2.13-2.1.0.jar"
)
SPARK_DELTA_STORE_JAR_FILE = (
    "gs://{{ var.value.code_bucketname }}/dependencies/delta-storage-2.2.0.jar"
)
PY_FILES = "gs://{{ var.value.code_bucketname }}/dist/loan_etl_pipeline_0.1.0.zip"
METASTORE_SERVICE_LOCATION = "projects/{{var.value.project_id}}/locations/{{var.value.region_name}}/services/{{var.value.metastore_cluster }}"


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
                    bucket.name, prefix="edw_data/downloaded-data/SME"
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
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    raw_prefixes = get_raw_prefixes()
    for rp in raw_prefixes:
        ed_code = rp.split("/")[-1]
        assets_bronze_profile_task = DataprocCreateBatchOperator(
            task_id=f"bronze_profile_{ed_code}",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": PYTHON_FILE_LOCATION,
                    "jar_file_uris": [SPARK_DELTA_JAR_FILE, SPARK_DELTA_STORE_JAR_FILE],
                    "python_file_uris": [PY_FILES],
                    "properties": {
                        "spark.executor.instances": 4,
                        "spark.driver.cores": 8,
                        "spark.executor.cores": 8,
                        "spark.executor.memory": "16g",
                    },
                    "args": [
                        f"--project={PROJECT_ID}",
                        f"--raw-bucketname=${RAW_BUCKET}",
                        f"--data-bucketname=${DATA_BUCKET}",
                        f"--source-prefix=mini_source/${ed_code}",
                        "--file-key=Loan_Data",
                        "--stage-name=profile_bronze_asset",
                    ],
                },
                "environment_config": {
                    "execution_config": {"subnetwork_uri": "default"},
                    "peripherals_config": {
                        "metastore_service": METASTORE_SERVICE_LOCATION,
                        "spark_history_server_config": {
                            "dataproc_cluster": PHS_CLUSTER_PATH,
                        },
                    },
                },
                "runtime_config": {
                    "properties": {"spark.app.name": "loan_etl_pipeline"}
                },
            },
            batch_id=f"batch-bronze-profile-{ed_code}",
        )
        assets_bronze_task = DataprocCreateBatchOperator(
            task_id=f"bronze_{ed_code}",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": PYTHON_FILE_LOCATION,
                    "jar_file_uris": [SPARK_DELTA_JAR_FILE, SPARK_DELTA_STORE_JAR_FILE],
                },
                "environment_config": {
                    "execution_config": {"subnetwork_uri": "default"},
                    "peripherals_config": {
                        "metastore_service": METASTORE_SERVICE_LOCATION,
                        "spark_history_server_config": {
                            "dataproc_cluster": PHS_CLUSTER_PATH,
                        },
                    },
                },
            },
            batch_id=f"create-bronze-tables-{ed_code}",
        )
        assets_silver_task = DataprocCreateBatchOperator(
            task_id=f"silver_{ed_code}",
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": PYTHON_FILE_LOCATION,
                    "jar_file_uris": [SPARK_DELTA_JAR_FILE, SPARK_DELTA_STORE_JAR_FILE],
                },
                "environment_config": {
                    "execution_config": {"subnetwork_uri": "default"},
                    "peripherals_config": {
                        "metastore_service": METASTORE_SERVICE_LOCATION,
                        "spark_history_server_config": {
                            "dataproc_cluster": PHS_CLUSTER_PATH,
                        },
                    },
                },
            },
            batch_id=f"create-silver-tables-{ed_code}",
        )

        assbronze_profile_task >> bronze_task >> silver_task
