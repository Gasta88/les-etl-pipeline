"""Runtime configuration resolved from environment variables.

Keeping this in one module means the ETL, the DAGs and the Makefile all read
the same names, and no employer-specific project or bucket is baked into the
code. See ``.env.example`` for the full list.
"""

import os


def _require(name):
    """Return a required environment variable or fail with a clear message."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Copy .env.example to .env and fill it in, or export it in the shell."
        )
    return value


def get_project_id():
    """GCP project id that owns the buckets and Dataproc resources."""
    return _require("GCP_PROJECT_ID")


def get_region():
    """GCP region for Dataproc and other regional resources."""
    return os.getenv("GCP_REGION", "europe-west3")


def get_code_bucket():
    """Bucket where packaged code and JAR dependencies are uploaded."""
    return os.getenv("CODE_BUCKET", f"data-lake-code-v2-{get_project_id()}")


def get_raw_bucket():
    """Bucket holding the raw provider files."""
    return os.getenv("RAW_BUCKET", "my_data")


def get_data_bucket():
    """Bucket holding the bronze/silver Delta and parquet tables."""
    return os.getenv("DATA_BUCKET", "my_data_lake_v2")


def get_phs_bucket():
    """Bucket backing the Spark persistent history server."""
    return os.getenv("PHS_BUCKET", f"spark-hist-repo-{get_project_id()}")


def get_phs_cluster():
    """Name of the Spark persistent history server cluster."""
    return os.getenv("PHS_CLUSTER", f"spark-hist-srv-{get_project_id()}")


def get_metastore_service():
    """Name of the Dataproc Metastore service."""
    return os.getenv("METASTORE_SERVICE", f"data-catalog-{get_project_id()}")
