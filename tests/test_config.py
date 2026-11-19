import pytest

from src.les_etl_pipeline.utils import config


def test_project_id_is_required(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    with pytest.raises(RuntimeError, match="GCP_PROJECT_ID"):
        config.get_project_id()


def test_project_id_comes_from_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "demo-project")
    assert config.get_project_id() == "demo-project"


def test_defaults_are_derived_from_project(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "demo-project")
    for name in ("GCP_REGION", "CODE_BUCKET", "RAW_BUCKET", "DATA_BUCKET"):
        monkeypatch.delenv(name, raising=False)

    assert config.get_region() == "europe-west3"
    assert config.get_code_bucket() == "data-lake-code-v2-demo-project"
    assert config.get_raw_bucket() == "my_data"
    assert config.get_data_bucket() == "my_data_lake_v2"


def test_bucket_overrides_win(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "demo-project")
    monkeypatch.setenv("RAW_BUCKET", "custom-raw")
    assert config.get_raw_bucket() == "custom-raw"
