import os
from unittest.mock import MagicMock

from dags.index_pipeline_dag import dag, trigger_fivetran_sync


def test_dag_basic_properties():
    """
    Basic checks:
    - dag_id is correct
    - the task count and names are correct
    - dependency order: sync_fivetran >> dbt_run >> dbt_test
    """
    assert dag.dag_id == "index_etl_pipeline"

    # There should be three tasks
    expected_task_ids = {"sync_fivetran", "dbt_run", "dbt_test"}
    assert set(dag.task_ids) == expected_task_ids

    sync_fivetran = dag.get_task("sync_fivetran")
    dbt_run = dag.get_task("dbt_run")
    dbt_test = dag.get_task("dbt_test")

    # Dependency check
    assert dbt_run in sync_fivetran.downstream_list
    assert dbt_test in dbt_run.downstream_list


def test_trigger_fivetran_skip_when_no_env(monkeypatch):
    """
    When env vars are missing, the function should simply return without raising.
    """
    # Ensure related env vars are cleared
    monkeypatch.delenv("FIVETRAN_API_KEY", raising=False)
    monkeypatch.delenv("FIVETRAN_API_SECRET", raising=False)
    monkeypatch.delenv("FIVETRAN_CONNECTOR_ID", raising=False)

    # Passing as long as no exception is raised
    trigger_fivetran_sync()


def test_trigger_fivetran_with_env_calls_api(monkeypatch):
    """
    With env vars configured:
    - requests.post should be called once
    - the URL should contain connector_id
    - auth should use (api_key, api_secret)
    """
    # Set fake environment variables
    monkeypatch.setenv("FIVETRAN_API_KEY", "dummy_key")
    monkeypatch.setenv("FIVETRAN_API_SECRET", "dummy_secret")
    monkeypatch.setenv("FIVETRAN_CONNECTOR_ID", "dummy_connector")

    # Mock requests.post to avoid a real call to Fivetran
    from dags.index_pipeline_dag import requests as requests_module

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"success": True}

    mock_post = MagicMock(return_value=mock_resp)
    monkeypatch.setattr(requests_module, "post", mock_post)

    # Call the function under test
    trigger_fivetran_sync()

    # Verify post was called once correctly
    mock_post.assert_called_once()
    called_url, called_kwargs = mock_post.call_args
    url = called_url[0]
    auth = called_kwargs.get("auth")

    assert "dummy_connector" in url
    assert auth == ("dummy_key", "dummy_secret")
