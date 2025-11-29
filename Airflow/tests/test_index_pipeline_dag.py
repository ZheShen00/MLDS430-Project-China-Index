import os
from unittest.mock import MagicMock

from dags.index_pipeline_dag import dag, trigger_fivetran_sync


def test_dag_basic_properties():
    """
    基础检查：
    - dag_id 正确
    - task 数量和名字正确
    - 依赖关系：sync_fivetran >> dbt_run >> dbt_test
    """
    assert dag.dag_id == "index_etl_pipeline"

    # 有三个 task
    expected_task_ids = {"sync_fivetran", "dbt_run", "dbt_test"}
    assert set(dag.task_ids) == expected_task_ids

    sync_fivetran = dag.get_task("sync_fivetran")
    dbt_run = dag.get_task("dbt_run")
    dbt_test = dag.get_task("dbt_test")

    # 依赖关系检查
    assert dbt_run in sync_fivetran.downstream_list
    assert dbt_test in dbt_run.downstream_list


def test_trigger_fivetran_skip_when_no_env(monkeypatch):
    """
    没有配置环境变量时，函数应该直接返回，不抛异常。
    """
    # 确保相关 env 都被清掉
    monkeypatch.delenv("FIVETRAN_API_KEY", raising=False)
    monkeypatch.delenv("FIVETRAN_API_SECRET", raising=False)
    monkeypatch.delenv("FIVETRAN_CONNECTOR_ID", raising=False)

    # 只要不抛异常就算通过
    trigger_fivetran_sync()


def test_trigger_fivetran_with_env_calls_api(monkeypatch):
    """
    配置了环境变量时：
    - 应该调用 requests.post 一次
    - URL 中包含 connector_id
    - 使用 (api_key, api_secret) 做 auth
    """
    # 设置假的环境变量
    monkeypatch.setenv("FIVETRAN_API_KEY", "dummy_key")
    monkeypatch.setenv("FIVETRAN_API_SECRET", "dummy_secret")
    monkeypatch.setenv("FIVETRAN_CONNECTOR_ID", "dummy_connector")

    # mock 掉 requests.post，避免真的调用 Fivetran
    from dags.index_pipeline_dag import requests as requests_module

    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.return_value = {"success": True}

    mock_post = MagicMock(return_value=mock_resp)
    monkeypatch.setattr(requests_module, "post", mock_post)

    # 调用被测函数
    trigger_fivetran_sync()

    # 验证 post 被正确调用了一次
    mock_post.assert_called_once()
    called_url, called_kwargs = mock_post.call_args
    url = called_url[0]
    auth = called_kwargs.get("auth")

    assert "dummy_connector" in url
    assert auth == ("dummy_key", "dummy_secret")
