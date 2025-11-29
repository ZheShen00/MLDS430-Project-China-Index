from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os

# ---------- 通用配置 ----------
default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------- Task 1: 触发 Fivetran 同步 ----------
def trigger_fivetran_sync():
    """
    调用 Fivetran API，强制触发一次 S3 -> Snowflake 的同步。

    需要在环境变量中配置：
      - FIVETRAN_API_KEY
      - FIVETRAN_API_SECRET
      - FIVETRAN_CONNECTOR_ID   （就是你 S3 这个 connector 的 ID）
    如果没配，就直接 print 一句，避免 DAG import error。
    """
    api_key = os.getenv("FIVETRAN_API_KEY")
    api_secret = os.getenv("FIVETRAN_API_SECRET")
    connector_id = os.getenv("FIVETRAN_CONNECTOR_ID")

    if not (api_key and api_secret and connector_id):
        print("Fivetran env vars not set, skip real API call.")
        return

    url = f"https://api.fivetran.com/v1/connectors/{connector_id}/force"
    resp = requests.post(url, auth=(api_key, api_secret))
    resp.raise_for_status()
    print("Triggered Fivetran sync:", resp.json())


# ---------- DAG 定义 ----------
with DAG(
    dag_id="index_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",          # ✅ 新版本用 schedule，而不是 schedule_interval
    catchup=False,
    description="Daily index ETL: Fivetran (S3 -> Snowflake) -> dbt (stg/dim/fact).",
    tags=["index", "etl"],
) as dag:

    # Task 1: Fivetran 同步
    sync_fivetran = PythonOperator(
        task_id="sync_fivetran",
        python_callable=trigger_fivetran_sync,
    )

    # dbt 项目的路径：容器里建议用 /usr/local/airflow/dags/index_dbt
    DBT_PROJECT_DIR = "/usr/local/airflow/dbt/index_dbt"

    # Task 2: 跑 dbt run，构建 stg_index_daily / dim_date / fact_index_daily
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir ."
        ),
    )

    # Task 3: 跑 dbt test，跑 schema.yml 里的 tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir ."
        ),
    )

    # 依赖关系：先同步 Fivetran，再跑 dbt run，再跑 dbt test
    sync_fivetran >> dbt_run >> dbt_test
