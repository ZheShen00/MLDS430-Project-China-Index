from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os

# ---------- Common Settings ----------
default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------- Task 1: Trigger Fivetran Sync ----------
def trigger_fivetran_sync():
    """
    Call the Fivetran API to force a one-time S3 -> Snowflake sync.

    Required environment variables:
      - FIVETRAN_API_KEY
      - FIVETRAN_API_SECRET
      - FIVETRAN_CONNECTOR_ID   (the ID of your S3 connector)
    If they are missing, just print a message to avoid a DAG import error.
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


# ---------- DAG Definition ----------
with DAG(
    dag_id="index_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    description="Daily index ETL: Fivetran (S3 -> Snowflake) -> dbt (stg/dim/fact).",
    tags=["index", "etl"],
) as dag:

    # Task 1: Fivetran sync
    sync_fivetran = PythonOperator(
        task_id="sync_fivetran",
        python_callable=trigger_fivetran_sync,
    )

    # Path to the dbt project: in the container, use /usr/local/airflow/dags/index_dbt
    DBT_PROJECT_DIR = "/usr/local/airflow/dbt/index_dbt"

    # Task 2: run dbt to build stg_index_daily / dim_date / fact_index_daily
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir ."
        ),
    )

    # Task 3: run dbt tests defined in schema.yml
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt test --profiles-dir ."
        ),
    )

    # Dependencies: Fivetran sync -> dbt run -> dbt test
    sync_fivetran >> dbt_run >> dbt_test

