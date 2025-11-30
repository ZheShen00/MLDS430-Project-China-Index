# China Index Data Platform

This repository contains a small data platform for analyzing Chinese stock indices. It bundles an Airflow orchestration layer, a dbt modeling project on Snowflake, and a Streamlit dashboard for exploration. Sample CSVs are also provided for local experimentation.

## Repository layout
- **Airflow/** – Dockerized Airflow project with a daily DAG that triggers a Fivetran S3→Snowflake sync and runs dbt models/tests. Key files: `docker-compose.override.yml`, `requirements.txt`, and `dags/index_pipeline_dag.py`.
- **index_dbt/** – dbt project targeting Snowflake, with staging/dimension/fact models and profile configuration.
- **streamlit/** – Streamlit analytics app that queries Snowflake and visualizes index performance.
- **data_sample/** – Example CSV extracts of the raw and staged index data.

## Data pipeline (Airflow + Fivetran + dbt)
The Airflow DAG `index_etl_pipeline` coordinates the daily build:
1. **sync_fivetran**: optional Python task to call the Fivetran API and force an S3→Snowflake sync when credentials are provided (`FIVETRAN_API_KEY`, `FIVETRAN_API_SECRET`, `FIVETRAN_CONNECTOR_ID`).
2. **dbt_run**: runs the dbt project located at `/usr/local/airflow/dbt/index_dbt` inside the Airflow container.
3. **dbt_test**: executes the dbt tests defined for the models.

The tasks run sequentially (`sync_fivetran >> dbt_run >> dbt_test`) and the DAG is scheduled daily without catchup. You can start Airflow via Docker Compose from the `Airflow/` directory; ensure the Snowflake and Fivetran environment variables are available to the containers.

## dbt models
The `index_dbt` project models the index data loaded into Snowflake:
- **stg_index_daily**: casts raw S3 ingests (`INDEX_DAILY_2019_2023_FILTERED`) to typed columns, standardizes the date to `trade_date`, and builds a `date_key` plus `unique_key` per index/date.
- **dim_date**: generates a six-year calendar starting in 2019 with year/month/quarter/weekday attributes keyed by `date_key`.
- **fact_index_daily**: joins the staging and date dimension to compute daily return, cumulative return, and 20-day rolling averages/volatility for each index.

Profiles are configured in `index_dbt/profiles.yml` for the Snowflake role `FIVETRAN_ROLE` and use private key authentication via `SNOWFLAKE_PRIVATE_KEY_PATH` and `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`.

To run dbt locally:
```bash
cd index_dbt
export SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/p8.key
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Streamlit dashboard
`streamlit/app.py` is a Streamlit app for interactive analysis:
- Connects to Snowflake using the same private key settings as dbt (`SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`).
- Lets users pick index codes and date ranges, then computes daily and cumulative returns, rolling averages, and volatility in pandas.
- Provides Altair charts and metrics for the selected indices with custom styling.

To run locally:
```bash
cd streamlit
set SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/p8.key
set SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"
streamlit run app.py
```

## Sample data
`data_sample/` includes `index_daily_2019_2023_filtered.csv` (raw-like data) and `STG_INDEX_DAILY.csv` (staged output) that can be used for local testing or loading into Snowflake for development.

## Requirements and environment
- Python dependencies for Airflow workers are listed in `Airflow/requirements.txt`; additional packages can be added via `Airflow/packages.txt` and the Airflow `Dockerfile`.
- Streamlit relies on `streamlit`, `snowflake-connector-python`, `pandas`, `numpy`, and `altair`, which can be installed with `pip install -r Airflow/requirements.txt` or a dedicated virtual environment.
- Ensure Snowflake connectivity (account `azb79167`, role `FIVETRAN_ROLE`, warehouse `FIVETRAN_WAREHOUSE`, database `FIVETRAN_DATABASE`, schema `MLDS430_KOALA_INDEX_RAW`) is available before running dbt or Streamlit.
