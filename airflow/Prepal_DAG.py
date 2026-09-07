# DAG workflow to run the extraction, load, and transform
# using the host alias prepal_postgres rather than localhost, ensures that once the Airflow container is live,
# it can immediately orchestrate our parallel ingestion tasks

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.providers.standard.operators.empty import EmptyOperator 
from airflow.providers.standard.operators.bash import BashOperator

#It points where DBT lives
DBT_PROJECT_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dbt")
)

# 1. Error handling: if a task fails it will retry once after 1 minute (useful for demo purposes)
default_args = {
    'owner': 'karin',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# 2. Define the DAG schedule
with DAG(
    dag_id='prepal_medallion_ingestion',
    default_args=default_args,
    description='Orchestrates PrePal Bronze ingestion, then triggers the dbt Silver/Gold build',
    schedule='0 2 * * *',  # Run daily at 2:00 AM
    start_date=datetime(2026, 7, 17),
    catchup=False,
) as dag:

    # 3. Tasks (What are we running?)
    start = EmptyOperator(
        task_id='start_pipeline'
    )

    extract_sap = SQLExecuteQueryOperator(
        task_id='extract_sap_orders',
        conn_id='prepal_postgres_conn',
        sql="CALL bronze_sap.usp_extract_sap_orders();"
    )

    sync_retail = SQLExecuteQueryOperator(
        task_id='sync_retail_transactions',
        conn_id='prepal_postgres_conn',
        sql="CALL bronze_onprem.usp_sync_retail_transactions();"
    )

    # 4. Bronze is loaded, now build Silver and Gold with dbt.
    # dbt build runs models in dependency order and stops on the first failed test, so a broken transformation never reaches the Gold layer that Power BI reads from.
    transform_with_dbt = BashOperator(
        task_id='transform_with_dbt',
        bash_command=f'cd "{DBT_PROJECT_DIR}" && dbt build --profiles-dir .',
    )

    # 5. Both Bronze loads must finish before dbt can safely build Silver/Gold layers
    start >> [extract_sap, sync_retail] >> transform_with_dbt
