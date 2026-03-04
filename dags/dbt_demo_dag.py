"""
Example DAG demonstrating dbt integration with Airflow.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DBT_PROJECT_DIR = Path("/opt/airflow/dbt_projects/demo")
DBT_PROFILES_DIR = DBT_PROJECT_DIR

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dbt_demo",
    default_args=default_args,
    description="Run dbt demo project",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "demo"],
) as dag:

    create_source_table = PostgresOperator(
        task_id="create_source_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS public.users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            deleted_at TIMESTAMP NULL
        );
        
        TRUNCATE TABLE public.users;
        
        INSERT INTO public.users (name, email) VALUES
            ('Alice Smith', 'alice@example.com'),
            ('Bob Jones', 'bob@example.com'),
            ('Charlie Brown', 'charlie@example.com');
        """,
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )

    cleanup = PostgresOperator(
        task_id="cleanup",
        postgres_conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS public.users CASCADE;
        DROP SCHEMA IF EXISTS dbt_demo CASCADE;
        """,
        trigger_rule="all_success",
    )

    create_source_table >> dbt_debug >> dbt_deps >> dbt_run >> dbt_test >> cleanup
