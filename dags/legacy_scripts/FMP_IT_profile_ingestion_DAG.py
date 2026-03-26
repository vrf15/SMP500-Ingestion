# FMP IT profile ingestion (1st and 15th of the month)

# Package imports
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# DAG definition
with DAG(
    dag_id="smp500_fmp_it_profile_ingestion_dag",
    default_args=default_args,
    description="Bi-monthly FMP IT profile ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 17),
    schedule="15 22 1,15 * *",
    catchup=False,
    tags=["smp500", "fmp", "it", "profile", "ingestion"],
) as dag:
    run_fmp_it_profile_ingestion = BashOperator(
        task_id="run_fmp_it_profile_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python FMP_IT_profile_ingestion.py"
        ),
    )

    run_fmp_it_profile_ingestion