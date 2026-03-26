# Daily FMP COMS Ingestion Orchestrator

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
    dag_id="smp500_fmp_coms_ingestion_dag",
    default_args=default_args,
    description="Daily FMP COMS ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 8),
    schedule="1 22 * * 1-5",
    catchup=False,
    tags=["smp500", "fmp", "coms", "ingestion"],
) as dag:
    run_fmp_coms_ingestion = BashOperator(
        task_id="run_fmp_coms_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion/orchestrated/FMP/Ingestion && "
            "python FMP_COMS_ingestion.py"
        ),
    )

    run_fmp_coms_ingestion