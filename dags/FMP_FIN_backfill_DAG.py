# FMP FIN Backfill Orchestrator

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

# DAG definition — manual trigger only, toggle off after initial run
with DAG(
    dag_id="smp500_fmp_fin_backfill_dag",
    default_args=default_args,
    description="FMP FIN backfill to S3 and Postgres",
    start_date=datetime(2026, 3, 25),
    schedule=None,
    catchup=False,
    tags=["smp500", "fmp", "fin", "backfill"],
) as dag:
    run_fmp_fin_backfill = BashOperator(
        task_id="run_fmp_fin_backfill",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion/orchestrated/FMP/Backfill && "
            "python FMP_FIN_backfill.py"
        ),
    )

    run_fmp_fin_backfill