# Weekly TNGO Batch 9 Backfill Orchestrator

# Package imports
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

# DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# DAG definition — Sunday backfill, toggle off after initial run
with DAG(
    dag_id="smp500_tngo_batch9_backfill_dag",
    default_args=default_args,
    description="Weekly TNGO Batch 9 backfill to S3 and Postgres",
    start_date=datetime(2026, 3, 21, tzinfo=pendulum.timezone("America/New_York")),
    schedule="30 1 * * 1",
    catchup=False,
    tags=["smp500", "tngo", "batch9", "backfill"],
) as dag:
    run_tngo_batch9_backfill = BashOperator(
        task_id="run_tngo_batch9_backfill",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python TNGO_batch9_backfill.py"
        ),
    )

    run_tngo_batch9_backfill