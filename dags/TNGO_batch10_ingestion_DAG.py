# Daily TNGO Batch 10 Ingestion Orchestrator

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

# DAG definition... we're trying out EST/EDT conversions this time... crossing fingers!
with DAG(
    dag_id="smp500_tngo_batch10_ingestion_dag",
    default_args=default_args,
    description="Daily TNGO Batch 1 ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 17, tzinfo=pendulum.timezone("America/New_York")),
    schedule="45 2 * * 2-6",
    catchup=False,
    tags=["smp500", "tngo", "batch10", "ingestion"],
) as dag:
    run_tngo_batch10_ingestion = BashOperator(
        task_id="run_tngo_batch10_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python TNGO_batch10_ingestion.py"
        ),
    )

    run_tngo_batch10_ingestion