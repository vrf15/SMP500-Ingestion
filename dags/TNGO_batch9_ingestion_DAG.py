# Daily TNGO Batch 9 Ingestion Orchestrator

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
    dag_id="smp500_tngo_batch9_ingestion_dag",
    default_args=default_args,
    description="Daily TNGO Batch 9 ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 17, tzinfo=pendulum.timezone("America/New_York")),
    schedule="45 1 * * 2-6",
    catchup=False,
    tags=["smp500", "tngo", "batch9", "ingestion"],
) as dag:
    run_tngo_batch9_ingestion = BashOperator(
        task_id="run_tngo_batch9_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python TNGO_batch9_ingestion.py"
        ),
    )

    run_tngo_batch9_ingestion