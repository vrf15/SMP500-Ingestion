# Daily TDAT SP MidCap 400 Ingestion Orchestrator

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

# DAG definition — single DAG for all 400 MidCap tickers, ~100 min runtime at 15s/ticker
with DAG(
    dag_id="smp500_tdat_ingestion_dag",
    default_args=default_args,
    description="Daily TDAT SP MidCap 400 ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 19, tzinfo=pendulum.timezone("America/New_York")),
    schedule="45 17 * * 1-5",
    catchup=False,
    tags=["smp500", "tdat", "midcap400", "ingestion"],
) as dag:
    run_tdat_ingestion = BashOperator(
        task_id="run_tdat_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python TDAT_ingestion.py"
        ),
    )

    run_tdat_ingestion