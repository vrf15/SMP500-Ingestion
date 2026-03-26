# TDAT SP MidCap 400 Backfill Orchestrator

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

# DAG definition — manual trigger only, toggle off after initial run
with DAG(
    dag_id="smp500_tdat_backfill_dag",
    default_args=default_args,
    description="TDAT SP MidCap 400 backfill to S3 and Postgres",
    start_date=datetime(2026, 3, 25, tzinfo=pendulum.timezone("America/New_York")),
    schedule=None,
    catchup=False,
    tags=["smp500", "tdat", "midcap400", "backfill"],
) as dag:
    run_tdat_backfill = BashOperator(
        task_id="run_tdat_backfill",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion/orchestrated/TDAT/Backfill && "
            "python TDAT_backfill.py"
        ),
    )

    run_tdat_backfill