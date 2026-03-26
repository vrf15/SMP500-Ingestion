# Bi-weekly FHUB Basic Financials Ingestion Orchestrator

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

# DAG definition — 1st and 15th of the month, might change this depending on ML applications
with DAG(
    dag_id="smp500_fhub_basic_financials_dag",
    default_args=default_args,
    description="Bi-weekly FHUB Basic Financials ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 20, tzinfo=pendulum.timezone("America/New_York")),
    schedule="30 21 1,15 * *",
    catchup=False,
    tags=["smp500", "fhub", "basic_financials", "ingestion"],
) as dag:
    run_fhub_basic_financials = BashOperator(
        task_id="run_fhub_basic_financials",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion/orchestrated/FHUB/Ingestion && "
            "python FHUB_basic_financials_ingestion.py"
        ),
    )

    run_fhub_basic_financials