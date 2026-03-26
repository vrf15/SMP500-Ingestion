# Weekly FMP IT Market Cap Ingestion Orchestrator

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
    dag_id="smp500_fmp_it_mktcap_ingestion_dag",
    default_args=default_args,
    description="Weekly FMP IT market cap ingestion to S3 and Postgres",
    start_date=datetime(2026, 3, 17),
    schedule="10 22 * * 1",
    catchup=False,
    tags=["smp500", "fmp", "it", "mktcap", "ingestion"],
) as dag:
    run_fmp_it_mktcap_ingestion = BashOperator(
        task_id="run_fmp_it_mktcap_ingestion",
        bash_command=(
            "cd /opt/airflow/dlt_scripts/smp500_ingestion && "
            "python FMP_IT_mktcap_ingestion.py"
        ),
    )

    run_fmp_it_mktcap_ingestion