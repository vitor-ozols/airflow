from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"],
):
    BashOperator(
        task_id="hello_task",
        bash_command='echo "Airflow funcionando com Docker Compose"',
    )
