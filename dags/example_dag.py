import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="EXAMPLE_DAG",
    schedule="30 1 * * *",
    start_date=datetime.datetime(2023, 4, 24),
    catchup=False,
    max_active_runs=1,
) as dag:

    start_task = DummyOperator(
        task_id='start_task'
    )

    dummy_task_1 = BashOperator(
        task_id='dummy_task_1',
        bash_command='echo "Running task 1"'
    )

    dummy_task_2 = BashOperator(
        task_id='dummy_task_2',
        bash_command='echo "Running task 2"'
    )

    start_task >> dummy_task_1 >> dummy_task_2
