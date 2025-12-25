import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="example_dag_v3",
    schedule=None,
    catchup=False,
) as dag:
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = BashOperator(task_id="task_2", bash_command="echo 1")
    task_3 = EmptyOperator(task_id="task_3")

    task_1 >> task_2 >> task_3
