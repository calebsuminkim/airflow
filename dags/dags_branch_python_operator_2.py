import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id="dag_branch_python_operator",
    schedule=None,
    catchup=False
) as dag:
    
    def select_random_task():
        import random
        task_list = ['A', 'B', 'C']
        selected_task = random.choice(task_list)
        if selected_task == 'A':
            return 'task_a'
        elif selected_task in ['B', 'C']:
            return ['task_b', 'task_c']
    
    def printing_func(**kwargs):
        print(f"Task selected : {kwargs['task_selected']}")
    
    python_branch_task = BranchPythonOperator(
        task_id="python_branch_task",
        python_callable=select_random_task,
    )

    task_a = PythonOperator(
        task_id="task_a",
        python_callable=printing_func,
        op_kwargs={'task_selected' : 'A'}
    )

    task_b = PythonOperator(
        task_id="task_b",
        python_callable=printing_func,
        op_kwargs={'task_selected' : 'B'}
    )
    
    task_c = PythonOperator(
        task_id="task_c",
        python_callable=printing_func,
        op_kwargs={'task_selected' : 'C'}
    )

    python_branch_task >> [task_a, task_b, task_c]