from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def select_books():
        books = ['NOVEL', 'MAGAZINE', 'FICTION', 'ESSAY']
        rand_int = random.randint(0, 3) # 0~3사이의 정수
        print(books[rand_int])

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_books# 어떤 함수를 실행시킬건지?
    )

    py_t1