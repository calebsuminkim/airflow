from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'doc0920ehrms@gmail.com',
        subject='팀원 확정 축하드립니다.',
        html_content='추카추카추'
    )