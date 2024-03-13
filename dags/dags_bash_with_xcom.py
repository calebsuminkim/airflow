from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command=
            "echo START &&"
            "echo XCOM_PUSHED"
            "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message') }} && " # xcom_push(키, 밸류)
            "echo COMPLETE" # 마지막줄이 최종 리턴값임.
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'PUSHED_VALUE' : "{{ ti.xcom_pull(key='bash_pushed') }}", # 키값 # xcom_pull(키)
            'RETURN_VALUE' : "{{ ti.xcom_pull(task_ids='bash_push') }}" # 태스크 아이디를 통해 해당 태스크의 리턴값을 찾아온다.
        },
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False # xcom에 푸쉬 여부. 기본값은 True
    )

    bash_push >> bash_pull