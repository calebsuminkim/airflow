import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from datetime import timedelta

email_str = Variable.get('email_target')
email_lst = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id='dags_sla_email_example',
    start_date=pendulum.datetime(2024, 5, 1, tz='Asia/Seoul'),
    schedule='*/10 * * * *',
    catchup=False,
    default_args={
        'sla' : timedelta(seconds=70),
        'email' : email_lst
    }
) as dag:
    
    task_slp_30s_sla_70s = BashOperator( # sla 70초 점유, 30초 수행
        task_id='task_slp_30s_sla_70s',
        bash_command='sleep 30'
    )

    task_slp_60s_sla_70s = BashOperator( # sla 70초 점유, 60초 수행
        task_id='task_slp_60s_sla_70s',
        bash_command='sleep 60'
    )

    task_slp_10s_sla_70s = BashOperator( # sla 70초 점유, 10초 수행
        task_id='task_slp_10s_sla_70s',
        bash_command='sleep 10'
    )

    task_slp_10s_sla_30s = BashOperator(
        task_id='task_slp_10s_sla_30s', # sla 30초로 명시, 10초 수행
        bash_command='sleep 10',
        sla=timedelta(seconds=30)
    )

    task_slp_30s_sla_70s >> task_slp_60s_sla_70s >> task_slp_10s_sla_70s >> task_slp_10s_sla_30s