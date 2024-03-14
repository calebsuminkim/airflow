import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    # 1안 : 오퍼레이터 외부에서 전역변수를 정의
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable: {var_value}"
    )

    # 2안(권장) : 오퍼레이터 내부에서 Jinja템플릿을 이용해 전역 변수를 가져오기
    bash_var_2 = BashOperator(
        task_id="bash_var_2"
        bash_command="echo variable:{{ var.value.sample_key }}"
    )