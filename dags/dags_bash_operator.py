from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", # 파이썬 파일(지금 이 파일)의 파일명과 일치시키는 걸 권장함
    schedule="0 0 * * *", # 분 시 일 월 요일 -> 매일 0시 0분에 실행
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"), # 한국 기준으로 2021/1/1부터 시작
    catchup=False, # start_date와 현재 날짜 사이 구간을 한번에 돌리지 않음(True는 돌림)
    # dagrun_timeout=datetime.timedelta(minutes=60), # DAG가 60분 이상 돌면 실패
    # tags=["example", "example2"],
    # params={"example_key": "example_value"}, # 태스크들 공통으로 쓸 매개변수들
) as dag:
    # [START howto_operator_bash]
    bash_t1 = BashOperator( # task명 = bash_t1.
        task_id="bash_t1", # task_id또한 task명과 일치시키는 걸 권장한다.
        bash_command="echo whoami",
    )
    # [END howto_operator_bash]

    bash_t2 = BashOperator( # task명 = bash_t1.
        task_id="bash_t2", # task_id또한 task명과 일치시키는 걸 권장한다.
        bash_command="echo $HOSTNAME", # HOSTNAME이라는 환경변수를 출력하라
    )
    # [END howto_operator_bash]

    bash_t1 >> bash_t2 # 태스크의 수행 관계 정의