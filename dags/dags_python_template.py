from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs): # 첫 번째 방식 : op_kwargs를 이용해 직접 변수를 지정
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date':'{{data_interval_end | ds}}'}
    )

    @task(task_id='python_t2') # 두 번째 방식 : op_kwargs에 이미 들어가 있는 jinja templates들을 꺼내서 쓰기
    def python_function2(**kwargs):
        print(kwargs)
        print('ds : ' + kwargs['ds'])
        print('ts : ' + kwargs['ts'])
        print('data_interval_start : ' + str(kwargs['data_interval_start']))
        print('data_interval_end : ' + str(kwargs['data_interval_end']))
        print('task_instance : ' + str(kwargs['ti']))

    python_t1 >> python_function2() # @task데코레이터를 사용했을 때는 함수만 써놔도 태스크가 생성된다.