import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    '''서울시 공공데이터 정보'''
    # http://openapi.seoul.go.kr:8088/(인증키)/xml/TnJbhntBassOpen/1/5/
    get_hr_data = SimpleHttpOperator(
        task_id='get_hr_data',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/xml/TnJbhntBassOpen/1/10/',
        method='GET',
        headers={
            'Content-Type':'application/json',
            'charset':'utf-8',
            'Accept':'*/*'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='get_hr_data') # SimpleHttpOperator가 가진 데이터를 가져오기
        import xml.etree.ElementTree as ET
        from pprint import pprint

        
        root = ET.fromstring(rslt)
        print(f'root : {root}, root/tag : {root.tag}, root/atrrib : {root.attrib}')
        for child in root:
            pprint(child.tag, child.atrrib)
        

    get_hr_data >> python_2()