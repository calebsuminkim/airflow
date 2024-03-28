import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id='dags_simple_http_operator_2',
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
            'Content-Type':'application/xml',
            'charset':'utf-8',
            'Accept':'*/*'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='get_hr_data') # SimpleHttpOperator가 가진 데이터를 가져오기
        
        import xml.etree.ElementTree as ET
        import pandas as pd
        from pprint import pprint
        #import json

        #pprint(json.loads(rslt))
        root = ET.fromstring(rslt)
        print('root : ', root)

        col_list = []
        for child in root:
            if child.tag == 'row':
                for i in child:
                    col_list.append(i.tag)
                break

        rslt_df = pd.DataFrame()

        for col in col_list:
            tmp_lst = []
            for row in root.iter(col):
                tmp_lst.append(row.text)
            rslt_df[col] = tmp_lst
        print(rslt_df)

        #print(f'root : {root}, root/tag : {root.tag}, root/attrib : {root.attrib}')
        #for child in root:
        #  print(f'Tag : {child.tag}, Content : {child.text}')
        
        

    get_hr_data >> python_2()