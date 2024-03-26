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
        xml_str = "<JBHNT_REQST_NO>H323202403252525</JBHNT_REQST_NO><SEX>남</SEX><AGE>76</AGE><ACDMCR_CMMN_CODE_SE>J00110</ACDMCR_CMMN_CODE_SE><ACDMCR_CMMN_CODE_SE_NM>대학_대학교</ACDMCR_CMMN_CODE_SE_NM><WORK_AREA_CMMN_CODE_SE_1_1>경기</WORK_AREA_CMMN_CODE_SE_1_1><WORK_AREA_CMMN_CODE_SE_1_2>용인시</WORK_AREA_CMMN_CODE_SE_1_2><WORK_AREA_CMMN_CODE_SE_2_1>서울</WORK_AREA_CMMN_CODE_SE_2_1><WORK_AREA_CMMN_CODE_SE_2_2>강남구</WORK_AREA_CMMN_CODE_SE_2_2><HOPE_JSSFC_CMMN_CODE_SE_1>542002</HOPE_JSSFC_CMMN_CODE_SE_1><HOPE_JSSFC_CMMN_CODE_SE_NM_1>건물 경비원(청사,학교,병원,상가,빌딩,공장 등)</HOPE_JSSFC_CMMN_CODE_SE_NM_1><HOPE_JSSFC_CAREER_YY_CO_1>2</HOPE_JSSFC_CAREER_YY_CO_1><HOPE_JSSFC_CAREER_MONTH_CO_1>0</HOPE_JSSFC_CAREER_MONTH_CO_1><HOPE_JSSFC_CAREER_YY_MM_1>2년0월</HOPE_JSSFC_CAREER_YY_MM_1><JBHNT_CRTFC_STTUS_CMMN_SE>J03402</JBHNT_CRTFC_STTUS_CMMN_SE><JBHNT_CRTFC_STTUS_CMMN_SE_NM>승인</JBHNT_CRTFC_STTUS_CMMN_SE_NM><SEARCH_KWRD_1/><SEARCH_KWRD_2/><SEARCH_KWRD_3/><SEARCH_KWRD_4/><SEARCH_KWRD_5/><REGIST_DT_HM>2024-03-25 17:53:45.0</REGIST_DT_HM><UPDT_DT_HM>2024-03-25 17:53:52.0</UPDT_DT_HM>"
        col_list = [elem.replace('/', '').rstrip('>') for elem in xml_str.split("<") if elem.startswith('/')]
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