import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id='dags_cyphers_january_ranking',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    schedule="30 6 * * *",
    catchup=False
) as dag:
    
    def get_info(**kwargs):
        import requests
        from bs4 import BeautifulSoup
        import selenium
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        import numpy as np
        import pandas as pd
        import datetime
        
        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        driver = webdriver.Chrome(options = options)
        
        url = 'https://cyphers.nexon.com/ranking/character/january'
        driver.get(url)
    
        nickNameIn_btn = driver.find_element(By.NAME, 'searchNickName') # 닉네임 입력부분
        nickNameIn_btn.send_keys("금석문")
        search_btn = driver.find_element(By.ID, 'search') # 검색 버튼
        search_btn.click()
        rank_list = driver.find_element(By.ID, 'rank_list')
        rank_list_splited = rank_list.text.split(' ')
        
        info_dict = {
            'rank' : rank_list_splited[0],
            'step' : rank_list_splited[2],
            'name' : rank_list_splited[4],
            'level' : rank_list_splited[5],
            'exp' : rank_list_splited[6]
        }

        driver.quit()
        
        return info_dict
    
    get_info = PythonOperator(
        task_id='get_info',
        python_callable=get_info
    )

    def insrt_postgres(postgres_conn_id, **kwargs): # 개선된 부분 : ip, username, passwd같은 매개변수를 받지 않게 됨
            from airflow.providers.postgres.hooks.postgres import PostgresHook # psycopg대신 Hook을 사용
            from contextlib import closing
            
            postgres_hook = PostgresHook(postgres_conn_id)

            cha_info = get_info() # ****** #

            with closing(postgres_hook.get_conn()) as conn: # 커넥션 객체 생성
                with closing(conn.cursor()) as cursor:
                    nickname = cha_info.get('name')
                    cha_rank = cha_info.get('rank')
                    step = cha_info.get('step')
                    cha_level = cha_info.get('level')
                    cha_exp = cha_info.get('exp')
                    sql = 'insert into cyphers_insrt values (%s, %s, %s, %s, %s);'
                    cursor.execute(sql, (nickname, cha_rank, step, cha_level, cha_exp))
                    conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id = 'insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )

    get_info >> insrt_postgres_with_hook