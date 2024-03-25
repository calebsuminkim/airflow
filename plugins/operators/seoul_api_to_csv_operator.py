from typing import Any
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt', 'crtr_date') # 대기오염 현황은 날짜 정보가 하나 더 필요해서 ctrt_date를 추가

    def __init__(self, dataset_nm, path, file_name, base_dt=None, crtr_date=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        
        # crtr_date가 있는 경우 endpoint를 다르게 주기 위한 추가 로직
        # if crtr_date is not None:
        #     self.endpoint = "/".join(['{{var.value.apikey_openapi_seoul_go_kr}}', 'json', dataset_nm, crtr_date])
        # else:
        #     self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
            
        
        self.crtr_date = crtr_date
        self.base_dt = base_dt
        

    def execute(self, context):
        import os
        
        self.log.info(f'엔드포인트:{self.endpoint}') # 추가

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path): # 해당 패스가 없으면 디렉토리 생성
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {
            'Content-Type':'application/json',
            'charset':'utf-8',
            'Accept':'*/*'
        }

        request_url = f'{base_url}/{start_row}/{end_row}'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        
        # ctrt_date매개변수를 위한 추가 로직
        if self.crtr_date is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.crtr_date}'

        response = requests.get(request_url, headers) # 리턴 형식 : string
        contents = json.loads(response.text) # 리턴 형식 : dictionary

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df
            
