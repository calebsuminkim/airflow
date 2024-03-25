from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_earthquake_and_airpolution',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''국내/외 지진 데이터'''
    tb_earthquakes_status = SeoulApiToCsvOperator(
        task_id='tb_earthquakes_status',
        dataset_nm='TbEqkKenvinfo',
        path='/opt/airflow/files/TbEqkKenvinfo/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='TbEqkKenvinfo.csv'
    )

    '''일일 대기오염 현황'''
    '''
    예시 : 
    2012년 12월 25일 평균 대기오염도
    http://openAPI.seoul.go.kr:8088/(인증키)/xml/DailyAverageAirQuality/1/5/20121225
    '''
    daily_avg_air_quality = SeoulApiToCsvOperator(
        task_id='daily_avg_air_quality',
        dataset_nm='DailyAverageAirQuality',
        crtr_date='{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}' # 추가 코드 : 데이터 기준 실행 날짜를 기준으로 해당 날짜의 데이터 가져오기 위함
        path='/opt/airflow/files/DailyAverageAirQuality/{{ data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='DailyAverageAirQuality.csv'
    )
    

    tb_earthquakes_status >> daily_avg_air_quality