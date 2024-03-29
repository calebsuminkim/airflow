from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
import datetime
import pendulum

with DAG(
    dag_id='dags_empty_with_edge_label',
    schedule=None,
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    empty_1 = EmptyOperator(
        task_id='empty_1'
    )

    empty_2 = EmptyOperator(
        task_id='empty_2'
    )

    empty_3 = EmptyOperator(
        task_id='empty_3'
    )

    empty_4 = EmptyOperator(
        task_id='empty_4'
    )

    empty_5 = EmptyOperator(
        task_id='empty_5'
    )

    empty_6 = EmptyOperator(
        task_id='empty_6'
    )


    empty_1 >> Label('1과 2사이') >> empty_2
    
    empty_2 >> Label('Start Branch_1') >> empty_3
    empty_2 >> Label('Start Branch_2') >> empty_4
    empty_2 >> Label('Start Branch_3') >> empty_5
    
    empty_3 >> Label('End Branch_1') >> empty_6
    empty_4 >> Label('End Branch_2') >> empty_6
    empty_5 >> Label('End Branch_3') >> empty_6