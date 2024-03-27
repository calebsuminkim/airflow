from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_postgres_hook',
    start_date=pendulum.datetime(2024, 3, 1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    # 방법2) Hook을 이용하는 방법.
    def insrt_postgres(postgres_conn_id, **kwargs): # 개선된 부분 : ip, username, passwd같은 매개변수를 받지 않게 됨
        from airflow.providers.postgres.hooks.postgres import PostgresHook # psycopg대신 Hook을 사용
        from contextlib import closing
        
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn: # 커넥션 객체 생성
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into py_opr_drct_insrt values (%s, %s, %s, %s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator( # Hook은 Custom/Python Operator내 함수에서 사용된다.
        task_id = 'insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'} # Admin에서 만들어 놓은 커넥션 이름을 밸류값으로 넘겨준다.
    )

    insrt_postgres_with_hook