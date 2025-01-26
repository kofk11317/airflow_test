from airflow import DAG
import datetime
import pendulum
with DAG(
    dag_id='dags_test',
    default_args={
        'owner': 'airflow',
        'start_date': pendulum.datetime(2020, 1, 1, tz='Asia/Seoul'),
        'end_date': pendulum.datetime(2020, 1, 2,tz='Asia/Seoul'),
    },

    schedule_interval='@daily',
    catchup=False, # 누락된 구간을 실행하지 않음
) as dag:
    pass