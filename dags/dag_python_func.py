from airflow import DAG
import pendulum
import random # 랜덤 모듈
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp_client
with DAG(
    dag_id='dag_python_func',
    default_args={
        'owner': 'airflow',
        'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Seoul')
    },
    catchup=False, # 누락된 구간을 실행하지 않음
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp_client
    )