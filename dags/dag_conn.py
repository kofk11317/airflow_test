from airflow import DAG
import datetime
import pendulum
from airflow.operators.empty import EmptyOperator
with DAG(
    dag_id='dag_conn',
    default_args={
        'owner': 'airflow',
        'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Seoul')
    },
    catchup=False, # 누락된 구간을 실행하지 않음
) as dag:
    t1 = EmptyOperator(
        task_id='task1'
    
    )
    t2 = EmptyOperator(task_id='task2')
    t3 = EmptyOperator(task_id='task3')
    t4 = EmptyOperator(task_id='task4')
    t5 = EmptyOperator(task_id='task5')

    t1 >> t2 >> [t3, t4] >> t5 # 같은 레벨에서 실행되는 경우에는 리스트로