from airflow import DAG
import pendulum
import random # 랜덤 모듈
from airflow.operators.python import PythonOperator
from airflow.decorators import task
with DAG(
    dag_id='dag_python_operator',
    default_args={
        'owner': 'airflow',
        'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Seoul')
    },
    catchup=False, # 누락된 구간을 실행하지 않음
) as dag:
    randint=random.randint(0, 3) # 1~100 사이의 랜덤한 정수
    def select_fruit():
        fruit = ['apple', 'banana', 'cherry']
        print(fruit[randint])

    
    def print_fruit(some_string: str):
        select_fruit()
        print(some_string)
        select_fruit()

 