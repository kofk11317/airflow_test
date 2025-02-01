from airflow import DAG
import pendulum
import random # 랜덤 모듈
from airflow.operators.python_operator import PythonOperator
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
    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )