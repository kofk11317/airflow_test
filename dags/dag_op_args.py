from airflow import DAG
import pendulum
from airflow.decorators import task
from common.common_func import register
with DAG(
    dag_id='dag_task_decorator',
    schedule='@daily',
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    catchup=False, # 누락된 구간을 실행하지 않음

 ) as dag:
    @task(task_id='task1')
    def print_context(text):
        print(text)
    #task id = 실행함수    
    task1=register(["hjkim",'man','kr'])

    