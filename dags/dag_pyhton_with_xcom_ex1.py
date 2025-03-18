from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
with DAG(
    dag_id='dag_pyhton_with_xcom_ex1',
    schedule='@daily',
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    catchup=False, # 누락된 구간을 실행하지 않음

 ) as dag:
    @task(task_id='task1')
    def xcom_push(**kwargs):
        return 'good'
    #task id = 실행함수    
    
    
    @task(task_id='task2')
    def xcom_pull_1(**kwargs):
        print(kwargs)
        ti = kwargs['ti']
        value=ti.xcom_pull(task_ids='task1')
        
        print(value)
    
    @task(task_id='task3')
    def xcom_pull_2(string,**kwargs):
       print(string)
    
    task1=xcom_push()
    task2=xcom_pull_1()
    xcom_pull_2(xcom_push())
    task1 >> task2