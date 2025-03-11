from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.python import PythonOperator
with DAG(
    dag_id='dag_pyhton_template',
    schedule='30 9 * * *', # 매일 09시 30분
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul'),
    catchup=False, # 누락된 구간을 실행하지 않음

 ) as dag:
    

    @task(task_id='task1')
    def print_(**kwargs):
        print(f"ds: {kwargs['ds']}")
        print(f'ts: {kwargs["ts"]}')# timestamp
        print(f'next_ds: {kwargs["next_ds"]}')
        print(f'data_interval_start: {kwargs["data_interval_start"]}')
        print(f'data_interval_end: {kwargs["data_interval_end"]}')
        print(f'task_instance: {kwargs["ti"]}')
        
        
    def pyhthon_template(start_date, end_date, **kwargs):

        print('python template')
        print(f'start_date: {start_date} \n end_date: {end_date}')
    t2=PythonOperator(
        task_id='t2',
        python_callable=pyhthon_template,
        op_args={'start_date': '{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}'}
    )
    print_ >> t2