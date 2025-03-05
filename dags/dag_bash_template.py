from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
with DAG(
    dag_id='dag_bash_template',
    default_args={
        'owner': 'airflow',
        'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Seoul')
    },
    catchup=False, # 누락된 구간을 실행하지 않음
) as dag:
    t1 = BashOperator(
        task_id='task1',
        bash_command='echo "Hello World time is :{{data_interval_end}}"',
    )
    t2 = BashOperator(
        task_id='task2',
        env={'START': '{{data_interval_start | ds}}', 'END': '{{data_interval_end | ds}}'
        },
        bash_command='echo "START: $START, END: $END"',
        
    )
 

    t1 >> t2