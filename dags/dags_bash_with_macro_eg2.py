import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dags_bash_with_macro_eg2",
    schedule = '10 0 * * 6#2', # 매월 둘째 주 토요일 00시 10분 # 실제 배치가 돌아가는 스케줄
                               # 여기서는 2025 1월 1일부터 시작하므로 2025년 1월 11일 00시 10분에 실행
    start_date = pendulum.datetime(2025, 1, 1, tz = "Asia/Seoul"), 
    catchup = False
) as dag:
    
    # START_DATE: 배치스켈줄 기준 2주전 월요일, END_DATE: 배치스켈줄 2주전 토요일
    bash_task_2 = BashOperator(
        task_id = 'bash_task_2',
        env = {
            'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 19)) | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days = 14)) | ds }}'
        },
        bash_command = 'echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )