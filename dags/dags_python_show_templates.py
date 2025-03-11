import datetime, pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id = "dags_python_show_templates",
    schedule = '30 9 * * *', # 매일 09시 30분
    start_date = pendulum.datetime(2025, 1, 10, tz = "Asia/Seoul"),
    catchup = True # 24.08.10 일자부터 24.08.28(현재 날짜) 일자 사이의 스케줄들을 한꺼번에 모두 실행
) as dag:
    
    @task(task_id = 'python_task')

    def show_templates(**kwargs):
        from pprint import pprint#구조가 복잡한 JSON 데이터를 디버깅 용도로 출력할 때 pprint를 자주 사용한다.
        pprint(kwargs) # 파라미터를 안 넘겨도 기본적으로 반환되는 값들이 있는데, Jinja 템플릿에서 제공하는 날짜 파라미터 값들이 여기 포함됨
        
    show_templates()