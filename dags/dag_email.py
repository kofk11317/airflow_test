from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator
with DAG(
    dag_id='dag_email',
    schedule="0 8 28 * *",    # 매월 1일 오전 8시에 실행
    catchup=False, # 누락된 구간을 실행하지 않음
    start_date=pendulum.datetime(2025, 1, 1, tz='Asia/Seoul')
    ) as dag:
        send_email = EmailOperator(
            task_id='send_email',
            to="kofk113@naver.com",
            subject="Hello, Airflow!", # 제목
            html_content="<h1>Hello, Airflow!</h1>"# 내용
        )