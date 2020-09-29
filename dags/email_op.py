import os
import airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator


file_path = os.path.join(os.getcwd(), "dags/gcpsecqurity.py")

default_args = {
    "owner": "dev",
    "depends_on_past": False,
    "start_date": datetime(2020,9,26),
    "end_date": datetime(2020,9,30),
}

dag = DAG(
    default_args=default_args,
    dag_id="send_email",
    schedule_interval="@daily",
)

html = """
    <h1>
        This is a test for email sending through airflow :)
    </h1>
"""

t1 = EmailOperator(
    task_id="send_mail",
    to='target_mail_address@gmail.com',
    subject="email automation airflow",
    html_content=html,
    files=[file_path],
    dag=dag,
)
