import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator

default_args = {
    "owner": "dev",
    "start_date": datetime(2020, 9, 27),
    "end_date": datetime(2020, 9, 30),
    "depends_on_past": False,
}
dag = DAG(
    dag_id="gcs_to_s3",
    default_args=default_args,
    schedule_interval="@daily"
)
t1 = GoogleCloudStorageToS3Operator(
    task_id="copy_data",
    bucket="img_objects_to_audio",
    google_cloud_storage_conn_id="gcs_conn",
    dest_aws_conn_id="aws_conn",
    dest_s3_key="s3://img-objects-to-audio/",
    dag=dag,
)
t2 = EmailOperator(
    task_id="send_conformation_mail",
    to="target_email_address@gmail.com",
    subject="data uploaded from gcs to s3",
    html_content="<h2>transfer operation is complated </h2>",
    dag=dag,
)

t1 >> t2

