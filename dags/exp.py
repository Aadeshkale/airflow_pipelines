import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

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
