import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': "dev",
    'start_date': datetime(2020, 9, 23),
    'end_date': datetime(2020, 9, 30),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10)
}


dag = DAG(
    dag_id="my-bq-task",
    default_args=default_args,
    schedule_interval='@daily'
)
filepath = "gs://backup-data-2020/" + 'out_' + datetime.now().strftime("%d_%b_%Y:%H:%M:%S.%f")+'.csv'

t1 = BigQueryCheckOperator(
    bigquery_conn_id="my-bq-conn",
    task_id="check_for_table",
    dag=dag,
    use_legacy_sql=False,
    sql="""select count(*) from `tech-289406.sample.covid`"""
)

t2 = BigQueryToCloudStorageOperator(
    task_id="dataset_to_gcs",
    source_project_dataset_table="sample.covid",
    destination_cloud_storage_uris=[filepath],
    export_format="CSV",
    field_delimiter=',',
    bigquery_conn_id="my-bq-conn",
    dag=dag,
)


t1 >> t2



