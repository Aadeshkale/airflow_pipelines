import os
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gcpsecurity.gcp_vm import ExecuteCheckVm

os.path.join(os.getcwd(), "/credentials/my_credentials.json")
SERVICE_ACCOUNT_FILE_PATH = os.getcwd() + '/credentials/my_credentials.json'
PROJECT_ID = "tech-289406"


def check():
    vm = ExecuteCheckVm(servive_account_file_path=SERVICE_ACCOUNT_FILE_PATH, project_id=PROJECT_ID)
    vm_result = vm.perform_check()

    def generate_csv(result):
        import datetime
        now = datetime.datetime.now()
        date_time = now.strftime("%Y_%m_%d_%H_%M_%S")
        filename = 'gcp_security_{}.csv'.format(date_time)
        with open(filename, 'w') as outcsv:
            headers = ["check_id", "result", "reason", "resource_list", "description"]
            writer = csv.DictWriter(outcsv, fieldnames=headers)
            writer.writeheader()
            for row in result:
                writer.writerow(row)
        print("Output write to:gcp_security.csv")

    generate_csv(vm_result)




default_args = {
    'owner': "aadesh",
    'start_date': datetime(2020, 9, 19),
    'end_date': datetime(2020, 9, 30),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
}

dag = DAG(
    dag_id="gcp_sec_check",
    default_args=default_args,
    schedule_interval="@daily",
)

t1 = PythonOperator(
    task_id="check_vm_1",
    python_callable=check,
    dag=dag,
)
# t2 = PythonOperator(
#     task_id="check_vm_2",
#     python_callable=check,
# )
t1