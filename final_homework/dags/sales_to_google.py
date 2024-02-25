from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sales_to_google',
    default_args=default_args,
    description='Process sales data',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src="/opt/airflow/files/{{ ds }}/sales_{{ds}}.json",
        dst="src1/sales/v1/{{ds_nodash[:4]}}/{{ds_nodash[4:6]}}/{{ds_nodash[6:]}}",
        bucket="de-07-sokil-sales-0822"
    )

    upload_file