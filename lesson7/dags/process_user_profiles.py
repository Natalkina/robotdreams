from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='process_user_profiles',
        start_date=datetime(2022, 9, 1),
        schedule_interval=None,
        catchup=False,

) as dag:


    load_data_to_silver = GCSToBigQueryOperator(
        task_id='load_data_to_bronze',
        bucket='sokil_final_project',
        source_objects=['data/user_profiles/*.json'],
        destination_project_dataset_table='de-07-natalia-sokil.silver.user_profiles',
        source_format='NEWLINE_DELIMITED_JSON',
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )



    load_data_to_silver
