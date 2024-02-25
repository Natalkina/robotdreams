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
    dag_id='processing_customets',
    default_args=default_args,
    description='Process customets data',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 5),
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:


    load_data_to_bronze = GCSToBigQueryOperator(
        task_id='load_data_to_bronze',
        bucket='sokil_final_project',
        source_objects=['data/customers/{{ ds.split("-")[0] }}-{{ ds.split("-")[1] }}-{{ ds.split("-")[2] | int }}/*.csv'],
        destination_project_dataset_table='de-07-natalia-sokil.bronze.customers',
        schema_fields=[
            {'name': 'client_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'registration_date', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        #gcp_conn_id='google-cloud',
    )

    transform_data_to_silver = BigQueryInsertJobOperator(
        task_id='transform_data_to_silver',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `de-07-natalia-sokil.silver.customers` AS
                    SELECT
                        client_id,
                        MAX(first_name) AS first_name,
                        MAX(last_name) AS last_name,
                        MAX(email) AS email,
                        MAX(PARSE_DATE('%Y-%m-%d', registration_date)) AS registration_date,
                        MAX(state) AS state
                    FROM `de-07-natalia-sokil.bronze.customers`
                    GROUP BY client_id
                """,
                "useLegacySql": False,
            }
        },
        location='EU',
        project_id='de-07-natalia-sokil',
    )

    load_data_to_bronze >> transform_data_to_silver
