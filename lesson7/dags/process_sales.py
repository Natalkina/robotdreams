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
    dag_id='processing_sales',
    default_args=default_args,
    description='Process sales data',
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 9, 30),
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:

    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bronze_dataset',
        dataset_id='bronze',
        project_id='de-07-natalia-sokil',
        location='EU',
    )

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_silver_dataset',
        dataset_id='silver',
        project_id='de-07-natalia-sokil',
        location='EU',
    )

    load_data_to_bronze = GCSToBigQueryOperator(
        task_id='load_data_to_bronze',
        bucket='sokil_final_project',
        source_objects=['data/sales/{{ ds.split("-")[0] }}-{{ ds.split("-")[1] }}-{{ ds.split("-")[2] | int }}/*.csv'],
        destination_project_dataset_table='de-07-natalia-sokil.bronze.sales',
        schema_fields=[
            {'name': 'CustomerId', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Product', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Price', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        field_delimiter=',',
        #gcp_conn_id='google-cloud',
    )

    create_silver_table = BigQueryInsertJobOperator(
        task_id='create_silver_table',
        configuration={
            "query": {
                "query": """
                        CREATE TABLE IF NOT EXISTS `de-07-natalia-sokil.silver.sales` (
                         client_id STRING,
                         purchase_date DATE,
                         product_name STRING,
                         price FLOAT64
                        )
                    """,
                "useLegacySql": False,
            }
        },
        location='EU',
        project_id='de-07-natalia-sokil',
    )


    transform_data_to_silver = BigQueryInsertJobOperator(
        task_id='transform_data_to_silver',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `de-07-natalia-sokil.silver.sales` (client_id, purchase_date, product_name, price)
                    SELECT
                        CustomerId AS client_id,
                        PARSE_DATE('%Y-%m-%d', PurchaseDate) AS purchase_date,
                        Product AS product_name,
                        CAST(REGEXP_REPLACE(Price, '[^0-9.]', '') AS FLOAT64) AS price
                    FROM `de-07-natalia-sokil.bronze.sales`
                """,
                "useLegacySql": False,
            }
        },
        location='EU',
        project_id='de-07-natalia-sokil',
    )

    create_bronze_dataset >> create_silver_dataset >> load_data_to_bronze >> create_silver_table >> transform_data_to_silver
