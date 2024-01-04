from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='process_sales',
    default_args=default_args,
    description='Process sales data',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval='0 1 * * *',
    catchup=True,
    max_active_runs=1,
) as dag:

    extract_task = SimpleHttpOperator(
        task_id="extract",
        endpoint='',
        method="POST",
        http_conn_id='job1',
        response_check=lambda response: True if 201 == response.status_code else False,
        data='{"date": "{{ ds }}", "raw_dir": "D:\\\\robotdreams\\\\raw\\\\sales\\\\{{ ds }}"}',
        headers={
            'Content-Type': 'application/json'
        }
    )

    convert_task = SimpleHttpOperator(
        task_id="convert",
        method="POST",
        endpoint='',
        http_conn_id='job2',
        response_check=lambda response: True if response.status_code == 201 else False,
        data='{"raw_dir": "D:\\\\robotdreams\\\\raw\\\\sales\\\\{{ ds }}", "stg_dir": "D:\\\\robotdreams\\\\stg\\\\sales\\\\{{ ds }}"}',
        headers={
            'Content-Type': 'application/json'
        }
    )

    extract_task >> convert_task