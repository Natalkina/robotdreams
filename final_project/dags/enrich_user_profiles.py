from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, \
    BigQueryCreateEmptyDatasetOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='enrich_user_profiles',
        start_date=datetime(2022, 9, 1),
        schedule_interval=None,
        catchup=False,

) as dag:
    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_gold_dataset',
        dataset_id='gold',
        project_id='de-07-natalia-sokil',
        location='EU',
    )

    create_user_profiles_enriched_table = BigQueryInsertJobOperator(
        task_id='create_user_profiles_enriched_table',
        configuration={
            "query": {
                "query": """
                            CREATE TABLE IF NOT EXISTS `de-07-natalia-sokil.gold.user_profiles_enriched` (
                            client_id STRING,
                            first_name STRING,
                            last_name STRING,
                            state STRING,
                            email STRING,
                            registration_date DATE,
                            birth_date DATE,
                            phone_number STRING
                            )
                        """,
                "useLegacySql": False,
            }
        },
        location='EU',
        project_id='de-07-natalia-sokil',
    )

    enrich_data_to_gold = BigQueryInsertJobOperator(
        task_id='enrich_data_to_gold',
        configuration={
            "query": {
                "query": """
                         MERGE INTO `de-07-natalia-sokil.gold.user_profiles_enriched` AS TARGET
        USING (
            SELECT
                c.client_id,
                IFNULL(c.first_name, SPLIT(u.full_name, ' ')[OFFSET(0)]) AS first_name,
                IFNULL(c.last_name, SPLIT(u.full_name, ' ')[SAFE_OFFSET(1)]) AS last_name,
                c.email,
                c.registration_date,
                IFNULL(c.state, u.state) AS state,
                u.birth_date,
                u.phone_number
            FROM `de-07-natalia-sokil.silver.customers` c
            JOIN `de-07-natalia-sokil.silver.user_profiles` u
            ON c.email = u.email
        ) AS SOURCE
        ON TARGET.email = SOURCE.email

        WHEN MATCHED THEN
            UPDATE SET
                TARGET.client_id = SOURCE.client_id,
                TARGET.first_name = SOURCE.first_name,
                TARGET.last_name = SOURCE.last_name,
                TARGET.registration_date = SOURCE.registration_date,
                TARGET.state = SOURCE.state,
                TARGET.birth_date = SOURCE.birth_date,
                TARGET.phone_number = SOURCE.phone_number

        WHEN NOT MATCHED THEN
            INSERT (client_id, first_name, last_name, email, registration_date, state, birth_date, phone_number)
            VALUES (SOURCE.client_id, SOURCE.first_name, SOURCE.last_name, SOURCE.email, SOURCE.registration_date, SOURCE.state, SOURCE.birth_date, SOURCE.phone_number);
                    """,
                "useLegacySql": False,
            }
        },
        location='EU',
        project_id='de-07-natalia-sokil',
    )

    create_gold_dataset >> enrich_data_to_gold
