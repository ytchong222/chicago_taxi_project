from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta  # Import timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from airflow.operators.dummy import DummyOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'teck_chong',
    'depends_on_past': False,
    'email': ['ytchong222@gmail.com'],  
    'email_on_failure': True,  
    'email_on_retry': True,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

# Define the DAG
with DAG(
    dag_id='load_taxi_data',
    default_args=default_args,
    description='A DAG to load data into BigQuery',
    schedule_interval='@daily',  
    catchup=False,
    start_date=datetime(2024, 8, 10),  
    end_date=datetime(2024, 8, 11),   
) as dag:

    start_task = DummyOperator(
        task_id='start_task',
    )
    def create_bq_dataset_if_not_exists(client, dataset_id, location='US'):
        try:
            client.get_dataset(dataset_id)
            print(f"Dataset {dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(client.dataset(dataset_id))
            dataset.location = location
            client.create_dataset(dataset)
            print(f"Created dataset {dataset_id}")

    def create_bq_table_if_not_exists(client, dataset_id, table_id, schema):
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            client.get_table(table_ref)
            print(f"Table {table_id} already exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_id}")

    def import_from_kaggle_to_bucket():
        client = bigquery.Client()
        project_id = 'bigquery-public-data'
        dataset_id = 'chicago_taxi_trips'
        table_id = 'taxi_trips'
        bucket_name = 'chicagotaxi_data'
        destination_uri = f'gs://{bucket_name}/chicago_taxi_trips/*.csv'

        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.CSV

        table_ref = f'{project_id}.{dataset_id}.{table_id}'

        extract_job = client.extract_table(table_ref, destination_uri, job_config=job_config)
        extract_job.result()

        print(f'Table {table_id} successfully exported to {destination_uri}')

    def run_load_data_to_bigquery():
        client = bigquery.Client()
        dataset_id = 'chicago_taxi_trips'
        table_id = 'taxi_trips'
        bucket_name = 'chicagotaxi_data'
        file_pattern = 'chicago_taxi_trips/*.csv'
        gcs_uri = f'gs://{bucket_name}/{file_pattern}'

        schema = [
            bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("taxi_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trip_start_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("trip_end_timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("trip_seconds", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("trip_miles", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pickup_census_tract", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("dropoff_census_tract", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("pickup_community_area", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("dropoff_community_area", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("fare", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tips", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tolls", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("extras", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("trip_total", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("payment_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("company", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pickup_latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pickup_longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("pickup_location", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("dropoff_latitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("dropoff_longitude", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("dropoff_location", "STRING", mode="NULLABLE")
        ]

        create_bq_dataset_if_not_exists(client, dataset_id)
        create_bq_table_if_not_exists(client, dataset_id, table_id, schema)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        load_job = client.load_table_from_uri(gcs_uri, f'{dataset_id}.{table_id}', job_config=job_config)
        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}")

    import_data_task = PythonOperator(
        task_id='import_from_kaggle',
        python_callable=import_from_kaggle_to_bucket,
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery_task',
        python_callable=run_load_data_to_bigquery,
    )

    start_task >> import_data_task >> load_data_task
