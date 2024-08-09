from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# Define the default arguments for the DAG
default_args = {
    'owner': 'teck_chong',
    'depends_on_past': False,
    'email': ['ytchong222@gmail.com'],  # Replace with your email address
    'email_on_failure': True,  # Enable email notifications on failure
    'email_on_retry': True,    # Enable email notifications on retry
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='load_taxi_monitoring',
    default_args=default_args,
    description='A DAG to load data into BigQuery',
    schedule_interval='@daily',  # Adjust the schedule interval as needed
    catchup=False,
    start_date=datetime(2024, 8, 8),  # Adjust the start date as needed
    end_date=datetime(2024, 8, 9),    # Adjust the end date as needed
) as dag:

    def import_from_kaggle_to_bucket():
        client = bigquery.Client()
        
        # Define the public dataset and table
        project_id = 'bigquery-public-data'
        dataset_id = 'chicago_taxi_trips'
        table_id = 'taxi_trips'

        # Define the Cloud Storage bucket and destination file path
        bucket_name = 'chicagotaxi_data'
        destination_uri = f'gs://{bucket_name}/chicago_taxi_trips/*.csv'

        # Configure the extract job
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.CSV

        # Define the table reference
        table_ref = f'{project_id}.{dataset_id}.{table_id}'

        # Start the export job
        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config
        )

        # Wait for the job to complete
        extract_job.result()

        print(f'Table {table_id} successfully exported to {destination_uri}')
        return {"message": "Import loaded successfully"}


    import_data_task = PythonOperator(
        task_id='import_from_kaggle',
        python_callable=import_from_kaggle_to_bucket,
    )
  

    def run_load_data_to_bigquery():
        # Path to your service account key file
        client = bigquery.Client()


        # Set the dataset name
        dataset_id = 'chicago_taxi_trips'  # Replace with your desired dataset name

        # Define the dataset reference
        dataset_ref = client.dataset(dataset_id)

        # Define the dataset
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set location (optional, default is US)

        # Create the dataset in BigQuery
        dataset = client.create_dataset(dataset)  # API request
        print(f"Created dataset {client.project}.{dataset.dataset_id}")

        # Set the table name
        table_id = 'taxi_trips'  # Replace with your desired table name

        # Define the Google Cloud Storage URI, using a wildcard to match all files
        bucket_name = 'chicagotaxi_data'  # Replace with your GCS bucket name
        file_pattern = 'chicago_taxi_trips/*.csv'  # Replace with the folder containing your CSV files in the bucket
        gcs_uri = f'gs://{bucket_name}/{file_pattern}'

        # Explicitly define the schema for the table
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

        # Define the table reference
        table_ref = dataset_ref.table(table_id)

        # Create a Table object
        table = bigquery.Table(table_ref, schema=schema)

        # Create the table in BigQuery
        table = client.create_table(table)  # API request

        print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
      
        # Define the job configuration
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row in each CSV file
            schema=schema,  # Use the explicitly defined schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to table if it exists
        )

        # Start the job to load CSV files into BigQuery
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )

        # Wait for the job to complete
        load_job.result()

        print(f"Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}")
        return {"message": "Data loaded successfully"}

    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery_task',
        python_callable=run_load_data_to_bigquery,
    )

    # Set task dependencies
    import_data_task >> load_data_task
