from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

# Import functions from your custom plugin
from custom_bigquery_plugin.bq_helpers import create_bq_dataset_if_not_exists, create_bq_table_if_not_exists, import_from_kaggle_to_bucket, run_load_data_to_bigquery

default_args = {
    'owner': 'teck_chong',
    'depends_on_past': False,
    'email': ['ytchong222@gmail.com'],  
    'email_on_failure': True,  
    'email_on_retry': True,    
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
}

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

    import_data_task = PythonOperator(
        task_id='import_from_kaggle',
        python_callable=import_from_kaggle_to_bucket,
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_bigquery_task',
        python_callable=run_load_data_to_bigquery,
    )
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='ls -l /home/airflow/gcs/plugins/dbt_taxi_project/',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /home/airflow/gcs/plugins/dbt_taxi_project/ && '
            'dbt run --profiles-dir /home/airflow/gcs/plugins/dbt_taxi_project'
        )
    )

    

    start_task >> import_data_task >> load_data_task >> check_files 
