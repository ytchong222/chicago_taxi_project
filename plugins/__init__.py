from airflow.plugins_manager import AirflowPlugin
from custom_bigquery_plugin.bq_helpers import create_bq_dataset_if_not_exists, create_bq_table_if_not_exists, import_from_kaggle_to_bucket, run_load_data_to_bigquery

class CustomBigQueryPlugin(AirflowPlugin):
    name = "custom_bigquery_plugin"
    helpers = [
        create_bq_dataset_if_not_exists,
        create_bq_table_if_not_exists,
        import_from_kaggle_to_bucket,
        run_load_data_to_bigquery
    ]
