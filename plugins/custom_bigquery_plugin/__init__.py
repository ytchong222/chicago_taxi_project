# plugins/custom_bigquery_plugin/__init__.py
# File: custom_bigquery_plugin/__init__.py
from custom_bigquery_plugin.bq_helpers import (
    create_bq_dataset_if_not_exists,
    create_bq_table_if_not_exists,
    import_from_kaggle_to_bucket,
    run_load_data_to_bigquery
)

# plugins/custom_bigquery_plugin/__init__.py
#import logging
#logging.basicConfig(level=logging.INFO)
#from airflow.plugins_manager import AirflowPlugin
