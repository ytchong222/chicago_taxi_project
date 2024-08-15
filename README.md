Task Flow 
load_taxi_data 1. import_from_kaggle --import_from_kaggle load files to gcp bucket bucket location "/chicagotaxi_data/chicago_taxi_trips/│"                ├── start_task
               2. load_data_to_bigquery_task--load_data_from bucket to bigquery "chicagotaxitrips-431912/chicago_taxi_trips/chicago_taxi_trips/taxi_trips"
               3. check_files -- for troubleshooting purpose check the files in the /us-central1-composortaxi-b32cfc72-bucket/chicago_taxi_trips/
			   4. dbt_run  --run all the DBT files     /us-central1-composortaxi-b32cfc72-bucket/plugins/dbt_taxi_project/models/example/
                                           ├── schema.yml
                                           ├── demand_pickup
                                           ├── dim_holiday.sql
                                           ├── holidays_impact.sql
                                           ├── monthly_max_avg_tips.sql
                                           ├── peak_hours.sql
                                           ├── top_100_overworkers.sql
                                           └── top_100_tips_last_3months.sql

the folders/files

chicago_taxi_trips/
├── dag
│     └──load_taxi_data 1. import_from_kaggle --import_from_kaggle load files to gcp bucket bucket location "/chicagotaxi_data/chicago_taxi_trips/│"                ├── start_task
│                       2. load_data_to_bigquery_task--load_data_from bucket to bigquery "chicagotaxitrips-431912/chicago_taxi_trips/chicago_taxi_trips/taxi_trips"
│                       3. check_files -- for troubleshooting purpose check the files in the /us-central1-composortaxi-b32cfc72-bucket/chicago_taxi_trips/
│						4. dbt_run  --run all the DBT files     /us-central1-composortaxi-b32cfc72-bucket/plugins/dbt_taxi_project/models/example/
│                                            ├── schema.yml
│                                            ├── demand_pickup
│                                            ├── dim_holiday.sql
│                                            ├── holidays_impact.sql
│                                            ├── monthly_max_avg_tips.sql
│                                            ├── peak_hours.sql
│                                            ├── top_100_overworkers.sql
│                                            └── top_100_tips_last_3months.sql
│
├── dbt_taxi_project/
│    └── models
│            └── example
│                 ├── schema.yml
│                 ├── demand_pickup
│                 ├── dim_holiday.sql
│                 ├── holidays_impact.sql
│                 ├── monthly_max_avg_tips.sql
│                 ├── peak_hours.sql
│                 ├── top_100_overworkers.sql
│                 └── top_100_tips_last_3months.sql
│ 
│ 
│            
│ 
│ 
│ 
│ 
│           
└── README.md



