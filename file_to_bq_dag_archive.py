


import airflow
from airflow import DAG
from datetime import datetime,timedelta,date,timezone


from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),    
}

dag = DAG(
    'file_to_bq_load_archive',
    default_args=default_args,
    description='CSV file to BigQuery DAG',
    schedule_interval=None, #'*/10 * * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
)

archive_folder_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y%m%d')
gcs_destination_folder = f"{archive_folder_date}/"

# Define the load task from GCS to BigQuery
load_gcs_file_to_bq = GCSToBigQueryOperator(
    task_id='load_csv_data_to_bigquery',
    bucket='us-central1-dev-e90074b9-bucket',  # Replace with your GCS bucket name
    source_objects='data/file_gcp_1.csv',  # Replace with the actual path of the CSV file
    destination_project_dataset_table='golden-torch-437212-b8.udco_ds_spch_refined.emp',  # Replace with your destination table
    source_format="CSV",  # Specify CSV format
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_APPEND",  # Write to the table if it exists, otherwise append
    schema_fields=[  # Define schema for CSV (change this based on your file structure)
        {"name": "id", "type": "INT64", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    ],
    autodetect=False,  # Autodetect should be false since we are defining the schema explicitly
    gcp_conn_id='google-cloud-platform',  # Ensure the correct connection ID is used for GCP
    skip_leading_rows=1,
    dag=dag,
)
archive_gcs_file = GCSToGCSOperator(
    task_id="archive_gcs_file",
    gcp_conn_id='google-cloud-platform',  # Google Cloud connection ID
    source_bucket='us-central1-dev-e90074b9-bucket',  # The source bucket name
    source_object= "data/*.csv",  # Use * to match all files in source bucket
    destination_bucket='us-central1-dev-e90074b9-bucket',  # The destination bucket name
    destination_object=f'Archive/{gcs_destination_folder}',  # Keep the same filenames in archive bucket
    move_object=True,  # Move the objects, not copy
    match_glob=True,  # Allow glob pattern matching
)

load_gcs_file_to_bq >> archive_gcs_file