from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
import pandas as pd
from datetime import timedelta

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'snowflake_to_bigquery2',
    default_args=default_args,
    description='Extract data from Snowflake and load it into BigQuery',
    schedule_interval='@daily',  # Adjust as necessary
    start_date=days_ago(1),
    catchup=False,
)

# Step 1: Extract data from Snowflake
def extract_from_snowflake():
    # Set up Snowflake connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Example query to extract historical data from Snowflake
    query = "SELECT id, value FROM TEST.public.cust"
    
    # Execute the query and fetch results as a Pandas DataFrame
    df = snowflake_hook.get_pandas_df(query)
    
    # Return the data as a list of records (records for BigQuery insertion)
    return df.to_dict(orient='records')

# Step 2: Load data to BigQuery
def load_to_bigquery(**kwargs):
    # Pull the records from XCom
    records = kwargs['ti'].xcom_pull(task_ids='extract_from_snowflake')
    
    # Set up BigQuery client
    client = bigquery.Client(project='ethereal-bison-447605-v7')
    
    # Define the BigQuery dataset and table
    dataset_id = 'refined'
    table_id = f'{dataset_id}.cust'
    
    # Load the records into BigQuery
    errors = client.insert_rows_json(table_id, records)
    
    if errors:
        raise Exception(f"Errors occurred while inserting rows into BigQuery: {errors}")
    else:
        print("Data successfully loaded into BigQuery.")

# Step 3: Set up PythonOperator tasks for extraction and loading
extract_task = PythonOperator(
    task_id='extract_from_snowflake',
    python_callable=extract_from_snowflake,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_task >> load_task
