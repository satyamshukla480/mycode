import os
import yaml
import airflow
from airflow import DAG
import uuid
import subprocess
import urllib.parse
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

from apache_beam.portability.common_urns import python_callable

# Create default arguments for the dag
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

domain_abbr = 'SPCH'
dataset ='routines'
job_id =str(uuid.uuid4())
project_id = 'golden-torch-437212-b8'
proc_name ='sp_emp_flat_load'
execution_date =airflow.utils.dates.days_ago(0)

def get_scheduler_info(context):
    scheduler='Airflow'
    base_url = context['conf'].get('webserver','BASE_URL')
    dag_run= context['dag_run']
    execution_date=urllib.parse.quote(dag_run.execution_date.isoformat())
    dag_url= f'{base_url}:/graph?dag_id={dag_run.dag_id}&root=&execution_date={execution_date}'
    scheduler=f'{str(dag_url)}'
    return scheduler

def python_task(**kwargs):
    scheduler=get_scheduler_info(kwargs)
    kwargs['ti'].xcom_push(key='scheduler', value =scheduler )




# Creating DAG object with proper indentation
dag = DAG(
    'sp_thru_dag',
    default_args=default_args,
    description='file_to_bq',
    schedule_interval='*/15 * * * *',  # Make sure this is indented properly
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=2),
)

GET_SCHEDULER_TASK =PythonOperator(
       task_id = 'GET_SCHEDULER',
       provide_context=True,
       python_callable= python_task,
       dag=dag
)

# Creating task with BigQueryInsertJobOperator
duplicate_to_unq = BigQueryInsertJobOperator(
    task_id='run_bq_query',
    configuration={
        "query": {
            "query": "call `{0}.{1}.{2}`('{3}','{4}','{5}','{6}','{7}');"
            .format(project_id,dataset,proc_name,domain_abbr,job_id,proc_name,execution_date,"{{ti.xcom_pull(task_ids='GET_SCHEDULER', key='scheduler')}}"),
            "useLegacySql": False,  # Set to False for standard SQL, True for legacy SQL
            "priority": 'BATCH',
            }
        },
    gcp_conn_id='google-cloud-platform',
    dag=dag,
)
GET_SCHEDULER_TASK>>duplicate_to_unq
