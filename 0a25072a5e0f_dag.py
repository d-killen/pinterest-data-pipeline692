from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta
from airflow import DAG


# define parameters
notebook_task = {
    'notebook_path': '/Users/danny.killen@gmail.com/batch_processing_notebook',
}

default_args = {
    'owner': 'danny.killen@gmail.com',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('0a25072a5e0f_dag',
    start_date=datetime(2023,12,12),
    schedule_interval='@daily',
    catchup=False
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run