from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 1, tzinfo=timezone('US/Eastern')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline_dag',
    default_args=default_args,
    description='DAG for a real-time data pipeline',
    schedule='0 8,20 * * *',  # Run at 8 AM and 8 PM (Eastern Time)
    catchup=False  # Only run the latest task instances
)

def run_pubsub_ingestion():
    import pubsub_ingestion
    pubsub_ingestion.main()

def run_transformation_pipeline():
    import transformation_pipeline
    transformation_pipeline.main()

def run_schema_definition():
    import schema_definition
    schema_definition.main()

t1 = PythonOperator(
    task_id='ingest_data',
    python_callable=run_pubsub_ingestion,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=run_transformation_pipeline,
    dag=dag,
)

t3 = PythonOperator(
    task_id='schema_definition',
    python_callable=run_schema_definition,
    dag=dag,
)

t1 >> t2 >> t3
