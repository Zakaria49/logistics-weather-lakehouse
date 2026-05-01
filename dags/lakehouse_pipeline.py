from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default settings for all tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1), # Start date in the past allows it to trigger immediately
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    'logistics_lakehouse_pipeline',
    default_args=default_args,
    description='End-to-end Medallion Architecture Pipeline',
    schedule_interval='@daily', # Run once a day at midnight
    catchup=False
) as dag:

    # Task 1: Ingestion (Bronze Layer)
    fetch_weather = BashOperator(
        task_id='fetch_bronze_weather',
        bash_command='docker exec lakehouse-spark python /home/jovyan/work/jobs/ingestion/fetch_weather.py'
    )

    # Task 2: Transformation (Silver Layer)
    transform_silver = BashOperator(
        task_id='process_silver_layer',
        bash_command='docker exec lakehouse-spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/work/jobs/processing/spark_transformation.py'
    )

    # Task 3: Serving (Gold Layer)
    load_gold = BashOperator(
        task_id='load_gold_layer',
        bash_command='docker exec lakehouse-spark spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.6.0 /home/jovyan/work/jobs/processing/load_to_postgres.py'
    )

    # Define the execution order (The beauty of Airflow!)
    fetch_weather >> transform_silver >> load_gold