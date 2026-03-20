from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import ฟังก์ชันจากไฟล์ที่อยู่ในโฟลเดอร์เดียวกัน
try:
    from ingest_taxi_data import ingest_taxi_data
    from clean_taxi_data import clean_taxi_data
except ImportWarning:
    pass

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 14)
}

with DAG(
    dag_id="nyc_taxi_pipeline_183",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data
    )

    clean_task = PythonOperator(
        task_id="clean_taxi_data",
        python_callable=clean_taxi_data
    )

    # เชื่อมต่อแค่ 2 task
    ingest_task >> clean_task