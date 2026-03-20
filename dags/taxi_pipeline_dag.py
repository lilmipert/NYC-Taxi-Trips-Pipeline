from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# 1. Import ฟังก์ชันให้ครบทั้ง 4 ตัว
try:
    from ingest_taxi_data import ingest_taxi_data
    from clean_taxi_data import clean_taxi_data
    from transform_taxi_data import transform_taxi_data
    from load_taxi_data import load_taxi_data
except ImportWarning:
    pass

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 14)
}

# 2. เปลี่ยนชื่อ dag_id เป็น v2 เพื่อบังคับให้ Airflow อัปเดตทันที
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

    # 3. เพิ่ม Task ของเพื่อน (Transform & Load)
    transform_task = PythonOperator(
        task_id="transform_taxi_data",
        python_callable=transform_taxi_data
    )

    load_task = PythonOperator(
        task_id="load_taxi_data",
        python_callable=load_taxi_data
    )

    # 4. เชื่อมต่อให้ครบ 4 Tasks
    ingest_task >> clean_task >> transform_task >> load_task