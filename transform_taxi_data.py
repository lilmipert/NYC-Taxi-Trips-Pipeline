import pandas as pd
import os
from airflow.utils.log.logging_mixin import LoggingMixin

def transform_taxi_data(**context):
    logger = LoggingMixin().log
    ti = context["ti"]
    
    # 1. ดึง Path ของไฟล์ที่ Clean แล้วจาก XCom
    clean_path = ti.xcom_pull(task_ids="clean_taxi_data", key="clean_path")
    
    if not clean_path or not os.path.exists(clean_path):
        raise FileNotFoundError(f"Cleaned file not found at {clean_path}")

    # 2. Load ข้อมูล
    df = pd.read_csv(clean_path)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    # 3. Transformation: หาผลรวมรายได้และจำนวนเที่ยวรายวัน
    daily_stats = df.groupby(df['tpep_pickup_datetime'].dt.date).agg({
        'total_amount': 'sum',
        'trip_distance': 'mean',
        'vendorid': 'count'
    }).rename(columns={'vendorid': 'trip_count'}).reset_index()

    # 4. Save ไฟล์ที่ Transform แล้ว
    transform_path = "/tmp/nyc_taxi_transformed.csv"
    daily_stats.to_csv(transform_path, index=False)
    
    logger.info(f"Transformation complete. Saved to {transform_path}")

    # 5. ส่ง Path ต่อไปให้ Task ถัดไป
    ti.xcom_push(key="transform_path", value=transform_path)