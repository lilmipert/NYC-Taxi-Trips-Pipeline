import shutil
import os
from airflow.utils.log.logging_mixin import LoggingMixin

def load_taxi_data(**context):
    logger = LoggingMixin().log
    ti = context["ti"]
    
    # 1. ดึง Path จาก Transform
    transform_path = ti.xcom_pull(task_ids="transform_taxi_data", key="transform_path")
    
    # 2. กำหนดที่อยู่ปลายทาง (เสมือนเป็น Data Warehouse/Storage)
    target_dir = "/opt/airflow/dags/final_output"
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
        
    target_file = os.path.join(target_dir, "daily_taxi_summary.csv")

    # 3. Copy ไฟล์
    if transform_path and os.path.exists(transform_path):
        shutil.copy(transform_path, target_file)
        logger.info(f"Successfully Loaded data to {target_file}")
    else:
        raise FileNotFoundError("Transformed file not found!")