import pandas as pd
from airflow.utils.log.logging_mixin import LoggingMixin


def clean_taxi_data(**context):

    logger = LoggingMixin().log

    # 1. Pull raw CSV path from XCom
    ti = context["ti"]
    raw_path = ti.xcom_pull(task_ids="ingest_taxi_data", key="taxi_raw_path")

    # 2. Load dataset
    df = pd.read_csv(raw_path)

    initial_rows = len(df)
    logger.info(f"Initial rows: {initial_rows}")

    # 3. Cleaning rules

    # Remove invalid fare_amount
    before = len(df)
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    logger.info(f"Removed {before - len(df)} rows due to invalid fare_amount")

    # Remove invalid trip_distance
    before = len(df)
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    logger.info(f"Removed {before - len(df)} rows due to invalid trip_distance")

    # Remove invalid coordinates (only if columns exist)
    coord_cols = [
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    ]

    if all(col in df.columns for col in coord_cols):

        before = len(df)

        df = df[
            (df["pickup_latitude"].between(40.4, 41.0)) &
            (df["pickup_longitude"].between(-74.3, -73.5)) &
            (df["dropoff_latitude"].between(40.4, 41.0)) &
            (df["dropoff_longitude"].between(-74.3, -73.5))
        ]

        logger.info(f"Removed {before - len(df)} rows due to invalid coordinates")

    else:
        logger.info("Coordinate columns not found, skipping coordinate cleaning step")

    # Drop rows with null values
    before = len(df)
    df = df.dropna(subset=["fare_amount", "trip_distance", "tpep_pickup_datetime"])
    logger.info(f"Removed {before - len(df)} rows due to null values")

    # Parse pickup datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])

    final_rows = len(df)
    logger.info(f"Final cleaned rows: {final_rows}")

    # 5. Save cleaned data
    clean_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)

    logger.info(f"Cleaned dataset saved to {clean_path}")

    # 6. Push cleaned path to XCom
    ti.xcom_push(
        key="clean_path",
        value=clean_path
    )