import pandas as pd
import logging

def clean_taxi_data(**context):

    # 1. Pull raw CSV path from XCom
    raw_path = context["ti"].xcom_pull(
        task_ids="ingest_taxi_data",
        key="raw_path"
    )

    logging.info(f"Loading raw data from {raw_path}")

    # 2. Load CSV
    df = pd.read_csv(raw_path)

    initial_rows = len(df)
    logging.info(f"Initial rows: {initial_rows}")

    # 3. Parse datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )

    # Cleaning rule 1: fare filter
    before = len(df)
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    logging.info(f"Removed {before - len(df)} rows by fare filter")

    # Cleaning rule 2: distance filter
    before = len(df)
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    logging.info(f"Removed {before - len(df)} rows by distance filter")

    # Cleaning rule 3: NYC coordinate filter
    before = len(df)
    df = df[
        df["pickup_latitude"].between(40.4, 41.0) &
        df["pickup_longitude"].between(-74.3, -73.5)
    ]
    logging.info(f"Removed {before - len(df)} rows by coordinate filter")

    # Cleaning rule 4: drop null values
    before = len(df)
    df = df.dropna(
        subset=["fare_amount", "trip_distance", "tpep_pickup_datetime"]
    )
    logging.info(f"Removed {before - len(df)} rows by null filter")

    # 5. Save cleaned data
    clean_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(clean_path, index=False)

    logging.info(f"Cleaned data saved to {clean_path}")
    logging.info(f"Final row count: {len(df)}")

    # 6. Push cleaned path to XCom
    context["ti"].xcom_push(
        key="clean_path",
        value=clean_path
    )
    # update