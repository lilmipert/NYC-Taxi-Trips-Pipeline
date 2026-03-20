import requests
import pandas as pd
import time
from airflow.utils.log.logging_mixin import LoggingMixin

def ingest_taxi_data(**kwargs):
    url = "https://data.cityofnewyork.us/resource/t29m-gskq.csv"
    output_path = "/tmp/nyc_taxi_raw.csv"

    logger = LoggingMixin().log

    for attempt in range(3):
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            break

        except requests.exceptions.RequestException as e:
            logger.warning(f"Download failed (attempt {attempt+1}/3): {e}")
            if attempt == 2:
                raise
            time.sleep(5)

    df = pd.read_csv(output_path)
    row_count = len(df)

    if row_count < 1000:
        raise ValueError("Downloaded dataset has less than 1000 rows")

    logger.info(f"Downloaded {row_count} rows from NYC Taxi dataset")

    kwargs["ti"].xcom_push(
        key="taxi_raw_path",
        value=output_path
    )