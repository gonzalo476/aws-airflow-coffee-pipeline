from airflow.sdk import dag, task
from airflow.datasets import Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os

BUCKET_RAW = Variable.get("COFFEE_S3_RAW_ZONE")

weather_csvs = Dataset("file:///opt/airflow/data/raw/weather/weather_*.csv")
weather_s3 = Dataset(f"s3://{BUCKET_RAW}/weather/")

tiff_csv = Dataset("file:///opt/airflow/data/raw/soil/soilgrid.csv")
tiff_s3 = Dataset(f"s3://{BUCKET_RAW}/soil/")

disasters_s3 = Dataset(f"s3://{BUCKET_RAW}/disasters/")

@dag(
    dag_id='coffee_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["coffee", "okinawa"]
)
def coffee_pipeline():

    @task(outlets=[weather_csvs])
    def extract_weather_task():
        from src.extract.nasa_weather import extract_weather
        extract_weather("20140101", "20241231", "/opt/airflow/data/raw/weather")

    @task(outlets=[weather_s3])
    def process_and_upload_weather():
        cities = ["Naha", "Okinawa", "Nago", "Kunigami"]
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        date = datetime.now().strftime('%Y-%m-%d')

        for city in cities:
            csv_path = f"/opt/airflow/data/raw/weather/weather_{city}.csv"
            df = pd.read_csv(csv_path)

            parquet_path = f"/tmp/weather_{city}.parquet"
            df.to_parquet(parquet_path, compression='snappy', index=False)

            s3_key = f'weather/{date}/weather_{city}.parquet'
            s3_hook.load_file(
                filename=parquet_path,
                key=s3_key,
                bucket_name=BUCKET_RAW,
                replace=True
            )

            os.remove(parquet_path)

            print(f"Done: {city}")
        
        print(f"All files uploaded to s3://{BUCKET_RAW}/weather/{date}/")

    @task(outlets=[tiff_csv])
    def process_tiff_task():
        from src.transform.tif_to_tabular import tiffs_to_dataframe
        tiffs_to_dataframe(
            output_csv="/opt/airflow/data/raw/soil/soilgrid.csv"
        )

    @task(outlets=[tiff_s3])
    def process_and_upload_tiff():
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        date = datetime.now().strftime('%Y-%m-%d')
        df = pd.read_csv("/opt/airflow/data/raw/soil/soilgrid.csv")

        parquet_path = "/tmp/soilgrid.parquet"
        df.to_parquet(parquet_path, compression='snappy', index=False)

        s3_key = f'soil/{date}/soilgrid.parquet'
        s3_hook.load_file(
            filename=parquet_path,
            key=s3_key,
            bucket_name=BUCKET_RAW,
            replace=True
        )

        os.remove(parquet_path)
        print(f"Done: soilgrid")

        print(f"All files uploaded to s3://{BUCKET_RAW}/soil/{date}/")

    @task(outlets=[disasters_s3])
    def process_and_upload_disasters():
        s3_hook = S3Hook(aws_conn_id="aws_conn")
        date = datetime.now().strftime('%Y-%m-%d')
        df = pd.read_csv("/opt/airflow/data/raw/disasters/typhoon_track_data.csv")

        parquet_path = "/tmp/typhoon.parquet"
        df.to_parquet(parquet_path, compression="snappy", index=False)

        s3_key = f'disasters/{date}/typhoon.parquet'
        s3_hook.load_file(
            filename=parquet_path,
            key=s3_key,
            bucket_name=BUCKET_RAW,
            replace=True
        )
        os.remove(parquet_path)
        print(f"Done: disasters")
        print(f"All files uploaded to s3://{BUCKET_RAW}/disasters/{date}/")

    extract_weather = extract_weather_task()
    upload_weather = process_and_upload_weather()
    extract_tiff = process_tiff_task()
    upload_tiff = process_and_upload_tiff()
    upload_disasters = process_and_upload_disasters()

    extract_weather >> upload_weather
    extract_tiff >> upload_tiff

coffee_pipeline()