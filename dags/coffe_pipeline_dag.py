from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id='coffee_pipeline',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["coffee", "okinawa"]
)
def coffee_pipeline():

    @task
    def extract_weather_task():
        from src.extract.nasa_weather import extract_weather
        extract_weather("20140101", "20241231", "/opt/airflow/data/raw")

    @task
    def process_tiff_task():
        from src.transform.tif_to_tabular import tiffs_to_dataframe
        tiffs_to_dataframe(
            output_csv="/opt/airflow/data/curated/soilgrid.csv"
        )

    extract_weather_task()
    process_tiff_task()

coffee_pipeline()