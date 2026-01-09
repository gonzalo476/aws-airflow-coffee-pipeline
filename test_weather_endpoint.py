import time
import requests
import pandas as pd
from datetime import datetime

PLACES = { 
    "Naha": {"lat": 26.21, "lon": 127.68},
    "Okinawa": {"lat": 26.33, "lon": 127.80},
    "Nago": {"lat": 26.59, "lon": 127.98},
    "Kunigami": {"lat": 26.75, "lon": 128.18}
}

BASE_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
PARAMETERS = "T2M,T2M_MAX,T2M_MIN,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"


def fetch_weather(lat, lon, start_date, end_date):
    payload = {
        "parameters": PARAMETERS,
        "community": "AG",
        "longitude": lon,
        "latitude": lat,
        "start": start_date,
        "end": end_date,
        "format": "JSON"
    }

    response = requests.get(BASE_URL, params=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_to_df(data, city):
    props = data["properties"]["parameter"]
    df = pd.DataFrame(props)
    df.index = pd.to_datetime(df.index, format="%Y%m%d")
    df.index.name = "date"
    df = df.reset_index()
    df["city"] = city
    return df


def save_partitioned(df, city, year):
    path = f"./data/raw/weather/city={city}/year={year}/data.csv"
    df.to_csv(path, index=False)


def run(year):
    start = f"{year}0101"
    end = f"{year}1231"

    for city, coords in PLACES.items():
        print(f"Fetching {city} - {year}")

        data = fetch_weather(coords["lat"], coords["lon"], start, end)
        df = transform_to_df(data, city)

        save_partitioned(df, city, year)

        time.sleep(1)


if __name__ == "__main__":
    run(2014)
