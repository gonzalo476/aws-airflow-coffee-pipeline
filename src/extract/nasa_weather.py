def extract_weather(start_date: str, end_date: str, output_path: str) -> None:
    import time, requests
    import pandas as pd

    places = { 
        "Naha": {"lat": 26.21, "lon": 127.68},
        "Okinawa": {"lat": 26.33, "lon": 127.80},
        "Nago": {"lat": 26.59, "lon": 127.98},
        "Kunigami": {"lat": 26.75, "lon": 128.18}
    }

    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    parameters = "T2M,T2M_MAX,T2M_MIN,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"

    for city, coords in places.items():
        payload = {
            "parameters": parameters,
            "community": "AG",
            "longitude": coords["lon"],
            "latitude": coords["lat"],
            "start": start_date,
            "end": end_date,
            "format": "JSON"
        }

        r = requests.get(base_url, params=payload, timeout=30)
        r.raise_for_status()

        df = pd.DataFrame(r.json()["properties"]["parameter"])
        df.index = pd.to_datetime(df.index)
        df.reset_index(inplace=True)
        df["city"] = city

        file = f"{output_path}/weather_{city}.csv"
        df.to_csv(file, index=False)

        time.sleep(1)