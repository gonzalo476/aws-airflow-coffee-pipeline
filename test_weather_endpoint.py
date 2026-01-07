
import time, requests
import pandas as pd

def get_weather():
    place_coords = {
        "Naha": {"lat": 26.21, "lon": 127.68},
        "Okinawa": {"lat": 26.33, "lon": 127.80},
        "Nago": {"lat": 26.59, "lon": 127.98},
        "Kunigami": {"lat": 26.75, "lon": 128.18}
    }
    
    base_url = r"https://power.larc.nasa.gov/api/temporal/daily/point"
    start_date = "20140101"
    end_date = "20141231"
    params_request = "T2M,T2M_MAX,T2M_MIN,PRECTOTCORR,RH2M,ALLSKY_SFC_SW_DWN"
    
    print(f"Starting downloading the dataset...\n")
    
    for name, coords in place_coords.items():
        print(f"Downloading: {name} dataset.")

        payload = {
            "parameters": params_request,
            "community": "AG",
            "longitude": coords['lon'],
            "latitude": coords['lat'],
            "start": start_date,
            "end": end_date,
            "format": "JSON"
        }
            
        try:
            response = requests.get(base_url, params=payload, verify=True, timeout=30.00)
            response.raise_for_status()
            data = response.json()

            properties = data['properties']['parameter']
            df = pd.DataFrame(properties)
 
            df.index = pd.to_datetime(df.index, format=r'%Y%m%d')
            df.index.name = 'Date'
            df = df.reset_index()

            df['City'] = name

            print("Downloading done.")
            
            time.sleep(1)

            print(f"Generating CSV File for '{name}' City.")

            if not df.empty:
                year_min = df['Date'].dt.year.min()
                year_max = df['Date'].dt.year.max()

                file_name = f"./data/raw/Okinawa_{name}_Weather_{year_min}_{year_max}.csv"
                df.to_csv(file_name, index=False)

                print(f"Dataset '{file_name}' ready!")
                print(f"Total: {len(df)}")
            
        except Exception as e:
            print(f"Error in {name}: {e}")
            return None

get_weather()
            