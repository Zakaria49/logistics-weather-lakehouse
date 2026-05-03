import requests
import csv
import os
from datetime import datetime

# Define the Bronze Drop Zone path
OUTPUT_DIR = "/home/jovyan/work/datalake/bronze/weather"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "weather_data.csv")

# Ensure the directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# These must match the Logistics script EXACTLY
CITIES = {
    "Casablanca": {"lat": 33.5898, "lon": -7.6038},
    "Rabat": {"lat": 34.0208, "lon": -6.8416},
    "Tangier": {"lat": 35.7594, "lon": -5.8339},
    "Marrakech": {"lat": 31.6294, "lon": -7.9810},
    "Fes": {"lat": 34.0331, "lon": -5.0002},
    "Oujda": {"lat": 34.6867, "lon": -1.9113},
    "Agadir": {"lat": 30.4201, "lon": -9.5981},
    "Laayoune": {"lat": 27.1252, "lon": -13.1625}
}

def fetch_weather_data():
    print("Fetching last 30 days of weather data from Open-Meteo API...")
    weather_records = []
    
    for city, coords in CITIES.items():
        # Open-Meteo API using past_days=30 to perfectly match the logistics generator
        url = (f"https://api.open-meteo.com/v1/forecast?"
               f"latitude={coords['lat']}&longitude={coords['lon']}"
               f"&past_days=30&forecast_days=1"
               f"&daily=precipitation_sum,wind_speed_10m_max,temperature_2m_max"
               f"&timezone=Africa%2FCasablanca")
        
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            daily = data.get("daily", {})
            dates = daily.get("time", [])
            precip = daily.get("precipitation_sum", [])
            wind = daily.get("wind_speed_10m_max", [])
            temp = daily.get("temperature_2m_max", [])
            
            # Loop through the 30 days of data
            for i in range(len(dates)):
                weather_records.append({
                    "city": city,
                    "date": dates[i],
                    "precipitation_mm": precip[i] if precip[i] is not None else 0.0,
                    "max_wind_kmh": wind[i] if wind[i] is not None else 0.0,
                    "max_temp_c": temp[i] if temp[i] is not None else 0.0
                })
            print(f"✅ Success: Downloaded 30 days of data for {city}")
        else:
            print(f"❌ Failed: Could not fetch data for {city}. Status {response.status_code}")

    return weather_records

def save_to_bronze(data):
    headers = ["city", "date", "precipitation_mm", "max_wind_kmh", "max_temp_c"]
    
    print(f"Writing data to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
        
    print("Bronze weather ingestion complete!")

if __name__ == "__main__":
    weather_dataset = fetch_weather_data()
    if weather_dataset:
        save_to_bronze(weather_dataset)