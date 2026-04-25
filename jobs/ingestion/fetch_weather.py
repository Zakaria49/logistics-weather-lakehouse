import pandas as pd
import requests
import os
import time

def fetch_weather():
    coords = {
        "Casablanca": (33.5731, -7.5898),
        "Rabat": (34.0209, -6.8416),
        "Kenitra": (34.2610, -6.5802),
        "Tangier": (35.7595, -5.8340),
        "Marrakesh": (31.6295, -7.9811)
    }

    all_weather = []
    # Using early 2026 data to ensure availability
    start_date = "2026-01-01"
    end_date = "2026-01-05"

    print("Connecting to Open-Meteo Historical API...")
    
    for city, (lat, lon) in coords.items():
        print(f"Fetching data for {city}...")
        
        # The direct API endpoint requesting daily max temp and total rainfall
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_max,precipitation_sum&timezone=Africa%2FCasablanca"
        
        try:
            response = requests.get(url)
            
            # Check if the HTTP request was successful
            if response.status_code == 200:
                data = response.json()
                daily_data = data.get('daily', {})
                
                if daily_data:
                    # Convert the JSON dictionary into a tabular DataFrame
                    df = pd.DataFrame({
                        'date': daily_data.get('time', []),
                        'max_temp_celsius': daily_data.get('temperature_2m_max', []),
                        'precipitation_mm': daily_data.get('precipitation_sum', []),
                        'city': city
                    })
                    all_weather.append(df)
                    print(f"  -> Success! Retrieved {len(df)} days of data.")
                else:
                    print(f"  -> Warning: No daily data found in JSON payload.")
            else:
                print(f"  -> API Error: Status Code {response.status_code}")
                
            # Polite 1-second delay so we don't overload the free API
            time.sleep(1)
            
        except Exception as e:
            print(f"  -> Request failed for {city}: {e}")

    # Combine and save the data
    if all_weather:
        final_df = pd.concat(all_weather, ignore_index=True)
        output_dir = "/home/jovyan/work/datalake/bronze/weather"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, "weather_data.csv")
        final_df.to_csv(file_path, index=False)
        print(f"\nPhase 2 Complete! Saved {len(final_df)} total records to {file_path}")
    else:
        print("\nError: Failed to fetch any data.")

if __name__ == "__main__":
    fetch_weather()