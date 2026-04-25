import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_logistics_data(num_records=1000):
    # Ensure the directory exists
    output_dir = "/home/jovyan/work/datalake/bronze/logistics"
    os.makedirs(output_dir, exist_ok=True)
    
    data = []
    locations = ["Casablanca", "Rabat", "Kenitra", "Tangier", "Marrakesh"]
    
    for i in range(num_records):
        shipment_id = f"SHIP-{1000 + i}"
        origin = random.choice(locations)
        # Ensure destination is different from origin
        destination = random.choice([loc for loc in locations if loc != origin])
        # Random timestamp within the last 48 hours
        timestamp = datetime.now() - timedelta(hours=random.randint(1, 48))
        # Random delay (70% chance of 0 delay, 30% chance of 1-120 mins)
        delay_minutes = random.randint(1, 120) if random.random() > 0.7 else 0
        
        data.append([shipment_id, origin, destination, timestamp.strftime('%Y-%m-%d %H:%M:%S'), delay_minutes])
    
    df = pd.DataFrame(data, columns=["shipment_id", "origin", "destination", "timestamp", "delay_minutes"])
    
    # Save to the Bronze layer
    file_path = os.path.join(output_dir, "logistics_data.csv")
    df.to_csv(file_path, index=False)
    print(f"Successfully generated {num_records} records at: {file_path}")

if __name__ == "__main__":
    generate_logistics_data()