import csv
import random
import uuid
import os
from datetime import datetime, timedelta

# Define the Bronze Drop Zone path (inside the Spark container)
OUTPUT_DIR = "/home/jovyan/work/datalake/bronze/logistics"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "logistics_data.csv")

# Ensure the directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define Moroccan logistics routes with engineered "Difficulty" scores (1 to 5)
# 1 = Flat highway (Easy) | 5 = Mountainous / Heavy Port Traffic (Hard)
ROUTES = [
    {"origin": "Casablanca", "destination": "Rabat", "difficulty": 1},
    {"origin": "Rabat", "destination": "Tangier", "difficulty": 2},
    {"origin": "Marrakech", "destination": "Fes", "difficulty": 3},
    {"origin": "Casablanca", "destination": "Oujda", "difficulty": 4},
    {"origin": "Tangier", "destination": "Agadir", "difficulty": 5},
    {"origin": "Agadir", "destination": "Laayoune", "difficulty": 4},
    {"origin": "Casablanca", "destination": "Marrakech", "difficulty": 2}
]

def generate_synthetic_logistics_data(num_records=1000):
    print(f"Generating {num_records} logistics records with injected correlations...")
    
    data = []
    # Generate data over the last 30 days to match weather API pulls
    base_date = datetime.now() - timedelta(days=30)
    
    for _ in range(num_records):
        route = random.choice(ROUTES)
        
        # Randomize the timestamp within the 30-day window
        random_days = random.randint(0, 30)
        random_hours = random.randint(0, 23)
        event_time = base_date + timedelta(days=random_days, hours=random_hours)
        
        # ---------------------------------------------------------
        # THE FIX: Engineering the Correlation Data
        # ---------------------------------------------------------
        # 1. Base Noise: Every truck has a chance of minor random delays (traffic, loading)
        random_noise = random.randint(0, 15)
        
        # 2. Route Penalty: Multiply difficulty by a random severity factor (5 to 12 minutes per difficulty level)
        # A difficulty 5 route will add 25-60 minutes. A difficulty 1 route adds 5-12 minutes.
        route_penalty = route["difficulty"] * random.randint(5, 12)
        
        # 3. Final Delay Calculation
        total_delay_minutes = random_noise + route_penalty
        
        # Occasional "perfect" runs with zero delay
        if random.random() < 0.15: 
            total_delay_minutes = 0

        # Append the record
        data.append({
            "shipment_id": str(uuid.uuid4())[:8],
            "event_timestamp": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "event_date": event_time.strftime("%Y-%m-%d"),
            "truck_id": f"TRK-{random.randint(100, 999)}",
            "origin": route["origin"],
            "destination": route["destination"],
            "route_difficulty_index": route["difficulty"],
            "delay_minutes": total_delay_minutes
        })
        
    return data

def save_to_bronze(data):
    # Define CSV headers
    headers = ["shipment_id", "event_timestamp", "event_date", "truck_id", "origin", "destination", "route_difficulty_index", "delay_minutes"]
    
    print(f"Writing data to {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)
        
    print("Bronze ingestion complete!")

if __name__ == "__main__":
    # Generate 1000 rows to ensure statistical significance for the Jupyter Notebook
    logistics_dataset = generate_synthetic_logistics_data(1000)
    save_to_bronze(logistics_dataset)