from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr
import os

def process_silver_layer():
    # 1. Initialize the Spark Engine
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("WeatherLogistics-SilverLayer") \
        .getOrCreate()
    
    # 2. Define Medallion Paths
    bronze_logistics = "/home/jovyan/work/datalake/bronze/logistics/logistics_data.csv"
    bronze_weather = "/home/jovyan/work/datalake/bronze/weather/weather_data.csv"
    silver_output = "/home/jovyan/work/datalake/silver/weather_logistics"

    # 3. Read the Raw Data
    print("Reading Bronze datasets...")
    logistics_df = spark.read.csv(bronze_logistics, header=True, inferSchema=True)
    weather_df = spark.read.csv(bronze_weather, header=True, inferSchema=True)

    # 4. Data Cleaning & Alignment
    # We assign a random date between Jan 1 and Jan 4, 2026 so it joins perfectly with our API data
    logistics_clean = logistics_df.withColumn(
        "join_date", 
        expr("date_add(to_date('2026-01-01'), cast(rand() * 4 as int))")
    )
    
    weather_clean = weather_df.withColumn("weather_date", to_date(col("date")))

    # 5. The Core Transformation (The Join)
    print("Joining Logistics events with Weather data...")
    enriched_df = logistics_clean.join(
        weather_clean,
        (logistics_clean.origin == weather_clean.city) & (logistics_clean.join_date == weather_clean.weather_date),
        "left"
    )

    # 6. Select and format the final Silver columns
    silver_df = enriched_df.select(
        col("shipment_id"),
        col("origin"),
        col("destination"),
        col("timestamp").alias("original_timestamp"),
        col("delay_minutes"),
        col("max_temp_celsius"),
        col("precipitation_mm")
    )

    # 7. Write to the Silver Layer as Parquet
    print(f"Writing optimized Parquet files to {silver_output}...")
    silver_df.write.mode("overwrite").parquet(silver_output)
    
    print("Phase 3 Complete! Silver layer is ready.")
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()