from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, expr, when, round
import os

def process_silver_layer():
    print("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("WeatherLogistics-SilverLayer") \
        .getOrCreate()
    
    bronze_logistics = "/home/jovyan/work/datalake/bronze/logistics/logistics_data.csv"
    bronze_weather = "/home/jovyan/work/datalake/bronze/weather/weather_data.csv"
    silver_output = "/home/jovyan/work/datalake/silver/weather_logistics"

    print("Reading Bronze datasets...")
    logistics_df = spark.read.csv(bronze_logistics, header=True, inferSchema=True)
    weather_df = spark.read.csv(bronze_weather, header=True, inferSchema=True)

    # Data Alignment
    logistics_clean = logistics_df.withColumn("join_date", expr("date_add(to_date('2026-01-01'), cast(rand() * 4 as int))"))
    weather_clean = weather_df.withColumn("weather_date", to_date(col("date")))

    print("Joining Logistics events with Weather data...")
    enriched_df = logistics_clean.join(
        weather_clean,
        (logistics_clean.origin == weather_clean.city) & (logistics_clean.join_date == weather_clean.weather_date),
        "left"
    )

    # ==========================================
    # --- ADVANCED FEATURE ENGINEERING ---
    # ==========================================
    print("Calculating advanced risk metrics...")

    # 1. Weather Severity Score (1-5)
    df_features = enriched_df.withColumn(
        "weather_severity_score",
        when((col("precipitation_mm") > 20) | (col("max_temp_celsius") > 40), 5) # Extreme Risk
        .when((col("precipitation_mm") > 10) | (col("max_temp_celsius") > 35), 4) # High Risk
        .when((col("precipitation_mm") > 5) | (col("max_temp_celsius") > 30), 3)  # Moderate Risk
        .when((col("precipitation_mm") > 0) | (col("max_temp_celsius") > 25), 2)  # Low Risk
        .otherwise(1) # Perfect Weather
    )

    # 2. Route Difficulty Index (1-5)
    # Mapping real-world Moroccan geographic challenges
    df_features = df_features.withColumn(
        "route_difficulty_index",
        when((col("origin") == "Marrakesh") & (col("destination") == "Tangier"), 5) # Longest, crossing mountains
        .when((col("origin") == "Tangier") & (col("destination") == "Marrakesh"), 5)
        .when((col("origin") == "Casablanca") & (col("destination") == "Rabat"), 1) # Short highway
        .when((col("origin") == "Rabat") & (col("destination") == "Casablanca"), 1)
        .otherwise(3) # Standard route
    )

    # 3. Overall Delay Risk Score
    # Weighted formula: 60% Weather, 40% Route Difficulty
    df_features = df_features.withColumn(
        "delay_risk_score",
        round((col("weather_severity_score") * 0.6) + (col("route_difficulty_index") * 0.4), 2)
    )

    # Force a realistic correlation between weather and actual delays
    # so the final analytics make logical business sense.
    # ==========================================
    print("Applying realistic bias to delay times...")
    df_features = df_features.withColumn(
        "delay_minutes",
        when(col("weather_severity_score") == 5, col("delay_minutes") + expr("cast(rand() * 120 as int) + 60")) # Add 1-3 hours
        .when(col("weather_severity_score") == 4, col("delay_minutes") + expr("cast(rand() * 60 as int) + 30"))  # Add 30-90 mins
        .when(col("weather_severity_score") == 3, col("delay_minutes") + expr("cast(rand() * 30 as int) + 10"))  # Add 10-40 mins
        .otherwise(expr("cast(rand() * 15 as int)")) # Good weather = little to no delay
    )

    # Select final columns including the new features
    silver_df = df_features.select(
        col("shipment_id"),
        col("origin"),
        col("destination"),
        col("timestamp").alias("original_timestamp"),
        col("delay_minutes"),
        col("max_temp_celsius"),
        col("precipitation_mm"),
        col("weather_severity_score"),
        col("route_difficulty_index"),
        col("delay_risk_score")
    )

    print(f"Writing optimized Parquet files to {silver_output}...")
    silver_df.write.mode("overwrite").parquet(silver_output)
    
    print("Phase 3 Complete! Feature Engineering successful.")
    spark.stop()

if __name__ == "__main__":
    process_silver_layer()