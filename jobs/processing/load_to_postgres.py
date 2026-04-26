from pyspark.sql import SparkSession

def load_to_dwh():
    print("Initializing Spark Session with PostgreSQL Driver...")
    # The driver is injected via the spark-submit command
    spark = SparkSession.builder \
        .appName("WeatherLogistics-GoldLayer") \
        .getOrCreate()

    # 1. Read the Silver Parquet Data
    silver_path = "/home/jovyan/work/datalake/silver/weather_logistics"
    print(f"Reading optimized Parquet data from {silver_path}...")
    df = spark.read.parquet(silver_path)

    # 2. Define the PostgreSQL Connection
    # Notice the URL uses 'lakehouse-postgres' (the Docker container name) and port 5432 (internal port)
    db_url = "jdbc:postgresql://lakehouse-postgres:5432/logistics_analytics"
    properties = {
        "user": "data_engineer",
        "password": "supersecret",
        "driver": "org.postgresql.Driver"
    }

    # 3. Write to the Database
    table_name = "fact_weather_logistics"
    print(f"Pushing data to PostgreSQL table: {table_name}...")
    
    # We use mode="overwrite" so if you run this multiple times, it replaces the table instead of duplicating data
    df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=properties)

    print("Phase 4 Complete! Data is now live in the Gold Layer.")
    spark.stop()

if __name__ == "__main__":
    load_to_dwh()