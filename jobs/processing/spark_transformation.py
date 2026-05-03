from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# Initialize Spark Session with MinIO (S3) capabilities
spark = SparkSession.builder \
    .appName("Logistics_Weather_Transformation") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://lakehouse-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Spark Session successfully created.")

# 1. Read Bronze Data
# Assuming local file paths inside the Spark container based on our previous setup
df_logistics = spark.read.csv("/home/jovyan/work/datalake/bronze/logistics/logistics_data.csv", header=True, inferSchema=True)
df_weather = spark.read.csv("/home/jovyan/work/datalake/bronze/weather/weather_data.csv", header=True, inferSchema=True)

# 2. Calculate Weather Severity Score (1 to 5)
df_weather = df_weather.withColumn(
    "weather_severity_score",
    F.when((F.col("precipitation_mm") > 20) | (F.col("max_wind_kmh") > 60), 5)
     .when((F.col("precipitation_mm") > 10) | (F.col("max_wind_kmh") > 40), 4)
     .when((F.col("precipitation_mm") > 5)  | (F.col("max_wind_kmh") > 25), 3)
     .when((F.col("precipitation_mm") > 0)  | (F.col("max_wind_kmh") > 15), 2)
     .otherwise(1)
)

# 3. Join Logistics and Weather Data
# We use F.col() here which is the safest way to reference columns in PySpark
df_silver = df_logistics.join(
    df_weather,
    (F.col("origin") == F.col("city")) & (F.col("event_date") == F.col("date")),
    "inner"
)

# 4. Calculate Final Delay Risk Score (60% Weather, 40% Route Difficulty)
df_silver = df_silver.withColumn(
    "delay_risk_score",
    F.round((F.col("weather_severity_score") * 0.2) + (F.col("route_difficulty_index") * 0.8), 2)
)

# 5. Select only the necessary columns for the Gold Layer
df_gold = df_silver.select(
    "shipment_id",
    "event_timestamp",
    "truck_id",
    "origin",
    "destination",
    "delay_minutes",
    "weather_severity_score",
    "route_difficulty_index",
    "delay_risk_score"
)

# 6. Write to MinIO as Optimized Parquet
s3_output_path = "s3a://datalake/silver/weather_logistics"

print(f"Writing transformed data to {s3_output_path}...")
df_gold.write.mode("overwrite").parquet(s3_output_path)

print("Silver transformation complete!")
spark.stop()