from pyspark.sql import SparkSession

# Initialize Spark with S3 and Postgres drivers
spark = SparkSession.builder \
    .appName("Load_Gold_To_Postgres") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://lakehouse-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "supersecret") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# 1. Read from the Silver/Gold MinIO Bucket
s3_input_path = "s3a://datalake/silver/weather_logistics"
print(f"Reading Parquet data from {s3_input_path}...")
df_gold = spark.read.parquet(s3_input_path)

# 2. Define PostgreSQL Connection Properties
# (Match these to your docker-compose Postgres credentials)
jdbc_url = "jdbc:postgresql://lakehouse-postgres:5432/logistics_analytics"
connection_properties = {
    "user": "data_engineer",
    "password": "supersecret",
    "driver": "org.postgresql.Driver"
}

# 3. Write Data to PostgreSQL
print("Loading data into PostgreSQL Data Warehouse...")
df_gold.write.jdbc(
    url=jdbc_url,
    table="fact_weather_logistics",
    mode="overwrite", # This ensures the table schema updates automatically!
    properties=connection_properties
)

print("Data successfully loaded into PostgreSQL!")
spark.stop()