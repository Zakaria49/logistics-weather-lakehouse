# 🚚 End-to-End Logistics Data Lakehouse & Predictive Analytics Pipeline

## 📖 Overview
This project is a complete, automated Data Engineering pipeline built to simulate, ingest, process, and analyze logistics supply chain data. It utilizes a **Medallion Architecture** to combine synthetic fleet data with real historical weather data (via Open-Meteo API) to calculate and validate a predictive **Delay Risk Score** for shipping routes in Morocco.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow
* **Data Processing:** Apache Spark (PySpark)
* **Data Lake (Object Storage):** MinIO (S3-compatible)
* **Data Warehouse:** PostgreSQL
* **Validation & Analytics:** Jupyter, Pandas, Seaborn
* **Infrastructure:** Docker & Docker Compose

## 🏗️ Architecture & Pipeline Flow

### 0. Orchestration Layer (Apache Airflow)
* **The Brain of the Operation:** The entire pipeline is scheduled and monitored by **Apache Airflow**. A custom DAG (`logistics_lakehouse_pipeline`) dictates the strict execution dependencies (Ingestion ➔ Spark Processing ➔ Data Warehouse Load), ensuring no downstream task runs until the upstream data is securely in the Data Lake.
  
### 1. Bronze Layer (Raw Data Ingestion)
* **Logistics Generator (`generate_logistics.py`):** A custom Python script that generates synthetic trucking data, injecting mathematical delay correlations based on the geographical difficulty of Moroccan routes (e.g., Tangier to Agadir).
* **Weather API Fetcher (`fetch_weather.py`):** Automatically pulls the last 30 days of real, historical weather data (Precipitation, Wind, Temp) for 8 Moroccan cities using the Open-Meteo API.
* **The Drop Zone:** For this local containerized deployment, the raw CSVs are ingested into a **local shared Docker volume**. This acts as our initial landing zone, bridging the local compute environment with the processing engine.

### 2. Silver Layer (Processing & Feature Engineering)
* **Spark Transformation (`spark_transformation.py`):** PySpark reads the raw CSVs from a local shared Docker volume (Bronze Drop Zone), joins the logistics and weather datasets on `city` and `event_date`, and engineers new features:
    * `weather_severity_score` (1-5 based on wind/rain thresholds).
    * `delay_risk_score`: A weighted algorithm designed to predict delivery bottlenecks.
* **The Data Lake (MinIO):** Once transformed, PySpark pushes the cleansed data over the network into **MinIO**, an S3-compatible cloud object storage system. By storing the Silver layer as optimized **Parquet** files in MinIO, the architecture successfully decouples compute from storage, mirroring an enterprise AWS S3 environment.

### 3. Gold Layer (Data Warehouse)
* **Postgres Loader (`load_to_postgres.py`):** Spark reads the cleansed Parquet data and overwrites the `fact_weather_logistics` table in PostgreSQL, making it immediately available for BI tools and Data Science validation.

---

## 📊 Business Impact & Machine Learning Validation

A key component of this project was validating the engineered pipeline data to ensure it provided actual business value. Using Jupyter Notebooks, I conducted a feature correlation analysis against the actual `delay_minutes`.

### 🔄 The Iterative Engineering Cycle
During initial testing, the `delay_risk_score` utilized a 60/40 weight split favoring weather. However, correlation matrices revealed that spatial route difficulty was a much stronger predictor of delays than historical weather in this dataset.

I executed a **weight optimization** in the PySpark processing layer, shifting the algorithm to respect the data reality (80% Route / 20% Weather). 

### 📈 Final Model Performance
The optimized pipeline successfully produced a highly predictive metric:
* **Route Difficulty Correlation:** 0.526
* **Optimized Delay Risk Score Correlation:** 0.527 🏆

The scatter plot below visually proves the effectiveness of the pipeline. It demonstrates a clear, linear relationship between the engineered Risk Score and real-world supply chain delays, accounting for both predictable bottlenecks and real-world variance (e.g., zero-delay "perfect runs").

![Feature Validation Scatter Plot](image_6d7ab7.png)

---

## 🚀 How to Run the Project Locally

1. **Clone the repository & start the infrastructure:**
   ```bash
   git clone [https://github.com/yourusername/logistics-lakehouse.git](https://github.com/yourusername/logistics-lakehouse.git)
   cd logistics-lakehouse
   docker-compose up -d
