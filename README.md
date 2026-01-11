# DataLakeHouse Project

## Introduction
This project constructs a modern Data Lakehouse using MinIO, Spark, Delta Lake, Trino (or Presto), and Hive Metastore. It orchestrates ETL pipelines with Dagster and provides a Streamlit-based application for data interaction and visualization.

The architecture enables:
- **Bronze Layer**: Raw data ingestion.
- **Silver Layer**: Cleaned and validated data.
- **Gold Layer**: Aggregated business-level data.
- **Platinum Layer**: Application-ready data marts.

## System Architecture

The system leverages Docker for containerization and consists of the following components:
- **MinIO**: S3-compatible object storage.
- **Apache Spark**: Distributed processing engine.
- **Delta Lake**: Storage layer bringing ACID transactions to Spark.
- **Hive Metastore**: Centralized metadata management.
- **Trino/Presto**: Distributed SQL query engine.
- **Dagster**: Data orchestrator.
- **Streamlit**: Web application interface.
- **MySQL**: Transactional database for source simulation and application backend.

## Getting Started

### Prerequisites
- Docker and Docker Compose installed.
- Python 3.9+ (for local development).

### Installation & Running

1. **Clone the repository:**
   ```bash
   git clone git@github.com:kythuatdulieu/EcomLake.git
   cd EcomLake
   ```

2. **Environment Setup**
   Copy the example environment file:
   ```bash
   cp env.example .env
   ```

3. **Start the Infrastructure:**
   Run the following command to start all services:
   ```bash
   docker compose up -d
   ```

### 5. Component Status Check
   After starting the services, verify they are running correctly:
   ```bash
   docker ps
   ```
   Ensure services like `spark-master`, `minio`, `mysql`, `de_dagster_dagit`, and others have a status of `Up`.

### 6. Data Feeding & Initialization
   The project includes a `Makefile` to simplify database initialization (required step for the first run):

   **Create schema and structure:**
   ```bash
   make mysql_create
   ```
   
   **Load sample data:**
   ```bash
   make mysql_load
   ```
   *Note: These commands populate the MySQL database which acts as the data source for the Bronze layer extraction.*

### 7. Access Services
   - **Dagster UI (ETL Orchestration)**: [http://localhost:3001](http://localhost:3001)
   - **MinIO Console (Object Storage)**: [http://localhost:9001](http://localhost:9001) (User/Pass: `minio` / `minio123`)
   - **Spark Master UI**: [http://localhost:8081](http://localhost:8081)
   - **Streamlit App (Dashboard)**: [http://localhost:8501](http://localhost:8501)
   - **MLflow UI (Model Tracking)**: [http://localhost:7893](http://localhost:7893)
   - **Metabase (BI)**: [http://localhost:3000](http://localhost:3000)

## Operational Flow

### Data Pipeline (ETL)
The system follows a Multi-Hop architecture defined in Dagster:

1.  **Ingest (Bronze Layer)**: 
    - Raw data is extracted from **MySQL** (simulating a production OLTP database).
    - Data is loaded into **MinIO** (Data Lake) as Parquet files via Polars.
    - *Assets:* `bronze_customer`, `bronze_order`, `bronze_product`, etc.

2.  **Refine (Silver Layer)**:
    - Data is read from MinIO Bronze layer.
    - Cleaning and transformation (schema validation, casting) are performed using **Spark**.
    - Cleaned data is written back to MinIO as Delta Lake tables (or Parquet).

3.  **Aggregated (Gold Layer)**:
    - Business-level aggregations and joining of Silver tables.
    - Ready for analysis and reporting.

### Application (Streamlit)
The Streamlit dashboard allows end-users to:
- **View Data**: Browse raw comments and product reviews.
- **Prediction**: Use pre-trained models to predict sentiment on new comments.
- **Chatbot (Text-to-SQL)**: Ask questions about the data in natural language (requires OpenAI API Key).

## Project Structure
- `src/`: Application source code.
  - `dashboard/`: Streamlit web application.
  - `etl_pipeline/`: Dagster definitions for Bronze, Silver, Gold layers.
- `infrastructure/`: Dockerfiles for Spark, Dagster, MLflow.
- `config/`: Configuration for Hive, Spark, Postgres.
- `data/`: Database initialization scripts (`db_scripts`) and raw data.
- `notebooks/`: Jupyter notebooks for experimentation.

## Authors
**Nguyễn Đức Linh & Vũ Trung Kiên**
