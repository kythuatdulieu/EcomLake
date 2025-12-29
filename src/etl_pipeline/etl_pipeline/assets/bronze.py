from dagster import asset, Output, Config
from pyspark.sql import DataFrame
from ..resources.spark_io_manager import get_spark_session

class BronzeConfig(Config):
    load_mode: str = "full"  # "full" or "incremental"

COMPUTE_KIND = "PySpark"
LAYER = "bronze"

def ingest_table(context, table_name: str, config: BronzeConfig) -> DataFrame:
    context.log.info(f"Starting ingestion for {table_name} in {config.load_mode} mode")
    
    # Credentials should ideally be in secrets/env, using defaults for now matching project
    jdbc_url = "jdbc:mysql://mysql:3306/olist?useSSL=false"
    db_properties = {
        "user": "root", 
        "password": "admin",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    with get_spark_session(None, f"Bronze Ingestion - {table_name}") as spark:
        # Read from MySQL
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
        
        # In a real incremental scenario, we would filter 'df' here based on a watermark.
        # Check if table has timestamp columns for incremental logic if needed.
        # For Olist, it's mostly static history, but we structure for it.
        
        context.log.info(f"Loaded {df.count()} rows from MySQL table {table_name}")
        return df

@asset(
    description="Load table 'customers' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_customer(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "customers", config)
    return Output(
        value=df,
        metadata={
            "table": "customers",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'sellers' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "seller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_seller(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "sellers", config)
    return Output(
        value=df,
        metadata={
            "table": "sellers",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'products' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "product"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_product(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "products", config)
    return Output(
        value=df,
        metadata={
            "table": "products",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'orders' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "order"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_order(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "orders", config)
    return Output(
        value=df,
        metadata={
            "table": "orders",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'order_items' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "orderitem"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_order_item(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "order_items", config)
    return Output(
        value=df,
        metadata={
            "table": "order_items",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'payments' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "payment"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_payment(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "payments", config)
    return Output(
        value=df,
        metadata={
            "table": "payments",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'order_reviews' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "orderreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_order_review(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "order_reviews", config)
    return Output(
        value=df,
        metadata={
            "table": "order_reviews",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'product_category_name_translation' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "productcategory"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_product_category(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "product_category_name_translation", config)
    return Output(
        value=df,
        metadata={
            "table": "product_category_name_translation",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )

@asset(
    description="Load table 'geolocation' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_geolocation(context, config: BronzeConfig) -> Output[DataFrame]:
    df = ingest_table(context, "geolocation", config)
    return Output(
        value=df,
        metadata={
            "table": "geolocation",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )
