from dagster import asset, Output, Config
from pyspark.sql import DataFrame, SparkSession
from ..resources.spark_resource import SparkSessionResource

class BronzeConfig(Config):
    load_mode: str = "full"  # "full" or "incremental"

COMPUTE_KIND = "PySpark"
LAYER = "bronze"

def ingest_table(context, spark: SparkSession, table_name: str, config: BronzeConfig) -> DataFrame:
    """
    Ingest table from MySQL using provided SparkSession.
    
    Args:
        context: Dagster context
        spark: Shared SparkSession from resource
        table_name: Name of MySQL table to ingest
        config: Bronze configuration
    
    Returns:
        Spark DataFrame with ingested data
    """
    context.log.info(f"Starting ingestion for {table_name} in {config.load_mode} mode")
    
    # Credentials should ideally be in secrets/env, using defaults for now matching project
    jdbc_url = "jdbc:mysql://mysql:3306/olist?useSSL=false&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=CONVERT_TO_NULL"

    db_properties = {
        "user": "root", 
        "password": "admin",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    # Read from MySQL using the provided spark session
    df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=db_properties)
    
    # Force repartition to 3 to utilize all 3 workers (since spark.cores.max=3)
    # This ensures that even for small tables, we see tasks executing on all workers.
    df = df.repartition(3)
    
    context.log.info(f"Loaded {df.count()} rows from MySQL table {table_name}")
    return df

@asset(
    description="Load table 'customers' from MySQL to Bronze Layer",
    io_manager_key="spark_io_manager",
    key_prefix=["bronze", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def bronze_customer(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "customers", config)
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
def bronze_seller(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "sellers", config)
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
def bronze_product(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "products", config)
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
def bronze_order(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "orders", config)
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
def bronze_order_item(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "order_items", config)
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
def bronze_payment(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "payments", config)
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
def bronze_order_review(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "order_reviews", config)
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
def bronze_product_category(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "product_category_name_translation", config)
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
def bronze_geolocation(context, spark_session_resource: SparkSessionResource, config: BronzeConfig) -> Output[DataFrame]:
    spark = spark_session_resource.get_session()
    df = ingest_table(context, spark, "geolocation", config)
    return Output(
        value=df,
        metadata={
            "table": "geolocation",
            "row_count": df.count(),
            "mode": config.load_mode
        }
    )
