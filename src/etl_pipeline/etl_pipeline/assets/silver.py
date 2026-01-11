from dagster import asset, AssetIn, Output, Config
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from ..resources.spark_io_manager import get_spark_session

class SilverConfig(Config):
    load_mode: str = "full"

COMPUTE_KIND = "PySpark"
LAYER = "silver"

# Silver cleaned customer
@asset(
    description="Clean 'customers' table from bronze layer",
    ins={
        "bronze_customer": AssetIn(
            key_prefix=["bronze", "customer"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "customer"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_customer(context, config: SilverConfig, bronze_customer: DataFrame) -> Output[DataFrame]:
    context.log.info(f"Processing silver_cleaned_customer in {config.load_mode} mode")
    
    # Transformation
    spark_df = bronze_customer.dropDuplicates().na.drop()
    
    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_customer",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'seller' table from bronze layer",
    ins={
        "bronze_seller": AssetIn(
            key_prefix=["bronze", "seller"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "seller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_seller(context, config: SilverConfig, bronze_seller: DataFrame) -> Output[DataFrame]:
    context.log.info(f"Processing silver_cleaned_seller in {config.load_mode} mode")
    
    spark_df = bronze_seller.na.drop()
    spark_df = spark_df.dropDuplicates(subset=["seller_id"])

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_sellers",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'product' table from bronze layer",
    ins={
        "bronze_product": AssetIn(
            key_prefix=["bronze", "product"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "product"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_product(context, config: SilverConfig, bronze_product: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_product.na.drop().dropDuplicates()
    
    columns_to_convert = [
        "product_description_length",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for column in columns_to_convert:
        spark_df = spark_df.withColumn(column, col(column).cast("integer"))

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_product",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'order_items' table from bronze layer",
    ins={
        "bronze_order_item": AssetIn(
            key_prefix=["bronze", "orderitem"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderitem"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_item(context, config: SilverConfig, bronze_order_item: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_order_item.withColumn("price", round(col("price"), 2).cast("double"))
    spark_df = spark_df.withColumn("freight_value", round(col("freight_value"), 2).cast("double"))
    spark_df = spark_df.na.drop().dropDuplicates()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_order_items",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'payment' table from bronze layer",
    ins={
        "bronze_payment": AssetIn(
            key_prefix=["bronze", "payment"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "payment"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_payment(context, config: SilverConfig, bronze_payment: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_payment.withColumn("payment_value", round(col("payment_value"), 2).cast("double"))
    spark_df = spark_df.withColumn("payment_installments", col("payment_installments").cast("integer"))
    spark_df = spark_df.na.drop().dropDuplicates()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_payments",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'order_review' table from bronze layer",
    ins={
        "bronze_order_review": AssetIn(
            key_prefix=["bronze", "orderreview"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "orderreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order_review(context, config: SilverConfig, bronze_order_review: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_order_review.drop("review_comment_title")
    spark_df = spark_df.na.drop().dropDuplicates()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_order_reviews",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'product_category' table from bronze layer",
    ins={
        "bronze_product_category": AssetIn(
            key_prefix=["bronze", "productcategory"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "productcategory"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_product_category(context, config: SilverConfig, bronze_product_category: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_product_category.dropDuplicates().na.drop()

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_product_category",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'orders' table from bronze layer",
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "order"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_order(context, config: SilverConfig, bronze_order: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_order.na.drop().dropDuplicates(["order_id"])

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_orders",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Create 'date' table from silver orders",
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "date"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_date(context, config: SilverConfig, bronze_order: DataFrame) -> Output[DataFrame]:
    # We can do transformation directly on the input df
    date_df = bronze_order.select("order_purchase_timestamp")
    date_df = date_df.na.drop().dropDuplicates()

    return Output(
        value=date_df,
        metadata={
            "table": "silver_date",
            "row_count": date_df.count(),
            "mode": config.load_mode
        },
    )


@asset(
    description="Clean 'geo' table from bronze layer",
    ins={
        "bronze_geolocation": AssetIn(
            key_prefix=["bronze", "geolocation"],
        ),
    },
    io_manager_key="spark_io_manager",
    key_prefix=["silver", "geolocation"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def silver_cleaned_geolocation(context, config: SilverConfig, bronze_geolocation: DataFrame) -> Output[DataFrame]:
    spark_df = bronze_geolocation.dropDuplicates().na.drop()
    
    # Filter coordinates for Brazil limits
    spark_df = spark_df.filter(
        (col("geolocation_lat") <= 5.27438888)
        & (col("geolocation_lng") >= -73.98283055)
        & (col("geolocation_lat") >= -33.75116944)
        & (col("geolocation_lng") <= -34.79314722)
    )

    return Output(
        value=spark_df,
        metadata={
            "table": "silver_cleaned_geolocation",
            "row_count": spark_df.count(),
            "mode": config.load_mode
        },
    )
