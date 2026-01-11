import os
from dagster import ConfigurableResource
from pyspark.sql import SparkSession
from pydantic import Field, PrivateAttr


class SparkSessionResource(ConfigurableResource):
    """
    Dagster resource that provides a shared SparkSession for all assets.
    This ensures all assets reuse the same Spark connection instead of creating separate sessions.
    """
    
    spark_master: str = Field(
        default="spark://spark-master:7077",
        description="Spark master URL"
    )
    endpoint_url: str = Field(
        default="minio:9000",
        description="MinIO endpoint URL"
    )
    minio_access_key: str = Field(
        default="minio",
        description="MinIO access key"
    )
    minio_secret_key: str = Field(
        default="minio123",
        description="MinIO secret key"
    )
    
    # Use PrivateAttr for internal state that shouldn't be part of the config
    _spark_session: SparkSession = PrivateAttr(default=None)
    
    def get_session(self) -> SparkSession:
        """
        Get or create the shared SparkSession.
        Uses getOrCreate() to ensure only one session exists.
        """
        if self._spark_session is None:
            self._spark_session = (
                SparkSession.builder
                .master(self.spark_master)
                .appName("Dagster ETL Pipeline")
                .config(
                    "spark.jars",
                    "/opt/spark/jars/delta-core_2.12-2.3.0.jar,"
                    "/opt/spark/jars/hadoop-aws-3.3.2.jar,"
                    "/opt/spark/jars/delta-storage-2.3.0.jar,"
                    "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,"
                    "/opt/spark/jars/s3-2.18.41.jar,"
                    "/opt/spark/jars/mysql-connector-java-8.0.19.jar",
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.hadoop.fs.s3a.endpoint", f"http://{self.endpoint_url}")
                .config("spark.hadoop.fs.s3a.access.key", self.minio_access_key)
                .config("spark.hadoop.fs.s3a.secret.key", self.minio_secret_key)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config('spark.hadoop.fs.s3a.aws.credentials.provider', 
                        'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
                .config('spark.sql.warehouse.dir', 's3a://lakehouse/')
                .config("hive.metastore.uris", "thrift://hive-metastore:9083")
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.executor.cores", "1")
                .config("spark.cores.max", "3")
                .config("spark.driver.memory", "512m")
                .config("spark.executor.memory", "512m")
                .config("spark.default.parallelism", "3")
                .config("spark.sql.shuffle.partitions", "3")
                .config("spark.pyspark.python", "python3")
                .config("spark.pyspark.driver.python", "python3")
                .enableHiveSupport()
                .getOrCreate()
            )
        
        return self._spark_session
    
    def teardown(self):
        """
        Optional cleanup method.
        In practice, we usually don't stop the session during a run
        to allow session reuse across multiple assets.
        """
        if self._spark_session is not None:
            # Don't stop - let Dagster manage lifecycle
            pass
