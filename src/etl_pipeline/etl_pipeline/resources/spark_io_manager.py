import os
from contextlib import contextmanager
from datetime import datetime
from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame


@contextmanager
def get_spark_session(config=None, run_id="Spark IO Manager"):
    if config is None:
        config = {}

    endpoint_url = config.get("endpoint_url") or os.getenv("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = config.get("minio_access_key") or os.getenv("MINIO_ACCESS_KEY", "minio")
    minio_secret_key = config.get("minio_secret_key") or os.getenv("MINIO_SECRET_KEY", "minio123")

    executor_memory = "1g" if run_id != "Spark IO Manager" else "1500m"
    try:
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(run_id)
            .config(
                "spark.jars",
                "/opt/spark/jars/delta-core_2.12-2.3.0.jar,"
                "/opt/spark/jars/hadoop-aws-3.3.2.jar,"
                "/opt/spark/jars/delta-storage-2.3.0.jar,"
                "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar,"
                "/opt/spark/jars/s3-2.18.41.jar,"
                "/opt/spark/jars/mysql-connector-java-8.0.19.jar,"
                "/opt/spark/jars/mlflow-spark-2.6.0.jar"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}")
            .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
            .config('spark.sql.warehouse.dir', f's3a://lakehouse/')
            .config("hive.metastore.uris", "thrift://hive-metastore:9083")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.executor.cores", "1")
            .config("spark.cores.max", "3")                
            .config("spark.default.parallelism", "3")
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.driver.memory", "512m")
            .config("spark.executor.memory", "512m")
            .config("spark.pyspark.python", "/usr/bin/python3")
            .config(
                "spark.driver.extraClassPath",
                "/opt/spark/jars/delta-core_2.12-2.3.0.jar:"
                "/opt/spark/jars/hadoop-aws-3.3.2.jar:"
                "/opt/spark/jars/delta-storage-2.3.0.jar:"
                "/opt/spark/jars/aws-java-sdk-bundle-1.11.1026.jar:"
                "/opt/spark/jars/s3-2.18.41.jar:"
                "/opt/spark/jars/mysql-connector-java-8.0.19.jar:"
                "/opt/spark/jars/mlflow-spark-2.6.0.jar"
            )
            .enableHiveSupport()
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")


class SparkIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: DataFrame):
        """
            Write output to s3a (aka minIO) as parquet file.
            Respects 'mode' in metadata: 'full' -> overwrite, 'incremental' -> append.
        """

        context.log.debug("(Spark handle_output) Writing output to MinIO ...")

        layer, _,table = context.asset_key.path
        table_name = str(table.replace(f"{layer}_", ""))
        
        # Determine write mode
        load_mode = context.metadata.get("mode", "full")
        write_mode = "append" if load_mode == "incremental" else "overwrite"
        
        try:
            spark = obj.sparkSession
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {layer}")
            
            context.log.info(f"Saving {table_name} to {layer} with mode={write_mode} (load_mode={load_mode})")
            obj.write.format("delta").mode(write_mode).saveAsTable(f"{layer}.{table_name}")
            context.log.debug(f"Saved {table_name} to {layer}")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")

    def load_input(self, context: InputContext) -> DataFrame:
        """
        Load input from s3a (aka minIO) from parquet file to spark.sql.DataFrame
        """

        # E.g context.asset_key.path: ['silver', 'goodreads', 'book']
        context.log.debug(f"Loading input from {context.asset_key.path}...")
        layer,_,table = context.asset_key.path
        table_name = str(table.replace(f"{layer}_",""))
        context.log.debug(f'loading input from {layer} layer - table {table_name}...')

        try:
            with get_spark_session(self._config) as spark:
                df = None
                df = spark.read.table(f"{layer}.{table_name}")
                context.log.debug(f"Loaded {df.count()} rows from {table_name}")
                return df
        except Exception as e:
            raise Exception(f"Error while loading input: {e}")
