import os
from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager

from . import assets
from .job import etl_pipeline_job, bronze_job, silver_job, gold_job
from .schedule import daily_etl_schedule
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.spark_io_manager import SparkIOManager
from .resources.spark_resource import SparkSessionResource


MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASES"),
    "user": os.getenv("MYSQL_ROOT_USER"),
    "password": os.getenv("MYSQL_ROOT_PASSWORD"),
}


MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
}

SPARK_CONFIG = {
    "spark_master": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "minio_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "minio_secret_key": os.getenv("MINIO_SECRET_KEY"),
}

resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "spark_io_manager": SparkIOManager(SPARK_CONFIG),
    "spark_session_resource": SparkSessionResource(
        spark_master=SPARK_CONFIG.get("spark_master") or "spark://spark-master:7077",
        endpoint_url=SPARK_CONFIG.get("endpoint_url") or "minio:9000",
        minio_access_key=SPARK_CONFIG.get("minio_access_key") or "minio",
        minio_secret_key=SPARK_CONFIG.get("minio_secret_key") or "minio123",
    ),
    "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
}

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[etl_pipeline_job, bronze_job, silver_job, gold_job],
    schedules=[daily_etl_schedule],
    resources=resources,
)

