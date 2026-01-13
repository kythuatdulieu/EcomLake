FROM apache/spark:v3.3.2

USER root

# Install tools and Python 3
RUN apt-get update && apt-get install -y curl vim wget net-tools iputils-ping python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install "pyspark==3.3.2" "dagster==1.5.9" "mlflow==2.6.0" "pandas==1.5.3" "numpy<2.0.0" "pyarrow==11.0.0" "dagstermill==0.21.9" "papermill==2.3.3" "dagster-postgres==0.21.9" "dagster-aws==0.21.9" "polars==0.16.16" "pydantic==1.10.10" "minio==7.1.13"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Download additional jars
# Spark 3.3.2 uses Hadoop 3.3.2
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/org/mlflow/mlflow-spark_2.12/2.6.0/mlflow-spark_2.12-2.6.0.jar \
    && mv s3-2.18.41.jar /opt/spark/jars \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/spark/jars \
    && mv mysql-connector-java-8.0.19.jar /opt/spark/jars \
    && mv hadoop-aws-3.3.2.jar /opt/spark/jars \
    && mv mlflow-spark_2.12-2.6.0.jar /opt/spark/jars