FROM apache/spark:v3.3.2 AS spark-base

FROM python:3.9.16-slim

# Install spark and java
ARG openjdk_version="17"

RUN apt-get update --yes && \
  apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps build-essential libssl-dev libffi-dev python3-dev make gcc && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy Spark from base image
COPY --from=spark-base /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Download jars (delta lake, mysql connector, hadoop-aws)
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
  && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
  && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
  && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
  && curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
  && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
  && mv delta-storage-2.3.0.jar /opt/spark/jars \
  && mv mysql-connector-java-8.0.19.jar /opt/spark/jars \
  && mv hadoop-aws-3.3.2.jar /opt/spark/jars \
  && mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars \
  && mv s3-2.18.41.jar /opt/spark/jars

# Add repository code
WORKDIR /opt/dagster/app/etl_pipeline  
COPY requirements.txt /opt/dagster/app/etl_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app/etl_pipeline

# CMD allows this to be overridden from run launchers or executors that want to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
# CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "etl_pipeline"]