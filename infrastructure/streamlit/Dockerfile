# app/Dockerfile

# Base stage for Spark
FROM apache/spark:v3.3.2 AS spark-base

FROM python:3.9.16-slim

WORKDIR /app

# Install Java, curl, git, and build essentials
ARG openjdk_version="17"
RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    "openjdk-${openjdk_version}-jre-headless" \
    procps \
    && rm -rf /var/lib/apt/lists/*

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

# Install python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy source code
COPY . .

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "Home.py", "--server.port=8501", "--server.address=0.0.0.0"]

