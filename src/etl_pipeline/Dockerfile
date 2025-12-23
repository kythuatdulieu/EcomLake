
FROM python:3.9.16-slim

# Install spark and java
ARG openjdk_version="17"

RUN apt-get update --yes && \
  apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps build-essential libssl-dev libffi-dev python3-dev make gcc && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

# Download spark neccessary jars
RUN curl -O https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
  && tar zxvf spark-3.4.0-bin-hadoop3.tgz \
  && rm -rf spark-3.4.0-bin-hadoop3.tgz \
  && mv spark-3.4.0-bin-hadoop3/ /usr/local/ \
  && rm -rf /usr/local/spark \
  && rm -rf /usr/local/spark-3.3.0-bin-hadoop3 \
  && ln -s /usr/local/spark-3.4.0-bin-hadoop3 /usr/local/spark

# Download jars (delta lake, mysql connector, hadoop-aws)
RUN curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
  && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
  && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
  && mv delta-core_2.12-2.2.0.jar /usr/local/spark/jars \
  && mv delta-storage-2.2.0.jar /usr/local/spark/jars \
  && mv mysql-connector-java-8.0.19.jar /usr/local/spark/jars \
  && mv hadoop-aws-3.3.4.jar /usr/local/spark/jars \
  && mv aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars

# Add repository code
WORKDIR /opt/dagster/app/etl_pipeline  
COPY requirements.txt /opt/dagster/app/etl_pipeline
RUN pip install --upgrade pip && pip install -r requirements.txt
COPY . /opt/dagster/app/etl_pipeline

# CMD allows this to be overridden from run launchers or executors that want to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]
# CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "etl_pipeline"]