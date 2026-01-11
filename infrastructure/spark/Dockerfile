FROM apache/spark:v3.3.2

USER root

# Install tools and Python 3
RUN apt-get update && apt-get install -y curl vim wget net-tools iputils-ping python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

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
    && mv s3-2.18.41.jar /opt/spark/jars \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/spark/jars \
    && mv mysql-connector-java-8.0.19.jar /opt/spark/jars \
    && mv hadoop-aws-3.3.2.jar /opt/spark/jars