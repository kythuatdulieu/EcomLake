FROM apache/spark:3.4.0

USER root

# Install tools
RUN apt-get update && apt-get install -y curl vim wget net-tools iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Fix jar for Kubernetes (optional) or other dependencies
# But mainly ensure we are using the right Spark image

# Download additional jars if needed.
# Spark 3.4.0 uses Hadoop 3.3.4.
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && mv s3-2.18.41.jar /opt/spark/jars \
    && mv aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/spark/jars \
    && mv mysql-connector-java-8.0.19.jar /opt/spark/jars \
    && mv hadoop-aws-3.3.4.jar /opt/spark/jars