FROM jupyter/all-spark-notebook:spark-3.3.2

USER root 

# Ensure /opt/spark exists for consistency
RUN if [ ! -d "/opt/spark" ]; then ln -s /usr/local/spark /opt/spark; fi

# Download jars
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && mv s3-2.18.41.jar /opt/spark/jars \
    && mv aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars \
    && mv hadoop-aws-3.3.2.jar /opt/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /opt/spark/jars \
    && mv delta-storage-2.3.0.jar /opt/spark/jars

RUN pip install minio==7.1.16 \
    dagster==1.1.20
