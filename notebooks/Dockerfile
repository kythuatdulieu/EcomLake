FROM jupyter/all-spark-notebook:python-3.9.13

USER root 

RUN curl -O https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz \
    && tar zxvf spark-3.4.0-bin-hadoop3.tgz \
    && rm -rf spark-3.4.0-bin-hadoop3.tgz \
    && mv spark-3.4.0-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && ln -s /usr/local/spark-3.4.0-bin-hadoop3 /usr/local/spark

# Download jars
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar\
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar \
    && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar \
    && mv s3-2.18.41.jar /usr/local/spark/jars \
    && mv aws-java-sdk-bundle-1.12.262.jar /usr/local/spark/jars \
    && mv hadoop-aws-3.3.4.jar /usr/local/spark/jars \
    && mv hadoop-common-3.3.4.jar /usr/local/spark/jars \
    && mv delta-core_2.12-2.3.0.jar /usr/local/spark/jars \
    && mv delta-storage-2.3.0.jar /usr/local/spark/jars

RUN pip install minio==7.1.16 \
    dagster==1.1.20
