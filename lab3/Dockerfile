FROM apache/flink:2.1-java21

# Version variables - easy to update!
ARG FLINK_VERSION=2.1.2
ARG ICEBERG_VERSION=1.11.0
ARG ICEBERG_FLINK_RUNTIME_VERSION=2.1
ARG HADOOP_VERSION=3.4.2

# Set working directory
WORKDIR /opt/flink

# Download Iceberg Flink runtime (with native VARIANT support!)
RUN echo "-> Iceberg Flink runtime ${ICEBERG_FLINK_RUNTIME_VERSION}" && \
    mkdir -p ./lib/iceberg && \
    curl -fsSL -o ./lib/iceberg/iceberg-flink-runtime-${ICEBERG_FLINK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${ICEBERG_FLINK_RUNTIME_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${ICEBERG_FLINK_RUNTIME_VERSION}-${ICEBERG_VERSION}.jar" && \
    curl -fsSL -o ./lib/iceberg/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
      "https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# Download Hadoop client (for S3 access)
RUN echo "-> Hadoop client ${HADOOP_VERSION}" && \
    mkdir -p ./lib/hadoop && \
    curl -fsSL -o ./lib/hadoop/hadoop-client-api-${HADOOP_VERSION}.jar \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar" && \
    curl -fsSL -o ./lib/hadoop/hadoop-client-runtime-${HADOOP_VERSION}.jar \
      "https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar"

# Download Parquet format connector and dependencies
RUN echo "-> Parquet format ${FLINK_VERSION}" && \
    curl -fsSL -o ./lib/flink-parquet.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-parquet/${FLINK_VERSION}/flink-parquet-${FLINK_VERSION}.jar" && \
    curl -fsSL -o ./lib/parquet-hadoop-1.13.1.jar \
      "https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar"

# Download S3 FileSystem Plugin (for s3a:// support)
RUN echo "-> S3 FileSystem Plugin ${FLINK_VERSION}" && \
    mkdir -p ./plugins/s3-fs-hadoop && \
    curl -fsSL -o ./plugins/s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
      "https://repo.maven.apache.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar" && \
    echo "✅ S3 plugin installed at ./plugins/s3-fs-hadoop/"

# Flatten lib directory
RUN mv ./lib/iceberg/*.jar ./lib/ && rmdir ./lib/iceberg && \
    mv ./lib/hadoop/*.jar ./lib/ && rmdir ./lib/hadoop && \
    echo "✅ Flink ${FLINK_VERSION} with Iceberg ${ICEBERG_VERSION} (native VARIANT support!)" && \
    ls -lh ./lib/iceberg-* ./lib/flink-parquet* 2>/dev/null || true && \
    ls -lh ./plugins/s3-fs-hadoop/
