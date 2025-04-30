FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install essential packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils procps openjdk-11-jdk python3 python3-pip wget curl git \
    net-tools iputils-ping nano gnupg lsb-release openssh-client openssh-server \
    supervisor sudo sshpass \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile.d/java.sh && \
    echo "export PATH=${JAVA_HOME}/bin:$PATH" >> /etc/profile.d/java.sh && \
    chmod +x /etc/profile.d/java.sh

# Install Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mkdir -p $HADOOP_HOME/logs

# Configure Hadoop logging
RUN mkdir -p $HADOOP_CONF_DIR && \
    echo -e "log4j.rootLogger=INFO, console\n\
log4j.appender.console=org.apache.log4j.ConsoleAppender\n\
log4j.appender.console.layout=org.apache.log4j.PatternLayout\n\
log4j.appender.console.layout.ConversionPattern=%d{ISO8601} %-5p %c{2}:%L - %m%n\n\
log4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR" > $HADOOP_CONF_DIR/log4j.properties

# Install Spark
# Install Spark
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Download and extract Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download all required Kafka integration JARs
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar -P $SPARK_HOME/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.2/kafka-clients-3.7.2.jar -P $SPARK_HOME/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar -P $SPARK_HOME/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar -P $SPARK_HOME/jars/ && \
    wget -q https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.12.18/scala-library-2.12.18.jar -P $SPARK_HOME/jars/
    
# Install Kafka (using 2.12 version for compatibility)
ENV KAFKA_VERSION=3.7.2
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$KAFKA_HOME/bin:$PATH

RUN wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.12-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_2.12-${KAFKA_VERSION}.tgz && \
    mv kafka_2.12-${KAFKA_VERSION} $KAFKA_HOME && \
    rm kafka_2.12-${KAFKA_VERSION}.tgz

# Install Python libraries
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir jupyter && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt

# Create HDFS data directories
RUN mkdir -p /opt/hadoop_data/hdfs/namenode /opt/hadoop_data/hdfs/datanode && \
    chmod -R 755 /opt/hadoop_data

# Copy Hadoop configs
COPY config/hadoop/ $HADOOP_CONF_DIR/

# Setup SSH for Hadoop
RUN mkdir /var/run/sshd && \
    ssh-keygen -A && \
    echo "root:root" | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/UsePAM yes/UsePAM no/' /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose necessary ports
EXPOSE 22 8888 9870 8088 8042 19888 9092 2181

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]