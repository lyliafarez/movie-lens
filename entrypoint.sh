#!/bin/bash
set -e

# Load environment
source /etc/profile.d/java.sh

# Export paths
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$KAFKA_HOME/bin

# Start SSH
echo "Starting SSH service..."
/usr/sbin/sshd

# Setup passwordless SSH
if [ ! -f ~/.ssh/id_rsa ]; then
  echo "Setting up passwordless SSH..."
  mkdir -p ~/.ssh
  ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
fi

# Configure SSH known hosts
echo -e "Host localhost\n   StrictHostKeyChecking no\nHost bigdata\n   StrictHostKeyChecking no" >> ~/.ssh/config
chmod 600 ~/.ssh/config

# Format HDFS if needed
if [ ! -d /opt/hadoop_data/hdfs/namenode/current ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

# Start Hadoop
echo "Starting HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh
echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh
echo "Starting HistoryServer..."
$HADOOP_HOME/bin/mapred --daemon start historyserver

# Configure Kafka
echo "Configuring Kafka..."
echo "listeners=PLAINTEXT://0.0.0.0:9092" >> $KAFKA_HOME/config/server.properties
echo "advertised.listeners=PLAINTEXT://bigdata:9092" >> $KAFKA_HOME/config/server.properties
echo "listener.security.protocol.map=PLAINTEXT:PLAINTEXT" >> $KAFKA_HOME/config/server.properties

# Start ZooKeeper
echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties
sleep 5

# Start Kafka
echo "Starting Kafka..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
sleep 15  # Give Kafka time to start

# Create test topic
echo "Creating Kafka topic..."
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --if-not-exists \
    --topic test-topic \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1

# Start producer and consumer
echo "Starting Kafka producer and consumer..."
python3 /opt/scripts/producer.py &
python3 /opt/scripts/consumer.py &

# Start Jupyter Notebook
echo "Starting Jupyter Notebook..."

# Start Jupyter Notebook
echo "Starting Jupyter Notebook..."
mkdir -p /notebooks
jupyter notebook \
  --ip=0.0.0.0 \
  --port=8888 \
  --allow-root \
  --NotebookApp.token='' \
  --NotebookApp.password='' \
  --notebook-dir=/notebooks \
  --no-browser &

# Set proper permissions for Spark
mkdir -p /opt/spark/work-dir
chmod 777 /opt/spark/work-dir

# Keep container running
tail -f /dev/null