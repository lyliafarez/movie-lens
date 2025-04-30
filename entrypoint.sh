#!/bin/bash
set -e

# Load Java environment
source /etc/profile.d/java.sh

# Export Hadoop and Kafka paths
export HADOOP_HOME=/opt/hadoop
export KAFKA_HOME=/opt/kafka
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$KAFKA_HOME/bin

# Debugging: Show PATH
echo "PATH = $PATH"
echo "JAVA_HOME = $JAVA_HOME"
echo "HADOOP_HOME = $HADOOP_HOME"
echo "KAFKA_HOME = $KAFKA_HOME"

# Start SSH service
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
mkdir -p ~/.ssh
echo -e "Host localhost\n   StrictHostKeyChecking no\nHost namenode\n   StrictHostKeyChecking no" >> ~/.ssh/config
chmod 600 ~/.ssh/config

# Format Hadoop filesystem if needed
if [ ! -d /opt/hadoop_data/hdfs/namenode/current ]; then
  echo "Formatting NameNode..."
  hdfs namenode -format -force -nonInteractive
fi

# Start Hadoop services
echo "Starting HDFS (NameNode and DataNode)..."
$HADOOP_HOME/sbin/start-dfs.sh

echo "Starting YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

echo "Starting MapReduce HistoryServer..."
$HADOOP_HOME/bin/mapred --daemon start historyserver

# Start ZooKeeper and Kafka
echo "Starting ZooKeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

sleep 5

echo "Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties


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

# Keep container alive
tail -f /dev/null
