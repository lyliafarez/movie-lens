# Big Data Pseudo-distributed Environment with Hadoop, Spark, Kafka, Python, and Jupyter

## 🌍 Project Overview
This project provides a ready-to-use Dockerized environment to work with:
- **Hadoop 3.3.6** (pseudo-distributed)
- **Spark 3.5.1** (standalone mode)
- **Kafka 3.6.1** (with Zookeeper)
- **Python 3** + **PySpark**
- **Jupyter Notebook**

## 📊 Architecture
- Hadoop HDFS for distributed file storage (single-node setup)
- Spark for batch and streaming data processing
- Kafka for streaming ingestion
- Python environment with Jupyter for development and experimentation

## 🔧 Project Structure
```
/
|-- Dockerfile
|-- docker-compose.yml
|-- Makefile
|-- requirements.txt
|-- config/
|   |-- hadoop/
|       |-- core-site.xml
|       |-- hdfs-site.xml
|       |-- mapred-site.xml
|       |-- yarn-site.xml
|-- notebooks/
|   |-- spark_kafka_demo.ipynb
|-- scripts/
    |-- spark_batch_csv_count.py
```

## 🔄 Quick Start

### 1. Build the Docker Image
```bash
make build
```

### 2. Launch the Environment
```bash
make up
```

This will start:
- Hadoop HDFS & YARN
- Kafka + Zookeeper
- Jupyter Notebook (accessible on http://localhost:8888)

### 3. Access the Container
```bash
make shell
```

### 4. Shut Down
```bash
make down
```

### 5. Clean Everything (containers, images, volumes)
```bash
make clean
```

## 📄 Notebooks & Scripts
- **spark_kafka_demo.ipynb** : Connects Spark Structured Streaming to a Kafka topic and displays the streamed data.
- **spark_batch_csv_count.py** : A simple Spark batch job reading a CSV file from HDFS and counting rows.

## 🔔 Notes
- Hadoop HDFS Web UI: [http://localhost:9870](http://localhost:9870)
- Ensure you manually create Kafka topics using:
  ```bash
  kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
  ```
- Upload datasets to HDFS:
  ```bash
  hdfs dfs -mkdir -p /datasets
  hdfs dfs -put your_file.csv /datasets/
  ```

---

Made with ❤️ by Marie
