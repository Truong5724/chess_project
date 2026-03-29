# Kafka Streaming Pipeline with Airflow Instructions

## 1. Initialize Kafka Cluster

### Generate Cluster ID
```bash
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

### Format Log Directories
```bash
$ bin/kafka-storage.sh format --standalone \
-t $KAFKA_CLUSTER_ID \
-c config/server.properties
```

### Start Kafka Server
```bash
$ bin/kafka-server-start.sh config/server.properties
```

## 2. Create and Verify Topic

### Create Topic
```bash
$ bin/kafka-topics.sh --create \
--topic raw-chess-data \
--bootstrap-server localhost:9092
```

### Describe Topic (Optional)
```bash
$ bin/kafka-topics.sh --describe \
--topic raw-chess-data \
--bootstrap-server localhost:9092
```

## 3. Simulate Streaming Data (Producers)

Run the producer script to send data into Kafka:

```bash
python producer.py
```

You can launch **multiple producers simultaneously** (in different terminals) to simulate:

- Multiple data sources.

- High-throughput streaming.  

## 4. Monitor Kafka Stream (Optional)

```bash
$ bin/kafka-console-consumer.sh \
--topic raw-chess-data \
--from-beginning \
--bootstrap-server localhost:9092
```

## 5. Start Airflow and Access UI

### Run Airflow
```bash
$ airflow standalone
```

### Access Web UI
Open your browser and go to:
```
http://localhost:8080
```

---

## 6. Monitor and Trigger DAGs

In the Airflow UI, you can:

- **Monitor DAG execution**
  - Check task states (running, success, failed).  

  - Observe task dependencies.

- **Trigger DAG manually**  
  - Execute consumer jobs on demand.  

- **Run on schedule**  
  - DAGs automatically execute based on defined schedules.  

---

## 7. Check Consumer Group Offsets (Optional)

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--describe \
--group chess-data-consumer
```

---

## 8. Reset Group Offsets (Reprocess Data - Optional)

```bash
bin/kafka-consumer-groups.sh \
--bootstrap-server localhost:9092 \
--group chess-data-consumer \
--topic raw-chess-data \
--reset-offsets \
--to-earliest \
--execute
```

## Overall Workflow

1. Initialize Kafka cluster.

2. Create topics.  

3. Run one or multiple producers to stream data.  

4. Start Airflow and open the UI.  

5. Trigger DAGs manually or wait for scheduled runs that execute consumer tasks.

6. Monitor task execution and data processing.  