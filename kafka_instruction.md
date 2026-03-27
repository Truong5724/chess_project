# Kafka Command Instructions

### 1. Generate a Cluster UUID
```
$ KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
```

### 2. Format Log Directories
```
$ bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

### 3. Start the Kafka Server
```
$ bin/kafka-server-start.sh config/server.properties
```

### 4. Create a topic
```
$ bin/kafka-topics.sh --create --topic raw-chess-data --bootstrap-server localhost:9092
```

### 5. Describe a topic
```
$ bin/kafka-topics.sh --describe --topic raw-chess-data --bootstrap-server localhost:9092
```

### 6. Read a topic
```
$ bin/kafka-console-consumer.sh --topic raw-chess-data --from-beginning --bootstrap-server localhost:9092
```

### 7. Read group offset
```
$ bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --describe \
    --group chess-data-consumer
```

### 8. Reset group offset 
```
$ bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group chess-data-consumer \
    --topic raw-chess-data \
    --reset-offsets \
    --to-earliest \
    --execute
```

