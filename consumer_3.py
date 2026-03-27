from kafka import KafkaConsumer
import requests
from dotenv import load_dotenv
import json
import os

load_dotenv()

POWER_BI_URL = os.getenv("POWER_BI_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

KAFKA_TOPIC = "processed-chess-data"
GROUP_ID = "powerbi-consumer"
BATCH_SIZE = 10

def uploadPowerBIStreamingDataset():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    try: 
        batch = []

        for message in consumer:
            data = message.value
            batch.append(data)

            if len(batch) >= BATCH_SIZE:
                try:
                    res = requests.post(POWER_BI_URL, json=batch)
                    if res.status_code == 200:
                        print(f"Success")
                    else:
                        print(f"Status code returned {res.status_code}")
                        print(res.text)

                except Exception as e:
                    print(f"Error: {e}")
                break  
    
    finally:
        consumer.close()
