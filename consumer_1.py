from kafka import KafkaConsumer, KafkaProducer
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import json
import re
import os

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
ACCOUNT = os.getenv("ACCOUNT")
WAREHOUSE = os.getenv("WAREHOUSE")
DATABASE = os.getenv("DATABASE")
SCHEMA = os.getenv("SCHEMA")

INPUT_TOPIC = "raw-chess-data"
OUTPUT_TOPIC = "processed-chess-data"
GROUP_ID = "chess-data-consumer"
BATCH_SIZE = 100

def uploadDataWarehouse():
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='earliest', # Fetch from start
        enable_auto_commit=True,       
        group_id=GROUP_ID,
        consumer_timeout_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=50
    )

    # Snowflake connection
    conn = connect(
        user=USERNAME,
        password=PASSWORD,
        account=ACCOUNT,
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA
    )

    print("Connected successfully to Snowflake")

    try:
        games_list = []

        for message in consumer:
            game = message.value  
            
            # Sub-dict
            white = game.get("white")
            black = game.get("black")

            accuracies = game.get("accuracies")
            eco = game.get("eco")
            # Check if accuracies and opening are valid 
            if not accuracies or not eco:
                continue

            game_transformed = {
                "WHITE_USERNAME": white.get("username"),
                "WHITE_ELO": white.get("rating"),
                "WHITE_RESULT": white.get("result"),
                "WHITE_ACCURACY": accuracies.get("white"),

                "BLACK_USERNAME": black.get("username"),
                "BLACK_ELO": black.get("rating"),
                "BLACK_RESULT": black.get("result"),
                "BLACK_ACCURACY": accuracies.get("black"),

                "TIME_CONTROL": game.get("time_control"),
                "TIME_CLASS": game.get("time_class"),
                "DATE_TIME": datetime.fromtimestamp(game.get("end_time")).isoformat(),
                "RATED": game.get("rated"),
                "OPENING": re.sub(r'(?<!O)-|-(?!O)', ' ', eco.split("/openings/")[-1])
            }

            # Push to new topic
            producer.send(OUTPUT_TOPIC, game_transformed)

            games_list.append(game_transformed)

            if len(games_list) >= BATCH_SIZE:
                df = pd.DataFrame(games_list)

                # Save as CSV file
                filename = "chess_games.csv"
                df.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)

                # Upload to Snowflake Data Warehouse
                success, nchunks, nrows, _ = write_pandas(conn, df, 'GAMES')
                print(f"Uploaded {nrows} rows")

                break 
    
    finally:
        consumer.close()
        producer.close()
        conn.close()