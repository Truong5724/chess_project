import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from kafka import KafkaProducer
import json

"""
Fetch archives of games since 2025 for the top 5 classical-rated players as of March 23, 2026 from Chess.com API.
Chess.com usernames for the top 5 players:
1. Magnus Carsen - margnuscarlsen
2. Hikaru Nakamura - hikaru
3. Fabiano Caruana - fabianocaruana
4. Addusattorov Nordirbek - chesswarrior7197
5. Vincent Keymer - vincentkeymer
"""

USERNAMES = ["magnuscarlsen", "hikaru", "fabianocaruana", "chesswarrior7197", "vincentkeymer"]
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_ATTEMPTS = 5
KAFKA_TOPIC = "raw-chess-data"
KAFKA_BOOTSTRAP = "localhost:9092"
MAX_WORKERS = 3

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=50,
)

# Fetch archives of games for each player
for username in USERNAMES:
    url = f"https://api.chess.com/pub/player/{username}/games/archives"

    for attempt in range(MAX_ATTEMPTS):
        try:
            res = requests.get(url, headers=HEADERS)

            if res.status_code == 200:
                data = res.json().get('archives', [])
                archives = [url for url in data if int(url.split('/')[-2]) >= 2025]

                def fetch_archive(archive):
                    for archive_attempt in range(MAX_ATTEMPTS):
                        try:
                            archive_res = requests.get(archive, headers=HEADERS)

                            if archive_res.status_code == 200:
                                archive_games = archive_res.json().get('games', [])

                                for game in archive_games:
                                    producer.send(KAFKA_TOPIC, game, key=username.encode())
                                break

                            else:
                                print(f"Error: Archive status code returned {archive_res.status_code}")

                        except Exception as e:
                            print(f"Error: {e}")

                        sleep_time = random.uniform(2, 5)
                        print(f"Archive attempt {archive_attempt + 1} failed. Retry in {sleep_time}s...")
                        time.sleep(sleep_time)

                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    list(tqdm(executor.map(fetch_archive, archives), total=len(archives), desc=f"{username} archives"))

                producer.flush()
                break

            else:
                print(f"Error: Status code returned {res.status_code}")
            
        except Exception as e:
            print(f"Error: {e}")
            
        sleep_time = random.uniform(5, 10)
        print(f"Attempt {attempt + 1} failed. Retry in {sleep_time}s...")
        time.sleep(sleep_time)

producer.close()