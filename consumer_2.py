from kafka import KafkaConsumer
import chess.pgn # chess.svg
# from IPython.display import display, SVG
from dotenv import load_dotenv
import io
import json
import time
import os

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = "raw-chess-data"
GROUP_ID = "chess-visualizer"

def visualizeGame():
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
        for message in consumer:
            game = message.value
            pgn_text = game.get("pgn")

            if not pgn_text:
                continue

            # Parse PGN
            pgn = io.StringIO(pgn_text)
            game = chess.pgn.read_game(pgn)

            print("White:", game.headers.get("White"))
            print("Black:", game.headers.get("Black"))
            print("Result:", game.headers.get("Result"))

            # Replay moves
            board = game.board()

            # Counter
            turn = "White"

            for i, move in enumerate(game.mainline_moves()):
                board.push(move)

                print("\n" + "-" * 30)
                print(f"Move {int(i / 2) + 1} - {turn}" + "\n")

                turn = "White" if turn == "Black" else "Black"

                # Lichess GUI Display - uncomment if use in Jupyter Notebook    
                # display(SVG(chess.svg.board(board=board, size=300)))

                print(board)   
                time.sleep(1)

            break
    
    finally:
        consumer.close()