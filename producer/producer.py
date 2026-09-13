import os
import json
import time
import requests
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "crypto_transactions"
SYMBOL = "BTCUSDT"
POLL_INTERVAL_SECONDS = 5

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_trades(symbol=SYMBOL, limit=5):
    # Note : /api/v3/trades ne supporte PAS fromId (contrairement à
    # /api/v3/historicalTrades ou /api/v3/aggTrades). On récupère donc
    # toujours les N dernières transactions, et on filtre nous-mêmes
    # les doublons ci-dessous.
    url = "https://api.binance.com/api/v3/trades"
    response = requests.get(url, params={"symbol": symbol, "limit": limit})
    response.raise_for_status()
    return response.json()

def run():
    print(f"Producer démarré — polling {SYMBOL} toutes les {POLL_INTERVAL_SECONDS}s (broker: {KAFKA_BROKER})")
    last_id = -1  # aucune transaction publiée pour l'instant

    while True:
        trades = fetch_trades()

        # Les trade_id Binance sont strictement croissants pour un symbole
        # donné : on ne garde que ceux jamais publiés.
        new_trades = [t for t in trades if t["id"] > last_id]

        if not new_trades:
            print("Aucune nouvelle transaction.")
        else:
            for trade in new_trades:
                message = {
                    "trade_id": trade["id"],
                    "symbol": SYMBOL,
                    "price": trade["price"],
                    "quantity": trade["qty"],
                    "quote_qty": trade["quoteQty"],
                    "trade_time": trade["time"],
                    "is_buyer_maker": trade["isBuyerMaker"],
                }
                producer.send(TOPIC, value=message)
                print(f"Publié: {message['trade_id']} - {message['price']}")

            producer.flush()
            last_id = max(t["id"] for t in new_trades)

        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    run()