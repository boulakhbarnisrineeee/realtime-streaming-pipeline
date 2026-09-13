import os
import json
from decimal import Decimal
from datetime import datetime, timezone
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import AddressTranslator

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
TOPIC = "crypto_transactions"
GROUP_ID = "crypto_consumer_group"
KEYSPACE = "crypto_streaming"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

class LocalAddressTranslator(AddressTranslator):
    def translate(self, addr):
        return "127.0.0.1"

print("Tentative de connexion à Cassandra...")
translator = LocalAddressTranslator() if CASSANDRA_HOST == "localhost" else None
cluster = Cluster([CASSANDRA_HOST], connection_class=AsyncioConnection, address_translator=translator)
session = cluster.connect(KEYSPACE)
print("Connecté à Cassandra avec succès.")

insert_by_symbol = session.prepare("""
    INSERT INTO transactions_by_symbol
    (symbol, trade_time, trade_id, price, quantity, quote_qty, is_buyer_maker)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

insert_by_id = session.prepare("""
    INSERT INTO transactions_by_id
    (trade_id, symbol, trade_time, price, quantity, quote_qty, is_buyer_maker)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

def transform(raw_transaction: dict) -> dict:
    return {
        "trade_id": raw_transaction["trade_id"],
        "symbol": raw_transaction["symbol"],
        "price": Decimal(raw_transaction["price"]),
        "quantity": Decimal(raw_transaction["quantity"]),
        "quote_qty": Decimal(raw_transaction["quote_qty"]),
        "trade_time": datetime.fromtimestamp(
            raw_transaction["trade_time"] / 1000, tz=timezone.utc
        ),
        "is_buyer_maker": raw_transaction["is_buyer_maker"],
    }

def save_to_cassandra(t: dict):
    session.execute(insert_by_symbol, (
        t["symbol"], t["trade_time"], t["trade_id"],
        t["price"], t["quantity"], t["quote_qty"], t["is_buyer_maker"]
    ))
    session.execute(insert_by_id, (
        t["trade_id"], t["symbol"], t["trade_time"],
        t["price"], t["quantity"], t["quote_qty"], t["is_buyer_maker"]
    ))

def run():
    print(f"Consumer démarré — écoute du topic '{TOPIC}' (broker: {KAFKA_BROKER})")
    for message in consumer:
        raw_transaction = message.value
        transaction = transform(raw_transaction)
        save_to_cassandra(transaction)
        print(f"Sauvegardé: {transaction['trade_id']} - {transaction['price']} @ {transaction['trade_time']}")

if __name__ == "__main__":
    run()