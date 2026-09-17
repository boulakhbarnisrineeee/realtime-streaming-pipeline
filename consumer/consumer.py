import os
import json
import logging
from decimal import Decimal
from datetime import datetime, timezone
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import AddressTranslator
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from common.logger import get_logger

logger = get_logger("consumer")

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
TOPIC = "crypto_transactions"
GROUP_ID = "crypto_consumer_group"
KEYSPACE = "crypto_streaming"

class LocalAddressTranslator(AddressTranslator):
    def translate(self, addr):
        return "127.0.0.1"

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def connect_to_cassandra():
    translator = LocalAddressTranslator() if CASSANDRA_HOST == "localhost" else None
    cluster = Cluster([CASSANDRA_HOST], connection_class=AsyncioConnection, address_translator=translator)
    return cluster.connect(KEYSPACE)

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

def save_to_cassandra(session, insert_by_symbol, insert_by_id, t: dict):
    session.execute(insert_by_symbol, (
        t["symbol"], t["trade_time"], t["trade_id"],
        t["price"], t["quantity"], t["quote_qty"], t["is_buyer_maker"]
    ))
    session.execute(insert_by_id, (
        t["trade_id"], t["symbol"], t["trade_time"],
        t["price"], t["quantity"], t["quote_qty"], t["is_buyer_maker"]
    ))

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    logger.info("Tentative de connexion à Cassandra...")
    session = connect_to_cassandra()
    logger.info("Connecté à Cassandra avec succès.")

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

    logger.info(f"Consumer démarré — écoute du topic '{TOPIC}' (broker: {KAFKA_BROKER})")
    for message in consumer:
        raw_transaction = message.value
        transaction = transform(raw_transaction)
        save_to_cassandra(session, insert_by_symbol, insert_by_id, transaction)
        logger.info(f"Sauvegardé: {transaction['trade_id']} - {transaction['price']} @ {transaction['trade_time']}")

if __name__ == "__main__":
    main()
    