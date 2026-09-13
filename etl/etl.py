import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from collections import defaultdict

import psycopg2
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.policies import AddressTranslator

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "crypto_analytics")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "etl_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "etl_password")
SYMBOL = "BTCUSDT"
KEYSPACE = "crypto_streaming"
CANDLE_INTERVAL_MINUTES = 5

class LocalAddressTranslator(AddressTranslator):
    def translate(self, addr):
        return "127.0.0.1"

def get_cassandra_session():
    translator = LocalAddressTranslator() if CASSANDRA_HOST == "localhost" else None
    cluster = Cluster([CASSANDRA_HOST], connection_class=AsyncioConnection, address_translator=translator)
    return cluster.connect(KEYSPACE)

def get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD
    )

def floor_to_interval(dt: datetime, minutes: int) -> datetime:
    """Arrondit un datetime au début de son intervalle de N minutes."""
    discard = timedelta(
        minutes=dt.minute % minutes,
        seconds=dt.second,
        microseconds=dt.microsecond
    )
    return dt - discard

def extract(session, symbol=SYMBOL, lookback_minutes=60):
    """Récupère les transactions récentes de Cassandra pour un symbole donné."""
    since = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    query = """
        SELECT trade_time, price, quantity
        FROM transactions_by_symbol
        WHERE symbol = %s AND trade_time >= %s
    """
    rows = session.execute(query, (symbol, since))
    return list(rows)

def transform(rows, symbol=SYMBOL):
    """Regroupe les transactions par intervalle de 5 min et calcule les OHLC."""
    buckets = defaultdict(list)
    for row in rows:
        candle_start = floor_to_interval(row.trade_time, CANDLE_INTERVAL_MINUTES)
        buckets[candle_start].append(row)

    candles = []
    for candle_start, trades in buckets.items():
        trades_sorted = sorted(trades, key=lambda t: t.trade_time)
        prices = [t.price for t in trades_sorted]
        candles.append({
            "symbol": symbol,
            "candle_start": candle_start,
            "open_price": prices[0],
            "high_price": max(prices),
            "low_price": min(prices),
            "close_price": prices[-1],
            "volume": sum(t.quantity for t in trades_sorted),
            "trade_count": len(trades_sorted),
        })
    return candles

def load(pg_conn, candles):
    """Insère (ou met à jour) les bougies dans PostgreSQL."""
    upsert_query = """
        INSERT INTO ohlc_candles
        (symbol, candle_start, open_price, high_price, low_price, close_price, volume, trade_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, candle_start)
        DO UPDATE SET
            high_price = GREATEST(ohlc_candles.high_price, EXCLUDED.high_price),
            low_price = LEAST(ohlc_candles.low_price, EXCLUDED.low_price),
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            trade_count = EXCLUDED.trade_count
    """
    with pg_conn.cursor() as cur:
        for c in candles:
            cur.execute(upsert_query, (
                c["symbol"], c["candle_start"], c["open_price"], c["high_price"],
                c["low_price"], c["close_price"], c["volume"], c["trade_count"]
            ))
    pg_conn.commit()

def run():
    print("ETL démarré — extraction depuis Cassandra...")
    session = get_cassandra_session()
    pg_conn = get_postgres_connection()

    rows = extract(session)
    print(f"{len(rows)} transactions extraites.")

    candles = transform(rows)
    print(f"{len(candles)} bougies OHLC calculées.")

    load(pg_conn, candles)
    print("Bougies chargées dans PostgreSQL avec succès.")

    pg_conn.close()

if __name__ == "__main__":
    run()