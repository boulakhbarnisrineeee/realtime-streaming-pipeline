CREATE KEYSPACE IF NOT EXISTS crypto_streaming
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE crypto_streaming;

CREATE TABLE IF NOT EXISTS transactions_by_symbol (
    symbol         TEXT,
    trade_time     TIMESTAMP,
    trade_id       BIGINT,
    price          DECIMAL,
    quantity       DECIMAL,
    quote_qty      DECIMAL,
    is_buyer_maker BOOLEAN,
    PRIMARY KEY (symbol, trade_time, trade_id)
) WITH CLUSTERING ORDER BY (trade_time DESC, trade_id DESC);

CREATE TABLE IF NOT EXISTS transactions_by_id (
    trade_id       BIGINT,
    symbol         TEXT,
    trade_time     TIMESTAMP,
    price          DECIMAL,
    quantity       DECIMAL,
    quote_qty      DECIMAL,
    is_buyer_maker BOOLEAN,
    PRIMARY KEY (trade_id)
);

