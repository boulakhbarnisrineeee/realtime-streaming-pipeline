CREATE TABLE IF NOT EXISTS ohlc_candles (
    symbol          VARCHAR(20) NOT NULL,
    candle_start    TIMESTAMPTZ NOT NULL,
    open_price      NUMERIC(20, 8) NOT NULL,
    high_price      NUMERIC(20, 8) NOT NULL,
    low_price       NUMERIC(20, 8) NOT NULL,
    close_price     NUMERIC(20, 8) NOT NULL,
    volume          NUMERIC(20, 8) NOT NULL,
    trade_count     INTEGER NOT NULL,
    PRIMARY KEY (symbol, candle_start)
);