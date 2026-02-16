CREATE TABLE IF NOT EXISTS stock_prices (
    trade_date        TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    open_price        REAL,
    high_price        REAL,
    low_price         REAL,
    close_price       REAL,
    volume            INTEGER,
    daily_change_pct  REAL,
    PRIMARY KEY (trade_date, ticker)
);