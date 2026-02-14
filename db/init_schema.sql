CREATE TABLE IF NOT EXISTS stock_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT,
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    adj_close REAL,
    volume INTEGER,
    created_at TEXT DEFAULT (datetime('now')),
    CONSTRAINT unique_ticker_date UNIQUE (ticker, date)
);