import yfinance as yf
import psycopg2
import pandas as pd

# Koneksi ke PostgreSQL
conn = psycopg2.connect(
    dbname="stock_project",
    user="postgres",
    password="your_password",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Big Cap 5 Stocks period 2020-2025
tickers = ["BBCA.JK", "BBRI.JK", "BMRI.JK", "TLKM.JK", "ASII.JK"]

for ticker in tickers:
    print(f"Fetching {ticker}...")
    df = yf.download(ticker, start="2020-01-01", end="2025-08-31")

    df.reset_index(inplace=True)
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO stock_prices (ticker, date, open, high, low, close, adj_close, volume)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (ticker, date) DO NOTHING
        """, (
            ticker,
            row['Date'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Adj Close'],
            row['Volume']
        ))
    conn.commit()

cur.close()
conn.close()
