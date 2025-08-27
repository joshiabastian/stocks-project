import yfinance as yf
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load .env
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Koneksi ke database
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()
    print("[INFO] Koneksi PosgreSQL berhasil.")
except Exception as e:
    print("[ERROR] Gagal koneksi: ", e)


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
