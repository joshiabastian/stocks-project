import yfinance as yf
import psycopg2
import datetime
import os
from dotenv import load_dotenv


def fetch_and_update():

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
        print("[INFO] Koneksi PostgreSQL berhasil.")
    except Exception as e:
        print("[ERROR] Gagal koneksi: ", e)


    # Big Cap 5 Stocks period daily
    tickers = ["BBCA.JK", "BBRI.JK", "BMRI.JK", "TLKM.JK", "ASII.JK"]
    today = datetime.date.today()

    for ticker in tickers:
        try:
            print(f"Updating {ticker} for {today}...")
            df = yf.download(ticker, start=today, end=today + datetime.timedelta(days=1))

            if not df.empty:
                row = df.iloc[0]
                cur.execute("""
                    INSERT INTO stock_prices (ticker, date, open, high, low, close, adj_close, volume)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (ticker, date) DO NOTHING
                """, (
                    ticker,
                    today,
                    row['Open'],
                    row['High'],
                    row['Low'],
                    row['Close'],
                    row['Adj Close'],
                    row['Volume']
                ))
        except Exception as e:
            print(f"[ERROR] Gagal update {ticker}: {e}")

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_update()
