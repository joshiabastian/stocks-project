import yfinance as yf
import psycopg2
import datetime

def fetch_and_update():
    conn = psycopg2.connect(
        dbname="stock_project",
        user="postgres",
        password="your_password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Big Cap 5 Stocks period daily
    tickers = ["BBCA.JK", "BBRI.JK", "BMRI.JK", "TLKM.JK", "ASII.JK"]
    today = datetime.date.today()

    for ticker in tickers:
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
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    fetch_and_update()
