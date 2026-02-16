import sqlite3
import pandas as pd

def get_last_date(db_file_path):
    conn = sqlite3.connect(db_file_path)
    last_date = pd.read_sql("SELECT MAX(trade_date) as last_date FROM stock_prices;", conn)
    conn.close()
    return pd.to_datetime(last_date['last_date'].iloc[0])