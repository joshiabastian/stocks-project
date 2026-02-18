import sqlite3
import pandas as pd

conn = sqlite3.connect("db/stock_data.db")
df = pd.read_sql("SELECT * FROM stock_prices LIMIT 10;", conn)
conn.close()

print(df)