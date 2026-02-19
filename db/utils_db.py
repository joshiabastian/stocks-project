import sqlite3
import logging
from datetime import datetime
import pandas as pd

log = logging.getLogger(__name__)

# func untuk mengambil data tanggal terakhir di database
def get_last_date(db_file_path: str) -> datetime | None:
    """
    Mengembalikan tanggal terakhir yang tersimpan di tabel stock_prices.
    Mengembalikan None jika tabel masih kosong.
    """
    try:
        with sqlite3.connect(db_file_path) as conn:
            result = pd.read_sql(
                "SELECT MAX(trade_date) AS last_date FROM stock_prices;", conn
            )
        raw = result["last_date"].iloc[0]
        if raw is None:
            log.info("Tabel stock_prices masih kosong, tidak ada last_date.")
            return None
        last_date = pd.to_datetime(raw)
        log.info(f"Tanggal terakhir di database: {last_date.date()}")
        return last_date
    except Exception as e:
        log.error(f"Gagal membaca last_date dari database: {e}")
        raise