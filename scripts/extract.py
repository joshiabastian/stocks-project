import logging
import yfinance as yf
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

# Ambil data per periode
def fetch_stock_data(tickers, start_date, end_date):
    """Mengambil data saham"""
    log.info(f"Mulai mengambil data saham {tickers} dari {start_date} sampai {end_date}")
    
    stock_data = yf.download(
        tickers, start=start_date, end=end_date,
        group_by='ticker', auto_adjust=True
    )
    
    if stock_data.empty:
        log.warning("Tidak ada data yang diambil. Periksa simbol saham atau rentang tanggal.")
        return None
    
    log.info("Pengambilan data per periode selesai.")
    return stock_data

# Ambil data harian
def fetch_daily_stock_data(tickers, last_date):
    """Mengambil data saham harian sejak last_date sampai hari ini"""
    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")

    log.info(f"Mulai mengambil data saham {tickers} dari {start_date} sampai {end_date}")

    stock_data = yf.download(
        tickers, start=start_date, end=end_date,
        group_by='ticker', auto_adjust=True
    )

    if stock_data.empty:
        log.warning("Tidak ada data baru yang diambil. Periksa simbol saham")
        return None

    log.info("Pengambilan data harian selesai.")
    return stock_data
