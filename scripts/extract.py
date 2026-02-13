import yfinance as yf
from config.logger import logger as log


def fetch_stock_data(tickers, start_date, end_date):
    """Mengambil data saham"""
    log.info(f"Mulai mengambil data saham {tickers} dari {start_date} sampai {end_date}")
    
    stock_data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker', auto_adjust=True)
    
    if stock_data.empty:
        log.warning("Tidak ada data yang diambil. Periksa simbol saham atau rentang tanggal.")
        return None
    
    log.info("Pengambilan data selesai.")
    return stock_data