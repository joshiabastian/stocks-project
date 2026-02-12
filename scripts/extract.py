import yfinance as yf
import pandas as pd
from config.logger import logger as log
from datetime import datetime


def fetch_stock_data(tickers, start_date, end_date):
    """Mengambil data saham"""
    log.info(f"Mulai mengambil data saham {tickers} dari {start_date} sampai {end_date}")