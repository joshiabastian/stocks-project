import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

log = logging.getLogger(__name__)


def fetch_stock_data(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame | None:
    """
    Mengambil data saham untuk periode tertentu.

    Args:
        tickers:    Daftar simbol saham, misal ['BBCA.JK', 'BBRI.JK'].
        start_date: Tanggal mulai format 'YYYY-MM-DD'.
        end_date:   Tanggal akhir format 'YYYY-MM-DD'.

    Returns:
        DataFrame hasil yfinance, atau None jika tidak ada data.
    """
    log.info(f"Mengambil data periode {start_date} s/d {end_date} untuk: {tickers}")

    stock_data = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    if stock_data.empty:
        log.warning("Tidak ada data yang diambil. Periksa simbol saham atau rentang tanggal.")
        return None

    log.info(f"Pengambilan data periode selesai. Shape: {stock_data.shape}")
    return stock_data


def fetch_daily_stock_data(tickers: list[str], last_date: datetime) -> pd.DataFrame | None:
    """
    Mengambil data saham harian sejak last_date + 1 hari hingga hari ini.
    Dipakai untuk mode incremental / update harian.

    Args:
        tickers:   Daftar simbol saham.
        last_date: Tanggal terakhir yang sudah ada di database.

    Returns:
        DataFrame hasil yfinance, atau None jika tidak ada data baru.
    """
    start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = datetime.today().strftime("%Y-%m-%d")

    if start_date >= end_date:
        log.info("Data sudah up-to-date, tidak ada rentang tanggal baru untuk diambil.")
        return None

    log.info(f"Mengambil data harian {start_date} s/d {end_date} untuk: {tickers}")

    stock_data = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )

    if stock_data.empty:
        log.warning(
            "Tidak ada data baru yang diambil. "
            "Kemungkinan: weekend, libur nasional, atau error koneksi. Segera dicek!"
        )
        return None

    log.info(f"Pengambilan data harian selesai. Shape: {stock_data.shape}")
    return stock_data