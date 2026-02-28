import logging
import pandas as pd

log = logging.getLogger(__name__)

# Kolom yang wajib ada 
REQUIRED_COLUMNS = ["trade_date", "ticker", "open_price", "high_price",
                    "low_price", "adj_close_price", "volume", "daily_change_pct"]

# func untuk transform data yang sudah di extract
def transform_stock_data(stock_data: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """
    Transformasi DataFrame hasil yfinance menjadi format siap simpan ke DB.

    - Mendukung MultiIndex (multi-ticker) maupun single ticker.
    - Menghitung daily_change_pct berdasarkan open vs close.
    - Memastikan output hanya berisi kolom yang ada di schema.

    Args:
        stock_data: DataFrame mentah dari yfinance.
        tickers:    Daftar simbol saham yang diproses.

    Returns:
        DataFrame gabungan semua ticker, kolom sesuai schema.
    """
    all_data = []

    for ticker in tickers:
        try:
            # Ambil data per ticker
            if isinstance(stock_data.columns, pd.MultiIndex):
                if ticker not in stock_data.columns.get_level_values(0):
                    log.warning(f"Ticker '{ticker}' tidak ditemukan di data, dilewati.")
                    continue
                df_ticker = stock_data[ticker].copy()
            else:
                df_ticker = stock_data.copy()

            # Rename kolom yfinance → nama kolom DB
            df_ticker.rename(
                columns={
                    "Open":      "open_price",
                    "High":      "high_price",
                    "Low":       "low_price",
                    "Close":     "adj_close_price",
                    "Volume":    "volume",
                },
                inplace=True,
            )

            # Reset index agar Date jadi kolom biasa
            df_ticker = df_ticker.reset_index().rename(columns={"Date": "trade_date"})
            df_ticker["trade_date"] = pd.to_datetime(df_ticker["trade_date"]).dt.strftime("%Y-%m-%d")
            df_ticker["ticker"] = ticker

            # Hitung perubahan harian (%)
            df_ticker["daily_change_pct"] = (
                (df_ticker["adj_close_price"] - df_ticker["open_price"])
                / df_ticker["open_price"]
                * 100
            ).round(4)

            # Buang baris yang semua harga-nya NaN (hari libur yfinance kadang inject baris kosong)
            df_ticker.dropna(subset=["open_price", "adj_close_price"], inplace=True)

            # Filter hanya kolom yang ada di schema, abaikan sisanya (mis. Adj Close)
            cols_to_keep = [c for c in REQUIRED_COLUMNS if c in df_ticker.columns]
            df_ticker = df_ticker[cols_to_keep]

            all_data.append(df_ticker)
            log.info(f"Transformasi '{ticker}' selesai — {len(df_ticker)} baris.")

        except Exception as e:
            log.error(f"Gagal transformasi ticker '{ticker}': {e}")
            continue

    if not all_data:
        raise ValueError("Tidak ada data yang berhasil ditransformasi.")

    return pd.concat(all_data, ignore_index=True)