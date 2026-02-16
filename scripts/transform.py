import logging
import pandas as pd

log = logging.getLogger(__name__)

def transform_stock_data(stock_data, tickers):
    """Transformasi data saham menjadi format siap simpan."""
    all_data = []
    for ticker in tickers:
        if isinstance(stock_data.columns, pd.MultiIndex):
            df_ticker = stock_data[ticker].copy()
        else:
            df_ticker = stock_data.copy()

        df_ticker.rename(columns={
            'Open': 'open_price',
            'High': 'high_price',
            'Low': 'low_price',
            'Close': 'close_price',
            'Adj Close': 'adj_close',
            'Volume': 'volume'
        }, inplace=True)

        df_ticker = df_ticker.reset_index().rename(columns={'Date': 'trade_date'})
        df_ticker['ticker'] = ticker
        df_ticker['daily_change_pct'] = (
            (df_ticker['close_price'] - df_ticker['open_price']) / df_ticker['open_price']
        ) * 100

        all_data.append(df_ticker)
        log.info(f"Transformasi {ticker} selesai, {len(df_ticker)} baris.")

    return pd.concat(all_data, ignore_index=True)