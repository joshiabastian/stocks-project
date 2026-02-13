import logging
from scripts.extract import fetch_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_data_to_db

if __name__ == "__main__":
    
    tickers = ['BBCA.JK', 'BBRI.JK', 'BMRI.JK', 'BBNI.JK']
    start_date = '2021-10-14'
    end_date = '2026-01-31'
    db_file_path = 'stock_data.db'
    
    stock_df = fetch_stock_data(tickers, start_date, end_date)
    
    if stock_df is not None:
        load_data_to_db(stock_df, 'stock_prices', db_file_path)
        
        logging.info("Proses pipeline data selesai. Data siap untuk analisis.")