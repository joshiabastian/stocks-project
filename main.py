import logging
from config.logger import setup_logger
from scripts.extract import fetch_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_data_to_db

def main():
    setup_logger()  # konfigurasi logger sekali di sini
    logger = logging.getLogger(__name__)
    logger.info("Memulai pipeline data saham...")

    tickers = ['BBCA.JK', 'BBRI.JK', 'BMRI.JK', 'BBNI.JK']
    start_date = '2021-10-14'
    end_date = '2026-01-31'
    db_file_path = 'stock_data.db'

    stock_df = fetch_stock_data(tickers, start_date, end_date)

    if stock_df is not None:
        transformed_df = transform_stock_data(stock_df, tickers)
        load_data_to_db(transformed_df, 'stock_prices', db_file_path)
        logger.info("Proses pipeline data selesai. Data siap untuk analisis.")
    else:
        logger.warning("Pipeline dihentikan karena tidak ada data yang diambil.")

if __name__ == "__main__":
    main()