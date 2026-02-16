import logging
from db.init_db import init_db
from db.utils_db import get_last_date
from config.logger import setup_logger
from scripts.extract import fetch_stock_data, fetch_daily_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_data_to_db


def main():
    # Setup logger sekali di sini
    setup_logger()
    logger = logging.getLogger(__name__)
    logger.info("Memulai pipeline data saham...")

    # Path database dan schema
    db_file_path = 'db/stock_data.db'
    schema_file = 'db/init_schema.sql'

    # Inisialisasi schema
    init_db(db_file_path, schema_file)

    # Parameter ETL
    tickers = ['BBCA.JK', 'BBRI.JK', 'BMRI.JK', 'BBNI.JK']
    start_date = '2021-10-14'
    end_date = '2026-01-31'

    # Extract
    stock_df = fetch_stock_data(tickers, start_date, end_date)

    if stock_df is not None:
        # Transform
        transformed_df = transform_stock_data(stock_df, tickers)

        # Load
        load_data_to_db(transformed_df, 'stock_prices', db_file_path)
        logger.info("Proses pipeline data selesai. Data siap untuk analisis.")
    else:
        logger.warning("Pipeline dihentikan karena tidak ada data yang diambil.")
        

if __name__ == "__main__":
    main()