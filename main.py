import logging
import sys
from config.logger import setup_logger
from db.init_db import init_db
from db.utils_db import get_last_date
from scripts.extract import fetch_stock_data, fetch_daily_stock_data
from scripts.transform import transform_stock_data
from scripts.load import load_data_to_db

# Konfirgurasi Path
DB_FILE_PATH = "db/stock_data.db"
SCHEMA_FILE = "db/init_schema.sql"
TABLE_NAME = "stock_prices"

# Data Saham yang inin di ambil
TICKERS = ["BBCA.JK", "BBRI.JK", "BMRI.JK", "BBNI.JK", "BNGA.JK"]

# Tanggal awal & akhir untuk ambil data per periode
START_DATE = "2003-01-01"
END_DATE = "2026-02-13"


# Ambil Data Saham Per periode
def run_full_pipeline():
    """Ingest penuh dari START_DATE sampai END_DATE. Untuk inisialisasi pertama kali."""
    logger = logging.getLogger(__name__)
    logger.info("=== Mode: FULL PIPELINE ===")

    stock_df = fetch_stock_data(TICKERS, START_DATE, END_DATE)
    if stock_df is None:
        logger.warning("Pipeline dihentikan — tidak ada data yang diambil.")
        return

    transformed_df = transform_stock_data(stock_df, TICKERS)
    load_data_to_db(transformed_df, TABLE_NAME, DB_FILE_PATH)
    logger.info("Full pipeline selesai.")


# Ambil Data Saham Harian
def run_daily_pipeline():
    """
    Incremental update: ambil data sejak hari setelah last_date di DB sampai hari ini.
    Jalankan ini setiap hari (mis. via cron/scheduler).
    """
    logger = logging.getLogger(__name__)
    logger.info("=== Mode: DAILY UPDATE ===")

    last_date = get_last_date(DB_FILE_PATH)
    if last_date is None:
        logger.warning(
            "Database masih kosong. Jalankan full pipeline terlebih dahulu: "
            "python main.py full"
        )
        return

    stock_df = fetch_daily_stock_data(TICKERS, last_date)
    if stock_df is None:
        logger.info("Tidak ada data baru. Pipeline harian selesai tanpa perubahan.")
        return

    transformed_df = transform_stock_data(stock_df, TICKERS)
    load_data_to_db(transformed_df, TABLE_NAME, DB_FILE_PATH)
    logger.info("Daily pipeline selesai.")


# Running
def main():
    setup_logger()
    logger = logging.getLogger(__name__)
    logger.info("Memulai ETL pipeline saham Indonesia...")

    # Inisialisasi schema
    init_db(DB_FILE_PATH, SCHEMA_FILE)

    # Full atau Daily (default Daily)
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if mode == "full":
        run_full_pipeline()
    elif mode == "daily":
        run_daily_pipeline()
    else:
        logger.error(f"Mode tidak dikenal: '{mode}'. Gunakan 'full' atau 'daily'.")
        sys.exit(1)

    logger.info("Pipeline selesai.")


if __name__ == "__main__":
    main()
