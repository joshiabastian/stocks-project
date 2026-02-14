import logging
import os
from sqlalchemy import create_engine

log = logging.getLogger(__name__)

def load_data_to_db(df, table_name, db_path):
    """Memuat data ke database SQLite dan simpan ke CSV."""
    log.info(f"Memuat data ke {db_path}")
    try:
        engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        os.makedirs("output", exist_ok=True)
        df.to_csv(f"output/{table_name}.csv", index=False)
        log.info("Data berhasil dimuat ke SQLite dan CSV.")
    except Exception as e:
        log.error(f"Gagal memuat data ke database: {e}")