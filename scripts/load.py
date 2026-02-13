import logging
from sqlalchemy import create_engine

def load_data_to_db(df, table_name, db_path):
    """Memuat data ke database SQLite."""
    logging.info(f"Memuat data ke {db_path}")
    try:
        engine = create_engine(f'sqlite:///{db_path}')
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info("Data berhasil dimuat ke SQLite.")
    except Exception as e:
        logging.error(f"Gagal memuat data ke database: {e}")