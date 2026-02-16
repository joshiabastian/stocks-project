import logging
import os

import pandas as pd
from sqlalchemy import create_engine

log = logging.getLogger(__name__)

OUTPUT_DIR = "output"


def load_data_to_db(df: pd.DataFrame, table_name: str, db_path: str) -> None:
    """
    Memuat DataFrame ke tabel SQLite dan menyimpan salinan CSV ke folder output.

    Menggunakan if_exists='append' sehingga data lama tidak tertimpa.
    Duplikat otomatis diabaikan oleh PRIMARY KEY constraint di SQLite
    (INSERT OR IGNORE via method=_upsert_ignore).

    Args:
        df:         DataFrame hasil transform.
        table_name: Nama tabel target di database.
        db_path:    Path file SQLite, misal 'db/stock_data.db'.
    """
    log.info(f"Memuat {len(df)} baris ke tabel '{table_name}' di '{db_path}'...")

    try:
        engine = create_engine(f"sqlite:///{db_path}")

        # Pakai INSERT OR IGNORE agar PRIMARY KEY (trade_date, ticker) tidak error duplikat
        # SQLite punya batas 999 SQL variables per query.
        # chunksize = floor(999 / 8 kolom) = 124, pakai 100 biar aman.
        with engine.begin() as conn:
            df.to_sql(
                table_name,
                con=conn,
                if_exists="append",
                index=False,
                method=_insert_or_ignore,
                chunksize=100,
            )

        log.info(f"Data berhasil dimuat ke SQLite.")

        # Simpan ke CSV
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(OUTPUT_DIR, f"{table_name}.csv")
        df.to_csv(csv_path, index=False)
        log.info(f"CSV disimpan di '{csv_path}'.")

    except Exception as e:
        log.error(f"Gagal memuat data ke database: {e}")
        raise


# ---------------------------------------------------------------------------
# Helper: INSERT OR IGNORE untuk SQLite (menghindari error duplikat PK)
# ---------------------------------------------------------------------------
def _insert_or_ignore(table, conn, keys, data_iter):
    """Custom insert method untuk pandas to_sql yang pakai INSERT OR IGNORE."""
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    insert_stmt = sqlite_insert(table.table).values(list(data_iter))
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
    conn.execute(do_nothing_stmt)