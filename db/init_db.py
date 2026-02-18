import sqlite3
import logging

log = logging.getLogger(__name__)

# func untuk inisialisasi database
def init_db(db_path: str, schema_file: str) -> None:
    """Inisialisasi database SQLite dengan schema dari file .sql."""
    try:
        with sqlite3.connect(db_path) as conn:
            with open(schema_file, "r") as f:
                sql_script = f.read()
            conn.executescript(sql_script)
            conn.commit()
        log.info(f"Database '{db_path}' berhasil diinisialisasi dengan schema '{schema_file}'.")
    except FileNotFoundError:
        log.error(f"File schema tidak ditemukan: {schema_file}")
        raise
    except sqlite3.Error as e:
        log.error(f"Gagal inisialisasi database: {e}")
        raise