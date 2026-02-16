import sqlite3
import logging

log = logging.getLogger(__name__)

def init_db(db_path, schema_file):
    """Inisialisasi database SQLite dengan schema dari file .sql"""
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            with open(schema_file, 'r') as f:
                sql_script = f.read()
            cursor.executescript(sql_script)
            conn.commit()
            log.info(f"Database {db_path} sudah diinisialisasi dengan {schema_file}")
    except Exception as e:
        log.error(f"Gagal inisialisasi database: {e}")