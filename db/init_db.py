import sqlite3

def init_db(db_path, schema_file):
    """Inisialisasi database SQLite dengan schema dari file .sql"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    with open(schema_file, 'r') as f:
        sql_script = f.read()
    cursor.executescript(sql_script)
    conn.commit()
    conn.close()
    print(f"Database {db_path} sudah diinisialisasi dengan {schema_file}")