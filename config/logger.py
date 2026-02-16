import logging
import os


def setup_logger(level=logging.INFO):
    """
    Inisialisasi root logger sekali di main.py.
    Semua modul yang pakai logging.getLogger(__name__) akan otomatis
    inherit konfigurasi ini (handler + level).
    """
    os.makedirs("log", exist_ok=True)

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
        handlers=[
            logging.FileHandler("log/data_pipeline.log"),
            logging.StreamHandler(),
        ],
    )