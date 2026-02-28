import logging
import os

# Func log 
def setup_logger(level=logging.INFO):
    """
    Inisialisasi root logger sekali di main.py.
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