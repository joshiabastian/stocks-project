import logging

# konfigurasi logging
def setup_logger(name=__name__, level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("data_pipeline.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(name)