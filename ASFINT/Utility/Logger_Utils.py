import logging
import os
from datetime import datetime

def get_logger(process_type: str) -> logging.Logger:
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    log_path = os.path.join(log_dir, f"{today}.log")

    logger = logging.getLogger(process_type)
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # Prevent duplicate handlers
        fh = logging.FileHandler(log_path)
        formatter = logging.Formatter(f"%(asctime)s - {process_type} - %(levelname)s - %(message)s")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
