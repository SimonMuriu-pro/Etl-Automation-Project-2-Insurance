import os
import logging
import sys

# Ensure logs directory exists
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

# Configure logger
logger = logging.getLogger("ETLLogger")
logger.setLevel(logging.INFO)

# File handler (UTF-8 encoding)
file_handler = logging.FileHandler(os.path.join(LOG_DIR, 'etl_pipeline.log'), encoding='utf-8')
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
file_handler.setFormatter(file_formatter)

# Console handler with UTF-8 encoding (optional, safe on most modern terminals)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(file_formatter)

# Add handlers only if none exist
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)  # comment this line if console logging is not wanted

# To avoid issues on Windows console, set environment/codepage to UTF-8 before running script:
# chcp 65001
