import logging
from logging.handlers import RotatingFileHandler
import os

LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs", "app.log")
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

file_handler = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3)
file_handler.setFormatter(fmt)
logger.addHandler(file_handler)

console = logging.StreamHandler()
console.setFormatter(fmt)
logger.addHandler(console)

