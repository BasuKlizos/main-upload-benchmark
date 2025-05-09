import logging
import os
from logging.handlers import TimedRotatingFileHandler

# Create logs directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

# Configure file handler
file_handler = TimedRotatingFileHandler(filename=os.path.join(log_dir, "app.log"), when="midnight", interval=1, backupCount=30, encoding="utf-8")
file_handler.setLevel(logging.ERROR)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

# Get uvicorn logger and add file handler
logger = logging.getLogger("uvicorn")
logger.addHandler(file_handler)
