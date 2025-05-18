# import logging
# import os
# from logging.handlers import TimedRotatingFileHandler

# # Create logs directory if it doesn't exist
# log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
# os.makedirs(log_dir, exist_ok=True)

# # Configure file handler
# file_handler = TimedRotatingFileHandler(filename=os.path.join(log_dir, "app.log"), when="midnight", interval=1, backupCount=30, encoding="utf-8")
# file_handler.setLevel(logging.ERROR)
# file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

# # Get uvicorn logger and add file handler
# logger = logging.getLogger("uvicorn")
# logger.addHandler(file_handler)

import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

# Create logs directory
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

# Create a named logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)

# Avoid adding handlers multiple times
if not logger.hasHandlers():
    # File handler
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Stream handler for Docker stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s - %(message)s"
    )
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)
