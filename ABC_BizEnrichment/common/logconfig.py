import os
import logging
from datetime import datetime
from django.conf import settings

# Logging setup
log_folder = getattr(settings, 'LOG_FOLDER', '')
log_level = getattr(settings, 'LOG_LEVEL', 'INFO').upper()  # Correcting typo and using LOG_LEVEL

# Check if log folder is provided and create it if necessary
if log_folder:
    os.makedirs(log_folder, exist_ok=True)

# Set up the logger
logger = logging.getLogger(__name__)

# Setting logging level from the settings or default to INFO
logger.setLevel(getattr(logging, log_level, logging.INFO))

# Set up file handler for logging to file
log_file = f"{log_folder}/app_log_{datetime.now().strftime('%Y%m%d')}.log"
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add file handler to the logger
logger.addHandler(file_handler)

# Add stream handler for console output
logger.addHandler(logging.StreamHandler())

# Example usage of the logger
logger.info("Logging setup complete.")
