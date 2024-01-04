import logging
import os
from datetime import datetime

# Create a 'logs' folder if it doesn't exist
logs_folder = 'logs'
os.makedirs(logs_folder, exist_ok=True)

# Get the current date and time for the log file name
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Configure the main log file with a handler that includes the log file location
log_file_path = os.path.join(logs_folder, f'app_{current_date}.log')
logging.basicConfig(filename=log_file_path, level=logging.INFO)

# Configure the log format
log_formatter = logging.Formatter('%(asctime)s')
log_handler = logging.FileHandler(log_file_path)
log_handler.setFormatter(log_formatter)

# Add the handler to the logger
logger = logging.getLogger(__name__)
logger.addHandler(log_handler)
